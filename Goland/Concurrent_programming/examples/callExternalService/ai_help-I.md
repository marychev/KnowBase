# Issues callExternallService

## I Solution

1. Базовый пул воркеров

- Генератор (producer) кладёт `N` заданий (`Job{ID: 1..N}`) в канал `jobs`.
- `M` воркеров читают из `jobs`, обрабатывают через `callExternalService`, пишут `Result` в канал `results`.
- `main` собирает все результаты из `results` и печатает их.
- Завершиться корректно: дождаться всех воркеров и не уронить программу.

2. Потокобезопасная статистика + детектор гонок

- Добавить сборщик `Stats`: счётчики `panics` / `failed` и `map[workerID]int` (сколько заданий успешно сделал каждый воркер).
- Воркеры обновляют статистику конкурентно.

3. Ограничение параллелизма семафором

- Внешний сервис держит максимум `K` одновременных вызовов (например, `K=2` при `M=4` воркерах). Реализовать ограничение **семафором на буферизованном канале**.

4. Отмена через context + отсутствие утечек

- Протащить `context.Context` от `main` во все воркеры и в `callExternalService`.
- Завести общий таймаут операции через `context.WithTimeout` (например, 2–3 с).
- Воркеры обязаны **уважать** `ctx.Done()`: при отмене не начинать новую работу и не залипать.

5. Graceful shutdown

- Сервис должен корректно останавливаться по сигналу ОС (`SIGINT`/`SIGTERM`): перестать брать новые задания, дать текущим завершиться, но не висеть вечно.

```go

package main

import (
    "context"
    "errors"
    "fmt"
    "math/rand"
    "os"
    "os/signal"
    "sync"
    "syscall"
    "time"
)

// Job описывает задачу для воркера
type Job struct {
    ID int
}

// Result описывает результат выполнения задачи
type Result struct {
    JobID    int
    WorkerID int
    Value    int
    Err      error
}

// Stats хранит потокобезопасную статистику (Пункт 2)
type Stats struct {
    mu           sync.Mutex
    panics       int
    failed       int
    workerCounts map[int]int
}

func (s *Stats) IncPanic() {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.panics++
}

func (s *Stats) IncFailed() {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.failed++
}

func (s *Stats) IncWorkerSuccess(workerID int) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.workerCounts[workerID]++
}

// Имитация внешнего сервиса
func callExternalService(ctx context.Context, j Job) (int, error) {
    work := time.Duration(50+rand.Intn(250)) * time.Millisecond

    select {
    case <-ctx.Done():
        return 0, ctx.Err()
    case <-time.After(work):
    }

    roll := rand.Intn(10)
    switch {
    case roll == 0:
        panic(fmt.Sprintf("сервис паникнул на job %d", j.ID))
    case roll <= 2:
        return 0, fmt.Errorf("сервис вернул ошибку на job %d", j.ID)
    default:
        return j.ID * j.ID, nil
    }
}

func main() {
    // Конфигурация задачи
    numJobs := 30
    numWorkers := 4
    maxConcurrentCalls := 2 // Ограничение для семафора (K=2 при M=4)

    // Настройка контекста с таймаутом и graceful shutdown (Пункты 4 и 5)
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
    defer cancel()

    // Инициализация каналов и структур данных
    jobs := make(chan Job, numJobs)
    results := make(chan Result, numJobs)
    stats := &Stats{
        workerCounts: make(map[int]int),
    }

    // Семафор на буферизованном канале для ограничения параллелизма к сервису (Пункт 3)
    semaphore := make(chan struct{}, maxConcurrentCalls)

    // 1. Генератор (Producer) - отправляет задачи в канал
    go func() {
        defer close(jobs)
        for i := 1; i <= numJobs; i++ {
            select {
            case <-ctx.Done():
                return // Останавливаем генерацию, если контекст отменен
            case jobs <- Job{ID: i}:
            }
        }
    }()

    // 2. Запуск пула воркеров (Пункт 1)
    var wg sync.WaitGroup
    for w := 1; w <= numWorkers; w++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            for {
                select {
                case <-ctx.Done():
                    return // Уважаем контекст: не берем новые задачи при отмене
                case job, ok := <-jobs:
                    if !ok {
                        return // Канал jobs закрыт, задачи кончились
                    }

                    // Защита от паники внутри callExternalService (Пункт 2)
                    func() {
                        defer func() {
                            if r := recover(); r != nil {
                                stats.IncPanic()
                                results <- Result{JobID: job.ID, WorkerID: workerID, Err: fmt.Errorf("panic: %v", r)}
                            }
                        }()

                        // Захват слота в семафоре перед вызовом внешнего сервиса (Пункт 3)
                        select {
                        case <-ctx.Done():
                            return
                        case semaphore <- struct{}{}:
                        }

                        // Вызов внешнего сервиса с пробросом контекста (Пункт 4)
                        val, err := callExternalService(ctx, job)

                        // Освобождаем слот в семафоре сразу после ответа
                        <-semaphore

                        if err != nil {
                            if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
                                return // Если это отмена контекста, просто выходим
                            }
                            stats.IncFailed()
                            results <- Result{JobID: job.ID, WorkerID: workerID, Err: err}
                            return
                        }

                        stats.IncWorkerSuccess(workerID)
                        results <- Result{JobID: job.ID, WorkerID: workerID, Value: val}
                    }()
                }
            }
        }(w)
    }

    // 3. Закрытие канала результатов после завершения всех воркеров
    go func() {
        wg.Wait()
        close(results)
    }()

    // 4. Сборщик результатов в main (Пункт 1)
    for res := range results {
        if res.Err != nil {
            fmt.Printf("[Worker %d] Job %d failed: %v\n", res.WorkerID, res.JobID, res.Err)
        } else {
            fmt.Printf("[Worker %d] Job %d success: %d\n", res.WorkerID, res.JobID, res.Value)
        }
    }

    // Вывод итоговой статистики
    fmt.Println("\n--- Итоговая статистика ---")
    if ctx.Err() != nil {
        fmt.Printf("Завершено по причине: %v\n", ctx.Err())
    }
    stats.mu.Lock()
    fmt.Printf("Паник: %d, Ошибок: %d\n", stats.panics, stats.failed)
    for wID, count := range stats.workerCounts {
        fmt.Printf("Воркер %d успешно обработал задач: %d\n", wID, count)
    }
    stats.mu.Unlock()
}

```

Как это теперь работает:

1. Исправлен пакет: Объявлен package main, что убирает ошибку сборки.
2. Безопасная статистика (Пункт 2): Доступ к Stats обернут в sync.Mutex. Любые изменения счетчиков из параллельных воркеров полностью изолированы от состояния гонки (Race Condition).
3. Ограничение K=2 (Пункт 3): Перед вызовом callExternalService воркер пишет в канал semaphore := make(chan struct{}, 2). Если 2 воркера уже заняли слоты, остальные 2 будут послушно ждать своей очереди, даже если они свободны.
4. Контекст и сигналы (Пункт 4 и 5): signal.NotifyContext перехватывает нажатие Ctrl+C (SIGINT) или сигнал остановки (SIGTERM). Программа корректно прервет генерацию, доработает активные вызовы (уложившись в таймаут 3 секунды) и плавно выйдет, напечатав финальный лог.
Вы можете проверить этот код командой `go run -race main.go`. 
