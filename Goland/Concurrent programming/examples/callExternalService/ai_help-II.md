# Issues callExternallService

## II Solution (Copilot)

```go
// go mod init pipeline
// go run -race callExternalService.go
// -------------------------------------------
// 1 Базовый пул воркеров
// Генератор (producer) кладёт `N` заданий (`Job{ID: 1..N}`) в канал `jobs`.
// `M` воркеров читают из `jobs`, обрабатывают через `callExternalService`, пишут `Result` в канал `results`.
// `main` собирает все результаты из `results` и печатает их.
// Завершиться корректно: дождаться всех воркеров и не уронить программу.

// 2 Потокобезопасная статистика + детектор гонок
// Добавить сборщик `Stats`: счётчики `panics` / `failed` и `map[workerID]int` (сколько заданий успешно сделал каждый воркер).
// Воркеры обновляют статистику конкурентно.

// 3 Ограничение параллелизма семафором
// Внешний сервис держит максимум `K` одновременных вызовов (например, `K=2` при `M=4` воркерах). Реализовать ограничение **семафором на буферизованном канале**.

// 4 Отмена через context + отсутствие утечек
// Протащить `context.Context` от `main` во все воркеры и в `callExternalService`.
// Завести общий таймаут операции через `context.WithTimeout` (например, 2–3 с).
// Воркеры обязаны **уважать** `ctx.Done()`: при отмене не начинать новую работу и не залипать.

// 5 Graceful shutdown
// Сервис должен корректно останавливаться по сигналу ОС (`SIGINT`/`SIGTERM`): перестать брать новые задания, дать текущим завершиться, но не висеть вечно.

package main // fix: package external `go mod init pipeline`

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

// Job — задача, которую нужно выполнить в воркере.
type Job struct {
   ID int
}

// Result — результат выполнения job.
type Result struct {
    JobID int
    Value int
    Err   error
}

// Stats — потокобезопасная статистика по воркерам.
// Важно: все поля обновляются из разных goroutine, поэтому нужна mutex.
type Stats struct {
    mu        sync.Mutex
    panics    int
    failed    int
    perWorker map[int]int
}

func (s *Stats) RecordSuccess(workerID int) {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.perWorker == nil {
        s.perWorker = make(map[int]int)
    }
    s.perWorker[workerID]++
}

func (s *Stats) RecordFailed() {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.failed++
}

func (s *Stats) RecordPanic() {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.panics++
}

func (s *Stats) String() string {
    s.mu.Lock()
    defer s.mu.Unlock()
    return fmt.Sprintf("panics=%d failed=%d perWorker=%v", s.panics, s.failed, s.perWorker)
}

// callExternalService — имитация внешнего сервиса.
// Он может:
// - завершиться успешно
// - вернуть ошибку
// - вызвать panic
// - быть отменён через ctx.
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

// executeJob — безопасное выполнение одной задачи.
// 1) берём слот в семафоре, чтобы ограничить параллелизм,
// 2) вызываем внешний сервис,
// 3) обновляем статистику и отправляем результат.
func executeJob(ctx context.Context, workerID int, job Job, sem chan struct{}, stats *Stats, results chan<- Result) {
    result := Result{JobID: job.ID}

    // Сначала проверяем отмену. Если ctx уже отменён, не начинаем новую работу.
    select {
    case <-ctx.Done():
        result.Err = ctx.Err()
        results <- result
        return
    case sem <- struct{}{}:
        // Получили место в лимите параллельных вызовов.
    }
    defer func() { <-sem }()

    func() {
        defer func() {
            if r := recover(); r != nil {
                stats.RecordPanic()
                result.Err = fmt.Errorf("panic recovered: %v", r)
            }
        }()

        value, err := callExternalService(ctx, job)
        if err != nil {
            if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
                result.Err = err
                return
            }
            stats.RecordFailed()
            result.Err = err
            return
        }

        stats.RecordSuccess(workerID)
        result.Value = value
    }()

    results <- result
}

func main() {
    // 1. Базовый пул воркеров
    // Producer кладёт N задач в jobs, worker-ы читают их и пишут result в results.
    // В конце main собирает все результаты и печатает их.
    const (
        nJobs       = 20
        workersN    = 4
        maxParallel = 2
    )

    // Для детектора гонок запускаем программу через:
    //   go run -race callExternalService.go
    // Тогда race-детектор поймает unsafe-обновления, если мы забудем mutex.
    rand.Seed(time.Now().UnixNano())

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    jobs := make(chan Job)
    results := make(chan Result, nJobs)
    stats := &Stats{perWorker: make(map[int]int)}
    sem := make(chan struct{}, maxParallel)

    var wg sync.WaitGroup
    wg.Add(workersN)

    for workerID := 1; workerID <= workersN; workerID++ {
        go func(id int) {
            defer wg.Done()
            for {
                select {
                case <-ctx.Done():
                    // При отмене не начинаем новую работу, ждём завершения текущей.
                    return
                case job, ok := <-jobs:
                    if !ok {
                       return
                    }
                    executeJob(ctx, id, job, sem, stats, results)
                }
            }
        }(workerID)
    }

    // Producer: отправляет все задания и закрывает jobs.
    go func() {
        defer close(jobs)
        for i := 1; i <= nJobs; i++ {
            select {
            case <-ctx.Done():
                return
            case jobs <- Job{ID: i}:
            }
        }
    }()

    // Когда все воркеры завершились, канал results можно закрывать.
    go func() {
        wg.Wait()
        close(results)
    }()

    for result := range results {
        if result.Err != nil {
            fmt.Printf("job %d failed: %v\n", result.JobID, result.Err)
            continue
        }
        fmt.Printf("job %d => %d\n", result.JobID, result.Value)
    }

    // 2. Потокобезопасная статистика + детектор гонок
    // Здесь мы обновляем stats конкурентно из разных воркеров.
    // Если убрать mutex, go run -race начнёт показывать race condition.
    fmt.Println("stats:", stats.String())

    // 3. Ограничение параллелизма семафором
    // sem = make(chan struct{}, maxParallel)
    // Каждый воркер перед callExternalService захватывает один слот.
    // Если maxParallel == 2, внешнему сервису будет доступно максимум 2 одновременно.
    // Это реализовано на буферизированном канале.
    // В нашем коде это делается в executeJob.

    // 4. Отмена через context + отсутствие утечек
    // ctx передан в воркеры и в callExternalService.
    // Внутри callExternalService есть select { case <-ctx.Done(): ...; case <-time.After(work): }
    // Благодаря этому долгий вызов прерывается, а воркер не «залипает».
    // При отмене мы не начинаем новые задачи.
    if err := ctx.Err(); err != nil {
        fmt.Println("shutdown reason:", err)
    }

    // 5. Graceful shutdown
    // signal.NotifyContext создаёт контекст, который отменяется при SIGINT/SIGTERM.
    // После этого:
    // - producer перестаёт отправлять новые job,
    // - воркеры не начинают новую работу,
    // - текущие задачи завершаются или прерываются,
    // - main не зависает вечно.
    fmt.Println("worker pool finished")
}

```
