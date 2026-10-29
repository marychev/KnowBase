// go mod init pipeline
// go run -race FILE_NAME.go
// -------------------------------------------
// 1 Базовый пул воркеров
// + Генератор (producer) кладёт `N` заданий (`Job{ID: 1..N}`) в канал `jobs`.
// + `M` воркеров читают из `jobs`, обрабатывают через `callExternalService`, пишут `Result` в канал `results`.
// + `main` собирает все результаты из `results` и печатает их.
// Завершиться корректно: дождаться всех воркеров и не уронить программу.

// 2 Потокобезопасная статистика + детектор гонок
// + Добавить сборщик `Stats`: счётчики `panics` / `failed` и `map[workerID]int` (сколько заданий успешно сделал каждый воркер).
// + Воркеры обновляют статистику конкурентно.

// + 3 Ограничение параллелизма семафором
// + Внешний сервис держит максимум `K` одновременных вызовов (например, `K=2` при `M=4` воркерах). Реализовать ограничение **семафором на буферизованном канале**.

// + 4 Отмена через context + отсутствие утечек
// + Протащить `context.Context` от `main` во все воркеры и в `callExternalService`.
// + Завести общий таймаут операции через `context.WithTimeout` (например, 2–3 с).
// + Воркеры обязаны **уважать** `ctx.Done()`: при отмене не начинать новую работу и не залипать.

// + 5 Graceful shutdown
// + Сервис должен корректно останавливаться по сигналу ОС (`SIGINT`/`SIGTERM`): перестать брать новые задания, дать текущим завершиться, но не висеть вечно.

package main // fix: package external `go mod init pipeline`

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ID int
type Job struct {
	ID ID
}

type Result struct {
	JobID    ID
	WorkerID ID
	Value    string
}

type Stats struct {
	mu                sync.Mutex
	panics            int
	failed            int
	WorkerJobsSuccess map[ID]int
}

func callExternalService(ctx context.Context, j Job) (ID, error) {
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

func RunWorkerPoolWithSemaphore(

	ctx context.Context,
	jobs []Job,
	numWorkers int,
	maxConcurrencyK int,

) ([]Result, *Stats) {
	numJobs := len(jobs)
	jobsCh := make(chan Job, numJobs)
	resultsCh := make(chan Result, numJobs)
	stats := &Stats{WorkerJobsSuccess: make(map[ID]int)}

	for _, j := range jobs {
		jobsCh <- j
	}
	close(jobsCh)

	// Переменные для отслеживания РЕАЛЬНОГО параллелизма в тесте
	var activeCalls int
	var maxObservedConcurrency int
	var mu sync.Mutex

	wg := &sync.WaitGroup{}

	// Перед запуском воркеров инициализируй канал-семафор
	semCh := make(chan struct{}, maxConcurrencyK)

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		workerID := ID(w)

		go func() {
			defer wg.Done()

			for job := range jobsCh {
				// [fix] Проверяем контекст в самом начале итерации.
				// Если контекст уже отменен, не берем новую работу.
				if ctx.Err() != nil {
					return
				}

				var isPanic bool
				var isSuccess bool

				// 1. Захватываем семафор (занимаем слот)
				// Если 2 воркера уже заняли слоты, 3-й воркер заблокируется на этой строчке
				// • struct{}{} — это пустая структура.
				// Она не занимает оперативной памяти, поэтому идеально подходит для сигналов и уведомлений.
				// Здесь важен сам факт отправки сигнала, а не то, что находится внутри
				// [bug]: semCh <- struct{}{}
				select {
				case <-ctx.Done():
					return
				case semCh <- struct{}{}: // Слот успешно занят, идем дальше
				}

				// Увеличиваем счетчик активных вызовов перед входом в сервис
				mu.Lock()
				activeCalls++
				if activeCalls > maxObservedConcurrency {
					maxObservedConcurrency = activeCalls
				}
				mu.Unlock()

				// [fix] Создаем контекст без отмены для выполнения самой операции.
				// Это гарантирует, что если воркер НАЧАЛ работу, callExternalService не оборвется.
				detachedCtx := context.WithoutCancel(ctx)

				_, err := func() (resultID ID, serviceErr error) {
					defer func() {
						if r := recover(); r != nil {
							serviceErr = fmt.Errorf("паника: %v", r)
							isPanic = true
							stats.mu.Lock()
							stats.panics++
							stats.mu.Unlock()
						}
					}()
					// [fix] Передаем защищенный detachedCtx вместо отменяемого ctx
					return callExternalService(detachedCtx, job)

					// return callExternalService(ctx, job)
				}()

				// Уменьшаем счетчик после выхода из сервиса
				mu.Lock()
				activeCalls--
				mu.Unlock()

				// 2. Освобождаем семафор (освобождаем слот для других)
				<-semCh

				value := fmt.Sprintf("processed jod: %v", job)
				if err != nil {
					value = err.Error()
					if !isPanic {
						stats.mu.Lock()
						stats.failed++
						stats.mu.Unlock()
					}
				} else {
					isSuccess = true
				}

				// Примечание: Также обрати внимание на блок отправки результатов в resultsCh.
				// Если контекст отменился, пока задача выполнялась, мы всё равно хотим сохранить
				// её результат в итоговом слайсе! Поэтому при отправке в канал результатов мы убираем
				// case <-ctx.Done(): return, чтобы не потерять уже вычисленный результат,
				// а просто делаем прямую отправку. Горутина всё равно выйдет на следующей итерации цикла,
				// проверив ctx.Err() != nil вверху!

				// [fix] Прямая отправка без прерывания: раз уж задача успешно выполнилась,
				// мы обязаны доставить её результат в resultsCh, а не выбрасывать.
				resultsCh <- Result{JobID: job.ID, WorkerID: workerID, Value: value}
				if isSuccess {
					stats.mu.Lock()
					stats.WorkerJobsSuccess[workerID]++
					stats.mu.Unlock()
				}

				// select {
				// case <-ctx.Done():
				// 	return
				// case resultsCh <- Result{JobID: job.ID, WorkerID: workerID, Value: value}:
				// 	if isSuccess {
				// 		stats.mu.Lock()
				// 		stats.WorkerJobsSuccess[workerID]++
				// 		stats.mu.Unlock()
				// 	}
				// }
			}
		}()
	}

	wg.Wait()
	close(resultsCh)

	// Печатаем, сколько воркеров ОДНОВРЕМЕННО находилось в сервисе
	fmt.Printf("\n[Semaphore Check] Пиковый параллелизм составил: %d (Лимит K=%d)\n", maxObservedConcurrency, maxConcurrencyK)

	// Если лимит превышен, запишем это в лог (или упадем в тесте, проверив это значение)
	// Для простоты передадим признак ошибки через stats.failed или выведем панику, если превышен лимит, чтобы тест упал:
	if maxObservedConcurrency > maxConcurrencyK {
		// Специально триггерим панику, чтобы наш тест упал, сигнализируя об отсутствии семафора
		panic(fmt.Sprintf("Превышен лимит параллелизма! Одновременно работало: %d воркеров (Лимит: %d)", maxObservedConcurrency, maxConcurrencyK))
	}

	var total []Result
	for r := range resultsCh {
		total = append(total, r)
	}

	return total, stats
}

func main() {
	const (
		numJobs         = 3
		numWorkers      = 2
		maxConcurrencyK = 2
	)

	// ctx, cancel := context.WithCancel(context.Background())
	// [fix] Заводим общий таймаут операции на 1 секунду
	// ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	// defer cancel()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// go func() {
	// 	time.Sleep(5 * time.Second)
	// 	cancel()
	// }()

	var jobs []Job
	for j := 1; j <= numJobs; j++ {
		jobs = append(jobs, Job{ID: ID(j)})

	}

	fmt.Println("[main] Запуск пула воркеров с семафором...")
	total, stats := RunWorkerPoolWithSemaphore(ctx, jobs, numWorkers, maxConcurrencyK)

	// Проверяем, почему завершилась обработка
	if ctx.Err() == context.DeadlineExceeded {
		fmt.Println("\n⚠️ Обработка была прервана по общему таймауту приложения!")
	}

	fmt.Printf("[main] Total results: %d\n", len(total))
	for _, r := range total {
		fmt.Printf(" result: %+v\n", r)
	}

	fmt.Printf("[end] Статистика пула: %+v\n", stats)
}
