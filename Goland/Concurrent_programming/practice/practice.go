// go mod init pipeline
// go run -race FILE_NAME.go

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
	Error    error
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
	semCh := make(chan struct{}, maxConcurrencyK)

	for _, j := range jobs {
		jobsCh <- j
	}
	close(jobsCh)

	// // Переменные для отслеживания РЕАЛЬНОГО параллелизма в тесте
	// var activeCalls int
	// var maxObservedConcurrency int
	// var mu sync.Mutex

	wg := &sync.WaitGroup{}

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		workerID := ID(w)

		go func() {
			defer wg.Done()

			for job := range jobsCh {
				// Проверяем контекст в самом начале итерации. Если контекст уже отменен, не берем новую работу.
				if ctx.Err() != nil {
					return
				}

				res, isSuccess := processJobCtx(ctx, job, workerID, semCh, stats)
				resultsCh <- res
				if isSuccess {
					stats.mu.Lock()
					stats.WorkerJobsSuccess[workerID]++
					stats.mu.Unlock()
				}

			}
		}()
	}

	wg.Wait()
	close(resultsCh)

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var jobs []Job
	for j := 1; j <= numJobs; j++ {
		jobs = append(jobs, Job{ID: ID(j)})

	}

	fmt.Println("[main] Запуск пула воркеров с семафором...")
	total, stats := RunWorkerPoolWithSemaphore(ctx, jobs, numWorkers, maxConcurrencyK)

	if ctx.Err() == context.DeadlineExceeded {
		fmt.Println("\n⚠️ Обработка была прервана по общему таймауту приложения!")
	}

	fmt.Printf("[main] Total results: %d\n", len(total))
	for _, r := range total {
		fmt.Printf(" result: %+v\n", r)
	}

	fmt.Printf("[end] Статистика пула: %+v\n", stats)
}

// processJobCtx обрабатывает одну задачу: следит за семафором, паниками, ошибками и метриками
func processJobCtx(ctx context.Context, job Job, workerID ID, semCh chan struct{}, stats *Stats) (Result, bool) {
	// 1. Захват семафора с учетом отмены контекста
	select {
	case <-ctx.Done():
		return Result{JobID: job.ID, WorkerID: workerID, Value: ctx.Err().Error()}, false
	case semCh <- struct{}{}:
	}

	var isPanic bool
	var isSuccess bool

	// 2. Вызов сервиса с защитой от паники
	detachedCtx := context.WithoutCancel(ctx)

	// Явно объявляем переменные для результата вызова внешнего сервиса
	var err error

	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("паника: %v", r)
				isPanic = true

				stats.mu.Lock()
				stats.panics++
				stats.mu.Unlock()
			}
		}()
		_, err = callExternalService(detachedCtx, job)
	}()

	// 3. Освобождаем семафор сразу после выхода из сервиса
	<-semCh

	// 4. Формируем результат и обновляем метрики
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

	return Result{JobID: job.ID, WorkerID: workerID, Value: value}, isSuccess
}
