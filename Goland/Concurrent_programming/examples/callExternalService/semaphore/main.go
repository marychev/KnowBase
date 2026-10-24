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

type Job struct {
	ID int
}

type Result struct {
	JobID    int
	WorkerID int
	Value    int
	Err      error
}

// Stats собирает результаты работы всех воркеров.
// Mutex защищает поля от одновременного изменения из разных goroutine.
type Stats struct {
	mu              sync.Mutex
	panics          int
	failed          int
	successByWorker map[int]int
}

func (s *Stats) recordSuccess(workerID int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.successByWorker[workerID]++
}

func (s *Stats) recordFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed++
}

func (s *Stats) recordPanic() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.panics++
}

func (s *Stats) snapshot() (panics, failed int, successByWorker map[int]int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	successByWorker = make(map[int]int, len(s.successByWorker))
	for workerID, count := range s.successByWorker {
		successByWorker[workerID] = count
	}
	return s.panics, s.failed, successByWorker
}

// callExternalService имитирует внешний вызов, который может завершиться
// успешно, вернуть ошибку или запаниковать.
func callExternalService(ctx context.Context, job Job) (int, error) {
	timer := time.NewTimer(time.Duration(50+rand.Intn(250)) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-timer.C:
	}

	switch roll := rand.Intn(10); {
	case roll == 0:
		panic(fmt.Sprintf("сервис паникнул на job %d", job.ID))
	case roll <= 2:
		return 0, fmt.Errorf("сервис вернул ошибку на job %d", job.ID)
	default:
		return job.ID * job.ID, nil
	}
}

func worker(
	id int,
	shutdownCtx context.Context,
	operationCtx context.Context,
	jobs <-chan Job,
	results chan<- Result,
	semaphore chan struct{},
	stats *Stats,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		// После сигнала или общего таймаута новые задания не берём.
		select {
		case <-shutdownCtx.Done():
			return
		case <-operationCtx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}

			// select может выбрать готовое задание одновременно с отменой,
			// поэтому перед началом работы отдельно проверяем оба контекста.
			if err := contextError(shutdownCtx, operationCtx); err != nil {
				results <- Result{JobID: job.ID, WorkerID: id, Err: err}
				return
			}

			results <- runJob(id, job, shutdownCtx, operationCtx, semaphore, stats)
		}
	}
}

func contextError(shutdownCtx, operationCtx context.Context) error {
	if err := shutdownCtx.Err(); err != nil {
		return err
	}
	return operationCtx.Err()
}

func runJob(
	workerID int,
	job Job,
	shutdownCtx context.Context,
	operationCtx context.Context,
	semaphore chan struct{},
	stats *Stats,
) (result Result) {
	result.JobID = job.ID
	result.WorkerID = workerID

	// Буферизованный канал ёмкостью K — семафор: в него одновременно
	// могут войти не более K вызовов внешнего сервиса.
	select {
	case semaphore <- struct{}{}:
		defer func() { <-semaphore }()
	case <-shutdownCtx.Done():
		result.Err = shutdownCtx.Err()
		return result
	case <-operationCtx.Done():
		result.Err = operationCtx.Err()
		return result
	}

	// Не начинаем внешний вызов, если сигнал пришёл во время ожидания семафора.
	if err := contextError(shutdownCtx, operationCtx); err != nil {
		result.Err = err
		return result
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			stats.recordPanic()
			result.Err = fmt.Errorf("panic recovered: %v", recovered)
		}
	}()

	value, err := callExternalService(operationCtx, job)
	if err != nil {
		result.Err = err
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			stats.recordFailure()
		}
		return result
	}

	stats.recordSuccess(workerID)
	result.Value = value
	return result
}

func main() {
	const (
		jobsCount    = 12
		workersCount = 4
		maxParallel  = 2
	)

	// Сигнал ОС останавливает producer и запрещает воркерам брать новые задачи.
	shutdownCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Общий таймаут гарантирует, что работа и ожидание текущих вызовов не зависнут.
	operationCtx, cancelOperation := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelOperation()

	jobs := make(chan Job)
	// Буфер размером N позволяет воркерам отправить все результаты,
	// пока main ждёт завершения канала results.
	results := make(chan Result, jobsCount)
	semaphore := make(chan struct{}, maxParallel)
	stats := &Stats{successByWorker: make(map[int]int)}

	var workers sync.WaitGroup
	workers.Add(workersCount)
	for id := 1; id <= workersCount; id++ {
		go worker(id, shutdownCtx, operationCtx, jobs, results, semaphore, stats, &workers)
	}

	// Producer всегда закрывает jobs и прекращает отправку при отмене.
	go func() {
		defer close(jobs)
		for id := 1; id <= jobsCount; id++ {
			select {
			case <-shutdownCtx.Done():
				return
			case <-operationCtx.Done():
				return
			case jobs <- Job{ID: id}:
			}
		}
	}()

	// Закрываем results только после того, как все воркеры завершились.
	go func() {
		workers.Wait()
		close(results)
	}()

	for result := range results {
		if result.Err != nil {
			fmt.Printf("job %d (worker %d) failed: %v\n", result.JobID, result.WorkerID, result.Err)
			continue
		}
		fmt.Printf("job %d (worker %d) => %d\n", result.JobID, result.WorkerID, result.Value)
	}

	panics, failed, successByWorker := stats.snapshot()
	fmt.Printf("stats: panics=%d failed=%d successByWorker=%v\n", panics, failed, successByWorker)

	if err := shutdownCtx.Err(); err != nil {
		fmt.Println("shutdown signal received:", err)
	}
	if err := operationCtx.Err(); err != nil {
		fmt.Println("operation stopped:", err)
	}
}
