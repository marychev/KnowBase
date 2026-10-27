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
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Job struct {
	ID int
}

type Result struct {
	JobID    int
	WorkerID int
	Value    string
}

type Stats struct {
	panics            int
	failed            int
	WorkerJobsSuccess map[int]int // map[WorkerID]int
}

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

func RunWorkerPool(ctx context.Context, jobs []Job, numWorkers int) []Result {
	numJobs := len(jobs)
	jobsCh := make(chan Job, numJobs)
	resultsCh := make(chan Result, numJobs)

	wg := &sync.WaitGroup{}

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			fmt.Printf("Worker #%d starting (jobs=%d)\n", w, len(jobsCh))

			for {
				select {
				case <-ctx.Done():
					fmt.Printf("Worker #%d finihed by Context signal\n", w)
					return
				case job, ok := <-jobsCh:
					if !ok {
						fmt.Printf("Worker #%d finihed so close jobs chanal\n", w)
						return
					}
					i, err := callExternalService(ctx, job)
					fmt.Println(" call:", i, err)

					value := fmt.Sprintf("processed jod: %v", job)
					if i == 0 {
						value = err.Error()
					}

					select {
					case <-ctx.Done():
						fmt.Println("Safe send result finihed by Context signal")
						return
					case resultsCh <- Result{JobID: job.ID, WorkerID: w, Value: value}:
					}
				}
			}
		}()
	}

	for j := 1; j <= numJobs; j++ {
		jobsCh <- Job{ID: j}
	}
	close(jobsCh)

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var total []Result
	for r := range resultsCh {
		total = append(total, r)
	}

	return total
}

func main() {
	const (
		numJobs    = 3
		numWorkers = 2
	)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Second)
		cancel()
	}()

	// Проверяем, почему завершилась обработка
	if ctx.Err() == context.DeadlineExceeded {
		fmt.Println("\n⚠️ Обработка была прервана по таймауту!")
	}

	var jobs []Job
	for j := 1; j <= numJobs; j++ {
		jobs = append(jobs, Job{ID: j})
	}

	total := RunWorkerPool(ctx, jobs, numWorkers)
	fmt.Printf("Total results: %d\n", len(total))
	for _, r := range total {
		fmt.Printf(" result: %+v\n", r)
	}

	fmt.Println("[end] Finished app!")
}
