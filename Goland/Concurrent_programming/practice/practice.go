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
	JobID int
	Value int
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

func worker(ctx context.Context, wid int, jobs <-chan Job, result chan<- Result, wg *sync.WaitGroup) {
	// воркеров читают из `jobs`, (init and waiting)
	// обрабатывают через `callExternalService`,
	// пишут `Result` в канал `results`.
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[*_*]воркер #%d упал: %v", wid, r)
			return
		}
	}()

	defer wg.Done()
	fmt.Printf("Воркер #%d запущен\n", wid)

	for j := range jobs {
		fmt.Printf("... воркер #%d читает job  %d\n", wid, j)

		resultCall, callErr := callExternalService(ctx, j)
		fmt.Printf("-> callExternalService вернул результат #%d пишет Result '%s'\n", resultCall, callErr)

		value := j.ID * 10
		result <- Result{JobID: j.ID, Value: value}
		fmt.Printf("-> воркер #%d пишет Value: %s\n", wid, value)
	}
}

func main() {
	const (
		numJobs    = 5
		numWorkers = 2
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobsCh := make(chan Job)
	resultCh := make(chan Result, numJobs)
	var wg sync.WaitGroup

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(ctx, w, jobsCh, resultCh, &wg)
	}

	// Генератор (producer) кладёт `N` заданий (`Job{ID: 1..N}`) в канал `jobs`.
	for j := 1; j <= numJobs; j++ {
		fmt.Printf("-> Отправляем задание %d\n", j)
		jobsCh <- Job{ID: j}
	}
	close(jobsCh)
	fmt.Println("Все задания отправлены, канал jobs закрыт.")

	for r := 1; r <= numJobs; r++ {
		result := <-resultCh
		fmt.Printf("<- Получен результат: %d\n", result)
	}
	// close(resultCh)

	wg.Wait()
	fmt.Println("Все задачи успешно обработаны.")
}
