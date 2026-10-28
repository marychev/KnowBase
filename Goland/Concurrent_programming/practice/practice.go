// go mod init pipeline
// go run -race callExternalService.go
// -------------------------------------------
// 1 Базовый пул воркеров
// + Генератор (producer) кладёт `N` заданий (`Job{ID: 1..N}`) в канал `jobs`.
// + `M` воркеров читают из `jobs`, обрабатывают через `callExternalService`, пишут `Result` в канал `results`.
// + `main` собирает все результаты из `results` и печатает их.
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

func RunWorkerPool(ctx context.Context, jobs []Job, numWorkers int) []Result {
	numJobs := len(jobs)
	jobsCh := make(chan Job, numJobs)
	resultsCh := make(chan Result, numJobs)

	wg := &sync.WaitGroup{}

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)

		// [fix] передаем w как параметр, чтобы избежать data race в старых версиях Go
		go func(workerID ID) {
			defer wg.Done()
			fmt.Printf("Worker #%d starting (jobs=%d)\n", workerID, len(jobsCh))

			for {
				select {
				case <-ctx.Done():
					fmt.Printf("Worker #%d finihed by Context signal\n", workerID)
					return
				case job, ok := <-jobsCh:
					if !ok {
						fmt.Printf("Worker #%d finihed so close jobs chanal\n", workerID)
						return
					}

					// [fix] Оборачиваем вызов сервиса в анонимную функцию с defer recover,
					// чтобы паника ОДНОЙ задачи не убивала весь воркер и другие задачи.
					resID, err := func() (resultID ID, serviceErr error) {
						defer func() {
							if r := recover(); r != nil {
								// Перехватываем панику и превращаем её в обычную ошибку
								serviceErr = fmt.Errorf("паника: %v", r)
							}
						}()
						return callExternalService(ctx, job)
					}()
					fmt.Println(" call:", resID, err)

					// i, err := callExternalService(ctx, job)
					// fmt.Println(" call:", i, err)

					value := fmt.Sprintf("processed jod: %v", job)
					if err != nil {
						value = err.Error()
					} else if resID == 0 {
						value = "unknow error (id as 0)"
					}

					select {
					case <-ctx.Done():
						fmt.Println("Safe send result finihed by Context signal")
						return
					case resultsCh <- Result{JobID: job.ID, WorkerID: ID(workerID), Value: value}:
					}
				}
			}
		}(ID(w))
	}

	for j := 1; j <= numJobs; j++ {
		jobsCh <- Job{ID: ID(j)}
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
		jobs = append(jobs, Job{ID: ID(j)})
	}

	total := RunWorkerPool(ctx, jobs, numWorkers)
	fmt.Printf("[main] Total results: %d\n", len(total))
	for _, r := range total {
		fmt.Printf(" result: %+v\n", r)
	}

	fmt.Println("[end] Finished app!")
}

/*


1. Критические ошибки в текущем коде

• 💥 Неперехваченные паники (Unrecovered Panics): Функция callExternalService с вероятностью 10% вызывает panic(). Поскольку воркеры работают в отдельных горутинах, любая паника внутри callExternalService приведет к падению всего приложения (crash). main не успеет ничего собрать.
• 🔄 Неправильная логика select в воркере (Glitched Select):go
select {
case <-ctx.Done(): return
case job, ok := <-jobsCh: // ...
}
Используйте код с осторожностью.
Если в jobsCh есть готовые задачи, select выбирает случайный открытый case. Даже если контекст уже отменен, воркер может проигнорировать ctx.Done() и взять задачу из канала.
Более того, если канал jobsCh закроется, case job, ok := <-jobsCh будет постоянно срабатывать (возвращая ok == false). Из-за случайного выбора в select воркер может уйти в бесконечный цикл вместо мгновенного выхода.
• 📋 Переменная jobs в main не используется: Вы создаете слайс jobs в функции main, но в RunWorkerPool передаете его размерность, а внутри пула заново генерируете задачи через for j := 1; j <= numJobs; j++. Это нарушает паттерн: пул должен принимать готовые задачи, а не генерировать их сам.
• ⏱️ Преждевременная проверка контекста: В main проверка if ctx.Err() == context.DeadlineExceeded стоит до вызова RunWorkerPool. На этом этапе контекст никогда не будет отменен, ведь вы только что запустили time.Sleep(5 * time.Second) в горутине.

2. Как исправить и улучшить пул воркеров

Для качественного обучения реализуйте следующие шаги:

Шаг А: Безопасный воркер (Восстановление после паник)

Каждый воркер должен защищать себя с помощью recover(). Если внешняя функция паникует, воркер должен поймать ошибку, зафиксировать её в Result и продолжить работу со следующей задачей, а не ронять приложение.

Шаг Б: Правильное чтение из каналов (Паттерн for range)

Вместо сложного select с ручной проверкой ok для чтения из канала лучше использовать for job := range jobsCh. Этот цикл автоматически и безопасно завершится, когда канал будет закрыт и из него вычитают все элементы.

Шаг В: Сбор метрик (Stats)

Вы объявили структуру Stats, но не используете её. В реальных пулах воркеров метрики (сколько упало в панику, сколько завершилось ошибкой) собираются конкурентно. Для этого используют sync.Mutex или пакет sync/atomic.

🚀 Эталонный и безопасный вариант кода

Вот исправленная и дополненная версия программы, учитывающая все правила Go-разработки:
go
package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
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
	Err      error
}

// Stats для сбора метрик (добавили Mutex для потокобезопасности)
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

func RunWorkerPool(ctx context.Context, jobs []Job, numWorkers int) ([]Result, Stats) {
	numJobs := len(jobs)
	jobsCh := make(chan Job, numJobs)
	resultsCh := make(chan Result, numJobs)

	stats := Stats{
		WorkerJobsSuccess: make(map[ID]int),
	}

	// Наполняем канал задачами ПЕРЕД запуском или параллельно.
	// В данном случае используем переданный слайс jobs.
	for _, j := range jobs {
		jobsCh <- j
	}
	close(jobsCh) // Закрываем сразу, так как размер зафиксирован

	wg := &sync.WaitGroup{}

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		workerID := ID(w)

		go func() {
			defer wg.Done()

			// Использование range — самый надежный способ вычитать канал до закрытия
			for job := range jobsCh {

				// Защита от паники внутри callExternalService
				res := func() Result {
					defer func() {
						if r := recover(); r != nil {
							stats.mu.Lock()
							stats.panics++
							stats.mu.Unlock()
							fmt.Printf("[Worker #%d] Поймана паника: %v\n", workerID, r)
						}
					}()

					// Проверяем контекст перед выполнением тяжелой задачи
					if ctx.Err() != nil {
						return Result{JobID: job.ID, WorkerID: workerID, Err: ctx.Err()}
					}

					resID, err := callExternalService(ctx, job)

					stats.mu.Lock()
					if err != nil {
						stats.failed++
					} else {
						stats.WorkerJobsSuccess[workerID]++
					}
					stats.mu.Unlock()

					if err != nil {
						return Result{JobID: job.ID, WorkerID: workerID, Err: err}
					}
					return Result{JobID: job.ID, WorkerID: workerID, Value: fmt.Sprintf("Успех: %d", resID)}
				}()

				// Безопасная отправка с учетом отмены контекста
				select {
				case <-ctx.Done():
					return
				case resultsCh <- res:
				}
			}
		}()
	}

	// Отдельная горутина для закрытия канала результатов
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var total []Result
	for r := range resultsCh {
		total = append(total, r)
	}

	return total, stats
}

func main() {
	// Инициализируем генератор случайных чисел
	rand.Seed(time.Now().UnixNano())

	const (
		numJobs    = 10
		numWorkers = 3
	)

	// Создаем контекст с таймаутом, чтобы проверить прерывание
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var jobs []Job
	for j := 1; j <= numJobs; j++ {
		jobs = append(jobs, Job{ID: ID(j)})
	}

	total, stats := RunWorkerPool(ctx, jobs, numWorkers)

	// Проверяем, почему завершилась обработка ПОСЛЕ выполнения пула
	if ctx.Err() == context.DeadlineExceeded {
		fmt.Println("\n⚠️ Обработка была прервана по таймауту контекста!")
	}

	fmt.Printf("\n[main] Получено результатов: %d из %d\n", len(total), numJobs)
	for _, r := range total {
		if r.Err != nil {
			fmt.Printf("  ID %d -> Ошибка: %v (Worker %d)\n", r.JobID, r.Err, r.WorkerID)
		} else {
			fmt.Printf("  ID %d -> %s (Worker %d)\n", r.JobID, r.Value, r.WorkerID)
		}
	}

	fmt.Printf("\nСтатистика пула:\n - Ошибок: %d\n - Паник перехвачено: %d\n", stats.failed, stats.panics)
	fmt.Printf(" Успешных задач по воркерам: %+v\n", stats.WorkerJobsSuccess)
}
Используйте код с осторожностью.
В рамках обучения, какой следующий шаг мы разберем? Я могу помочь тебе:
• Реализовать динамический Producer, который поставляет задачи непрерывно (а не пачкой)
• Написать Rate Limiter, чтобы воркеры не делали больше N запросов в секунду
• Разобрать, как работает потокобезопасность через Mutex / Atomic на примере сбора Stats

Что выберем?

*/
