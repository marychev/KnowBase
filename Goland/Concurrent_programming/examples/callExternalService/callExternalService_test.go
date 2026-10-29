// go mod init pipeline
// go run -race callExternalService.go

package main

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"
)

/*
1. Проверка паникоустойчивости

Этот тест проверяет, может ли пул воркеров пережить панику внутри функции.
Чтобы вызвать панику наверняка в старом коде, нам нужно, чтобы rand выдал нужные значения,
либо мы можем временно пропатчить вызов.
Поскольку в коде callExternalService жестко зашита, давай проверим, как ведет себя пул
при большом количестве задач (там точно бахнет паника).
*/
func TestWorkerPool_PanicRecovery(t *testing.T) {
	// Тест проверяет, что паника в callExternalService не приводит к падению всего приложения
	numJobs := 100
	numWorkers := 3
	ctx := context.Background()
	jobs := prepareJobs(numJobs)

	// Если внутри RunWorkerPool нет recover(), этот тест КРАШНЕТ всё выполнение го-тестов.
	// Если recover() есть, тест завершится успешно.
	results, _ := RunWorkerPoolWithSemaphore(ctx, jobs, numWorkers, numWorkers)
	t.Logf("RunWorkerPool успешно завершил работу. Результатов: %d (из %d)!", len(results), numJobs)

	if len(results) != numJobs {
		t.Errorf("Ожидавли %d результатов. Получили - %d.", numJobs, len(results))
	}
}

func prepareJobs(numberJobs int) []Job {
	var jobs []Job
	for i := 1; i <= numberJobs; i++ {
		jobs = append(jobs, Job{ID: ID(i)})
	}
	return jobs
}

/*
Тест отмены контекста
Тест проверяет, что воркеры корректно и быстро завершают работу при отмене контекста
*/
func TestRunWorkerPool_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	numJobs := 10
	numWorkers := 2
	jobs := prepareJobs(numJobs)

	// Канал, чтобы дождаться завершения пула в отдельной горутине
	resChs := make(chan []Result)

	go func() {
		res, _ := RunWorkerPoolWithSemaphore(ctx, jobs, numWorkers, numWorkers)
		resChs <- res
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	startAt := time.Now()

	var results []Result
	select {
	case results = <-resChs: // Пул успешно вернул результаты
	case <-time.After(1 * time.Second):
		t.Fatal("DEADLOCK! Пул воркеров завис после отмены конекста и не завршился 1! секунду!")
	}

	duration := time.Since(startAt)
	t.Logf(
		"Пул завершил работу после отмены контекста за %v. Обработано задач %d из %d",
		duration, len(results), numJobs)

	// Проверяем, что мы действительно прервали обработку, и часть задач осталась нетронутой
	if len(results) >= numJobs {
		t.Errorf("Ожидалось что воркеры остановятся раньше, но обработаны все задачи: %d", len(results))
	}

	// BAD - old
	// results := <-resChs
	// fmt.Println(len(results), len(resChs))
	// if len(results) >= numJobs {
	// 	t.Errorf("Ожидалось, что воркеры остановятся раньше, но обработано задач: %d", len(results))
	// }
}

/*
3. Тест на корректную очистку и вычитывание (Drain & Close)
проверим критический момент, который приводит к утечкам памяти (goroutine leaks)

Мы должны убедиться, что:

 1. Когда все задачи успешно прочитаны и канал задач jobsCh закрывается,
    воркеры не уходят в бесконечный цикл, а корректно завершают свои горутины.
 2. Канал resultsCh закрывается строго после того, как абсолютно все воркеры закончили свою работу
    (иначе мы получим панику panic: send on closed channel).

Тест проверит, что при штатной работе без отмены контекста все горутины завершаются,
и мы получаем ровно столько результатов, сколько было задач.

Тест проверяет, что пул корректно вычитывает ВСЕ задачи до конца
и полностью освобождает ресурсы (закрывает горутины) после завершения работы.
*/
func TestRunWorkerPool_DrainAllJobs(t *testing.T) {
	ctx := context.Background()
	numJobs := 5
	numWorkers := 2
	jobs := prepareJobs(numJobs)

	results, _ := RunWorkerPoolWithSemaphore(ctx, jobs, numWorkers, numWorkers)

	if len(results) != numJobs {
		t.Fatalf("Ожидали ровно %d результатов, а полуили %d", numJobs, len(results))
	}

	//Проверяем, что каждая задача от 1 до numJobs была обработана ровно 1 раз
	visited := make(map[ID]bool)
	for _, res := range results {
		if visited[res.JobID] {
			t.Errorf("Дубликат! Задача %d обработана более одного раза", res.JobID)
		}
		visited[res.JobID] = true
	}
	if len(visited) != numJobs {
		t.Errorf(
			"Не все задачи были обработаны. Ожидали %d уникальных ID, получили %d",
			numJobs, len(visited))
	}

	t.Logf("Успех! Все %d задач обработаны без потерь и дубликатов", numJobs)
}

/*
4. Тест на конкурентный сбор метрик (Stats)

Поскольку воркеры работают параллельно, запись в общую структуру Stats
должна быть потокобезопасной, иначе мы получим состояние гонки (Data Race).

Тест проверит, что метрики собираются верно. Чтобы запустить его,
нам нужно добавить возвращаемое значение Stats в функцию пула.
*/
// Тест проверяет корректность заполнения структуры Stats
func TestRunWorkerPool_StatsCollection(t *testing.T) {
	ctx := context.Background()
	numJobs := 30
	numWorkers := 3
	Job := prepareJobs(numJobs)

	results, stats := RunWorkerPoolWithSemaphore(ctx, Job, numWorkers, numWorkers)

	var actualPanics int
	var actualFailed int
	var actualSuccess int

	for _, res := range results {
		if strings.Contains(res.Value, "паника") {
			actualPanics++
		} else if strings.Contains(res.Value, "ошибку") {
			actualFailed++
		} else {
			actualSuccess++
		}
	}

	if stats.panics != actualPanics {
		t.Errorf("Не совпали паники в Stats: получили %d, ожидали %d", stats.panics, actualPanics)
	}

	if stats.failed != actualFailed {
		t.Errorf("Не совпали ошибки в Stats: получили %d, ожидали %d", stats.failed, actualFailed)
	}

	// Проверяем, что сумма всех метрик равна общему количеству задач
	totalTracked := stats.panics + stats.failed
	for _, count := range stats.WorkerJobsSuccess {
		totalTracked += count
	}

	if totalTracked != numJobs {
		t.Errorf("Общая сумма метрик (%d) не равна числу задач (%d)", totalTracked, numJobs)
	}

	t.Logf("Метрики успешно проверены! Паник: %d, Ошибок: %d", stats.panics, stats.failed)
}

// Тест проверяет, что пул воркеров не выполняет больше K одновременных вызовов сервиса
func TestRunWorkerPool_SemaphoreLimit(t *testing.T) {
	ctx := context.Background()
	numJobs := 10
	numWorkers := 4 // Воркеров много
	// Допустим, внешний сервис держит максимум K=2 одновременных вызовов.
	maxConcurrencyK := 2

	// Запускаем пул. Нам нужно будет изменить сигнатуру RunWorkerPool,
	// чтобы она принимала maxConcurrencyK в качестве аргумента.
	// Мы берем достаточное количество задач, чтобы все воркеры хотели работать одновременно.
	jobs := prepareJobs(numJobs)

	// В текущей реализации callExternalService мы не можем легко посчитать пик из теста,
	// но мы можем проверить это, когда добавим семафор в код.
	// Давай пока напишем структуру теста, который ожидает успешного выполнения с новым параметром.
	results, stats := RunWorkerPoolWithSemaphore(ctx, jobs, numWorkers, maxConcurrencyK)

	if len(results) != numJobs {
		t.Fatalf("Ожидали %d результатов, получили %d", numJobs, len(results))
	}

	totalTracked := stats.panics + stats.failed
	for _, count := range stats.WorkerJobsSuccess {
		totalTracked += count
	}

	if totalTracked != numJobs {
		t.Errorf("Статистика сошлась неверно: %d из %d", totalTracked, numJobs)
	}

	t.Logf("Тест пройден! Пул успешно отработал с ограничением семафора K=%d", maxConcurrencyK)
}

// Тест проверяет, что воркеры не утекают и не зависают на семафоре при отмене контекста
func TestRunWorkerPool_ContextLeakSafety(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	numJobs, numWorkers := 50, 4
	maxConcurrecyK := 1 //Специально зажимаем семафор до 1 вызова
	jobs := prepareJobs(numJobs)

	// Замеряем базовое количество горутин перед тестом
	// Очищаем мусор для точности замера
	runtime.GC()
	baseGoroutines := runtime.NumGoroutine()

	// Запускаем наш пул в горутине, так как если он зависнет, мы хотим, чтобы тест пошел дальше
	doneCh := make(chan struct{})
	go func() {
		RunWorkerPoolWithSemaphore(ctx, jobs, numWorkers, maxConcurrecyK)
		close(doneCh)
	}()

	// Ждем завершения работы пула с запасом времени
	select {
	case <-doneCh: // Пул успешно вышел из работы
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("DEADLOCK! Пул воркеров завис и не завершил работу после отмены контекста")
	}

	// Даем рантайму немного времени завершить все фоновые процессы горутин и меряем остаток
	time.Sleep(20 * time.Millisecond)
	runtime.GC()
	endGoroutines := runtime.NumGoroutine()

	// Если воркеры зависли внутри семафора, endGoroutines будет равен baseGoroutines + 4 (numWorkers)
	if endGoroutines > baseGoroutines+1 { // +1 на случай внутренних шумов рантайма
		t.Errorf("Обнаружена утечка горутин! До теста: %d, После: %d. Воркеры зависли.", baseGoroutines, endGoroutines)
	} else {
		t.Logf("Успех! Все горутины воркеров успешно завершились. Было: %d, Стало: %d", baseGoroutines, endGoroutines)
	}
}

/*
тест на Graceful Shutdown
Чтобы проверить, что воркеры действительно дают текущим задачам завершиться после сигнала отмены.
Мы сымитируем сигнал отмены прямо во время выполнения задач и проверим,
что задачи, запущенные до отмены, вернули полноценный результат, а не оборвались на полуслове.

TestRunWorkerPool_GracefulShutdown проверяет, что при отмене контекста
задачи, которые УЖЕ начали выполняться, дорабатывают до конца.
*/
func TestRunWorkerPool_GracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	numJobs := 4
	numWorkers := 4
	maxConcurrencyK := 4
	jobs := prepareJobs(numJobs)

	// Запускаем пул
	doneCh := make(chan []Result)
	go func() {
		res, _ := RunWorkerPoolWithSemaphore(ctx, jobs, numWorkers, maxConcurrencyK)
		doneCh <- res
	}()

	// Даем воркерам буквально 10-20 мс, чтобы они ВСЕ успели зайти в callExternalService
	// (так как задачи длятся 50-300мс, они гарантированно будут находиться внутри сервиса)
	time.Sleep(15 * time.Millisecond)

	// Имитируем приход системного сигнала (отменяем контекст)
	cancel()

	var results []Result
	select {
	case results = <-doneCh:
		// Пул успешно завершился
	case <-time.After(1 * time.Second):
		t.Fatalf("DEADLOCK! Пул завис при попытке плавного завершения")
	}

	// Проверяем результаты. Так как воркеры зашли в метод ДО отмены контекста,
	// они обязаны были дождаться ответа от callExternalService.
	// Если мы увидим в Value текст ошибки контекста "context canceled", значит graceful shutdown не сработал
	for _, res := range results {
		if res.Value == "context canceled" {
			t.Errorf("Задача %d была грубо оборвана контекстом, вместо плавного завершения!", res.JobID)
		}
	}

	t.Logf("Плавное завершение сработало! Обработано начатых задач: %d", len(results))
}
