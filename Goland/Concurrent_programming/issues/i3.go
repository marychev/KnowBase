/*
Реализуй воркер, который:

В бесконечном цикле читает задания из канала jobs, обрабатывает каждое задание (например, time.Sleep(500ms) + вывод).
При этом воркер должен останавливаться по сигналу stopCh (канал struct{}), прерывая обработку задания по таймауту (например, 1 секунда) через select.

Структура цикла. Внутри обработки:
- select между:
- time.After(timeout) — считаем, что задание "не успело";
- stopCh — немедленный выход;
- завершение обработки (можно смоделировать через второй канал или просто time.Sleep + ещё один select).

Требования:

main:
- Запускает воркера(ов),
- Отправляет несколько заданий,
- Через некоторое время посылает stop (закрывает stopCh),
- Корректно завершает программу.

Воркер:
- Уважает сигнал об остановке,
- Корректно завершает работу.

*/

package main

import (
	"fmt"
	"sync"
	"time"
)

// worker читает задания из jobs и обрабатывает их до тех пор,
// пока не закроют stopCh. Обработка каждого задания ограничена таймаутом.
func worker(id int, jobs <-chan int, stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done() // сообщаем main, что воркер завершился, что бы ни случилось

	fmt.Printf("Воркер #%d запущен\n", id)

	for {
		// Внешний select: ждём либо новое задание, либо сигнал стоп.
		// Пока заданий нет, воркер висит здесь и всё равно слышит stopCh.
		select {
		case <-stopCh:
			fmt.Printf("Воркер #%d: стоп между заданиями, выходим\n", id)
			return

		case job, ok := <-jobs:
			if !ok { // канал jobs закрыли — новых заданий не будет
				fmt.Printf("Воркер #%d: канал заданий закрыт, выходим\n", id)
				return
			}

			// Внутренний select: обрабатываем ОДНО задание с таймаутом.
			// Если не вернулись из функции — переходим к следующей итерации for.
			if stop := process(id, job, stopCh); stop {
				return
			}
		}
	}
}

// process обрабатывает одно задание. Возвращает true, если пришёл сигнал
// остановки и воркеру пора выходить.
func process(id, job int, stopCh <-chan struct{}) (stop bool) {
	// "Работу" запускаем в отдельной горутине, чтобы её завершение
	// стало каналом, за которым может следить select.
	done := make(chan struct{})
	go func() {
		time.Sleep(500 * time.Millisecond) // имитация полезной работы
		close(done)                        // сигнал "готово"
	}()

	select {
	case <-done:
		fmt.Printf("Воркер #%d выполнил задание %d\n", id, job)
		return false

	case <-time.After(1 * time.Second):
		fmt.Printf("Воркер #%d: задание %d не успело за таймаут\n", id, job)
		return false

	case <-stopCh:
		fmt.Printf("Воркер #%d: стоп во время задания %d, прерываемся\n", id, job)
		return true
	}
}

func main() {
	jobs := make(chan int)
	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	// Запускаем одного воркера (можно и пул — цикл по id).
	wg.Add(1)
	go worker(1, jobs, stopCh, &wg)

	// Отправляем несколько заданий.
	for j := 1; j <= 5; j++ {
		jobs <- j
	}

	// Через некоторое время говорим "стоп".
	time.Sleep(1500 * time.Millisecond)
	fmt.Println("Main отправляет сигнал СТОП")
	close(stopCh)

	// Ждём, пока воркер корректно завершится, и только потом выходим.
	wg.Wait()
	fmt.Println("Программа завершена.")
}


// II Solution

func worker2(id int, jobs <-chan int, stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select{
		case <-stopCh:
			fmt.Printf("Worker %d get 'stopCH'!", id)
			return

		case job, ok := <-jobs:
			if !ok {
				return
			}
			
			done := make(chan struct{})
			go func() {
				time.Sleep(500 * time.Millisecond)
				close(done)
			}()

			select{
			case <-done:
				fmt.Printf("Worker %d done job %d\n", id, job)
			case <-time.After(1 * time.Second):
				fmt.Printf("считаем, что задание %d не успело ", job)
			}
			case <-stopCh:
				fmt.Printf("Worker %d get 'stopCH'!", id)
				return

		}
	}
}


func main2() {
	jobs := make(chan int, 10)
	stopCh := make(chan struct{})

	// Запускает воркера(ов)
	var wg sync.WaitGroup
	numWorkers := 1
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker2(w, jobs, stopCh, &wg)
	}

	// Отправляет несколько заданий
	numJobs := 5
	for j := 1; j <= numJobs; j++ {
		jobs <- j
	}
	close(jobs)
	fmt.Println("All jobs send! Chanal 'jobs' closed")

	// Через некоторое время посылает stop (закрывает stopCh)
	time.Sleep(2 * time.Second)
	close(stopCh)

	// Корректно завершает программу
	wg.Wait()
	fmt.Println("Program has finished")
}