package main

import (
	"fmt"
	"sync"
	"time"
)

// worker читает задачи из jobs, возводит в квадрат и пишет в results.
func worker(id int, jobs <-chan int, results chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	for j := range jobs {
		time.Sleep(1 * time.Second) // имитируем тяжёлую работу
		results <- j * j
	}
}

// runPool гоняет N задач через M воркеров.
// bufSize задаёт ёмкость каналов: 0 — небуферизованные, >0 — буферизованные.
// Это единственная разница между «буфер» и «небуфер» версиями.
func runPool(N, M, bufSize int) {
	jobs := make(chan int, bufSize)
	results := make(chan int, bufSize)

	var wg sync.WaitGroup
	for w := 1; w <= M; w++ {
		wg.Add(1)
		go worker(w, jobs, results, &wg)
	}

	// генератор задач: в отдельной горутине, чтобы main мог параллельно
	// читать results (обязательно при bufSize == 0, безвредно при буфере).
	go func() {
		for i := 0; i < N; i++ {
			fmt.Println("Send task I:", i)
			jobs <- i
		}
		close(jobs)
	}()

	// когда все воркеры закончат — закрываем results, чтобы range ниже завершился.
	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		fmt.Printf("<- Получен результат: %d\n", result)
	}
}

func main() {
	fmt.Println("== буферизованный (bufSize = N) ==")
	runPool(5, 3, 5)

	fmt.Println("== небуферизованный (bufSize = 0) ==")
	runPool(5, 3, 0)
}
