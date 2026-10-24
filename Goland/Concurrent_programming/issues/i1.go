// ../Concurrent programming/issues# go run -race i1.go
package main

import (
	"fmt"
	"sync"
)

// counter — глобальный счётчик, общий для всех горутин.
var counter int

func main() {
	var wg sync.WaitGroup

	// 100 горутин
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// каждая увеличивает counter 1000 раз
			for j := 0; j < 1000; j++ {
				counter++ // <-- гонка: чтение+запись без синхронизации
			}
		}()
	}

	wg.Wait()
	fmt.Println("Counter:", counter) // ожидаем 100000, но получим меньше
}
