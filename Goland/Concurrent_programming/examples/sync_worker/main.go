package main

import (
	"fmt"
	"sync_worker" // Adjust the import path as needed
	"time"
)

func main() {
	pool := sync_worker.NewWorkerPool(2, 2)
	pool.Start()

	fmt.Println("[Main] Пул запущен. Отправляем 5 задач...")

	// Example of adding tasks to the pool
	for i := 0; i < 5; i++ {
		task := &sync_worker.Task{}
		err := pool.AddToQueue(task)
		if err != nil {
			fmt.Printf("[Main] Задача %d отклонена: %v\n", i, err)
		} else {
			fmt.Printf("[Main] Задача %d добавлена в очередь.\n", i)
		}
	}

	time.Sleep((sync_worker.sleep + 1) * time.Second)
	fmt.Println("[Main] Останавливаем пул...")
	pool.Stop()
	fmt.Println("[Main] Пул успешно остановлен!")
}
