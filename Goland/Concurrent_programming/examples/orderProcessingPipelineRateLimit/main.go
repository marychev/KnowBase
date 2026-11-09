package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Order struct {
	ID     int
	Amount int
}

type Result struct {
	OrderID int
	Success bool
	Error   error
}

func createOrder(pk int) Order {
	time.Sleep(200 * time.Millisecond)
	return Order{
		ID:     pk,
		Amount: pk * 100,
	}
}

func createResult(orderID int, success bool, err error) Result {
	time.Sleep(500 * time.Millisecond)
	return Result{
		OrderID: orderID,
		Success: success,
		Error:   err,
	}
}

func genOrders(ctx context.Context, n int) <-chan Order {
	order := make(chan Order)
	go func() {
		defer close(order)
		for i := 0; i < n; i++ {
			// 1. Сначала проверяем, не отменен ли контекст еще до создания заказа
			if ctx.Err() != nil {
				fmt.Println("Order generation canceled before creating order")
				return
			}

			ord := createOrder(i)

			select {
			case <-ctx.Done():
				fmt.Println("Order generation canceled")
				return
			case order <- ord:
				fmt.Printf("Order %d generated\n", i)
			}

		}
	}()

	return order
}

func main() {
	numOrders := 20
	numWorkers := 3
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ordersCh := genOrders(ctx, numOrders)
	resultsCh := make(chan Result)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("Worker %d: context canceled, stopping\n", workerID)
					return
				case order, ok := <-ordersCh:
					if !ok {
						fmt.Printf("Worker %d: orders channel closed, stopping\n", workerID)
						return
					}

					res := createResult(order.ID, true, nil)
					select {
					case <-ctx.Done():
						fmt.Printf("Worker %d: context canceled while sending result, stopping\n", workerID)
						return
					case resultsCh <- res:
						fmt.Printf("Worker %d: processed order %d\n", workerID, order.ID)
					}
				}
			}
		}(w)
	}

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	successCount := 0
	errorCount := 0

	for result := range resultsCh {
		if result.Success {
			successCount++
		} else {
			errorCount++
		}
	}

	fmt.Printf("Total Success: %d, Total Errors: %d\n", successCount, errorCount)

	fmt.Println("App END")
}
