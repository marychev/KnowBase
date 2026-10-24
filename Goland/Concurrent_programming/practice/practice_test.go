// Goland/Concurrent_programming/practice/
// practice_test.go
package mypackage

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestDoWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	resultCh := make(chan string, 1)

	go doWork(resultCh)

	select {
	case result := <-resultCh:
		if result != "Work complete" {
			t.Errorf("Fail result: %s", result)
		}
	case <-ctx.Done():
		t.Fatal("Test did not complete within the allowed time (tineout)")
	}
}

func TestCounterWithRace(t *testing.T) {
	var counter int
	var wg sync.WaitGroup
	var mu sync.Mutex // защищает доступ к counter

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			counter++ // <-- Гонка данных!
			mu.Unlock()
		}()
	}

	wg.Wait()
	if counter != 1000 {
		t.Errorf("Ожидали 1000, получили %d", counter)
	}
}
