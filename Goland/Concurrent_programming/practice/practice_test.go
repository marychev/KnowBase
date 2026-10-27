// Goland/Concurrent_programming/practice/
// go test -race practice_test.go
package main

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func prepareJobs(numberJobs int) []Job {
	var jobs []Job
	for i := 1; i <= numberJobs; i++ {
		jobs = append(jobs, Job{ID: i})
	}
	return jobs
}

func prepareResultValues(numberJobs int) map[string]bool {
	expectedOutputs := make(map[string]bool, numberJobs)
	for i := 1; i <= numberJobs; i++ {
		expectedOutputs[fmt.Sprintf("processed jod: {%d}", i)] = false
	}
	return expectedOutputs
}

// Позитивный тест: проверяем, что все задачи обрабатываются
func TestRunWorkerPool_Success(t *testing.T) {
	ctx := context.Background()
	numWorkers := 2
	numJobs := 5
	jobs := prepareJobs(numJobs)

	results := RunWorkerPool(ctx, jobs, numWorkers)

	// 1. Проверяем общее количество
	if got, want := len(results), len(jobs); got != want {
		t.Fatalf("количество результатов: получили %d, ожидали %d", got, want)
	}

	// Проверяем, что все задачи из списка были обработаны
	// Так как воркеры работают конкурентно, порядок в слайсе результатов может быть случайным.
	// Поэтому мы проверяем наличие подстроки.
	expectedOutputs := prepareResultValues(numJobs)

	for _, res := range results {
		if _, exists := expectedOutputs[res.Value]; exists {
			expectedOutputs[res.Value] = true
		}
	}

	for val, found := range expectedOutputs {
		if !found {
			t.Errorf("Результат '%s' не был найден", val)
		}
	}

}

// Тест отмены контекста
func TestRunWorkerPool_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	numJobs := 5000
	numWorkers := 4

	jobs := prepareJobs(numJobs)
	resChs := make(chan []Result)

	go func() {
		_resChs := RunWorkerPool(ctx, jobs, numWorkers)
		fmt.Println(len(_resChs), " resChs ---> ")

		resChs <- _resChs
		fmt.Println(len(_resChs), "<--- resChs ")
	}()

	time.Sleep(1 * time.Second)
	cancel()

	results := <-resChs

	fmt.Println(len(results), len(resChs))

	if len(results) >= numJobs {
		t.Errorf("Ожидалось, что воркеры остановятся раньше, но обработано задач: %d", len(results))
	}
}
