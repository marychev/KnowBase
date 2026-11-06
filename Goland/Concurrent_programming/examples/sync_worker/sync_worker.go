package sync_worker

import (
	"errors"
	"sync"
	"time"
)

// Написать асинхронный обработчик задач как библиотеку
// Клиент передаёт на вход некоторый объект (Task) с данными для выполнения задачи
// в нашем примере будем использовать пустую структуру.

// - Обработчик одновременно может обрабатывать не более N задач
// - и не более X задач могут быть поставлены в очередь на обработку.
// + Если нет места в очереди, сразу возвращаем клиенту ошибку.

// - Задача берется в обработку если имеются на это свободные обработчики.
// + Имитируем длительность обработки через time.Sleep(5*time.Second).
// - Как только очередная задача выполнилась – берём следующую задачу из очереди.
// - Если в очереди пусто, ожидаем новых задач от клиентов.

// - Буфер.канал

const sleep = 5

var (
	ErrQueueFull       = errors.New("task queue is full")
	ErrContextCanceled = errors.New("context canceled")
	ErrPoolStopped     = errors.New("worker pool stopped")
)

type TaskI interface {
	Do()
}

type Task struct{}

func (t *Task) Do() {
	time.Sleep(sleep * time.Second)
}

type WorkerPoolInterface interface {
	Start()
	Stop()
	AddToQueue(t TaskI) error
}

type workerPool struct {
	n     int
	queue chan TaskI
	// signal chan os.Signal
	// f      func()
	wg       sync.WaitGroup
	isClosed bool
	mu       sync.Mutex
}

func NewWorkerPool(n, x int) WorkerPoolInterface {
	return &workerPool{
		n:     n,
		queue: make(chan TaskI, x),
		// signal: make(chan os.Signal),
	}
}

func (p *workerPool) work() {
	defer p.wg.Done()
	for task := range p.queue {
		task.Do()
	}
}

func (p *workerPool) Start() {
	for i := 0; i < p.n; i++ {
		p.wg.Add(1)
		go p.work()
	}
}

func (p *workerPool) Stop() {
	// p.signal <- os.Kill
	p.mu.Lock()
	if p.isClosed {
		p.mu.Unlock()
		return
	}
	p.isClosed = true
	p.mu.Unlock()

	close(p.queue)
	p.wg.Wait()
}

func (p *workerPool) AddToQueue(t TaskI) error {
	// select {
	// case <-p.signal:
	// 	return ErrContextCanceled
	// default:
	// }
	p.mu.Lock()
	if p.isClosed {
		p.mu.Unlock()
		return ErrPoolStopped
	}
	p.mu.Unlock()

	select {
	case p.queue <- t:
		return nil
	default:
		// Если канал полностью заполнен (достигнут предел X),
		// Go мгновенно переходит в блок default вместо блокировки.
		return ErrQueueFull
	}
}
