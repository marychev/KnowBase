// etl_pipeline.go
//
//   genWeb   ─ normalize ─┐
//                         ├─ merge (fan-in) ─ Event ── batch(size + timeout) ─ consume
//   genApp   ─ normalize ─┘

package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// --- Структуры источников: разная форма, близкий смысл ---

type WebEvent struct {
	SessionID string
	URL       string
	TS        int64 // unix seconds
}

type AppEvent struct {
	DeviceID  string
	Screen    string
	EventTime time.Time
}

// --- Общий нормализованный вид ---

type Event struct {
	Source string // web      | app
	UserID string // sessions | device
	Action string // url      | screen
	At     time.Time
}

// --- Генераторы ---

func genWeb(ctx context.Context, every time.Duration) <-chan WebEvent {
	out := make(chan WebEvent)
	go func() {
		defer close(out)
		t := time.NewTicker(every)
		defer t.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				i++
				ev := WebEvent{
					SessionID: "web-" + strconv.Itoa(i),
					URL:       "/p/" + strconv.Itoa(rand.Intn(5)),
					TS:        time.Now().Unix(),
				}
				// Отправку тоже прикрываем ctx, иначе на отмене
				// горутина зависнет на out <- ev, если читателя уже нет.
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func normalizeWeb(ctx context.Context, in <-chan WebEvent) <-chan Event {
	out := make(chan Event)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-in:
				if !ok {
					return
				}

				ev := Event{
					Source: "web",
					UserID: e.SessionID,
					Action: e.URL,
					At:     time.Unix(e.TS, 0),
				}

				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func genApp(ctx context.Context, every time.Duration) <-chan AppEvent {
	out := make(chan AppEvent)
	go func() {
		defer close(out)
		t := time.NewTicker(every)
		defer t.Stop()
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				i++
				ev := AppEvent{
					DeviceID:  "dev-" + strconv.Itoa(i),
					Screen:    "screen_" + strconv.Itoa(rand.Intn(5)),
					EventTime: time.Now(),
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func normalizeApp(ctx context.Context, in <-chan AppEvent) <-chan Event {
	out := make(chan Event)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-in:
				if !ok {
					return
				}

				ev := Event{
					Source: "app",
					UserID: e.DeviceID,
					Action: e.Screen,
					At:     e.EventTime,
				}

				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func merge(ctx context.Context, in1, in2 <-chan Event) <-chan Event {
	out := make(chan Event)
	var wg sync.WaitGroup

	_output := func(c <-chan Event) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-c:
				if !ok {
					return
				}

				select {
				case out <- e:
				case <-ctx.Done():
					return
				}
			}
		}
	}

	wg.Add(2)
	go _output(in1)
	go _output(in2)
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func consume(in <-chan []Event) {
	n := 0
	for b := range in {
		n++
		web, app := 0, 0
		for _, e := range b {
			switch e.Source {
			case "web":
				web++
			case "app":
				app++
			}
		}
		log.Printf("batch #%d: %d событий (web=%d app=%d)", n, len(b), web, app)
	}
	log.Printf("готово, всего батчей: %d", n)
}

/*
func batchWithContex(ctx context.Context, in <-chan Event, maxSize int, timeout time.Duration) <-chan []Event {
	out := make(chan []Event)

	go func() {
		defer close(out)

		buf := make([]Event, 0, maxSize)
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		_flush := func() {
			if len(buf) == 0 {
				return
			}
			select {
			case out <- buf:
			case <-ctx.Done():
			}

			// Почему это важно? В Go слайс — это лишь заголовок (ссылка на массив в памяти).
			// Если бы мы сделали просто очистку в виде buf = buf[:0] (сброс длины к нулю), то,
			// отправив buf в канал, мы бы продолжали использовать тот же самый массив памяти
			// для новой пачки. Приложение, которое читает из out, получило бы порчу данных (Data Race),
			// так как мы бы перезаписывали элементы прямо у него под носом. Выделение нового слайса
			// через make полностью изолирует старую пачку.
			buf = make([]Event, 0, maxSize)
		}

		for {
			select {
			case <-ctx.Done():
				_flush()
				return
			case <-timer.C:
				_flush()
				timer.Reset(timeout)
			case ev, ok := <-in:
				if !ok {
					_flush()
					return
				}

				buf = append(buf, ev)
				if len(buf) == maxSize {
					_flush()

					// это сброс таймера при досрочном заполнении пачки
					// (когда сработал лимит по maxSize, а не по времени).
					if !timer.Stop() {
						select {
						case <-timer.C:
						// пытается забрать значение из канала. Если оно там было — мы его «сжигаем».
						// Если канала пуст — default спасает нас от зависания.
						default:
						}
					}
					timer.Reset(timeout)
				}
			}
		}
	}()

	return out
}
*/

func batch(in <-chan Event, maxSize int, timeout time.Duration) <-chan []Event {
	out := make(chan []Event)

	go func() {
		defer close(out)

		buf := make([]Event, 0, maxSize)
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		_flush := func() {
			if len(buf) == 0 {
				return
			}
			out <- buf
			buf = make([]Event, 0, maxSize)
		}

		for {
			select {
			case <-timer.C:
				_flush()
				timer.Reset(timeout)
			case ev, ok := <-in:
				if !ok {
					_flush()
					return
				}

				buf = append(buf, ev)
				if len(buf) == maxSize {
					_flush()

					// это сброс таймера при досрочном заполнении пачки
					// (когда сработал лимит по maxSize, а не по времени).
					if !timer.Stop() {
						select {
						case <-timer.C:
						// пытается забрать значение из канала. Если оно там было — мы его «сжигаем».
						// Если канала пуст — default спасает нас от зависания.
						default:
						}
					}
					timer.Reset(timeout)
				}
			}
		}
	}()

	return out
}

func RunPipeline(ctx context.Context, web <-chan WebEvent, app <-chan AppEvent, maxSize int, timeout time.Duration) <-chan []Event {
	webNorm := normalizeWeb(ctx, web)
	appNorm := normalizeApp(ctx, app)
	merged := merge(ctx, webNorm, appNorm)
	return batch(merged, maxSize, timeout)
}

func main() {
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancel()

	// webGenCh := genWeb(ctx, 400*time.Millisecond)
	// appGenCh := genApp(ctx, 500*time.Millisecond)

	// webNormCh := normalizeWeb(ctx, webGenCh)
	// appNormCh := normalizeApp(ctx, appGenCh)
	// mergeCh := merge(ctx, webNormCh, appNormCh)

	// batches := batch(ctx, mergeCh, 5, 1*time.Second)
	// consume(batches)

	// // for batch := range batches {
	// // 	fmt.Printf("Получена пачка из %d элементов:\n", len(batch))
	// // 	for _, ev := range batch {
	// // 		fmt.Printf("[%s]: %s | %s | %s\n",
	// // 			ev.Source, ev.UserID, ev.Action, ev.At.Format("15:04:05"))
	// // 	}
	// // }
}
