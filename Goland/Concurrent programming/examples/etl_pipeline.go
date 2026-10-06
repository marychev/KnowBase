// etl_pipeline.go
//
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
	"time"
	"fmt"
	"runtime"
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

func createWebEvent(sessionID string, url string) WebEvent {
	return WebEvent{
		SessionID: sessionID,
		URL:       url,
		TS:        time.Now().Unix(),
	}
}

func createAppEvent(deviceID string, screen string) AppEvent {
	return AppEvent{
		DeviceID:  deviceID,
		Screen:    screen,
		EventTime: time.Now(),
	}
}

func webEventPrint(e WebEvent) {
	fmt.Printf(
		"[%s] ID :%s | URL: %s\n",
		time.Unix(e.TS, 0).Format("15:04:05"),
		e.SessionID,
		e.URL,
	)
}

func appEventPrint(e AppEvent) {
	fmt.Printf(
		"[%s] ID :%s | Screen: %s\n",
		e.EventTime.Format("15:04:05"),
		e.DeviceID,
		e.Screen,
	)
}


// --- Генераторы ---

/*
шаблон генератора данных (паттерн Generator) с использованием каналов и горутин. 
Функция genWeb асинхронно генерирует поток случайных событий веб-сайта (WebEvent) 
с заданным интервалом и корректно завершает работу при отмене контекста, 
предотвращая утечки памяти.
<-chan WebEvent: Возвращает канал только для чтения (receive-only channel). 
Вызывающий код сможет только читать из него события, 
но не сможет случайно закрыть его или отправить туда что-то лишнее.
*/
func genWeb(ctx context.Context, every time.Duration) <-chan WebEvent {
	// Создается небуферизированный канал.
	out := make(chan WebEvent)
	
	// Запускается новая горутина. Сама функция genWeb не ждет выполнения этого кода, 
	// она моментально возвращает канал out и завершается.	
	go func() {
		// Как только горутина завершит работу (по любой причине), канал out закроется.
		defer close(out)
		
		// Создается таймер (тикер), который будет отправлять сигнал 
		// в свой внутренний канал t.C через равные промежутки времени.
		t := time.NewTicker(every)
		
		// Гарантирует, что ресурсы тикера будут освобождены при выходе из горутины. 
		// Без этого тикер продолжит работать в фоне, вызывая утечку памяти.
		defer t.Stop()
		
		i := 0
		for {
			select {
			case <-ctx.Done():
				// Если контекст отменили (вызвали cancel()), этот case сработает. 
				// Горутина выполнит return, сработают все defer 
				return
			case <-t.C:
				// Срабатывает каждый раз, когда подходит время (заданный интервал every)
				i++
				ev := createWebEvent("web-" + strconv.Itoa(i), "/p/" + strconv.Itoa(rand.Intn(5)))

				// Защита от блокировки при отправке (Важный нюанс!)
				// Отправку тоже прикрываем ctx, иначе на отмене
				// горутина зависнет на out <- ev, если читателя уже нет.
				select {
				case out <- ev: // После того как событие ev создано, его нужно отправить в канал out
				case <-ctx.Done():
					// если вызывающий код перестал читать и отменил контекст
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
				ev := createAppEvent("dev-" + strconv.Itoa(i), "screen_" + strconv.Itoa(rand.Intn(5)))
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


func main() {
	// Создаем контекст с таймаутом на 5 секунд.
	// По истечении этого времени ctx.Done() закроется автоматически
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	
	// всегда вызывать cancel() для освобождения ресурсов контекста
	defer cancel()

	fmt.Println("Запуск генератора событий (работает 5 секунд)...")

	every := 500 * time.Millisecond
	webEventsChan := genWeb(ctx, every)
	appEventsChan := genApp(ctx, every)

	// Читаем из канала. Цикл сам завершится, когда канал закроется.
	// Канал закроется благодаря defer close(out) внутри genWeb при отмене контекста.
	for event := range webEventsChan {
		webEventPrint(event)
	}

	// TODO: Этот код не выводится. Если закоментировать genWeb, то выводится только appEventPrint.
	for event := range appEventsChan {
		appEventPrint(event)
	}

	fmt.Println("горутин в программе:", runtime.NumGoroutine())
}
