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


func normWeb(we <-chan WebEvent) <-chan Event {
    echan := make(chan Event)
    
    go func() {
        defer close(echan)
        
        for j := range we {
            e := Event{
                Source: "web",
                UserID: we.SessionID,
                Action: we.URL,
                At:     time.Unix(we.TS)
            }
            
            echan <- e
        }
    }()
    
    return echan
}


func normApp(ae <- chan AppEvent) <-chan Event {
    echan := make(chan Event)
    
    go func() {
        defer close(echan)
        
        for j := range we {
            e := Event{
                Source: "app",
                UserID: ae.DeviceID,
                Action: ae.Screen,
                At:     ae.At
            }
            
            echan <- e
        }
    }()
    
    return echan
}


func merge(c1 <- chan Event, c1 <- chan Event) <-chan Event {
    
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




// продолжить если 1 кан закрылся а др.нет

// попроси у ИИ задач

