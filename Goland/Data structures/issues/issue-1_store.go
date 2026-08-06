// 1. Реализуйте систему хранения и поиска событий.

package main

import (
	"fmt"
	"time"
	"sync"
)	

type Event struct {
	ID        int
	Type      string
	Data	  string
	Timestamp time.Time
}

type EventStore struct {
	// Хранилище — это инкапсуляция: снаружи не должно быть возможности случайно 
	// записать что-то в events или сбросить nextID. Доступ только через методы
	mu 	   sync.RWMutex
	events map[int]Event
	nextID int
}

func NewEventStore() *EventStore {
	return &EventStore{
		events: make(map[int]Event),
		nextID: 1,
	}
}

func (es *EventStore) Add(eventType string, data string) int {
	// создать событие, добавить в хранилище, вернуть ID
	es.mu.Lock()
	defer es.mu.Unlock()

	id := es.nextID
	es.events[id] = Event{
		ID:        id,
		Type:      eventType,
		Data:      data,
		Timestamp: time.Now(),
	}
	es.nextID++
	return id
}

func (es *EventStore) GetAll() []Event {
    // вернуть копию всех событий
	// Если вернуть напрямую внутреннюю структуру, внешний код сможет её испортить. 
	// Это нарушение инкапсуляции. Создавая новый слайс, мы гарантируем: 
	// что бы вызывающий ни делал со слайсом — наша мапа events останется целой.
	es.mu.RLock()
	defer es.mu.RUnlock()

	events := make([]Event, 0, len(es.events))
	for _, event := range es.events {
		events = append(events, event)
	}
	return events
}

func (es *EventStore) GetByID(id int) (Event, bool) {
	es.mu.RLock()
	defer es.mu.RUnlock()

	event, ok := es.events[id]
	return event, ok
}

func (es *EventStore) Count() int {
	es.mu.RLock()
	defer es.mu.RUnlock()	
	return len(es.events)
}

func (es *EventStore) GetByType(eventType string) []Event {
	return es.Filter(func(e Event) bool {
		return e.Type == eventType
	})
}

func (es *EventStore) FindAfter(timestamp time.Time) []Event {
    return es.Filter(func(e Event) bool {
		return e.Timestamp.After(timestamp)
	})
}

func (es *EventStore) GetRange(startID, endID int) []Event {
    // включая startID и endID
	if startID > endID {
	    return []Event{}   
	}

	es.mu.RLock()
	defer es.mu.RUnlock()
	
    result := make([]Event, 0, endID-startID+1)
	for id := startID; id <= endID; id++ {
		if event, ok := es.events[id]; ok {
			result = append(result, event)
		}
	}
    return result
}

func (es *EventStore) Filter(predicate func(Event) bool) []Event {
    // вернуть события, для которых predicate вернул true
	es.mu.RLock()
	defer es.mu.RUnlock()

	result := make([]Event, 0)
	for _, event := range es.events {
		if predicate(event) {
			result = append(result, event)
		}
	}
    return result
}


func main() {
	store := NewEventStore()	

	id1 := store.Add("user.login", "user: alice")
	id2 := store.Add("user.logout", "user: alice")

	if event, ok := store.GetByID(id1); ok {
		fmt.Printf("Event %d: %s - %s at %v\n", 
            event.ID, event.Type, event.Data, event.Timestamp)
	}
	if event, ok := store.GetByID(id2); ok {
		fmt.Printf("Event %d: %s - %s at %v\n", 
            event.ID, event.Type, event.Data, event.Timestamp)
	}
	fmt.Printf("Total events: %d\n", store.Count())

	loginEvents := store.Filter(func(e Event) bool {
		return e.Type == "user.login"
	})
	fmt.Printf("Login events found: %d\n", len(loginEvents))

	// то же самое короче, через специализированный метод
	fmt.Printf("Login events (via GetByType): %d\n", len(store.GetByType("user.login")))

	// go run -race issues/issue-1_store.go 
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.Add("test", "data")
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = store.Count()
		}()
	}
	wg.Wait()

}
