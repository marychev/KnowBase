// ../Concurrent programming/issues# go run -race i_main.go
/*
Смоделируй сервис, который агрегирует данные из трёх «внешних сервисов»:
- fetchProfile(ctx), fetchOrders(ctx), fetchRecs(ctx):

каждая функция:
- спит случайное время (до 500–800ms),
- с некоторой вероятностью «паникует» или возвращает ошибку.

Требования:

1. Fan‑out + fan‑in:
- В main (или в обработчике запроса) запусти три горутины для трёх вызовов.
- Используй sync.WaitGroup и/или каналы, чтобы собрать результаты в общую структуру UserData.
- Все три вызова должны уважать общий context.Context с таймаутом (например, 1 секунда).

2. Ошибки и паники:
- Внутри каждой «внешней» горутины поставь defer + recover, чтобы паника конвертировалась в ошибку и отправлялась обратно через канал ошибок.
- Если любой из сервисов «падает» или возвращает ошибку — агрегатор должен вернуть ошибку или частичный результат (на твой выбор, но задокументируй поведение).

3. Graceful shutdown:
- Добавь context верхнего уровня, который можно отменить, имитируя SIGTERM:
- - например, через time.AfterFunc вызвать cancel().
- При отмене:
- - новые запросы не обрабатываются,
- - текущие — либо успевают за таймаут, либо возвращают ошибку.

ПОВЕДЕНИЕ ПРИ ОШИБКАХ (задокументировано):
Агрегатор возвращает ЧАСТИЧНЫЙ результат — UserData с теми полями, что успели
прийти, — И объединённую ошибку (errors.Join) по всем упавшим сервисам.
Один сбойный сервис не обнуляет данные двух других.
*/
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// UserData — общая структура, куда собираем ответы трёх сервисов (fan-in).
type UserData struct {
	Profile string
	Orders  string
	Recs    string
}

// piece — один «кусочек» ответа от одной горутины.
// Один канал таких кусочков вместо трёх разных каналов делает fan-in
// тривиальным: просто `for p := range out`.
type piece struct {
	field string // "profile" | "orders" | "recs" — куда класть значение
	value string // полезные данные (если ошибки нет)
	err   error  // ошибка или сконвертированная паника (иначе nil)
}

// Внешние сервисы
// Каждый: спит 500–800ms, но УВАЖАЕТ ctx (через select с ctx.Done()),
// и с некоторой вероятностью паникует или возвращает ошибку.

func fetchProfile(ctx context.Context) (string, error) {
	return simulate(ctx, "profile", `{"id":1,"name":"Alice"}`)
}

func fetchOrders(ctx context.Context) (string, error) {
	return simulate(ctx, "orders", `{"orders":[1,2,3]}`)
}

func fetchRecs(ctx context.Context) (string, error) {
	return simulate(ctx, "recs", `{"recs":["item_a","item_b"]}`)
}

// simulate — общая «начинка» внешнего сервиса.
func simulate(ctx context.Context, name, payload string) (string, error) {
	// 20% — паника (её поймает recover в runFetch и превратит в ошибку).
	if rand.Float64() < 0.2 {
		panic(fmt.Sprintf("%s: неожиданный сбой (nil pointer и т.п.)", name))
	}
	// 20% — обычная ошибка.
	if rand.Float64() < 0.2 {
		return "", fmt.Errorf("%s: сервис вернул 500", name)
	}

	// Случайная сетевая задержка 500–800ms.
	d := time.Duration(500+rand.Intn(301)) * time.Millisecond

	// Спим НЕ через голый time.Sleep, а через select — чтобы при таймауте
	// или отмене (SIGTERM) сразу выйти, а не досыпать до конца.
	select {
	case <-time.After(d):
		return payload, nil
	case <-ctx.Done():
		return "", ctx.Err() // context deadline exceeded / context canceled
	}
}

// -------------------- Fan-out обёртка --------------------

// runFetch запускается в отдельной горутине (fan-out).
// Здесь стоит defer + recover: паника ловится ТОЛЬКО в своей горутине
// поэтому страховка живёт именно тут, а не в main. 
// Результат в любом случае уходит одним сообщением в out.
func runFetch(
	ctx context.Context,
	field string,
	fn func(context.Context) (string, error),
	out chan<- piece,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Страховка: паника -> error -> канал. Ровно один send на горутину:
	// либо сюда (при панике), либо в обычную ветку ниже.
	defer func() {
		if r := recover(); r != nil {
			out <- piece{field: field, err: fmt.Errorf("%s: паника: %v", field, r)}
		}
	}()

	value, err := fn(ctx)
	out <- piece{field: field, value: value, err: err}
}

// -------------------- Fan-in агрегатор --------------------

// aggregate — обработчик одного запроса: fan-out трёх сервисов и fan-in
// результатов в UserData. Уважает переданный ctx (таймаут/отмену).
func aggregate(ctx context.Context) (UserData, error) {
	var wg sync.WaitGroup
	out := make(chan piece, 3) // буфер = число сервисов: горутины не блокируются на send

	// Fan-out: по горутине на сервис.
	jobs := []struct {
		field string
		fn    func(context.Context) (string, error)
	}{
		{"profile", fetchProfile},
		{"orders", fetchOrders},
		{"recs", fetchRecs},
	}
	for _, j := range jobs {
		wg.Add(1)
		go runFetch(ctx, j.field, j.fn, out, &wg)
	}

	// Отдельная горутина закрывает канал, когда все воркеры завершились —
	// это позволяет собирать результат через `range`.
	go func() {
		wg.Wait()
		close(out)
	}()

	// Fan-in: собираем частичный результат и копим ошибки.
	var data UserData
	var errs []error
	for p := range out {
		if p.err != nil {
			errs = append(errs, p.err)
			continue // поле оставляем пустым — это и есть "частичность"
		}
		switch p.field {
		case "profile":
			data.Profile = p.value
		case "orders":
			data.Orders = p.value
		case "recs":
			data.Recs = p.value
		}
	}

	// errors.Join вернёт nil, если ошибок не было, иначе — все сразу.
	return data, errors.Join(errs...)
}

// main: graceful shutdown 
func main() {
	// rand.Seed не нужен: с Go 1.20 глобальный источник math/rand
	// автоматически сеется случайным значением при старте программы.

	// Контекст верхнего уровня — "жизнь" сервиса целиком.
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Имитация SIGTERM: через 900ms отменяем rootCtx.
	// (В реальном сервисе это делал бы signal.NotifyContext.)
	time.AfterFunc(900*time.Millisecond, func() {
		log.Println(">>> получен SIGTERM, отменяем rootCtx (новые запросы больше не принимаем)")
		cancel()
	})

	// Гоняем несколько запросов подряд, чтобы увидеть все три требования:
	// первые успевают, а те, что после "SIGTERM", — отклоняются.
	for i := 1; i <= 5; i++ {
		// Graceful: не начинаем новый запрос, если сервис уже останавливается.
		if rootCtx.Err() != nil {
			log.Printf("запрос #%d ОТКЛОНЁН: сервис останавливается (%v)", i, rootCtx.Err())
			continue
		}

		// Таймаут одного запроса = 1s, но он ещё и наследует отмену rootCtx.
		reqCtx, reqCancel := context.WithTimeout(rootCtx, 1*time.Second)
		data, err := aggregate(reqCtx)
		reqCancel() // освобождаем ресурсы сразу после запроса (не копим defer в цикле)

		if err != nil {
			log.Printf("запрос #%d: частичный результат: %+v | ошибки: %v", i, data, err)
		} else {
			log.Printf("запрос #%d: успех: %+v", i, data)
		}

		time.Sleep(300 * time.Millisecond) // пауза между запросами
	}

	log.Println("main завершает работу.")
}
