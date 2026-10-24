# Примеры из реальных сервисов | Examples from real-world services

## Параллельные запросы к нескольким внешним сервисам (Fan-out/Fan-in)

Проблема: Вашему сервису нужно собрать данные для пользователя из нескольких микросервисов: профиль, историю заказов и рекомендации. Запросы идут по сети и занимают разное время (например, 150мс, 300мс, 200мс). Если делать их последовательно, общее время будет суммой (650мс). Это слишком медленно.

Решение: Мы используем Fan-out, чтобы запустить все запросы параллельно, и Fan-in, чтобы собрать результаты в одну структуру, как только они все прибудут. Для надежности добавим таймаут через context.

Код: Агрегатор данных

```go
package main

import (
    "context"
    "fmt"
    "net/http"
    "net/http/httptest"
    "sync"
    "time"
)

// Структура для хранения агрегированных данных
type UserProfile struct {
    ID          int
    ProfileData string
    OrdersData  string
    RecsData    string
}

// startMockServer запускает локальный тестовый HTTP-сервер с тремя эндпоинтами.
// /profile и /orders отвечают быстро, /recommendations — медленно (600ms):
// он не уложится в таймаут запроса (400ms) и вернет ошибку context deadline exceeded.
func startMockServer() *httptest.Server {
    mux := http.NewServeMux()

    mux.HandleFunc("/profile", func(w http.ResponseWriter, r *http.Request) {
        time.Sleep(50 * time.Millisecond)
        fmt.Fprintln(w, `{"id":1,"name":"Alice"}`)
    })

    mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
        time.Sleep(100 * time.Millisecond)
        fmt.Fprintln(w, `{"orders":[1,2,3]}`)
    })

    // Этот эндпоинт намеренно медленный — он не уложится в таймаут
    mux.HandleFunc("/recommendations", func(w http.ResponseWriter, r *http.Request) {
        time.Sleep(600 * time.Millisecond)
        fmt.Fprintln(w, `{"recs":["item_a","item_b"]}`)
    })

    return httptest.NewServer(mux)
}

// fetchService - это воркер, который делает один запрос
func fetchService(ctx context.Context, client *http.Client, url string, dataCh chan<- string, wg *sync.WaitGroup) {
    defer wg.Done()

    // Создаем под-запрос с таймаутом, чтобы не ждать вечно
    // Советую попробовать поставить разные временные значения и понаблюдать что будет происходить)
    reqCtx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
    defer cancel()

    req, _ := http.NewRequestWithContext(reqCtx, "GET", url, nil)
    resp, err := client.Do(req)

    if err != nil {
        // Если не удалось, отправляем ошибку
        dataCh <- fmt.Sprintf("error fetching %s: %v", url, err)
        return
    }
    defer resp.Body.Close()
    dataCh <- fmt.Sprintf("success from %s (status: %d)", url, resp.StatusCode)
}

func main() {
    // Запускаем локальный mock-сервер вместо реальных внешних URL
    server := startMockServer()
    defer server.Close()

    client := &http.Client{Timeout: 1 * time.Second}
    services := []string{
        server.URL + "/profile",         // ответит быстро (~50ms)
        server.URL + "/orders",          // ответит быстро (~100ms)
        server.URL + "/recommendations", // намеренно медленный (~600ms) — упрется в таймаут 400ms
    }

    // 1. Создаем контекст с общим таймаутом для всей операции
    // Советую попробовать поставить разные временные значения и понаблюдать что будет происходить)
    ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
    defer cancel()

    // 2. Fan-out: Запускаем горутины для каждого сервиса
    var wg sync.WaitGroup
    dataCh := make(chan string, len(services)) // Буферизованный канал для результатов

    for _, url := range services {
        wg.Add(1)
        go fetchService(ctx, client, url, dataCh, &wg)
    }

    // 3. Запускаем отдельную горутину, которая закроет канал, когда все воркеры закончат
    go func() {
        wg.Wait()
        close(dataCh)
    }()

    // 4. Fan-in: Собираем результаты
    var profile UserProfile
    _ = profile // в реальном коде здесь был бы парсинг JSON и заполнение структуры
    for result := range dataCh {
        fmt.Println("Получен результат:", result)
    }

    fmt.Println("\nОперация завершена. Общий дедлайн не понадобился, ctx.Err():", ctx.Err())
}
```

## Ограничение запросов к БД/HTTP через семафор на каналах

Проблема: Ваш сервис может обрабатывать тысячи запросов в секунду, но база данных или внешний API, к которому вы обращаетесь, может выдержать только 50 одновременных подключений. Без контроля вы рискуете уронить БД или получить IP-бан.

Решение: Используем буферизованный канал как семафор (счетчик разрешений). Размер буфера — это максимальное количество одновременных подключений.

Код: Ограничитель параллелизма
Этот пример показывает, как использовать буферизованный канал как семафор для ограничения количества одновременно выполняющихся операций.

```go
package main

import (
    "fmt"
    "sync"
    "time"
)

// processTask имитирует долгую операцию (запрос в БД)
func processTask(taskID int, semaphore chan struct{}, wg *sync.WaitGroup) {
    defer wg.Done()

    // 1. Занимаем "слот" в семафоре.
    // Если все слоты заняты, эта строка заблокируется, пока один не освободится.
    semaphore <- struct{}{}
    defer func() { <-semaphore }() // 2. ОБЯЗАТЕЛЬНО освобождаем слот при выходе из функции

    fmt.Printf("Задача %d начала выполняться\n", taskID)
    time.Sleep(500 * time.Millisecond) // Имитируем работу с БД
    fmt.Printf("Задача %d завершилась\n", taskID)
}

func main() {
    const maxConcurrency = 5 // Ограничим до 5 для наглядности
    const totalTasks = 20

    // Создаем семафор с емкостью 5
    semaphore := make(chan struct{}, maxConcurrency)
    var wg sync.WaitGroup

    fmt.Printf("Запускаем %d задач с ограничением параллелизма в %d\n", totalTasks, maxConcurrency)

    for i := 1; i <= totalTasks; i++ {
        wg.Add(1)
        go processTask(i, semaphore, &wg)
    }

    wg.Wait()
    fmt.Println("Все задачи выполнены.")
}
```

## Graceful Shutdown: сигнал ОС → контекст → остановка воркеров

Проблема: Когда вы перезапускаете сервис (например, при деплое), вы не хотите просто "убивать" процесс. Если в этот момент он обрабатывал запрос или писал в базу, это может привести к потере данных или коррупции. Нужно, чтобы сервис:

- Перестал принимать новые запросы.
- Дождался, пока все текущие (in-flight) запросы завершатся.
- Только после этого корректно завершил работу.

Решение: Цепочка команд: Сигнал операционной системы (Ctrl+C) -> Контекст отмены -> Воркеры уважают контекст и останавливаются.

код: Сервер с плавной остановкой

```go
package main

import (
    "context"
    "log"
    "os/signal"
    "sync"
    "syscall"
    "time"
)

// worker имитирует фоновую задачу (например, обработку очереди)
func worker(ctx context.Context, id int, wg *sync.WaitGroup) {
    defer wg.Done()
    log.Printf("Воркер #%d запущен", id)

    for {
        select {
        case <-ctx.Done(): // Проверяем, не пришел ли сигнал отмены
            log.Printf("Воркер #%d получил сигнал остановки и завершает работу", id)
            return
        default:
            // Имитируем полезную работу
            log.Printf("Воркер #%d работает...", id)
            time.Sleep(1 * time.Second)
        }
    }
}

func main() {
    log.Println("Сервис запускается...")

    // 1. Создаем контекст, который будет отменен по сигналу ОС
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop() // Освобождаем ресурсы при выходе

    var wg sync.WaitGroup

    // 2. Запускаем нескольких воркеров, передавая им контекст
    for i := 1; i <= 3; i++ {
        wg.Add(1)
        go worker(ctx, i, &wg)
    }

    // 3. Блокируем main до получения сигнала остановки
    <-ctx.Done()
    log.Println("Получен сигнал остановки (SIGINT/SIGTERM). Ожидаем завершения воркеров...")

    // 4. Устанавливаем таймаут для graceful shutdown
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // Используем канал, чтобы дождаться, либо завершения воркеров, либо таймаута
    done := make(chan struct{})
    go func() {
        wg.Wait()
        close(done)
    }()

    select {
    case <-done:
        log.Println("Все воркеры завершили работу. Сервис останавливается.")
    case <-shutdownCtx.Done():
        log.Println("Таймаут graceful shutdown завершен. Принудительная остановка.")
    }
}
/* 
- Как только сигнал приходит (например, вы нажимаете Ctrl+C), ctx.Done() закрывается. Это срабатывает во всех воркерах, и они начинают процедуру завершения.
- main запускает wg.Wait() (в отдельной горутине с каналом done), чтобы дождаться, пока все воркеры не вызовут wg.Done().
- Дополнительно добавлен shutdownCtx с таймаутом, чтобы сервис не "завис" бесконечно, если какой-то воркер не смог корректно остановиться.
*/
```
