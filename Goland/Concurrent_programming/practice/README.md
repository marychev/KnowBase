# General issues

1. Базовый пул воркеров

+ Генератор (producer) кладёт `N` заданий (`Job{ID: 1..N}`) в канал `jobs`.
+ `M` воркеров читают из `jobs`, обрабатывают через `callExternalService`, пишут `Result` в канал `results`.
+ `main` собирает все результаты из `results` и печатает их.
+ Завершиться корректно: дождаться всех воркеров и не уронить программу.

2. Потокобезопасная статистика + детектор гонок

+ Добавить сборщик `Stats`: счётчики `panics` / `failed` и `map[workerID]int` (сколько заданий успешно сделал каждый воркер).
+ Воркеры обновляют статистику конкурентно.

3. Ограничение параллелизма семафором

+ Внешний сервис держит максимум `K` одновременных вызовов (например, `K=2` при `M=4` воркерах). Реализовать ограничение **семафором на буферизованном канале**.

4. Отмена через context + отсутствие утечек

+ Протащить `context.Context` от `main` во все воркеры и в `callExternalService`.
+ Завести общий таймаут операции через `context.WithTimeout` (например, 2–3 с).
+ Воркеры обязаны **уважать** `ctx.Done()`: при отмене не начинать новую работу и не залипать.

5. Graceful shutdown
+ Сервис должен корректно останавливаться по сигналу ОС (`SIGINT`/`SIGTERM`): перестать брать новые задания, дать текущим завершиться, но не висеть вечно.


## Run

```sh
cd Goland/Concurrent_programming/practice/
go run -race practice.go
```

Run all tests

```sh
go test -v ./...
```

Run tests onather methods

```sh
go test -race
go test -v -run TestRunWorkerPool_PanicRecovery
go test -v .
go test -v -run TestRunWorkerPool_ContextCancellation
go test -v -run TestRunWorkerPool_DrainAllJobs
go test -v -race -run TestRunWorkerPool_StatsCollection
go test -v -run TestRunWorkerPool_SemaphoreLimit
go test -v -run TestRunWorkerPool_ContextLeakSafety
go test -v -run TestRunWorkerPool_GracefulShutdown
go test -v ./...
```

## Упражнения для самостоятельной практики (от простого к сложному)

Не пиши пока сложные пулы. Открой пустой файл и сделай 3 маленьких задания:

• Задание 1 (Каналы и Range): Создай горутину-генератор, которая просто кидает в канал числа от 1 до 5 и закрывает канал. А в main вычитай их через for num := range ch и распечатай. Это закрепит понимание, как каналы закрываются и читаются.

• Задание 2 (Защита мьютексом): Запусти в цикле 1000 горутин, и пусть каждая делает count++ в одну общую переменную int. Запусти с флагом -race — увидишь ошибку. А потом оберни этот count++ в мьютекс и убедись, что ошибка ушла, а итоговое число ровно 1000.

• Задание 3 (Маленький таймаут): Напиши select, который ждет либо данные из канала (который сработает через 2 секунды), либо time.After(500 * time.Millisecond). Убедись, что программа выходит по таймауту.