// cd Goland/Concurrent\ programming/examples && go run -race i_ex.go
package main

import (
    "fmt"
    "sync"
    "time"
)

// func worker(id int, jobs <-chan int, results chan<- int, wg *sync.WaitGroup) {
//     defer wg.Done() // gorutine finished
//     fmt.Printf("Worker #%d has started \n", id)

//     for j := range jobs {  // zzzz .... wait chanel data
//         fmt.Printf("Worker #%d got task: %d\n", id, j)
//         time.Sleep(time.Second)
//         results <- j * j
//         fmt.Printf("Worker #%d finished task: %d\n", id, j)
//     }
//     // fmt.Printf("Воркер #%d завершил работу, так как канал заданий закрыт.\n", id)
// }



func main() {
    //  Небуферизированный канал не дает производителю убежать 
    // вперед потребителя — это и есть его синхронизирующее свойство.
    // jobs := make(chan int)
    jobs := make(chan int, 10) // разъединяет горутины
    results := make(chan int, 10)

    var wg sync.WaitGroup 
    numWorkers := 3
    for w := 1; w <= numWorkers; w++ {
        wg.Add(1)
        go worker(w, jobs, results, &wg)
    }

    // fmt.Println("Dispatcher starting send tesks...")
    numJobs := 5
    for i := 1; i <= numJobs; i++ {
        // fmt.Printf("Producer send number: %d\n", i)
        fmt.Printf("-> Dispatcher send task: %d\n", i)
        jobs <- i
    }

    close(jobs) // !!!
    fmt.Println("Dispatcher send all tasks and close chanel")

    for r := 1; r <= numJobs; r++ {
        result := <-results
        fmt.Printf("<- Got result: %d\n", result)
    }

    wg.Wait()
    fmt.Println("Programm end")
}