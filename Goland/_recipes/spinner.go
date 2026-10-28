package main

import (
	"fmt"
	"time"
)

func main() {
	go spinner(5000 * time.Millisecond)
	const N = 40
	fib := fib(N)
	fmt.Printf("\rFibonacci (%d) = %d\n", N, fib)
}

func spinner(delay time.Duration) {
	for {
		for _, r := range `-\|/` {
			fmt.Printf("\r%c", r)
			time.Sleep(delay)
		}
	}
}

func fib(x int) int {
	if x < 2 {
		return x
	}
	return fib(x-1) + fib(x-2)
}
