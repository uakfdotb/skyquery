package main

import (
	"../../router"
	"os"
)

func main() {
	router.Evaluate(os.Args[1])
}
