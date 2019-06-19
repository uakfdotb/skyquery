package main

import (
	"./pipeline"

	"os"
)

func main() {
	if len(os.Args) >= 2 {
		name := os.Args[1]
		ops := pipeline.GetPipeline()
		ops[name].Execute()
	} else {
		pipeline.RunPipeline()
	}
}
