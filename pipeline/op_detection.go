package main

import (
	"fmt"
)

func MakeDetectionOperator(op *Operator) {
	op.RootFunc = func(frame *Frame) {
		detections := getFrameDetections(op.Name, frame)
		if Debug {
			fmt.Printf("[%s] got %d detections\n", op.Name, len(detections))
		}
		for _, child := range op.Children {
			child.DetFunc(frame, detections)
		}
	}
}
