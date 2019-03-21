package pipeline

import (
	"github.com/mitroadmaps/gomapinfer/common"
)

func getIoU(a common.Rectangle, b common.Rectangle) float64 {
	intersectRect := a.Intersection(b)
	intersectArea := intersectRect.Area()
	unionArea := a.Area() + b.Area() - intersectArea
	return intersectArea / unionArea
}
