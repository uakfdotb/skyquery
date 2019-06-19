package pipeline

import (
	"github.com/mitroadmaps/gomapinfer/common"

	"strconv"
	"strings"
)

func getIoU(a common.Rectangle, b common.Rectangle) float64 {
	intersectRect := a.Intersection(b)
	intersectArea := intersectRect.Area()
	unionArea := a.Area() + b.Area() - intersectArea
	return intersectArea / unionArea
}

func abs(x int) int {
	if x < 0 {
		return -x
	} else {
		return x
	}
}

func IntSliceAvg(a []int) int {
	var sum int = 0
	for _, x := range a {
		sum += x
	}
	return sum / len(a)
}

func encodeIntSlice(a []int) string {
	strs := make([]string, len(a))
	for i := range a {
		strs[i] = strconv.Itoa(a[i])
	}
	return strings.Join(strs, ",")
}

func decodeIntSlice(s string) []int {
	var a []int
	for _, part := range strings.Split(s, ",") {
		if part == "" {
			continue
		}
		x, err := strconv.Atoi(part)
		if err != nil {
			panic(err)
		}
		a = append(a, x)
	}
	return a
}
