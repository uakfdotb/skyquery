package pipeline

import (
	"strconv"
	"strings"
	"time"
)

const ErrorRateInterval time.Duration = time.Minute

func GetErrorRateCells(region string) [][2]int {
	if region == "" {
		return nil
	}
	parseInt := func(s string) int {
		x, err := strconv.Atoi(s)
		if err != nil {
			panic(err)
		}
		return x
	}
	parts := strings.Split(region, " ")
	sx, sy, ex, ey := parseInt(parts[0]), parseInt(parts[1]), parseInt(parts[2]), parseInt(parts[3])
	var cells [][2]int
	for x := sx; x <= ex; x++ {
		for y := sy; y <= ey; y++ {
			cells = append(cells, [2]int{x, y})
		}
	}
	return cells
}
