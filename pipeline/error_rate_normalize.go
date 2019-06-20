package pipeline

import (
	"time"
)

// Normalize error rates from parents so that the rates are emitted every ErrorRateInterval.
// This is needed for some operators like PATTERN.
func MakeNormalizeErrorRate(op *Operator, operands map[string]string) {
	var rates map[[2]int]int
	var lastTime time.Time

	// we only create observations every PatternGranularity
	op.LookBehind = ErrorRateInterval

	op.InitFunc = func(frame *Frame) {
		driver.DeleteMatrixAfter(op.Name, frame.Time)
		matrix := LoadMatrix(op.Name)
		rates = make(map[[2]int]int)
		for _, md := range matrix {
			lastTime = md.Time
			rates[[2]int{md.I, md.J}] = md.Val
		}
		op.updateChildRerunTime(frame.Time)
	}

	op.MatFunc = func(frame *Frame, matrixData []*MatrixData) {
		if frame.Time.Before(lastTime) {
			return
		}

		for _, md := range matrixData {
			rates[[2]int{md.I, md.J}] = md.Val
		}

		if frame.Time.Sub(lastTime) < ErrorRateInterval {
			return
		}

		// generate new observations for all cells
		var obsCells [][2]int
		for cell := range rates {
			obsCells = append(obsCells, cell)
		}
		for _, cell := range obsCells {
			AddMatrixData(op.Name, cell[0], cell[1], rates[cell], "", frame.Time)
		}
		lastTime = frame.Time
	}

	op.Loader = op.MatrixLoader
}
