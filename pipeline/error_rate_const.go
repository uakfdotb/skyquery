package pipeline

import (
	"time"
)

func MakeConstErrorRate(op *Operator, operands map[string]string) {
	regionCells := GetErrorRateCells(operands["region"])
	var seenCells map[[2]int]bool
	var lastTime time.Time

	// we only create observations every ErrorRateInterval
	// so if we rerun starting in the middle of an interval, we miss all of the
	//  parent data that came in the beginning of the interval
	// instead, we rerun from previous interval, and ignore parent data up until
	//  lastTime
	op.LookBehind = ErrorRateInterval

	op.InitFunc = func(frame *Frame) {
		driver.DeleteMatrixAfter(op.Name, frame.Time)
		matrix := LoadMatrix(op.Name)
		seenCells = make(map[[2]int]bool)
		for _, md := range matrix {
			lastTime = md.Time
			seenCells[[2]int{md.I, md.J}] = true
		}

		op.updateChildRerunTime(frame.Time)
	}

	op.MatFunc = func(frame *Frame, matrixData []*MatrixData) {
		// ignore parent data that has already been processed
		if frame.Time.Before(lastTime) {
			return
		}

		for _, md := range matrixData {
			seenCells[[2]int{md.I, md.J}] = true
		}

		if frame.Time.Sub(lastTime) < ErrorRateInterval {
			return
		}

		// generate new observations for all cells
		obsCells := regionCells
		if len(obsCells) == 0 {
			// no cells means user wants us to derive from the known cells
			for cell := range seenCells {
				obsCells = append(obsCells, cell)
			}
		}
		for _, cell := range obsCells {
			AddMatrixData(op.Name, cell[0], cell[1], 1, "", frame.Time)
		}
		lastTime = frame.Time
	}

	op.Loader = op.MatrixLoader
}
