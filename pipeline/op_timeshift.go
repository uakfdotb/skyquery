package pipeline

import (
	"time"
)

const TimeShiftDuration time.Duration = -time.Hour

func MakeTimeShiftOperator(op *Operator, operands map[string]string) {
	op.InitFunc = func(frame *Frame) {
		driver.DeleteMatrixAfter(op.Name, frame.Time.Add(TimeShiftDuration))
		op.updateChildRerunTime(frame.Time.Add(TimeShiftDuration))
	}

	op.MatFunc = func(frame *Frame, matrixData []*MatrixData) {
		for _, md := range matrixData {
			AddMatrixData(op.Name, md.I, md.J, md.Val, "", frame.Time.Add(TimeShiftDuration))
		}
	}

	op.Loader = op.MatrixLoader
}
