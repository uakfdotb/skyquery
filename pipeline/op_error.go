package pipeline

// Error rate operator should emit an error rate every error interval.
// So at those intervals, we simply increment matrix value (error) by the
// specified rate.
// But we reset to zero if the cell is visible in the frame.
func MakeErrorOperator(op *Operator) {
	var matrix map[[2]int]*MatrixData

	op.InitFunc = func(frame *Frame) {
		db.Exec(
			"DELETE FROM matrix_data WHERE dataframe = ? AND time >= ?",
			op.Name, frame.Time,
		)

		matrix = LoadMatrix(op.Name)

		rerunTime := frame.Time
		op.RerunTime = &rerunTime
	}

	op.MatFunc = func(frame *Frame, matrixData []*MatrixData) {
		for _, parentMD := range matrixData {
			cell := [2]int{parentMD.I, parentMD.J}
			prevMD := matrix[cell]
			// increment error
			var val int = 0
			if prevMD != nil {
				val = prevMD.Val
			}
			val += parentMD.Val
			// but zero if visible
			if isCellInFrame(cell, frame) {
				val = 0
			}
			myMD := AddMatrixData(op.Name, cell[0], cell[1], val, "", frame.Time)
			matrix[cell] = myMD
		}
	}

	op.Loader = op.MatrixLoader
}
