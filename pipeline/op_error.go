package pipeline

// Error rate operator should emit an error rate every error interval.
// So at those intervals, we simply increment matrix value (error) by the
// specified rate.
// But we reset to zero if the cell is visible in the frame.
func MakeErrorOperator(op *Operator, operands map[string]string) {
	var matrix map[[2]int]*MatrixData

	op.InitFunc = func(frame *Frame) {
		driver.DeleteMatrixAfter(op.Name, frame.Time)
		matrix = LoadMatrix(op.Name)
		op.updateChildRerunTime(frame.Time)
	}

	op.MatFunc = func(frame *Frame, matrixData []*MatrixData) {
		for cell := range GetCellsInFrame(frame) {
			if matrix[cell] == nil || matrix[cell].Val == 0 {
				continue
			}
			matrix[cell] = AddMatrixData(op.Name, cell[0], cell[1], 0, "", frame.Time)
		}

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
			if IsCellInFrame(cell, frame, MatrixGridSize) {
				val = 0
			}
			matrix[cell] = AddMatrixData(op.Name, cell[0], cell[1], val, "", frame.Time)
		}
	}

	op.Loader = op.MatrixLoader
}
