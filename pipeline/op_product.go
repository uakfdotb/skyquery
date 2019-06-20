package pipeline

// Element-wise product of parent matrices.
func MakeProductOperator(op *Operator, operands map[string]string) {
	invertRight := operands["invertright"] == "yes"
	parent1 := op.Parents[0]
	parent2 := op.Parents[1]
	var matrix1 map[[2]int]*MatrixData
	var matrix2 map[[2]int]*MatrixData

	op.InitFunc = func(frame *Frame) {
		driver.DeleteMatrixAfter(op.Name, frame.Time)
		matrix1 = driver.LoadMatrixBefore(parent1.Name, frame.Time)
		matrix2 = driver.LoadMatrixBefore(parent2.Name, frame.Time)
		op.updateChildRerunTime(frame.Time)
	}

	op.Func = func(frame *Frame, pd ParentData) {
		newCells := make(map[[2]int]bool)
		for _, md := range pd.MatrixData[0] {
			cell := [2]int{md.I, md.J}
			matrix1[cell] = md
			newCells[cell] = true
		}
		for _, md := range pd.MatrixData[1] {
			cell := [2]int{md.I, md.J}
			matrix2[cell] = md
			newCells[cell] = true
		}
		for cell := range newCells {
			if matrix1[cell] == nil || matrix2[cell] == nil {
				continue
			}
			left := matrix1[cell].Val
			right := matrix2[cell].Val
			if invertRight {
				right = 1 - right
			}
			AddMatrixData(op.Name, cell[0], cell[1], left * right, "", frame.Time)
		}
	}

	op.Loader = op.MatrixLoader
}
