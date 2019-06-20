package pipeline

// Element-wise product of parent matrices.
func MakeOpenParkingOperator(op *Operator, operands map[string]string) {
	countParent := op.Parents[1]
	var maxes map[[2]int]*MatrixData
	var countMatrix map[[2]int]*MatrixData

	op.InitFunc = func(frame *Frame) {
		driver.DeleteMatrixAfter(op.Name, frame.Time)
		maxes = LoadMatrix("maxes")
		countMatrix = driver.LoadMatrixBefore(countParent.Name, frame.Time)
		op.updateChildRerunTime(frame.Time)
	}

	op.Func = func(frame *Frame, pd ParentData) {
		// update counts
		for _, md := range pd.MatrixData[1] {
			countMatrix[[2]int{md.I, md.J}] = md
		}

		// propogate rates
		for _, md := range pd.MatrixData[0] {
			cell := [2]int{md.I, md.J}
			val := md.Val
			if maxes[cell] != nil && countMatrix[cell] != nil && maxes[cell].Val > 0 {
				max := maxes[cell].Val
				count := countMatrix[cell].Val
				difference := abs(count - max)
				if difference == 0 || difference == 1 {
					val += 15
				} else if difference == 2 {
					val += 10
				} else if difference == 3 {
					val += 5
				} else if difference == 4 {
					val += 2
				}
			}
			AddMatrixData(op.Name, cell[0], cell[1], val, "", frame.Time)
		}
	}

	op.Loader = op.MatrixLoader
}
