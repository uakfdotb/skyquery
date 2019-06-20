package pipeline

// Zero matrix cells that are adjacent to a <= 0 cell.
func MakeThinOperator(op *Operator, operands map[string]string) {
	var parentMatrix map[[2]int]*MatrixData

	op.InitFunc = func(frame *Frame) {
		driver.DeleteMatrixAfter(op.Name, frame.Time)
		parentMatrix = driver.LoadMatrixBefore(op.Parents[0].Name, frame.Time)
		op.updateChildRerunTime(frame.Time)
	}

	op.MatFunc = func(frame *Frame, matrixData []*MatrixData) {
		for _, md := range matrixData {
			parentMatrix[[2]int{md.I, md.J}] = md

			// only add matrix data if it's not thinned
			good := true
			/*for i := -1; i <= 1; i++ {
				for j := -1; j <= 1; j++ {
					adjMD := parentMatrix[[2]int{md.I + i, md.J + j}]
					if adjMD == nil || adjMD.Val <= 0 {
						good = false
					}
				}
			}*/
			for _, p := range [][2]int{[2]int{-1, 0}, [2]int{1, 0}, [2]int{0, -1}, [2]int{0, 1}, [2]int{0, 0}} {
				adjMD := parentMatrix[[2]int{md.I + p[0], md.J + p[1]}]
				if adjMD == nil || adjMD.Val <= 0 {
					good = false
				}
			}
			if good {
				AddMatrixData(op.Name, md.I, md.J, md.Val, "", frame.Time)
			}
		}
	}

	op.Loader = op.MatrixLoader
}
