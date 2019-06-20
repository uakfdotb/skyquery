package pipeline

func MakeDetectionOperator(op *Operator, operands map[string]string) {
	op.Loader = op.DetectionLoader
}

func MakeMatrixOperator(op *Operator, operands map[string]string) {
	op.Loader = op.MatrixLoader
}
