package pipeline

func MakeDetectionOperator(op *Operator) {
	op.Loader = op.DetectionLoader
}
