package pipeline

import (
	"fmt"
	"strconv"
	"time"
)

/*
intersection strategy:

INTERSECTION(sequences, image, mode=['all', 'any'])
Returns filtered sequences where either all or any of the detections in the sequence intersect a cell in the image that has value > 0.

For image:
- At init, need to load the current state of the image for all cells.
- Then update this state as we execute the operator.

For sequences:
- At init, load unterminated sequences to get set of already incorporated sequences.
- Whenever we see a new sequence, determine if it passes the intersection state based on current image state.
- Maintain that state so we don't need to re-evaluate later.
*/

// TODO: we should be able to have different matrix grid size for each matrix, currently we assume they all the same

// Filters for sequences that intersect with an image.
// Filtering can be either all detections in the sequence intersect, or any intersect.
func MakeIntersectOperator(op *Operator, operands map[string]string) {
	mode := "all"
	if operands["mode"] == "any" {
		mode = "any"
	}
	matrix := make(map[[2]int]int)

	getMatrixVal := func(cell [2]int, t time.Time) int {
		val, ok := matrix[cell]
		if ok {
			return val
		}
		md := driver.GetMatrixDataBefore(op.Parents[1].Name, cell[0], cell[1], t)
		if md == nil {
			matrix[cell] = 0
		} else {
			matrix[cell] = md.Val
		}
		return matrix[cell]
	}

	// map from parent sequence ID to our sequence
	sequences := make(map[int]*Sequence)

	// set of parent sequence IDs that failed the intersection test
	rejectedSeqs := make(map[int]bool)

	op.InitFunc = func(frame *Frame) {
		driver.UndoSequences(op.Name, frame.Time)

		for _, seq := range GetUnterminatedSequences(op.Name) {
			parentID, _ := strconv.Atoi(seq.GetMetadata()[0])
			sequences[parentID] = seq
		}

		op.updateChildRerunTime(frame.Time)
	}

	op.Func = func(frame *Frame, pd ParentData) {
		seqs := pd.Sequences[0]
		matrixData := pd.MatrixData[0]

		// update matrix
		for _, md := range matrixData {
			matrix[[2]int{md.I, md.J}] = md.Val
		}

		// evaluate new sequences
		// TODO: should we do evaluation only when sequence is about to disappear?
		// (so we use the latest image data)
		for _, seq := range seqs {
			if sequences[seq.ID] != nil || rejectedSeqs[seq.ID] {
				continue
			}

			// determine intersection depending on mode
			var okay bool
			if mode == "all" {
				okay = true
				for _, member := range seq.Members {
					cell := ToCell(member.Detection.Polygon.Bounds().Center(), MatrixGridSize)
					val := getMatrixVal(cell, frame.Time)
					if val <= 0 {
						okay = false
						break
					}
				}
			} else if mode == "any" {
				okay = false
				for _, member := range seq.Members {
					cell := ToCell(member.Detection.Polygon.Bounds().Center(), MatrixGridSize)
					val := getMatrixVal(cell, frame.Time)
					if val > 0 {
						okay = true
						break
					}
				}
			}

			if !okay {
				rejectedSeqs[seq.ID] = true
				continue
			}

			mySeq := NewSequence(op.Name, seq.Time)
			mySeq.AddMetadata(fmt.Sprintf("%d", seq.ID), seq.Time)
			for _, member := range seq.Members {
				mySeq.AddMember(member.Detection, seq.Time)
			}
			mySeq.Terminate(mySeq.Members[len(mySeq.Members)-1].Detection.Time)
			sequences[seq.ID] = mySeq
		}
	}

	op.Loader = op.SequenceLoader
}
