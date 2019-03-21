package pipeline

import (
	"github.com/mitroadmaps/gomapinfer/common"

	"fmt"
	"math"
	"time"
)

/*
TO_MATRIX strategy:

operator takes a function operand
	func(cell, frame, sequences)
	returns the value given the selected frame and the sequences
TO_MATRIX chooses which frame to use intelligently
	for each cell, maintain cellStatus{videoID, bestFrame, bestDistance}
	choose bestCell based on maximizing the minimum distance from cell boundaries to the frame boundaries
	bestFrame: includes both the frame itself, along with the sequences we got at that frame
	if another video comes in, then just overwrite it
		TODO: should keep one cell status per video perhaps
	once cell is no longer visible in the video, run the function on bestFrame and create new entry
		time of the entry is the time when that cell leaves the field of view
another TODO: videos should have a termination frame for operators like SEQ_MERGE and TO_MATRIX
*/

const MatrixGridSize float64 = 256

// duration in the past to look at when re-running this operator
// this is needed to reconstruct the selection of best cell to look at
// as long as MatrixLookBehind>=1 frame, we will create the same matrix_data
// but the value will only be correct if MatrixLookBehind is larger than
// the time that a point may be visible in the video (which is related to the drone speed)
const MatrixLookBehind time.Duration = 30*time.Second

type ToMatrixAggFunc func(cell [2]int, prev int, frame *Frame, seqs []*Sequence) int
var ToMatrixAggFuncs = map[string]ToMatrixAggFunc{
	"count": func(cell [2]int, prev int, frame *Frame, seqs []*Sequence) int {
		return len(seqs)
	},
	"count_sum": func(cell [2]int, prev int, frame *Frame, seqs []*Sequence) int {
		return prev + len(seqs)
	},
}

func toCell(p common.Point) [2]int {
	return [2]int{
		int(math.Floor(p.X / MatrixGridSize)),
		int(math.Floor(p.Y / MatrixGridSize)),
	}
}

func getCellRect(cell [2]int) common.Rectangle {
	cellPoint := common.Point{float64(cell[0]), float64(cell[1])}
	return common.Rectangle{
		cellPoint.Scale(MatrixGridSize),
		cellPoint.Add(common.Point{1, 1}).Scale(MatrixGridSize),
	}
}

func isCellInFrame(cell [2]int, frame *Frame) bool {
	cellRect := getCellRect(cell)
	for _, p := range cellRect.ToPolygon() {
		if !frame.Bounds.Contains(p) {
			return false
		}
	}
	return true
}

// Converts sequences to matrix using an aggregation function of the form:
//  func(cell, prev_value, frame, sequences)
// For every sequence of video frames where a cell is visible, the aggregation
//  function will be called on the cell at the frame where the cell is most centered.
// Aggregation functions include:
// * COUNT - count # current sequences
// * COUNT_SUM - count # current sequences, and add to previous count
func MakeToMatrixOperator(op *Operator) {
	aggFunc := ToMatrixAggFuncs["count"]

	op.LookBehind = MatrixLookBehind

	// status is used to select the best frame for each cell,
	// where the cell is closest to the center
	type bestFrame struct {
		frame *Frame
		sequences []*Sequence
		distance float64
	}
	type cellStatus struct {
		videoID int
		bestFrame bestFrame
	}
	cellStatuses := make(map[[2]int]cellStatus)

	// returns map from cells visible in current frame to the distances from
	// those cells to the frame boundaries
	getCellsInFrame := func(frame *Frame) map[[2]int]float64 {
		frameRect := frame.Bounds.Bounds()
		startCell := toCell(frameRect.Min)
		endCell := toCell(frameRect.Max)
		frameCells := make(map[[2]int]float64)
		processCell := func(cell [2]int) {
			if !isCellInFrame(cell, frame) {
				return
			}
			cellRect := getCellRect(cell)
			var worstDistance float64 = -1
			for _, cellSegment := range cellRect.ToPolygon().Segments() {
				for _, frameSegment := range frame.Bounds.Segments() {
					d := cellSegment.DistanceToSegment(frameSegment)
					if worstDistance == -1 || d < worstDistance {
						worstDistance = d
					}
				}
			}
			frameCells[cell] = worstDistance
		}
		for i := startCell[0]; i <= endCell[0]; i++ {
			for j := startCell[1]; j <= endCell[1]; j++ {
				processCell([2]int{i, j})
			}
		}
		return frameCells
	}

	var firstFrameTime time.Time
	op.InitFunc = func(frame *Frame) {
		db.Exec(
			"DELETE FROM matrix_data WHERE dataframe = ? AND time >= ?",
			op.Name, frame.Time,
		)

		firstFrameTime = frame.Time
		op.RerunTime = &firstFrameTime
	}

	op.SeqFunc = func(frame *Frame, seqs []*Sequence) {
		if frame.Bounds.Bounds().Area() > 1500*1500 {
			return
		}
		frameCells := getCellsInFrame(frame)

		// get location of sequences at this frame
		seqLocations := make(map[int]*common.Point)
		for _, seq := range seqs {
			seqLocations[seq.ID] = seq.LocationAt(frame.Time)
		}

		fmt.Printf("[%s] got %d cells in frame with %d seq locations, %d seqs\n", op.Name, len(frameCells), len(seqLocations), len(seqs))

		// update cell status based on frameCells
		for cell, distance := range frameCells {
			status, ok := cellStatuses[cell]
			if ok && frame.VideoID == status.videoID && status.bestFrame.distance > distance {
				continue
			}
			cellRect := getCellRect(cell)
			var relevantSeqs []*Sequence
			for _, seq := range seqs {
				location := seqLocations[seq.ID]
				if location == nil {
					continue
				}
				if !cellRect.Contains(*location) {
					continue
				}
				relevantSeqs = append(relevantSeqs, seq)
			}
			cellStatuses[cell] = cellStatus{
				videoID: frame.VideoID,
				bestFrame: bestFrame{
					frame: frame,
					sequences: relevantSeqs,
					distance: distance,
				},
			}
		}

		if frame.Time.Before(firstFrameTime) {
			return
		}

		// run aggregation function on best frames of cells that left
		for cell, status := range cellStatuses {
			if _, ok := frameCells[cell]; ok {
				continue
			}
			fmt.Printf("[%s] frame %d/%d: adding observation at cell %v\n", op.Name, frame.VideoID, frame.Idx, cell)
			prevData := GetLatestMatrixData(op.Name, cell[0], cell[1])
			var prev int = 0
			if prevData != nil {
				prev = prevData.Val
			}
			val := aggFunc(cell, prev, status.bestFrame.frame, status.bestFrame.sequences)
			AddMatrixData(op.Name, cell[0], cell[1], val, "", frame.Time)
			delete(cellStatuses, cell)
		}
	}

	op.Loader = op.MatrixLoader
}
