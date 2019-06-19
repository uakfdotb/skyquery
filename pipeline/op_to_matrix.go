package pipeline

import (
	"github.com/mitroadmaps/gomapinfer/common"

	"fmt"
	"math"
	"strconv"
	"strings"
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

const MatrixGridSize float64 = 512

// duration in the past to look at when re-running this operator
// this is needed to reconstruct the selection of best cell to look at
// as long as MatrixLookBehind>=1 frame, we will create the same matrix_data
// but the value will only be correct if MatrixLookBehind is larger than
// the time that a point may be visible in the video (which is related to the drone speed)
const MatrixLookBehind time.Duration = 30*time.Second

type ToMatrixAggFunc func(cell [2]int, prev int, metadata string, frame *Frame, seqs []*Sequence) (int, string)
var ToMatrixAggFuncs = map[string]ToMatrixAggFunc{
	"count": func(cell [2]int, prev int, metadata string, frame *Frame, seqs []*Sequence) (int, string) {
		return len(seqs), ""
	},
	"count_sum": func(cell [2]int, prev int, metadata string, frame *Frame, seqs []*Sequence) (int, string) {
		return prev + len(seqs), ""
	},
	"count_old_sum": func(cell [2]int, prev int, metadata string, frame *Frame, seqs []*Sequence) (int, string) {
		prevIDs := decodeIntSlice(metadata)
		prevIDSet := make(map[int]bool)
		for _, id := range prevIDs {
			prevIDSet[id] = true
		}
		var curIDs []int
		curIDSet := make(map[int]bool)
		var countNew int
		for _, seq := range seqs {
			curIDs = append(curIDs, seq.ID)
			curIDSet[seq.ID] = true
			if !prevIDSet[seq.ID] {
				countNew++
			}
		}
		var countOld int
		for _, id := range prevIDs {
			if !curIDSet[id] {
				countOld++
			}
		}
		return prev + countOld, encodeIntSlice(curIDs)
	},
	"avg_speed": func(cell [2]int, prev int, metadata string, frame *Frame, seqs []*Sequence) (int, string) {
		metaParts := strings.Split(metadata, ",")
		var sum, count float64
		if len(metaParts) == 2 {
			sum, _ = strconv.ParseFloat(metaParts[0], 64)
			count, _ = strconv.ParseFloat(metaParts[1], 64)
		}
		for _, seq := range seqs {
			first := seq.Members[0].Detection
			last := seq.Members[len(seq.Members)-1].Detection
			if last.Time.Sub(first.Time) <= 0 {
				continue
			}
			d := first.Polygon.Bounds().Center().Distance(last.Polygon.Bounds().Center())
			t := last.Time.Sub(first.Time).Seconds()
			speed := d / t
			sum += speed
			count++
		}
		if count == 0 {
			return 0, ""
		} else {
			return int(sum / count), fmt.Sprintf("%v,%v", sum, count)
		}
	},
}

func ToCell(p common.Point, gridSize float64) [2]int {
	return [2]int{
		int(math.Floor(p.X / gridSize)),
		int(math.Floor(p.Y / gridSize)),
	}
}

func GetCellRect(cell [2]int, gridSize float64) common.Rectangle {
	cellPoint := common.Point{float64(cell[0]), float64(cell[1])}
	return common.Rectangle{
		cellPoint.Scale(gridSize),
		cellPoint.Add(common.Point{1, 1}).Scale(gridSize),
	}
}

func IsCellInFrame(cell [2]int, frame *Frame, gridSize float64) bool {
	cellRect := GetCellRect(cell, gridSize)
	for _, p := range cellRect.ToPolygon() {
		if !frame.Bounds.Contains(p) {
			return false
		}
	}
	return true
}

// Returns map from cells visible in current frame to the distances from
// those cells to the frame boundaries
func GetCellsInFrame(frame *Frame) map[[2]int]float64 {
	frameRect := frame.Bounds.Bounds()
	startCell := ToCell(frameRect.Min, MatrixGridSize)
	endCell := ToCell(frameRect.Max, MatrixGridSize)
	frameCells := make(map[[2]int]float64)
	processCell := func(cell [2]int) {
		if !IsCellInFrame(cell, frame, MatrixGridSize) {
			return
		}
		cellRect := GetCellRect(cell, MatrixGridSize)
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

// Converts sequences to matrix using an aggregation function of the form:
//  func(cell, prev_value, frame, sequences)
// For every sequence of video frames where a cell is visible, the aggregation
//  function will be called on the cell at the frame where the cell is most centered.
// Aggregation functions include:
// * COUNT - count # current sequences
// * COUNT_SUM - count # current sequences, and add to previous count
func MakeToMatrixOperator(op *Operator, operands map[string]string) {
	funcName := operands["func"]
	if funcName == "" {
		funcName = "count"
	}
	aggFunc := ToMatrixAggFuncs[funcName]
	ignoreZero := operands["ignore_zero"] == "yes"
	unionSeqs := operands["union_seqs"] == "yes"

	op.LookBehind = MatrixLookBehind

	// status is used to select the best frame for each cell,
	// where the cell is closest to the center
	type bestFrame struct {
		frame *Frame
		sequences map[int]*Sequence
		distance float64
	}
	type cellStatus struct {
		videoID int
		bestFrame bestFrame
	}
	cellStatuses := make(map[[2]int]cellStatus)

	var firstFrameTime time.Time
	op.InitFunc = func(frame *Frame) {
		driver.DeleteMatrixAfter(op.Name, frame.Time)
		firstFrameTime = frame.Time
		op.updateChildRerunTime(firstFrameTime)
	}

	getRelevantSequences := func(seqs []*Sequence, cell [2]int, seqLocations map[int]*common.Point) map[int]*Sequence {
		cellRect := GetCellRect(cell, MatrixGridSize)
		relevantSeqs := make(map[int]*Sequence)
		for _, seq := range seqs {
			location := seqLocations[seq.ID]
			if location == nil {
				continue
			}
			if !cellRect.Contains(*location) {
				continue
			}
			relevantSeqs[seq.ID] = seq
		}
		return relevantSeqs
	}

	op.SeqFunc = func(frame *Frame, seqs []*Sequence) {
		frameCells := GetCellsInFrame(frame)

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
				if unionSeqs {
					relevantSeqs := getRelevantSequences(seqs, cell, seqLocations)
					for _, seq := range relevantSeqs {
						cellStatuses[cell].bestFrame.sequences[seq.ID] = seq
					}
				}
				continue
			}
			relevantSeqs := getRelevantSequences(seqs, cell, seqLocations)
			if ignoreZero && len(relevantSeqs) == 0 {
				continue
			}
			if unionSeqs {
				for _, seq := range cellStatuses[cell].bestFrame.sequences {
					relevantSeqs[seq.ID] = seq
				}
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
			prevData := driver.GetLatestMatrixData(op.Name, cell[0], cell[1])
			var prev int = 0
			var metadata string
			if prevData != nil {
				prev = prevData.Val
				metadata = prevData.Metadata
			}
			var sequences []*Sequence
			for _, seq := range status.bestFrame.sequences {
				sequences = append(sequences, seq)
			}
			val, metadata := aggFunc(cell, prev, metadata, status.bestFrame.frame, sequences)
			AddMatrixData(op.Name, cell[0], cell[1], val, metadata, frame.Time)
			delete(cellStatuses, cell)
		}
	}

	op.Loader = op.MatrixLoader
}
