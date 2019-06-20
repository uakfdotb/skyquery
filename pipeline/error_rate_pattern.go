package pipeline

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const PatternGranularity time.Duration = time.Hour
const PatternRecurs time.Duration = 24*time.Hour
const PatternHistory int = 7

// Pattern: for cases where values are expected to recur on a fixed interval.
// pattern_optimizer(parent, recurs=daily, granularity=1*hour, mode=[ABSOLUTE, ERROR], window=7*day)
// Here, the user specifies that the pattern recurs daily, and the system should
//  set the error rate based on historical data from the current hour.
// Window is how far in the past we should base the error rate on.
// Two modes are supported:
//  ABSOLUTE: the parent is an absolute value, so error rate is proportional to
//            historical delta in that value
//  ERROR: the parent is an error value, so error rate is proportional to the parent
//         divided by time since last observation

// what should we store in metadata?
// we will emit every hour so probably we only need to store things about the current hour
// at least for absolute it could be: current value | list of past stuff for (window/recurs) intervals
// then when we update we look at two matrix datas: previous hour's, and previous day's at same hour

func MakePatternErrorRate(op *Operator, operands map[string]string) {
	parent := op.Parents[0]
	isAbsolute := operands["mode"] == "absolute"
	regionCells := GetErrorRateCells(operands["region"])

	// TODO: maybe should change this to a list
	var matrix map[[2]int]*MatrixData

	// parent data emitted during the current interval
	//var parentMatrix map[[2]int][]*MatrixData

	var lastTime time.Time

	// we only create observations every PatternGranularity
	//op.LookBehind = PatternGranularity

	op.InitFunc = func(frame *Frame) {
		driver.DeleteMatrixAfter(op.Name, frame.Time)
		matrix = LoadMatrix(op.Name)
		//parentMatrix = make(map[[2]int][]*MatrixData)
		for _, md := range matrix {
			lastTime = md.Time

			/*parentMD := driver.GetMatrixDataBefore(parent.Name, cell[0], cell[1], lastTime)
			if parentMD != nil {
				parentMatrix[cell] = []*MatrixData{parentMD}
			}*/
		}
		fmt.Printf("last time! %v\n", lastTime)
		op.updateChildRerunTime(frame.Time)
	}

	// metadata is lines like "past-delta1,past-delta2,..."
	// each line corresponds to a different interval
	parseMetadata := func(metadata string) [][]int {
		var list [][]int
		for _, line := range strings.Split(metadata, "\n") {
			var history []int
			for _, s := range strings.Split(line, ",") {
				if s == "" {
					continue
				}
				x, err := strconv.Atoi(s)
				if err != nil {
					panic(err)
				}
				history = append(history, x)
			}
			list = append(list, history)
		}
		return list
	}

	encodeMetadata := func(list [][]int) string {
		var lines []string
		for _, history := range list {
			var strs []string
			for _, x := range history {
				strs = append(strs, strconv.Itoa(x))
			}
			lines = append(lines, strings.Join(strs, ","))
		}
		return strings.Join(lines, "\n")
	}

	getInterval := func(t time.Time) int {
		recurs := int(PatternRecurs / time.Second)
		granularity := int(PatternGranularity / time.Second)
		return (int(t.Unix()) % recurs) / granularity
	}

	getAffectedIntervals := func(t1 time.Time, t2 time.Time) []int {
		numIntervals := int(PatternRecurs / PatternGranularity)
		interval1 := getInterval(t1)
		interval2 := getInterval(t2)
		if interval1 == interval2 {
			return []int{interval2}
		}
		interval := interval1
		var intervals []int
		for interval != interval2 {
			interval = (interval + 1) % numIntervals
			intervals = append(intervals, interval)
		}
		return intervals
	}

	op.MatFunc = func(frame *Frame, matrixData []*MatrixData) {
		if frame.Time.Before(lastTime) {
			return
		}

		for _, md := range matrixData {
			cell := [2]int{md.I, md.J}
			if matrix[cell] != nil {
				continue
			}
			metadata := make([][]int, int(PatternRecurs / PatternGranularity))
			matrix[cell] = AddMatrixData(op.Name, cell[0], cell[1], 1, encodeMetadata(metadata), frame.Time)
		}

		lastInterval := getInterval(lastTime)
		curInterval := getInterval(frame.Time)
		if curInterval != lastInterval {
			parentMatrix := make(map[[2]int][]*MatrixData)
			for cell := range matrix {
				md := driver.GetMatrixDataBefore(parent.Name, cell[0], cell[1], lastTime)
				if md == nil {
					continue
				}
				parentMatrix[cell] = append(parentMatrix[cell], md)
			}
			for _, md := range driver.GetMatrixDatasAfter(parent.Name, lastTime) {
				cell := [2]int{md.I, md.J}
				parentMatrix[cell] = append(parentMatrix[cell], md)
			}

			// generate new observations for all cells
			obsCells := regionCells
			if len(obsCells) == 0 {
				// no cells means user wants us to derive from the known cells
				for cell := range parentMatrix {
					obsCells = append(obsCells, cell)
				}
			}
			var min int = -1
			var max int = -1
			for _, cell := range obsCells {
				// first load the previous metadata
				var metadata [][]int
				if matrix[cell] != nil {
					metadata = parseMetadata(matrix[cell].Metadata)
				} else {
					metadata = make([][]int, int(PatternRecurs / PatternGranularity))
				}
				// next compute delta, and update the intervals affected
				if len(parentMatrix[cell]) >= 2 {
					prevData := parentMatrix[cell][0]
					latestData := parentMatrix[cell][len(parentMatrix[cell]) - 1]
					affectedIntervals := getAffectedIntervals(prevData.Time, latestData.Time)
					var delta int = 0
					if isAbsolute {
						delta = abs(latestData.Val - prevData.Val)
					} else {
						for _, md := range parentMatrix[cell][1:] {
							delta += md.Val
						}
					}
					delta = delta / len(affectedIntervals)
					for _, interval := range affectedIntervals {
						metadata[interval] = append(metadata[interval], delta)
						for len(metadata[interval]) > PatternHistory {
							metadata[interval] = metadata[interval][1:]
						}
					}
					// update the parent metadata to only include latest from this interval
					parentMatrix[cell] = []*MatrixData{latestData}
				}
				// finally grab the delta corresponding to current interval
				var curDelta int
				if len(metadata[curInterval]) > 0 {
					curDelta = IntSliceAvg(metadata[curInterval])
				}
				if curDelta < 1 {
					curDelta = 1
				}
				//fmt.Printf("cell=%v, curdelta=%v, lastint=%v, curint=%v, metadata: %v\n", cell, curDelta, lastInterval, curInterval, metadata)
				matrix[cell] = AddMatrixData(op.Name, cell[0], cell[1], curDelta, encodeMetadata(metadata), frame.Time)
				if min == -1 || curDelta < min {
					min = curDelta
				}
				if max == -1 || curDelta > max {
					max = curDelta
				}
			}
			fmt.Printf("min=%d, max=%d\n", min, max)
			lastTime = frame.Time
		}

		/*for _, md := range matrixData {
			parentMatrix[[2]int{md.I, md.J}] = append(parentMatrix[[2]int{md.I, md.J}], md)
		}*/
	}

	op.Loader = op.MatrixLoader
}
