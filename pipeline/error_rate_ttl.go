package pipeline

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const TTL int = 24
const DurationBetweenObs time.Duration = time.Hour

// Another issue: this dataframe will be huge since we insert an observation
// for ALL cells every minute. Eventually maybe we should be able to say "only
// store the latest N minutes of data for this data frame". When re-running from
// earlier timestamp, well, we just don't re-run it in that case, just stick to
// the error rate that was previously used.

func MakeTTLErrorRate(op *Operator, operands map[string]string) {
	regionCells := GetErrorRateCells(operands["region"])
	var matrix map[[2]int]*MatrixData
	var lastTime time.Time

	// cached TTL map
	// -1: saw a > 0 observation in parent matrix
	// >=0: # visits where parent matrix was <= 0
	// time: last time we updated the map
	type visit struct {
		count int
		time int
	}
	var badVisits map[[2]int]visit

	// we only create observations every ErrorRateInterval
	// so if we rerun starting in the middle of an interval, we miss all of the
	//  parent data that came in the beginning of the interval
	// instead, we rerun from previous interval, and ignore parent data up until
	//  lastTime
	op.LookBehind = ErrorRateInterval

	op.InitFunc = func(frame *Frame) {
		driver.DeleteMatrixAfter(op.Name, frame.Time)
		matrix = LoadMatrix(op.Name)
		badVisits = make(map[[2]int]visit)
		for _, md := range matrix {
			lastTime = md.Time

			metadataParts := strings.Split(md.Metadata, ",")
			count, err := strconv.Atoi(metadataParts[0])
			if err != nil {
				panic(err)
			}
			visitTime, err := strconv.Atoi(metadataParts[1])
			if err != nil {
				panic(err)
			}
			badVisits[[2]int{md.I, md.J}] = visit{count, visitTime}
		}

		op.updateChildRerunTime(frame.Time)
	}

	op.MatFunc = func(frame *Frame, matrixData []*MatrixData) {
		// ignore parent data that has already been processed
		if frame.Time.Before(lastTime) {
			return
		}

		// update badVisits
		for _, md := range matrixData {
			cell := [2]int{md.I, md.J}
			if md.Val > 0 {
				badVisits[cell] = visit{-1, 0}
				continue
			}

			// increment bad visits only if cell is not perma-active, and visits hasn't already
			// exceeded threshold
			v := badVisits[cell]
			if v.count >= 0 && v.count < TTL && md.Time.Sub(time.Unix(int64(v.time), 0)) >= DurationBetweenObs {
				badVisits[cell] = visit{v.count + 1, int(md.Time.Unix())}
			}
		}

		if frame.Time.Sub(lastTime) < ErrorRateInterval {
			return
		}

		// generate new observations for all cells
		obsCells := regionCells
		if len(obsCells) == 0 {
			// no cells means user wants us to derive from the known cells
			for cell := range badVisits {
				obsCells = append(obsCells, cell)
			}
		}
		for _, cell := range obsCells {
			v := badVisits[cell]
			metadata := fmt.Sprintf("%d,%d", v.count, v.time)
			var errorRate int
			if v.count >= TTL {
				errorRate = 0
			} else {
				errorRate = 1
			}
			matrix[cell] = AddMatrixData(op.Name, cell[0], cell[1], errorRate, metadata, frame.Time)
		}
		lastTime = frame.Time
	}

	op.Loader = op.MatrixLoader
}
