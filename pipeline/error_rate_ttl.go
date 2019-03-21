package pipeline

import (
	"fmt"
	"strconv"
	"time"
)

const ErrorRateInterval time.Duration = time.Minute
const TTL int = 2

// Currently region is based on where we have flown in the past.
// But in practice it should be possible for user to define the region
// instead of having to manually fly there first.

// Another issue: this dataframe will be huge since we insert an observation
// for ALL cells every minute. Eventually maybe we should be able to say "only
// store the latest N minutes of data for this data frame". When re-running from
// earlier timestamp, well, we just don't re-run it in that case, just stick to
// the error rate that was previously used.

func MakeTTLErrorRate(op *Operator) {
	var matrix map[[2]int]*MatrixData
	var lastTime time.Time

	// cached TTL map
	// -1: saw a > 0 observation in parent matrix
	// >=0: # visits where parent matrix was <= 0
	var badVisits map[[2]int]int

	// we only create observations every ErrorRateInterval
	// so if we rerun starting in the middle of an interval, we miss all of the
	//  parent data that came in the beginning of the interval
	// instead, we rerun from previous interval, and ignore parent data up until
	//  lastTime
	op.LookBehind = ErrorRateInterval

	op.InitFunc = func(frame *Frame) {
		db.Exec(
			"DELETE FROM matrix_data WHERE dataframe = ? AND time >= ?",
			op.Name, frame.Time,
		)

		matrix = LoadMatrix(op.Name)
		badVisits = make(map[[2]int]int)
		for _, md := range matrix {
			lastTime = md.Time

			visits, err := strconv.Atoi(md.Metadata)
			if err != nil {
				panic(err)
			}
			badVisits[[2]int{md.I, md.J}] = visits
		}

		rerunTime := frame.Time
		op.RerunTime = &rerunTime
	}

	op.MatFunc = func(frame *Frame, matrixData []*MatrixData) {
		fmt.Printf("error rate %v %v %v\n", frame.Time, lastTime, frame.Time.Sub(lastTime))
		// ignore parent data that has already been processed
		if frame.Time.Before(lastTime) {
			return
		}

		// update badVisits
		for _, md := range matrixData {
			cell := [2]int{md.I, md.J}
			if md.Val > 0 {
				badVisits[cell] = -1
				continue
			}

			// increment bad visits only if cell is not perma-active, and visits hasn't already
			// exceeded threshold
			if badVisits[cell] >= 0 && badVisits[cell] < TTL {
				badVisits[cell]++
			}
		}

		if frame.Time.Sub(lastTime) < ErrorRateInterval {
			return
		}

		// generate new observations for all known cells
		for cell := range badVisits {
			visits := badVisits[cell]
			metadata := fmt.Sprintf("%d", visits)
			var errorRate int
			if visits >= TTL {
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
