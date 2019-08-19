package simulator

import (
	"../pipeline"

	"fmt"
	"math"
	"time"
)

type Predictor3 struct {
	driver *pipeline.InMemoryDriver
	dataframe string

	// Period in terms of intervals when Predict is called.
	period int

	interval int
	max int
	lastSeenObsTime time.Time

	// observation samples
	// cell -> [2]int{cycle, number of intervals elapsed} -> observed rates
	cyclicSamples map[[2]int]map[[2]int][]float64

	prevSamples map[[2]int][]PrevSample
	stddevs map[[2]int]float64
}

func NewPredictor3(driver *pipeline.InMemoryDriver, dataframe string, period int) *Predictor3 {
	return &Predictor3{
		driver: driver,
		dataframe: dataframe,
		period: period,
		cyclicSamples: make(map[[2]int]map[[2]int][]float64),
		prevSamples: make(map[[2]int][]PrevSample),
		stddevs: make(map[[2]int]float64),
	}
}

// new predictor that models the rates rather than the values
func (p *Predictor3) Predict() map[[2]int]Prediction {
	cycle := p.interval % p.period

	// process unseen observations
	df := p.driver.DFs[p.dataframe].MatrixData
	curCells := make(map[[2]int]*pipeline.MatrixData)
	latestTime := p.lastSeenObsTime
	for _, md := range df {
		if !md.Time.After(p.lastSeenObsTime) {
			continue
		}
		if md.Time.After(latestTime) {
			latestTime = md.Time
		}

		cell := [2]int{md.I, md.J}
		curCells[cell] = md
		if p.cyclicSamples[cell] == nil {
			p.cyclicSamples[cell] = make(map[[2]int][]float64)
		}
		if len(p.prevSamples[cell]) > 0 {
			// tabulate previous values for the last (period) intervals via linear interpolation
			var prevTable []float64
			curSample := PrevSample{
				interval: p.interval,
				val: md.Val,
			}
			prevIdx := len(p.prevSamples[cell]) - 1
			for histsize := 0; histsize < p.period; histsize++ {
				prevSample := p.prevSamples[cell][prevIdx]
				interval := p.interval - histsize
				curWeight := interval - prevSample.interval
				prevWeight := curSample.interval - interval
				interp := float64(curWeight * curSample.val + prevWeight * prevSample.val) / float64(curWeight + prevWeight)
				prevTable = append(prevTable, interp)
				for prevIdx >= 0 && interval == p.prevSamples[cell][prevIdx].interval {
					curSample = p.prevSamples[cell][prevIdx]
					prevIdx--
				}
				if prevIdx < 0 {
					break
				}
			}
			for len(prevTable) < 2*p.period {
				prevTable = append(prevTable, prevTable[len(prevTable) - 1])
			}

			// add rate samples for all recent intervals and all histsizes
			prevSample := p.prevSamples[cell][len(p.prevSamples[cell]) - 1]
			for interval := prevSample.interval + 1; interval <= p.interval; interval++ {
				age := p.interval - interval
				for histsize := 1; histsize < len(prevTable) - age; histsize++ {
					cycle := interval % p.period
					cur := prevTable[age]
					prev := prevTable[age + histsize]
					rate := cur - prev
					k := [2]int{cycle, histsize}
					p.cyclicSamples[cell][k] = append(p.cyclicSamples[cell][k], rate)
				}
			}
		}
		p.prevSamples[cell] = append(p.prevSamples[cell], PrevSample{
			interval: p.interval,
			val: md.Val,
		})

		if md.Val > p.max {
			p.max = md.Val
		}
	}
	p.lastSeenObsTime = latestTime

	// update standard deviations
	matrix := pipeline.LoadMatrix(p.dataframe)
	badCells := make(map[[2]int]bool)
	for cell, _ := range matrix {
		prevSample := p.prevSamples[cell][len(p.prevSamples[cell]) - 1]
		if prevSample.interval == p.interval {
			p.stddevs[cell] = 0
			continue
		}
		histsize := p.interval - prevSample.interval
		if histsize >= p.period {
			histsize = p.period - 1
		}
		k := [2]int{cycle, histsize}
		stddev := getStddev(p.cyclicSamples[cell][k], p.max, 0)
		if len(p.cyclicSamples[cell][k]) < 3 {
			p.stddevs[cell] += 99
			continue
		}
		p.stddevs[cell] = stddev
		if stddev > 0.5 {
			badCells[cell] = true
		}
	}

	// cluster cells into coarser granularity
	for badCell := range badCells {
		smallCell := [2]int{
			int(math.Floor(float64(badCell[0]) / 8)),
			int(math.Floor(float64(badCell[1]) / 8)),
		}
		var aggCells [][2]int
		for i := 0; i <= 7; i++ {
			for j := 0; j <= 7; j++ {
				cell := [2]int{smallCell[0] * 8 + i, smallCell[1] * 8 + j}
				if !badCells[cell] {
					continue
				}
				delete(badCells, cell)
				aggCells = append(aggCells, cell)
			}
		}
		if len(aggCells) == 1 {
			continue
		}

		var samples []float64
		for _, cell := range aggCells {
			prevSample := p.prevSamples[cell][len(p.prevSamples[cell]) - 1]
			histsize := p.interval - prevSample.interval
			if histsize >= p.period {
				histsize = p.period - 1
			}
			k := [2]int{cycle, histsize}
			for i, sample := range p.cyclicSamples[cell][k] {
				if i >= len(samples) {
					samples = append(samples, sample)
				} else {
					samples[i] += sample
				}
			}
		}
		stddev := getStddev(samples, p.max, 0) / float64(len(aggCells))
		/*okay := true
		for _, cell := range aggCells {
			if p.stddevs[cell] < stddev {
				okay = false
			}
		}
		if !okay {
			continue
		}*/
		fmt.Printf("agg on %v pre=%v post=%v\n", aggCells, p.stddevs[badCell], stddev)
		for _, cell := range aggCells {
			p.stddevs[cell] = stddev
		}
	}

	// accumulate predictions for cells that weren't visited on this interval
	// for visited cells, copy the sampled value
	predictions := make(map[[2]int]Prediction)
	for cell, _ := range matrix {
		var prediction Prediction
		prediction.Stddev = p.stddevs[cell]
		prediction.Val = func() float64 {
			if curCells[cell] != nil {
				return float64(curCells[cell].Val)
			}
			prevSample := p.prevSamples[cell][len(p.prevSamples[cell]) - 1]
			histsize := p.interval - prevSample.interval
			if histsize >= p.period {
				histsize = p.period - 1
			}
			k := [2]int{cycle, histsize}
			if len(p.cyclicSamples[cell][k]) < 1 {
				return float64(prevSample.val)
			}
			change := getMean(p.cyclicSamples[cell][k])
			val := float64(prevSample.val) + change
			if val < 0 {
				val = 0
			}
			return val
		}()

		predictions[cell] = prediction
	}

	p.interval++
	return predictions
}
