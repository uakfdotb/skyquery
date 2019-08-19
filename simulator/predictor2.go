package simulator

// (hopefully) improved predictor that computes rate changes for every length of interval up to period

import (
	"../pipeline"

	//"fmt"
	"time"
)

type Predictor2 struct {
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
	maxes map[[2]int]int
}

func NewPredictor2(driver *pipeline.InMemoryDriver, dataframe string, period int, maxes map[[2]int]int) *Predictor2 {
	return &Predictor2{
		driver: driver,
		dataframe: dataframe,
		period: period,
		cyclicSamples: make(map[[2]int]map[[2]int][]float64),
		prevSamples: make(map[[2]int][]PrevSample),
		stddevs: make(map[[2]int]float64),
		maxes: maxes,
	}
}

// new predictor that models the rates rather than the values
func (p *Predictor2) Predict() map[[2]int]Prediction {
	cycle := p.interval % p.period

	// process unseen observations
	df := p.driver.DFs[p.dataframe].MatrixData
	curCells := make(map[[2]int]int)
	latestTime := p.lastSeenObsTime
	for _, md := range df {
		cell := [2]int{md.I, md.J}

		val := md.Val
		/*var val int
		if md.Val >= p.maxes[cell] {
			val = 0
		} else {
			val = 1
		}*/
		/*val := md.Val
		if len(p.prevSamples[cell]) > 0 {
			val = p.prevSamples[cell][len(p.prevSamples[cell])-1].val + md.Val
		}*/

		if !md.Time.After(p.lastSeenObsTime) {
			continue
		}
		if md.Time.After(latestTime) {
			latestTime = md.Time
		}

		curCells[cell] = val
		if p.cyclicSamples[cell] == nil {
			p.cyclicSamples[cell] = make(map[[2]int][]float64)
		}
		if len(p.prevSamples[cell]) > 0 {
			// tabulate previous values for the last (period) intervals via linear interpolation
			var prevTable []float64
			curSample := PrevSample{
				interval: p.interval,
				val: val,
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
			val: val,
		})

		if val > p.max {
			p.max = val
		}
	}
	p.lastSeenObsTime = latestTime

	// update standard deviations
	matrix := pipeline.LoadMatrix(p.dataframe)
	for cell, _ := range matrix {
		if p.maxes[cell] == 0 {
			p.stddevs[cell] = 0
			continue
		}
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
		if len(p.cyclicSamples[cell][k]) < 3 {
			p.stddevs[cell] += 99
			continue
		}
		if false {
			p.stddevs[cell] += 1
			continue
		}
		stddev := getStddev(p.cyclicSamples[cell][k], p.max, 0)
		/*if stddev <= 0.2 {
			stddev = 0.2
		}*/
		/*if p.interval - prevSample.interval >= p.period/2 {
			p.stddevs[cell] += 99
			continue
		}*/
		/*if p.interval - prevSample.interval >= p.period {
			stddev *= float64(p.interval - prevSample.interval) / float64(p.period - 1)
		}*/
		p.stddevs[cell] = stddev
		//fmt.Printf("compute stddev=%v cell=%v prev=%v cur=%v\n", stddev, cell, prevSample, p.interval)
	}

	// accumulate predictions for cells that weren't visited on this interval
	// for visited cells, copy the sampled value
	predictions := make(map[[2]int]Prediction)
	for cell, _ := range matrix {
		var prediction Prediction
		prediction.Stddev = p.stddevs[cell]
		prediction.Val = func() float64 {
			if curval, ok := curCells[cell]; ok {
				return float64(curval)
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
