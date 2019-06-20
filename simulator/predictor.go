package simulator


import (
	"../pipeline"

	"fmt"
	"math"
	"time"
)

type Predictor struct {
	driver *pipeline.InMemoryDriver
	dataframe string

	// Period in terms of intervals when Predict is called.
	period int

	interval int
	max int
	lastSeenObsTime time.Time
	cyclicSamples map[[2]int]map[int][]int
	prevSamples map[[2]int]*PrevSample
	stddevs map[[2]int]float64
}

// Most recent processed sample recorded at a cell.
type PrevSample struct {
	interval int
	val int
}

type Prediction struct {
	Val float64
	Stddev float64
}

func NewPredictor(driver *pipeline.InMemoryDriver, dataframe string, period int) *Predictor {
	return &Predictor{
		driver: driver,
		dataframe: dataframe,
		period: period,
		cyclicSamples: make(map[[2]int]map[int][]int),
		prevSamples: make(map[[2]int]*PrevSample),
		stddevs: make(map[[2]int]float64),
	}
}

func getMean(a []int) float64 {
	if len(a) == 0 {
		return 0
	}
	var sum float64 = 0
	for _, x := range a {
		sum += float64(x)
	}
	return sum / float64(len(a))
}

func getStddev(a []int, max int, extras int) float64 {
	if len(a) == 0 {
		return float64(max)
	}
	mean := getMean(a)
	var sqdevsum float64 = 0
	for _, x := range a {
		d := float64(x) - mean
		sqdevsum += d * d
	}
	//sqdevsum += float64(max*max*extras)
	stddev := math.Sqrt(sqdevsum / float64(len(a)))
	return (stddev * float64(len(a)) + float64(max*extras)) / float64(len(a) + extras)
}

// Process unseen samples for the configured dataframe.
// And return predictions for this interval.
// Predict should be called at regular intervals corresponding to the configured period.
func (p *Predictor) Predict() map[[2]int]Prediction {
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
			p.cyclicSamples[cell] = make(map[int][]int)
		}
		if p.prevSamples[cell] == nil {
			p.cyclicSamples[cell][cycle] = append(p.cyclicSamples[cell][cycle], md.Val)
		} else {
			prevSample := p.prevSamples[cell]
			l := p.interval - prevSample.interval
			for j := 1; j <= l; j++ {
				jcycle := (prevSample.interval + j) % p.period
				cval := (j * md.Val + (l - j) * prevSample.val) / l
				p.cyclicSamples[cell][jcycle] = append(p.cyclicSamples[cell][jcycle], cval)
			}
		}
		p.prevSamples[cell] = &PrevSample{
			interval: p.interval,
			val: md.Val,
		}

		if md.Val > p.max {
			p.max = md.Val
		}
	}
	p.lastSeenObsTime = latestTime

	// increment standard deviations
	matrix := pipeline.LoadMatrix(p.dataframe)
	for cell, _ := range matrix {
		stddev := getStddev(p.cyclicSamples[cell][cycle], p.max, 0)
		if len(p.cyclicSamples[cell][cycle]) < 3 {
			stddev += 5
		}
		if stddev > 0 {
			stddev = 1
		}
		p.stddevs[cell] += stddev
	}

	// reset standard deviations of cells visited in this interval
	for cell := range curCells {
		p.stddevs[cell] = 0
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
			prevSample := p.prevSamples[cell]
			prevCycle := prevSample.interval % p.period
			if len(p.cyclicSamples[cell][cycle]) < 1 || len(p.cyclicSamples[cell][prevCycle]) < 1 {
				return float64(prevSample.val)
			}
			prevMean := getMean(p.cyclicSamples[cell][prevCycle])
			curMean := getMean(p.cyclicSamples[cell][cycle])
			val := float64(prevSample.val) + curMean - prevMean
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
