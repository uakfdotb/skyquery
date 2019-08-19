package simulator


import (
	"../pipeline"

	//"fmt"
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
	cyclicSamples map[[2]int]map[int][]float64
	prevSamples map[[2]int]*PrevSample
	stddevs map[[2]int]float64
	maxes map[[2]int]int
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

func NewPredictor(driver *pipeline.InMemoryDriver, dataframe string, period int, maxes map[[2]int]int) *Predictor {
	return &Predictor{
		driver: driver,
		dataframe: dataframe,
		period: period,
		cyclicSamples: make(map[[2]int]map[int][]float64),
		prevSamples: make(map[[2]int]*PrevSample),
		stddevs: make(map[[2]int]float64),
		maxes: maxes,
	}
}

func getMean(a []float64) float64 {
	if len(a) == 0 {
		return 0
	}
	var sum float64 = 0
	for _, x := range a {
		sum += x
	}
	return sum / float64(len(a))
}

func getStddev(a []float64, max int, extras int) float64 {
	if len(a) == 0 {
		return float64(max)
	}
	mean := getMean(a)
	var sqdevsum float64 = 0
	for _, x := range a {
		d := x - mean
		sqdevsum += d * d
	}
	//sqdevsum += float64(max*max*extras)
	stddev := math.Sqrt(sqdevsum / float64(len(a)))
	return (stddev * float64(len(a)) + float64(max*extras)) / float64(len(a) + extras)
}

// new predictor that models the rates rather than the values
/*func (p *Predictor) Predict() map[[2]int]Prediction {
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
			p.cyclicSamples[cell] = make(map[int][]float64)
		}
		if p.prevSamples[cell] != nil {
			prevSample := p.prevSamples[cell]
			rate := float64(md.Val - prevSample.val) / float64(p.interval - prevSample.interval)
			for j := prevSample.interval + 1; j <= p.interval; j++ {
				p.cyclicSamples[cell][j % p.period] = append(p.cyclicSamples[cell][j % p.period], rate)
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
			var rateSum float64
			var bad bool
			for j := prevSample.interval + 1; j <= p.interval; j++ {
				jcycle := j % p.period
				if len(p.cyclicSamples[cell][jcycle]) < 1 {
					bad = true
					break
				}
				rateSum += getMean(p.cyclicSamples[cell][jcycle])
			}
			if bad {
				return float64(prevSample.val)
			}
			val := float64(prevSample.val) + rateSum
			if val < 0 {
				val = 0
			}
			return val
		}()

		predictions[cell] = prediction
	}

	p.interval++
	return predictions
}*/

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
		cell := [2]int{md.I, md.J}

		//val := md.Val
		var val int
		if md.Val >= p.maxes[cell] {
			val = 0
		} else {
			val = 1
		}

		if !md.Time.After(p.lastSeenObsTime) {
			continue
		}
		if md.Time.After(latestTime) {
			latestTime = md.Time
		}

		curCells[cell] = &pipeline.MatrixData{
			Val: val,
		}
		if p.cyclicSamples[cell] == nil {
			p.cyclicSamples[cell] = make(map[int][]float64)
		}
		if p.prevSamples[cell] == nil {
			p.cyclicSamples[cell][cycle] = append(p.cyclicSamples[cell][cycle], float64(val))
		} else {
			prevSample := p.prevSamples[cell]
			l := p.interval - prevSample.interval
			for j := 1; j <= l; j++ {
				jcycle := (prevSample.interval + j) % p.period
				cval := float64(j * val + (l - j) * prevSample.val) / float64(l)
				p.cyclicSamples[cell][jcycle] = append(p.cyclicSamples[cell][jcycle], cval)
			}
		}
		p.prevSamples[cell] = &PrevSample{
			interval: p.interval,
			val: val,
		}

		if val > p.max {
			p.max = val
		}
	}
	p.lastSeenObsTime = latestTime

	// increment standard deviations
	matrix := pipeline.LoadMatrix(p.dataframe)
	for cell, _ := range matrix {
		if p.maxes[cell] == 0 {
			p.stddevs[cell] = 0
			continue
		}
		if false {
			p.stddevs[cell] += 1
			continue
		}
		stddev := getStddev(p.cyclicSamples[cell][cycle], p.max, 0)
		if len(p.cyclicSamples[cell][cycle]) < 3 {
			stddev += 5
		}
		p.stddevs[cell] += stddev
		//p.stddevs[cell] += getMean(p.cyclicSamples[cell][cycle])
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
