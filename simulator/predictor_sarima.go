package simulator

// predictor2 but for SARIMA

import (
	"../pipeline"

	"fmt"
	"encoding/json"
	"io/ioutil"
	"os/exec"
	"time"
)

type Predictor4 struct {
	driver *pipeline.InMemoryDriver
	dataframe string

	// Period in terms of intervals when Predict is called.
	period int

	interval int
	max int
	lastSeenObsTime time.Time

	// observation samples
	// cell -> observations
	samples map[[2]int][]float64
	predictions map[[2]int]map[int][2]float64
	stddevs map[[2]int]float64
}

func NewPredictor4(driver *pipeline.InMemoryDriver, dataframe string, period int) *Predictor4 {
	return &Predictor4{
		driver: driver,
		dataframe: dataframe,
		period: period,
		samples: make(map[[2]int][]float64),
		predictions: make(map[[2]int]map[int][2]float64),
		stddevs: make(map[[2]int]float64),
	}
}

func (p *Predictor4) updatePredictions() {
	var cellList [][2]int
	var sampleList [][]float64
	for cell, samples := range p.samples {
		if getStddev(samples, p.max, 0) == 0 {
			continue
		}
		cellList = append(cellList, cell)
		sampleList = append(sampleList, samples)
	}
	bytes, err := json.Marshal(sampleList)
	if err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile("query.json", bytes, 0644); err != nil {
		panic(err)
	}
	bytes, err = exec.Command("python", "sarima.py", "query.json").CombinedOutput()
	if err != nil {
		fmt.Println(string(bytes))
		panic(err)
	}
	var predictions [][][2]float64
	if err := json.Unmarshal(bytes, &predictions); err != nil {
		panic(err)
	}
}

// new predictor that models the rates rather than the values
func (p *Predictor4) Predict() map[[2]int]Prediction {
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
		if len(p.samples[cell]) == 0 {
			for interval := 0; interval <= p.interval; interval++ {
				p.samples[cell] = append(p.samples[cell], float64(md.Val))
			}
		} else {
			prevVal := p.samples[cell][len(p.samples[cell]) - 1]
			prevInterval := len(p.samples[cell]) - 1
			l := p.interval - prevInterval
			for j := 1; j <= l; j++ {
				cval := (float64(j * md.Val) + float64(l - j) * prevVal) / float64(l)
				p.samples[cell] = append(p.samples[cell], cval)
			}
		}
		if md.Val > p.max {
			p.max = md.Val
		}
	}
	p.lastSeenObsTime = latestTime

	if p.interval != 0 && p.interval % (7*24*4) == 0 {
		p.updatePredictions()
	}

	// update standard deviations
	matrix := pipeline.LoadMatrix(p.dataframe)
	for cell, _ := range matrix {
		if len(p.samples[cell]) >= p.interval - 1 {
			p.stddevs[cell] = 0
		} else if len(p.samples[cell]) < 3 * 24 * 4 {
			p.stddevs[cell] += 99
		} else if getStddev(p.samples[cell], p.max, 0) == 0 {
			p.stddevs[cell] = 0
		} else if p.predictions[cell] != nil {
			p.stddevs[cell] = p.predictions[cell][p.interval][1]
		} else {
			p.stddevs[cell] += 99
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
			} else if p.predictions[cell] != nil {
				return p.predictions[cell][p.interval][0]
			} else {
				return p.samples[cell][len(p.samples[cell]) - 1]
			}
		}()
		predictions[cell] = prediction
	}

	p.interval++
	return predictions
}
