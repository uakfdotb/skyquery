package main

import (
	"github.com/mitroadmaps/gomapinfer/common"
	"./simulator"
	"./router"
	"./pipeline"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"time"
)

var RecordInterval time.Duration = 15*time.Minute

type Predictor struct {
	driver *pipeline.InMemoryDriver
	dataframe string

	interval int
	max int
	lastSeenObsTime time.Time
	cyclicSamples map[[2]int]map[int][]int
	prevSamples map[[2]int]*PrevSample
}

type  PrevSample struct {
	interval int
	val int
}

func NewPredictor(driver *pipeline.InMemoryDriver, dataframe string) *Predictor {
	return &Predictor{
		driver: driver,
		dataframe: dataframe,
		cyclicSamples: make(map[[2]int]map[int][]int),
		prevSamples: make(map[[2]int]*PrevSample),
	}
}

func getStddev(a []int, max int, extras int) float64 {
	if len(a) == 0 {
		return float64(max)
	}
	var sum float64 = 0
	for _, x := range a {
		sum += float64(x)
	}
	mean := sum / float64(len(a))
	var sqdevsum float64 = 0
	for _, x := range a {
		d := float64(x) - mean
		sqdevsum += d * d
	}
	//sqdevsum += float64(max*max*extras)
	stddev := math.Sqrt(sqdevsum / float64(len(a)))
	return (stddev * float64(len(a)) + float64(max*extras)) / float64(len(a) + extras)
}

// should be called at the end of each interval
func (p *Predictor) Predict() {
	cycle := p.interval % 96

	// process unseen observations
	df := p.driver.DFs[p.dataframe].MatrixData
	curCells := make(map[[2]int]bool)
	for _, md := range df {
		if !md.Time.After(p.lastSeenObsTime) {
			continue
		}
		cell := [2]int{md.I, md.J}
		curCells[cell] = true
		if p.cyclicSamples[cell] == nil {
			p.cyclicSamples[cell] = make(map[int][]int)
		}
		if p.prevSamples[cell] == nil {
			p.cyclicSamples[cell][cycle] = append(p.cyclicSamples[cell][cycle], md.Val)
		} else {
			prevSample := p.prevSamples[cell]
			l := p.interval - prevSample.interval
			for j := 1; j <= l; j++ {
				jcycle := (prevSample.interval + j) % 96
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
	if len(df) > 0 {
		p.lastSeenObsTime = df[len(df)-1].Time
	}


	// create mds with predictions for cells that weren't visited on this interval
	// also increment error rate of all cells
	matrix := pipeline.LoadMatrix(p.dataframe)
	for cell, _ := range matrix {
		// error rate
		stddev := getStddev(p.cyclicSamples[cell][cycle], p.max, 0)
		if len(p.cyclicSamples[cell][cycle]) < 3 {
			stddev += 5
		}
		pipeline.AddMatrixData("error_rate", cell[0], cell[1], int(stddev*100), "", p.lastSeenObsTime)

		// predictions
		if curCells[cell] {
			continue
		}
		if p.prevSamples[cell] == nil {
			continue
		}
		prevSample := p.prevSamples[cell]
		prevCycle := prevSample.interval % 96
		if len(p.cyclicSamples[cell][cycle]) < 1 || len(p.cyclicSamples[cell][prevCycle]) < 1 {
			continue
		}
		prevMean := pipeline.IntSliceAvg(p.cyclicSamples[cell][prevCycle])
		curMean := pipeline.IntSliceAvg(p.cyclicSamples[cell][cycle])
		val := prevSample.val + curMean - prevMean
		if val < 0 {
			val = 0
		}
		pipeline.AddMatrixData("sd_counts", cell[0], cell[1], val, "", p.lastSeenObsTime)
	}

	p.interval++
}

func main() {
	/*cellRect := pipeline.GetCellRect([2]int{-7, -13}, simulator.GridSize)
	framePoly := common.Polygon{
		common.Point{-1817.6, -3353.6},
		common.Point{-1817.6, -3046.4},
		common.Point{-1510.4, -3046.4},
		common.Point{-1510.4, -3353.6},
	}
	for _, p := range cellRect.ToPolygon() {
		fmt.Printf("is %v in %v? %v\n", p, framePoly, framePoly.Contains(p))
	}
	return*/

	prefix := os.Args[1]
	dbname := os.Args[2]
	pipeline.SetDBName(dbname)
	db := pipeline.NewDatabase()
	driver := pipeline.GetDriver().(*pipeline.InMemoryDriver)

	//db.Exec("DELETE FROM matrix_data")
	//db.Exec("DELETE FROM video_frames")

	start := time.Date(2018, time.May, 7, 11, 0, 0, 0, time.UTC)
	end := time.Date(2018, time.May, 21, 11, 0, 0, 0, time.UTC)
	rect := common.Rectangle{
		common.Point{-6000, -12500},
		common.Point{6000, -500},
	}
	sd := simulator.LoadSanDiego(start, end, rect)
	fmt.Printf("bounds: %v\n", sd.Bounds())

	maxes := sd.GetMaxes()
	/*fmt.Println(maxes)
	fmt.Println(len(maxes))
	uniques := make(map[[2]int]bool)
	for cell3d := range sd.Grid {
		uniques[[2]int{cell3d[0], cell3d[1]}] = true
	}
	fmt.Println(len(uniques))
	return*/
	minCell := pipeline.ToCell(rect.Min, simulator.GridSize)
	maxCell := pipeline.ToCell(rect.Max, simulator.GridSize)
	var cells [][2]int
	for x := minCell[0]; x <= maxCell[0]; x++ {
		for y := minCell[1]; y <= maxCell[1]; y++ {
			cell := [2]int{x, y}
			cells = append(cells, cell)
			pipeline.AddMatrixData("error", x, y, 999999, "", start.Add(-time.Hour))
			pipeline.AddMatrixData("maxes", x, y, maxes[cell], "", start.Add(-time.Hour))
		}
	}

	base := pipeline.ToCell(rect.Center(), simulator.GridSize)
	router := router.Router{
		Dataframe: "error",
		Base: base,
	}
	s := &simulator.Simulation{
		DataSources: map[string]simulator.DataSource{
			"sd_counts": sd.GetCount,
			"sd_new": sd.GetNew,
		},
		Time: start,
		Router: router,
		Base: base,
	}
	for i := 0; i < 4; i++ {
		s.AddDrone()
	}
	predictor := NewPredictor(driver, "sd_counts")
	for s.Time.Before(end) {
		//db.Exec("DELETE FROM matrix_data WHERE dataframe != 'sd_counts' AND time < ? AND val != '999999'", s.Time.Add(-2*time.Minute))
		// delete old matrix data
		t := s.Time.Add(-2*time.Hour)
		for name, df := range driver.DFs {
			if name == "sd_counts" || name == "sd_new" || name == "maxes" {
				continue
			}
			for _, md := range df.MatrixData {
				if md.Val == 999999 || !md.Time.Before(t) {
					continue
				}
				delete(df.MatrixData, md.ID)
			}
		}

		db.Exec("UPDATE dataframes SET rerun_time = ?", s.Time)
		fmt.Println(s.Time)
		s.Run(int(15*time.Minute/simulator.TimeStep))
		predictor.Predict()
		pipeline.RunPipeline()
	}
	fmt.Printf("%v\n", s.Drones[0].Route)

	saveMap := func(fname string, m map[string]map[int]int) {
		bytes, err := json.Marshal(m)
		if err != nil {
			panic(err)
		}
		if err := ioutil.WriteFile(fname, bytes, 0644); err != nil {
			panic(err)
		}
	}
	save := func(fname string, dataframe string) map[string]map[int]int {
		intervals := int(end.Sub(start) / RecordInterval)
		m := make(map[string]map[int]int)
		intervalTimes := make(map[int]time.Time)
		for interval := 0; interval < intervals; interval++ {
			t := start.Add(time.Duration(interval) * RecordInterval)
			intervalTimes[interval] = t
		}
		for x := minCell[0]; x <= maxCell[0]; x++ {
			for y := minCell[1]; y <= maxCell[1]; y++ {
				m[fmt.Sprintf("%d %d", x, y)] = make(map[int]int)
			}
		}
		for _, md := range driver.DFs[dataframe].MatrixData {
			interval := int(md.Time.Sub(start) / RecordInterval)
			m[fmt.Sprintf("%d %d", md.I, md.J)][interval] = md.Val
		}
		/*for cell := range m {
			var prev int = 0
			for interval := 0; interval < intervals; interval++ {
				if _, ok := m[cell][interval]; ok {
					prev = m[cell][interval]
					continue
				}
				m[cell][interval] = prev
			}
		}*/
		saveMap(fname, m)
		return m
	}
	predCounts := save(prefix + "_counts.json", "sd_counts")
	save(prefix + "_new.json", "sd_new")
	actualCounts := simulator.SaveGTData(sd.GetCount, cells, start, end, RecordInterval, "gt_counts.json")

	// for gt new data, clear the seen, otherwise everything is seen already
	sd.Seen = make(map[string]bool)
	simulator.SaveGTData(sd.GetNew, cells, start, end, RecordInterval, "gt_new.json")

	countsToOpen := func(m map[string]map[int]int) map[string]map[int]int {
		open := make(map[string]map[int]int)
		for cell, max := range maxes {
			s := fmt.Sprintf("%d %d", cell[0], cell[1])
			open[s] = make(map[int]int)
			for interval := range m[s] {
				if m[s][interval] < max {
					open[s][interval] = 1
				} else {
					open[s][interval] = 0
				}
			}
		}
		return open
	}

	predOpen := countsToOpen(predCounts)
	actualOpen := countsToOpen(actualCounts)
	saveMap(prefix + "_open.json", predOpen)
	saveMap("gt_open.json", actualOpen)

	/*dbDriver := pipeline.NewDatabaseDriver(db)
	for _, md := range driver.DFs["sd_counts"].MatrixData {
		var mdCopy pipeline.MatrixData
		mdCopy = *md
		dbDriver.AddMatrixData("sd_counts", &mdCopy)
	}*/
}
