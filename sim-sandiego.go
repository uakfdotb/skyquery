package main

import (
	"github.com/mitroadmaps/gomapinfer/common"
	"./simulator"
	"./router"
	"./pipeline"

	"github.com/chobie/go-gaussian"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"time"
)

var RecordInterval time.Duration = 15*time.Minute

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
	pipeline.Quiet = true
	pipeline.SetDBName(dbname)
	db := pipeline.NewDatabase()
	driver := pipeline.GetDriver().(*pipeline.InMemoryDriver)

	//db.Exec("DELETE FROM matrix_data")
	//db.Exec("DELETE FROM video_frames")

	start := time.Date(2018, time.May, 7, 11, 0, 0, 0, time.UTC)
	//end := time.Date(2018, time.May, 28, 11, 0, 0, 0, time.UTC)
	end := time.Date(2018, time.July, 2, 11, 0, 0, 0, time.UTC)
	rect := common.Rectangle{
		common.Point{-6000, -12500},
		common.Point{6000, -500},
	}
	sd := simulator.LoadSanDiego(start, end, rect)
	fmt.Printf("bounds: %v\n", sd.Bounds())

	maxes := sd.GetMaxes3()
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
			pipeline.AddMatrixData("error", x, y, 99999999999, "", start.Add(-time.Hour))
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
	for i := 0; i < 1; i++ {
		s.AddDrone()
	}
	predictor := simulator.NewPredictor2(driver, "sd_counts", 96, maxes)
	for s.Time.Before(end) {
		//db.Exec("DELETE FROM matrix_data WHERE dataframe != 'sd_counts' AND time < ? AND val != '99999999999'", s.Time.Add(-2*time.Minute))
		// delete old matrix data
		t := s.Time.Add(-2*time.Hour)
		for name, df := range driver.DFs {
			if name == "sd_counts" || name == "sd_new" || name == "maxes" || name == "predictions" {
				continue
			}
			for _, md := range df.MatrixData {
				if md.Val == 99999999999 || !md.Time.Before(t) {
					continue
				}
				delete(df.MatrixData, md.ID)
			}
		}

		preTime := s.Time
		db.Exec("UPDATE dataframes SET rerun_time = ?", preTime)
		fmt.Println(preTime)
		s.Run(int(15*time.Minute/simulator.TimeStep))
		predictions := predictor.Predict()

		if true { // direct case
			for cell, prediction := range predictions {
				pipeline.AddMatrixData("error", cell[0], cell[1], int(prediction.Stddev*100), "", preTime)
				pipeline.AddMatrixData("predictions", cell[0], cell[1], int(prediction.Val), "", preTime)

				/*var val int
				if prediction.Val < 0.5 {
					val = 0
				} else {
					val = 1
				}
				pipeline.AddMatrixData("predictions", cell[0], cell[1], val, "", preTime)*/
			}
		} else { // thresholded case
			for cell, prediction := range predictions {
				var pOpen float64
				if prediction.Stddev == 0 {
					if prediction.Val > float64(maxes[cell]) - 0.5 {
						pOpen = 0
					} else {
						pOpen = 1
					}
				} else {
					dist := gaussian.NewGaussian(prediction.Val, prediction.Stddev*prediction.Stddev)
					pOpen = dist.Cdf(float64(maxes[cell])-0.5)
				}
				stddev := math.Sqrt(pOpen * (1 - pOpen))
				var val int
				if pOpen > 0.5 {
					val = 1
				} else {
					val = 0
				}
				//fmt.Printf("val=%v, stddev=%v, p=%v, out-stddev=%v, out-value=%v\n", prediction.Val, prediction.Stddev, pOpen, stddev, val)
				pipeline.AddMatrixData("error", cell[0], cell[1], int(stddev*100), "", preTime)
				//pipeline.AddMatrixData("error", cell[0], cell[1], int(prediction.Stddev*100), "", preTime)
				pipeline.AddMatrixData("predictions", cell[0], cell[1], val, "", preTime)
			}
		}

		//pipeline.RunPipeline()
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
	save(prefix + "_predictions.json", "predictions")
	save(prefix + "_new.json", "sd_new")
	//actualCounts := simulator.SaveGTData(sd.GetCount, cells, start, end, RecordInterval, "gt_counts_8week50.json")

	// for gt new data, clear the seen, otherwise everything is seen already
	sd.Seen = make(map[string]bool)
	//simulator.SaveGTData(sd.GetNew, cells, start, end, RecordInterval, "gt_new_8week50.json")

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

	//predOpen := countsToOpen(predictions)
	predOpen := countsToOpen(predCounts)
	//actualOpen := countsToOpen(actualCounts)
	saveMap(prefix + "_open.json", predOpen)
	//saveMap("gt_open_8week50.json", actualOpen)

	/*dbDriver := pipeline.NewDatabaseDriver(db)
	for _, md := range driver.DFs["sd_counts"].MatrixData {
		var mdCopy pipeline.MatrixData
		mdCopy = *md
		dbDriver.AddMatrixData("sd_counts", &mdCopy)
	}*/
}
