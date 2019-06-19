package main

import (
	//"github.com/mitroadmaps/gomapinfer/common"
	"github.com/mitroadmaps/gomapinfer/image"
	"./golib"
)

const GridSize int = 8

func main() {
	db := golib.NewDatabase()
	//crosswalks := image.ReadImage("crosswalks.png")
	crosswalks := image.ReadImage("cycling-lanes.png")
	matrix := make(map[[2]int]bool)
	// go through all crosswalk pixels at set associated grid cell
	for i := 0; i < len(crosswalks); i++ {
		for j := 0; j < len(crosswalks[i]); j++ {
			if crosswalks[i][j][0] > 128 {
				continue
			}
			matrix[[2]int{i/GridSize, j/GridSize}] = true
		}
	}
	// dilate matrix by 2 cells
	/*for cell := range matrix {
		for i := -1; i <= 1; i++ {
			for j := -1; j <= 1; j++ {
				matrix[[2]int{cell[0]+i, cell[1]+j}] = true
			}
		}
	}*/
	// insert into db
	for i := 0; i < 750; i++ {
		for j := 0; j < 750; j++ {
			var val int
			if matrix[[2]int{i, j}] {
				val = 1
			} else {
				val = 0
				continue
			}
			//db.Exec("INSERT INTO matrix_data (dataframe, time, i, j, val, metadata) VALUES ('crosswalks', '2018-01-01 00:00:00', ?, ?, ?, '')", i, j, val)
			db.Exec("INSERT INTO matrix_data (dataframe, time, i, j, val, metadata) VALUES ('cycling_lanes', '2018-01-01 00:00:00', ?, ?, ?, '')", i, j, val)
		}
	}
}
