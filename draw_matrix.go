package main

import (
	//"github.com/mitroadmaps/gomapinfer/common"
	"github.com/mitroadmaps/gomapinfer/image"
	"./golib"

	"fmt"
)

const GridSize int = 8

func main() {
	db := golib.NewDatabase()
	ortho := image.ReadImage("apr02-ortho.jpg")
	matrix := make(map[[2]int]int)
	rows := db.Query("SELECT i, j, val FROM matrix_data WHERE dataframe = 'parked_counts' ORDER BY id")
	for i := 0; i < 200; i++ {
		for j := 0; j < 200; j++ {
			matrix[[2]int{i, j}] = 0
		}
	}
	for rows.Next() {
		var i, j, val int
		rows.Scan(&i, &j, &val)
		/*if val > 0 {
			val += 20
		}*/
		matrix[[2]int{i, j}] = val
	}
	var min, max int
	for _, val := range matrix {
		min = val
		max = val
		break
	}
	for _, val := range matrix {
		if val < min {
			min = val
		}
		if val > max {
			max = val
		}
		if val > 0 {
			fmt.Println(val)
		}
	}
	min = 0
	max = 1
	normalize := func(val int) uint8 {
		norm := float64(val - min) / float64(max - min)
		if norm < 0 {
			norm = 0
		} else if norm > 1 {
			norm = 1
		}
		return uint8(norm * 255)
	}
	for cell, val := range matrix {
		normVal := normalize(val)
		center := [2]int{
			cell[0]*GridSize + GridSize/2,
			cell[1]*GridSize + GridSize/2,
		}
		//image.DrawRect(ortho, center[0], center[1], GridSize/2, [3]uint8{normVal, normVal, normVal})
		//image.DrawTransparent(ortho, center[0], center[1], GridSize/2, [3]int{int(normVal), -1, -1})
		if normVal > 200 {
			image.DrawRect(ortho, center[0], center[1], GridSize/2, [3]uint8{255, 0, 0})
		}
	}
	image.WriteImage("out.jpg", ortho)
}
