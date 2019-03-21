package main

import (
	//"github.com/mitroadmaps/gomapinfer/common"
	"github.com/mitroadmaps/gomapinfer/image"
	"./golib"
)

func main() {
	db := golib.NewDatabase()
	ortho := image.ReadImage("ortho_orig.jpg")
	matrix := make(map[[2]int]int)
	rows := db.Query("SELECT i, j, val FROM matrix_data WHERE dataframe = 'error_rate' ORDER BY id")
	for rows.Next() {
		var i, j, val int
		rows.Scan(&i, &j, &val)
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
	}
	normalize := func(val int) uint8 {
		norm := float64(val - min) / float64(max - min)
		return uint8(norm * 255)
	}
	for cell, val := range matrix {
		normVal := normalize(val)
		center := [2]int{
			cell[0]*256 + 128,
			cell[1]*256 + 128,
		}
		image.DrawRect(ortho, center[0], center[1], 128, [3]uint8{normVal, normVal, normVal})
	}
	image.WriteImage("out.jpg", ortho)
}
