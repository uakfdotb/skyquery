package main

import (
	"github.com/mitroadmaps/gomapinfer/common"
	"github.com/mitroadmaps/gomapinfer/image"
	"./golib"

	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

func floodfill(bin [][]bool, x int, y int) common.Rectangle {
	if x < 0 || x >= len(bin) || y < 0 || y >= len(bin[x]) || !bin[x][y] {
		return common.EmptyRectangle
	}
	bin[x][y] = false
	rect := common.Point{float64(x), float64(y)}.Bounds()
	for i := -1; i <= 1; i++ {
		for j := -1; j <= 1; j++ {
			r := floodfill(bin, x + i, y + j)
			if r == common.EmptyRectangle {
				continue
			}
			rect = rect.ExtendRect(r)
		}
	}
	return rect
}

func getDetections(bin [][]bool) []common.Rectangle {
	var detections []common.Rectangle
	for i := range bin {
		for j := range bin[i] {
			if !bin[i][j] {
				continue
			}
			rect := floodfill(bin, i, j)
			if rect.Lengths().X >= 8 && rect.Lengths().Y >= 8 {
				detections = append(detections, rect)
			}
		}
	}
	return detections
}

func main() {
	db := golib.NewDatabase()

	videoID := os.Args[1]
	dir := os.Args[2]
	dataframe := os.Args[3]

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, fi := range files {
		im := image.ReadGrayImage(dir + fi.Name())
		bin := image.Binarize(im, 200)
		detections := getDetections(bin)
		if len(detections) == 0 {
			continue
		}
		frameIdx := strings.Split(fi.Name(), ".png")[0]
		var frameID int
		var t time.Time
		db.QueryRow("SELECT id, time FROM video_frames WHERE video_id = ? AND idx = ?", videoID, frameIdx).Scan(&frameID, &t)
		for _, detection := range detections {
			var points []string
			for _, p := range detection.ToPolygon() {
				points = append(points, fmt.Sprintf("%v,%v", int(p.X*4), int(p.Y*4)))
			}
			polygon := strings.Join(points, " ")
			db.Exec("INSERT INTO detections (dataframe, time, frame_polygon, frame_id) VALUES (?, ?, ?, ?)", dataframe, t, polygon, frameID)
		}
	}
}
