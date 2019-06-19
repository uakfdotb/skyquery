package main

import (
	"github.com/mitroadmaps/gomapinfer/common"
	"github.com/mitroadmaps/gomapinfer/image"
	"./golib"

	"io/ioutil"
	"strconv"
	"strings"
)

func main() {
	db := golib.NewDatabase()
	detections := make(map[int][]common.Polygon)
	rows := db.Query(
		"SELECT detections.frame_polygon, video_frames.idx FROM detections, video_frames WHERE detections.frame_id = video_frames.id AND dataframe = 'cars' AND video_id = 2",// AND detections.polygon IS NOT NULL",
	)
	for rows.Next() {
		var polyStr string
		var frameIdx int
		rows.Scan(&polyStr, &frameIdx)
		poly := golib.ParsePolygon(polyStr)
		detections[frameIdx] = append(detections[frameIdx], poly)
	}
	files, err := ioutil.ReadDir("frames/2/")
	if err != nil {
		panic(err)
	}
	for _, fi := range files {
		frameIdx, _ := strconv.Atoi(strings.Split(fi.Name(), ".jpg")[0])
		if frameIdx < 3470-10 || frameIdx > 3470+10 {
			continue
		}
		im := image.ReadImage("frames/2/" + fi.Name())
		for _, poly := range detections[frameIdx] {
			for _, segment := range poly.Segments() {
				for _, p := range common.DrawLineOnCells(int(segment.Start.X), int(segment.Start.Y), int(segment.End.X), int(segment.End.Y), len(im), len(im[0])) {
					image.DrawRect(im, p[0], p[1], 1, [3]uint8{255, 255, 0})
				}
			}
		}
		image.WriteImage("out/" + fi.Name(), im)
	}
}
