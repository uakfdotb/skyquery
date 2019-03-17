package main

import (
	"github.com/mitroadmaps/gomapinfer/common"
	"github.com/mitroadmaps/gomapinfer/image"
	"./golib"
)

func main() {
	db := golib.NewDatabase()
	ortho := image.ReadImage("ortho_orig.jpg")
	sequences := make(map[int][]common.Rectangle)
	rows := db.Query("SELECT sm.sequence_id, d.polygon FROM sequence_members AS sm, detections AS d WHERE d.id = sm.detection_id ORDER BY sm.id")
	for rows.Next() {
		var sequenceID int
		var polyStr string
		rows.Scan(&sequenceID, &polyStr)
		rect := golib.ParsePolygon(polyStr).Bounds()
		sequences[sequenceID] = append(sequences[sequenceID], rect)
	}
	/*rows := db.Query("SELECT polygon FROM detections WHERE polygon IS NOT NULL AND polygon != ''")
	for rows.Next() {
		var polyStr string
		rows.Scan(&polyStr)
		rect := golib.ParsePolygon(polyStr).Bounds()
		sequences[len(sequences)] = []common.Rectangle{rect}
	}*/
	// draw points at every detection
	/*for _, seq := range sequences {
		if len(seq) < 5 {
			continue
		}
		for _, rect := range seq {
			center := rect.Center()
			image.DrawRect(ortho, int(center.X), int(center.Y), 1, [3]uint8{255, 255, 0})
		}
	}*/
	// draw trajectories
	for _, seq := range sequences {
		if len(seq) < 5 {
			continue
		}
		prevCenter := seq[0].Center()
		for _, rect := range seq[1:] {
			curCenter := rect.Center()
			for _, p := range common.DrawLineOnCells(int(prevCenter.X), int(prevCenter.Y), int(curCenter.X), int(curCenter.Y), len(ortho), len(ortho[0])) {
				image.DrawRect(ortho, p[0], p[1], 0, [3]uint8{255, 255, 0})
			}
			prevCenter = curCenter
		}
	}
	// draw detections based on stopped vs not stopped
	/*for _, object := range objects {
		if len(object.Rectangles) < 5 {
			continue
		}
		firstRect := object.Rectangles[0]
		lastRect := object.Rectangles[len(object.Rectangles) - 1]
		d := firstRect.Center().Distance(lastRect.Center())
		stopped := d < 70
		var color [3]uint8
		if stopped {
			color = [3]uint8{255, 0, 0}
		} else {
			color = [3]uint8{255, 255, 0}
		}
		for _, rect := range object.Rectangles {
			center := rect.Center()
			image.DrawRect(ortho, int(center.X), int(center.Y), 1, color)
		}
	}*/
	image.WriteImage("out.jpg", ortho)
}
