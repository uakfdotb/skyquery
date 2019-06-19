package main

import (
	"github.com/mitroadmaps/gomapinfer/common"
	"github.com/mitroadmaps/gomapinfer/image"
	"./pipeline"
)

func main() {
	ortho := image.ReadImage("apr02-ortho.jpg")

	/*sequences := make(map[int][]common.Rectangle)
	db := pipeline.NewDatabase()
	rows := db.Query("SELECT polygon FROM detections, video_frames WHERE polygon IS NOT NULL AND polygon != '' AND detections.frame_id = video_frames.id AND video_frames.video_id IN (2, 3, 4)")
	for rows.Next() {
		var polyStr string
		rows.Scan(&polyStr)
		rect := pipeline.ParsePolygon(polyStr).Bounds()
		sequences[len(sequences)] = []common.Rectangle{rect}
	}
	// draw points at every detection
	for _, seq := range sequences {
		for _, rect := range seq {
			center := rect.Center()
			image.DrawRect(ortho, int(center.X), int(center.Y), 1, [3]uint8{255, 255, 0})
		}
	}*/

	// draw trajectories
	sequences := pipeline.GetSequences("hazards")
	for _, seq := range sequences {
		prevCenter := seq.Members[0].Detection.Polygon.Bounds().Center()
		for _, member := range seq.Members[1:] {
			curCenter := member.Detection.Polygon.Bounds().Center()
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
