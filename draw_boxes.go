package main

import (
	"../../../gomapinfer/common"
	"../../../gomapinfer/image"
	"../../../drones/dronevid/lib"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
)

const NumFrames int = 500
const OrthoPath string = "/home/favyen/drone-data/mar05/ortho_sm.jpg"
const FramePath string = "/home/favyen/drone-data/mar05/frames/"

func getIstr(i int) string {
	istr := strconv.Itoa(i)
	for len(istr) < 6 {
		istr = "0" + istr
	}
	return istr
}

func main() {
	bytes, err := ioutil.ReadFile("frame_boxes.json")
	if err != nil {
		panic(err)
	}
	var frameBoxes [][]lib.BoundingBox
	if err := json.Unmarshal(bytes, &frameBoxes); err != nil {
		panic(err)
	}
	bytes, err = ioutil.ReadFile("frame_polygons.json")
	if err != nil {
		panic(err)
	}
	var framePolygons [][][4][2]float64
	if err := json.Unmarshal(bytes, &framePolygons); err != nil {
		panic(err)
	}
	ortho := image.ReadImage(OrthoPath)
	for frameIdx := 2; frameIdx <= NumFrames; frameIdx++ {
		frame := image.ReadImage(FramePath + getIstr(frameIdx) + ".jpg")
		for _, box := range frameBoxes[frameIdx] {
			segments := [][4]int{
				[4]int{box.Left/2, box.Top/2, box.Left/2, box.Bottom/2},
				[4]int{box.Right/2, box.Top/2, box.Right/2, box.Bottom/2},
				[4]int{box.Left/2, box.Top/2, box.Right/2, box.Top/2},
				[4]int{box.Left/2, box.Bottom/2, box.Right/2, box.Bottom/2},
			}
			for _, segment := range segments {
				sx, sy, ex, ey := segment[0], segment[1], segment[2], segment[3]
				for _, p := range common.DrawLineOnCells(sx, sy, ex, ey, len(frame), len(frame[0])) {
					frame[p[0]][p[1]] = [3]uint8{255, 255, 0}
				}
			}
		}
		rect := common.EmptyRectangle
		for _, polygon := range framePolygons[frameIdx] {
			for _, p := range polygon {
				rect = rect.Extend(common.Point{p[0], p[1]})
			}
		}
		rect = rect.AddTol(64)
		startX, startY, endX, endY := int(rect.Min.X), int(rect.Min.Y), int(rect.Max.X), int(rect.Max.Y)
		if startX < 0 {
			startX = 0
		}
		if endX >= len(ortho) {
			endX = len(ortho)-1
		}
		if startY < 0 {
			startY = 0
		}
		if endY >= len(ortho[1]) {
			endY = len(ortho[1])-1
		}
		orthoCrop := image.Crop(ortho, startX, startY, endX, endY)
		for _, polygon := range framePolygons[frameIdx] {
			var segments [][4]int
			for i := range polygon {
				p1 := polygon[i]
				p2 := polygon[(i+1)%len(polygon)]
				segments = append(segments, [4]int{int(p1[0]) - startX, int(p1[1]) - startY, int(p2[0]) - startX, int(p2[1]) - startY})
			}
			for _, segment := range segments {
				sx, sy, ex, ey := segment[0], segment[1], segment[2], segment[3]
				for _, p := range common.DrawLineOnCells(sx, sy, ex, ey, len(orthoCrop), len(orthoCrop[0])) {
					orthoCrop[p[0]][p[1]] = [3]uint8{255, 255, 0}
				}
			}
		}
		image.WriteImage(fmt.Sprintf("out/%d.frame.png", frameIdx), frame)
		image.WriteImage(fmt.Sprintf("out/%d.ortho.png", frameIdx), orthoCrop)
	}
}
