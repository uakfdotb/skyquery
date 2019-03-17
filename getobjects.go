package main

import (
	"../../../gomapinfer/common"

	goslgraph "github.com/cpmech/gosl/graph"

	"encoding/json"
	"io/ioutil"
	"log"
)

type Object struct {
	ID int
	StartFrame int
	Rectangles []common.Rectangle
}

func main() {
	bytes, err := ioutil.ReadFile("frame_polygons.json")
	if err != nil {
		panic(err)
	}
	var framePolygons [][][4][2]float64
	if err := json.Unmarshal(bytes, &framePolygons); err != nil {
		panic(err)
	}
	frameRectangles := make([][]common.Rectangle, len(framePolygons))
	for frameIdx := range framePolygons {
		for _, polygon := range framePolygons[frameIdx] {
			rect := common.EmptyRectangle
			for _, p := range polygon {
				rect = rect.Extend(common.Point{p[0], p[1]})
			}
			frameRectangles[frameIdx] = append(frameRectangles[frameIdx], rect)
		}
	}
	var objects []*Object
	activeObjects := make(map[int]*Object)
	for frameIdx, rects := range frameRectangles {
		// 1) match active objects to current boxes
		// 1a) if match fails, remove object from active set
		// 1b) if match succeeds, remove the box from candidate match set
		// 2) add remaining boxes as new objects if confidence is high enough
		rectMap := make(map[int]common.Rectangle)
		for i, rect := range rects {
			rectMap[i] = rect
		}
		hungarianMatcher(frameIdx, activeObjects, rectMap)
		for _, rect := range rectMap {
			object := &Object{len(objects), frameIdx, []common.Rectangle{rect}}
			objects = append(objects, object)
			activeObjects[object.ID] = object
		}
	}
	func() {
		bytes, err := json.Marshal(objects)
		if err != nil {
			panic(err)
		}
		if err := ioutil.WriteFile("objects.json", bytes, 0644); err != nil {
			panic(err)
		}
	}()
}

func getIoU(a common.Rectangle, b common.Rectangle) float64 {
	intersectRect := a.Intersection(b)
	intersectArea := intersectRect.Area()
	unionArea := a.Area() + b.Area() - intersectArea
	return intersectArea / unionArea
}

func hungarianMatcher(frameIdx int, activeObjects map[int]*Object, rectMap map[int]common.Rectangle) {
	if len(activeObjects) == 0 || len(rectMap) == 0 {
		return
	}

	var activeObjectList []*Object
	for _, object := range activeObjects {
		activeObjectList = append(activeObjectList, object)
	}
	type RectID struct {
		ID int
		Rect common.Rectangle
	}
	var rectList []RectID
	for id, rect := range rectMap {
		rectList = append(rectList, RectID{id, rect})
	}

	// create cost matrix for hungarian algorithm
	// rows: existing objects (activeObjects)
	// cols: current detections (rectMap)
	// values: 1-IoU if overlap is non-zero, or 10 otherwise
	costMatrix := make([][]float64, len(activeObjectList))
	for i, object := range activeObjectList {
		costMatrix[i] = make([]float64, len(rectList))
		objRect := object.Rectangles[len(object.Rectangles) - 1]

		for j, rectID := range rectList {
			curRect := rectID.Rect
			iou := getIoU(objRect, curRect)
			var cost float64
			if iou > 0.99 {
				cost = 0.01
			} else if iou > 0.1 {
				cost = 1 - iou
			} else {
				cost = 10
			}
			costMatrix[i][j] = cost
		}
	}

	munkres := &goslgraph.Munkres{}
	munkres.Init(len(activeObjectList), len(rectList))
	munkres.SetCostMatrix(costMatrix)
	munkres.Run()

	for i, j := range munkres.Links {
		object := activeObjectList[i]
		if j < 0 || costMatrix[i][j] > 0.9 {
			lastFrameIdx := object.StartFrame + len(object.Rectangles) - 1
			if frameIdx - lastFrameIdx >= 5 {
				log.Printf("(%d) inactive object, length=%d, lastrect=%v", frameIdx, len(object.Rectangles), object.Rectangles[len(object.Rectangles)-1])
				delete(activeObjects, object.ID)
			}
			continue
		}
		rectID := rectList[j].ID
		matchRect := rectMap[rectID]
		delete(rectMap, rectID)

		lastRect := object.Rectangles[len(object.Rectangles)-1]
		for object.StartFrame + len(object.Rectangles)  < frameIdx {
			object.Rectangles = append(object.Rectangles, lastRect)
		}

		object.Rectangles = append(object.Rectangles, matchRect)
	}
}
