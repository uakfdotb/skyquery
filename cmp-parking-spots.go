package main

import (
	"github.com/mitroadmaps/gomapinfer/common"
	"github.com/mitroadmaps/gomapinfer/image"
	"./pipeline"

	goslgraph "github.com/cpmech/gosl/graph"

	"encoding/json"
	"fmt"
	"io/ioutil"
)

const GridSize int = 8

func main() {
	db := pipeline.NewDatabase()
	ortho := image.ReadImage("apr02-ortho.jpg")
	outjpg := image.ReadImage("parking-mask.jpg")
	idx := common.NewGridIndex(64)
	var points []common.Point
	idxContains := func(query common.Point) bool {
		for _, id := range idx.Search(query.Bounds().AddTol(40)) {
			if query.Distance(points[id]) < 40 {
				return true
			}
		}
		return false
	}
	rows := db.Query("SELECT id FROM sequences WHERE dataframe = 'parked_cars'")
	for rows.Next() {
		var id int
		rows.Scan(&id)
		var xlist, ylist []int
		detectionRows := db.Query("SELECT d.polygon FROM detections AS d, sequence_members AS sm WHERE sm.sequence_id = ? AND sm.detection_id = d.id", id)
		for detectionRows.Next() {
			var s string
			detectionRows.Scan(&s)
			polygon := pipeline.ParsePolygon(s)
			var sum common.Point
			for _, p := range polygon {
				sum = sum.Add(p)
			}
			avg := sum.Scale(1/float64(len(polygon)))
			xlist = append(xlist, int(avg.X))
			ylist = append(ylist, int(avg.Y))
		}
		p := common.Point{
			float64(pipeline.IntSliceAvg(xlist)),
			float64(pipeline.IntSliceAvg(ylist)),
		}
		if outjpg[int(p.X)][int(p.Y)][0] > 128 {
			continue
		}
		if idxContains(p) {
			continue
		}
		points = append(points, p)
		idx.Insert(len(points) - 1, p.Bounds())
	}

	var jsonData [][2]int
	bytes, err := ioutil.ReadFile("cambridge-output.json")
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(bytes, &jsonData); err != nil {
		panic(err)
	}
	var gt []common.Point
	for _, p := range jsonData {
		gt = append(gt, common.Point{float64(p[0]), float64(p[1])})
	}

	costMatrix := make([][]float64, len(gt))
	for i, p := range gt {
		costMatrix[i] = make([]float64, len(points))

		for j, candidate := range points {
			d := p.Distance(candidate)
			var cost float64
			if d < 100 {
				cost = 1
			} else {
				cost = 1000
			}
			costMatrix[i][j] = cost
		}
	}

	munkres := &goslgraph.Munkres{}
	munkres.Init(len(gt), len(points))
	munkres.SetCostMatrix(costMatrix)
	munkres.Run()

	matchedIDs := make(map[int]bool)
	for i, j := range munkres.Links {
		if j < 0 || costMatrix[i][j] > 1 {
			continue
		}
		matchedIDs[j] = true
	}
	match := len(matchedIDs)

	/*var match int
	matchedIDs := make(map[int]bool)
	for _, p := range gt {
		var bestDistance float64 = 50
		var bestIdx int = -1
		for i, candidate := range points {
			if matchedIDs[i] {
				continue
			}
			d := p.Distance(candidate)
			if d < bestDistance {
				bestDistance = d
				bestIdx = i
			}
		}
		if bestIdx == -1 {
			continue
		}
		matchedIDs[bestIdx] = true
		match++
	}*/
	fmt.Printf("precision=%v, recall=%v\n", float64(match)/float64(len(points)), float64(match)/float64(len(gt)))

	for i, p := range points {
		if matchedIDs[i] {
			image.DrawRect(ortho, int(p.X), int(p.Y), 20, [3]uint8{0, 0, 255})
		} else {
			image.DrawRect(ortho, int(p.X), int(p.Y), 20, [3]uint8{255, 0, 0})
		}
	}
	for _, p := range gt {
		image.DrawRect(ortho, int(p.X), int(p.Y), 20, [3]uint8{0, 255, 0})
	}
	image.WriteImage("out.jpg", ortho)
}
