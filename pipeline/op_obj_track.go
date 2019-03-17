package main

import (
	"github.com/mitroadmaps/gomapinfer/common"
	goslgraph "github.com/cpmech/gosl/graph"

	"fmt"
	"time"
)

func MakeObjTrackOperator(op *Operator) {
	sequences := getUnterminatedSequences(op.Name)
	op.DetFunc = func(frame *Frame, detections []*Detection) {
		if Debug {
			fmt.Printf("[%s] matching %d detections with %d active sequences\n", op.Name, len(detections), len(sequences))
		}

		detectionMap := make(map[int]*Detection)
		for _, detection := range detections {
			detectionMap[detection.ID] = detection
		}
		matches := hungarianMatcher(sequences, detectionMap)
		for seqID, detection := range matches {
			sequences[seqID].AddMember(detection, "", frame.Time)
		}

		// new sequences for unmatched detections
		for _, detection := range detectionMap {
			seq := NewSequence(op.Name)
			seq.AddMember(detection, "", frame.Time)
			sequences[seq.ID] = seq
		}

		// terminate old sequences, also create list to pass to children
		var sequenceList []*Sequence
		for _, seq := range sequences {
			sequenceList = append(sequenceList, seq)
			lastTime := seq.Members[len(seq.Members)-1].Detection.Time
			if frame.Time.Sub(lastTime) < 2*time.Second {
				continue
			}
			seq.Terminate(frame.Time)
			delete(sequences, seq.ID)
		}

		for _, child := range op.Children {
			child.SeqFunc(frame, sequenceList)
		}
	}
}

func getIoU(a common.Rectangle, b common.Rectangle) float64 {
	intersectRect := a.Intersection(b)
	intersectArea := intersectRect.Area()
	unionArea := a.Area() + b.Area() - intersectArea
	return intersectArea / unionArea
}

// Returns map from sequences to detection that should be added corresponding to that sequence.
// Also removes detections from the map that matched with a sequence.
func hungarianMatcher(sequences map[int]*Sequence, detections map[int]*Detection) map[int]*Detection {
	if len(sequences) == 0 || len(detections) == 0 {
		return nil
	}

	var sequenceList []*Sequence
	for _, seq := range sequences {
		sequenceList = append(sequenceList, seq)
	}
	var detectionList []*Detection
	for _, detection := range detections {
		detectionList = append(detectionList, detection)
	}

	// create cost matrix for hungarian algorithm
	// rows: existing sequences (sequenceList)
	// cols: current detections (detectionList)
	// values: 1-IoU if overlap is non-zero, or 10 otherwise
	costMatrix := make([][]float64, len(sequenceList))
	for i, seq := range sequenceList {
		costMatrix[i] = make([]float64, len(detectionList))
		seqRect := seq.Members[len(seq.Members) - 1].Detection.Polygon.Bounds()

		for j, detection := range detectionList {
			curRect := detection.Polygon.Bounds()
			iou := getIoU(seqRect, curRect)
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
	munkres.Init(len(sequenceList), len(detectionList))
	munkres.SetCostMatrix(costMatrix)
	munkres.Run()

	matches := make(map[int]*Detection)
	for i, j := range munkres.Links {
		seq := sequenceList[i]
		if j < 0 || costMatrix[i][j] > 0.9 {
			continue
		}
		detection := detectionList[j]
		matches[seq.ID] = detection
		delete(detections, detection.ID)
	}
	return matches
}
