package pipeline

import (
	"fmt"
	"time"
)

type LoadFunc func(frame *Frame) ParentData

type ParentData struct {
	Detections [][]*Detection
	Sequences [][]*Sequence
	MatrixData [][]*MatrixData
}

func (pd ParentData) Append(other ParentData) ParentData {
	return ParentData{
		append(pd.Detections, other.Detections...),
		append(pd.Sequences, other.Sequences...),
		append(pd.MatrixData, other.MatrixData...),
	}
}

type Operator struct {
	Name string
	RerunTime time.Time
	ChildRerunTime time.Time

	// Returns LoadFunc to feed this operator into a child.
	Loader func(frames []*Frame) LoadFunc

	// Prepare to execute this operator starting from the specified frame.
	// If this operator has LookBehind, the specified frame is based on the
	// parent's rerun-time. But Func() will be called on earlier frames.
	InitFunc func(frame *Frame)

	// Execute the oeprator on this frame.
	Func func(frame *Frame, parentData ParentData)

	// Convenience functions if you don't want to implement Func, for operators
	// that simply accept detections only or sequences only.
	// DefaultFunc will pass parentData to these if they are set.
	DetFunc func(frame *Frame, detections []*Detection)
	SeqFunc func(frame *Frame, sequences []*Sequence)
	MatFunc func(frame *Frame, matrixData []*MatrixData)

	Parents []*Operator
	Children []*Operator

	// Look behind duration.
	// Some operators require seeing frames and/or parent data from before the
	// parent's rerun-time, in case their Func() makes decisions based on previous
	// frames. This duration is how long the operator wants to see into the past.
	LookBehind time.Duration
}

func (op *Operator) updateChildRerunTime(t time.Time) {
	if t.Before(op.ChildRerunTime) {
		op.ChildRerunTime = t
	}
}

func (op *Operator) DefaultFunc(frame *Frame, parentData ParentData) {
	// try to pass into DetFunc or SeqFunc
	if op.DetFunc != nil {
		op.DetFunc(frame, parentData.Detections[0])
	} else if op.SeqFunc != nil {
		op.SeqFunc(frame, parentData.Sequences[0])
	} else if op.MatFunc != nil {
		op.MatFunc(frame, parentData.MatrixData[0])
	} else {
		op.Func(frame, parentData)
	}
}

func (op *Operator) DetectionLoader(frames []*Frame) LoadFunc {
	detections := GetDetectionsAfter(op.Name, frames[0].Time)
	frameDetections := make(map[int][]*Detection)
	for _, detection := range detections {
		frameDetections[detection.FrameID] = append(frameDetections[detection.FrameID], detection)
	}
	return func(frame *Frame) ParentData {
		if Debug {
			fmt.Printf("[%s] load frame %d/%d: feeding %d detections\n", op.Name, frame.VideoID, frame.Idx, len(frameDetections[frame.ID]))
		}
		return ParentData{
			Detections: [][]*Detection{frameDetections[frame.ID]},
		}
	}
}

func (op *Operator) SequenceLoader(frames []*Frame) LoadFunc {
	sequences := GetSequencesAfter(op.Name, frames[0].Time)
	fmt.Printf("[%s] preloaded %d sequences before %v\n", op.Name, len(sequences), frames[0].Time)
	frameSeqs := make(map[int][]*Sequence)
	activeSeqs := make(map[int]*Sequence)
	for _, seq := range sequences {
		frameID := seq.Members[0].Detection.FrameID
		t := seq.Members[0].Detection.Time
		if t.After(frames[0].Time) {
			frameSeqs[frameID] = append(frameSeqs[frameID], seq)
		} else {
			activeSeqs[seq.ID] = seq
		}
	}
	return func(frame *Frame) ParentData {
		for _, seq := range frameSeqs[frame.ID] {
			activeSeqs[seq.ID] = seq
		}
		for _, seq := range activeSeqs {
			endTime := seq.Members[len(seq.Members)-1].Detection.Time
			if endTime.Before(frame.Time) {
				delete(activeSeqs, seq.ID)
			}
		}
		var seqList []*Sequence
		for _, seq := range activeSeqs {
			seqList = append(seqList, seq)
		}
		if Debug {
			fmt.Printf("[%s] load frame %d/%d: feeding %d sequences\n", op.Name, frame.VideoID, frame.Idx, len(seqList))
		}
		return ParentData{
			Sequences: [][]*Sequence{seqList},
		}
	}
}

func (op *Operator) MatrixLoader(frames []*Frame) LoadFunc {
	// send matrix data on the first frame satisfying frame.time >= md.time
	matrixDatas := driver.GetMatrixDatasAfter(op.Name, frames[0].Time)
	mdMap := make(map[int]*MatrixData)
	for _, md := range matrixDatas {
		mdMap[md.ID] = md
	}
	return func(frame *Frame) ParentData {
		var mds []*MatrixData
		for _, md := range mdMap {
			if frame.Time.Before(md.Time) {
				continue
			}
			mds = append(mds, md)
			delete(mdMap, md.ID)
		}
		if Debug {
			fmt.Printf("[%s] load frame %d/%d: feeding %d matrix datas\n", op.Name, frame.VideoID, frame.Idx, len(mds))
		}
		return ParentData{
			MatrixData: [][]*MatrixData{mds},
		}
	}
}

// Called when operator is done updating based on parents.
// We reset our rerun time and update children rerun time to be less than the ChildRerunTime.
func (op *Operator) PropogateRerunTime() {
	db.Exec("UPDATE dataframes SET rerun_time = NOW() WHERE name = ?", op.Name)
	if op.RerunTime.Before(op.ChildRerunTime) {
		op.ChildRerunTime = op.RerunTime
	}
	for _, child := range op.Children {
		if op.ChildRerunTime.Before(child.RerunTime) {
			child.RerunTime = op.ChildRerunTime
			db.Exec("UPDATE dataframes SET rerun_time = ? WHERE name = ?", child.RerunTime, child.Name)
		}
	}
}

// Feed data from parent operators into this operator.
func (op *Operator) Execute() {
	// rerun time is minimum child-rerun-time of our parents
	// if we have look-behind, then we add in frames from before rerun time
	startTime := op.RerunTime
	if op.LookBehind > 0 {
		startTime = startTime.Add(-op.LookBehind)
	}
	frames := driver.GetFramesStartingFrom(startTime)

	// might get no frames if there is no work to do!
	if len(frames) == 0 {
		fmt.Printf("skip %s because no frames to process\n", op.Name)
		return
	}

	// filter out frames where the bounds changes too much from previous frame
	// TODO: move this to match-sift.py
	var goodFrames []*Frame
	goodFrames = append(goodFrames, frames[0])
	for i := 1; i < len(frames); i++ {
		rect1 := frames[i-1].Bounds.Bounds()
		rect2 := frames[i].Bounds.Bounds()
		if getIoU(rect1, rect2) < 0.9 || rect2.Area() > 1500*1500 {
			continue
		}
		bad := false
		segments := frames[i].Bounds.Segments()
		for j := range segments  {
			seg1 := segments[j]
			seg2 := segments[(j + 1) % len(segments)]
			angle := seg1.Vector().AngleTo(seg2.Vector())
			if angle < 1.45 || angle > 1.7 {
				bad = true
			}
		}
		if bad {
			continue
		}
		goodFrames = append(goodFrames, frames[i])
	}
	//frames = goodFrames

	// identify the rerun frame, i.e., first frame after rerunTime
	var rerunFrame *Frame
	for _, frame := range frames {
		if !frame.Time.Before(op.RerunTime) {
			rerunFrame = frame
			break
		}
	}

	if rerunFrame == nil {
		fmt.Printf("skip %s because no rerun frame found\n", op.Name)
		return
	}

	if op.InitFunc != nil {
		op.InitFunc(rerunFrame)
	}

	// collect load funcs from parents
	var loadFuncs []LoadFunc
	for _, parent := range op.Parents {
		loadFuncs = append(loadFuncs, parent.Loader(frames))
	}

	for _, frame := range frames {
		var pd ParentData
		for _, loadFunc := range loadFuncs {
			pd = pd.Append(loadFunc(frame))
		}
		op.DefaultFunc(frame, pd)
	}

	// update rerun times
	op.PropogateRerunTime()
}
