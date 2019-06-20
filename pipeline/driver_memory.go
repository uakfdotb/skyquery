package pipeline

import (
	"github.com/mitroadmaps/gomapinfer/common"

	"time"
)

type inMemoryMetadata struct {
	t time.Time
	v string
}

type InMemoryDF struct {
	MatrixData map[int]*MatrixData
	Sequences map[int]*Sequence
	Metadata map[int][]inMemoryMetadata
	Counter int
}

type InMemoryDriver struct {
	DFs map[string]*InMemoryDF

	// ordered by time
	Frames []*Frame
}

func NewInMemoryDriver() Driver {
	return &InMemoryDriver{
		DFs: make(map[string]*InMemoryDF),
	}
}

func (d *InMemoryDriver) ensure(dataframe string) *InMemoryDF {
	if d.DFs[dataframe] == nil {
		d.DFs[dataframe] = &InMemoryDF{
			MatrixData: make(map[int]*MatrixData),
			Sequences: make(map[int]*Sequence),
			Metadata: make(map[int][]inMemoryMetadata),
		}
	}
	return d.DFs[dataframe]
}

func (d *InMemoryDriver) DeleteMatrixAfter(dataframe string, t time.Time) {
	df := d.ensure(dataframe)
	for _, md := range df.MatrixData {
		if !md.Time.Before(t) {
			delete(df.MatrixData, md.ID)
		}
	}
}

func (d *InMemoryDriver) DeleteMatrixSatisfying(dataframe string, f func(md *MatrixData) bool) {
	df := d.ensure(dataframe)
	for _, md := range df.MatrixData {
		if f(md) {
			delete(df.MatrixData, md.ID)
		}
	}
}

// Load the latest matrix data for every cell that has had at least one observation.
func (d *InMemoryDriver) LoadMatrixBefore(dataframe string, t time.Time) map[[2]int]*MatrixData {
	df := d.ensure(dataframe)
	m := make(map[[2]int]*MatrixData)
	for _, md := range df.MatrixData {
		if !md.Time.Before(t) {
			continue
		}
		cell := [2]int{md.I, md.J}
		if m[cell] == nil || m[cell].Time.Before(md.Time) {
			m[cell] = md
		}
	}
	return m
}

func (d *InMemoryDriver) AddMatrixData(dataframe string, md *MatrixData) {
	df := d.ensure(dataframe)
	df.MatrixData[df.Counter] = md
	md.ID = df.Counter
	df.Counter++
}

func (d *InMemoryDriver) GetLatestMatrixData(dataframe string, i int, j int) *MatrixData {
	df := d.ensure(dataframe)
	var bestMD *MatrixData
	for _, md := range df.MatrixData {
		if md.I != i || md.J != j {
			continue
		}
		if bestMD == nil || md.Time.After(bestMD.Time) {
			bestMD = md
		}
	}
	return bestMD
}

func (d *InMemoryDriver) GetMatrixDataBefore(dataframe string, i int, j int, t time.Time) *MatrixData {
	df := d.ensure(dataframe)
	var bestMD *MatrixData
	for _, md := range df.MatrixData {
		if md.I != i || md.J != j || md.Time.After(t) {
			continue
		}
		if bestMD == nil || md.Time.After(bestMD.Time) {
			bestMD = md
		}
	}
	return bestMD
}

func (d *InMemoryDriver) GetMatrixDatasAfter(dataframe string, t time.Time) []*MatrixData {
	df := d.ensure(dataframe)
	var mds []*MatrixData
	for _, md := range df.MatrixData {
		if !md.Time.Before(t) {
			mds = append(mds, md)
		}
	}
	return mds
}

func (d InMemoryDriver) GetPredecessorFrames(t time.Time, count int) []*Frame {
	var endIdx int = len(d.Frames)
	for idx, frame := range d.Frames {
		if !frame.Time.Before(t) {
			endIdx = idx
			break
		}
	}
	startIdx := endIdx - count
	if startIdx < 0 {
		startIdx = 0
	}
	return d.Frames[startIdx:endIdx]
	//return driver2.GetPredecessorFrames(t, count)
}

func (d *InMemoryDriver) AddFrame(idx int, t time.Time, bounds common.Polygon) *Frame {
	frame := &Frame{
		ID: len(d.Frames),
		Idx: idx,
		Time: t,
		Bounds: bounds,
	}
	// insert to maintain sorted by time order
	for i := len(d.Frames); i >= 0; i-- {
		if i == 0 || !d.Frames[i-1].Time.After(t) {
			d.Frames = append(d.Frames, nil)
			copy(d.Frames[i+1:], d.Frames[i:])
			d.Frames[i] = frame
			break
		}
	}
	return frame
	//return driver2.AddFrame(idx, t, bounds)
}

func (d *InMemoryDriver) GetFramesStartingFrom(t time.Time) []*Frame {
	var startIdx int = len(d.Frames)
	for i := range d.Frames {
		if !d.Frames[i].Time.Before(t) {
			startIdx = i
			break
		}
	}
	return d.Frames[startIdx:]
	//return driver2.GetFramesStartingFrom(t)
}

func (d *InMemoryDriver) AddSequence(dataframe string, t time.Time) *Sequence {
	df := d.ensure(dataframe)
	seq := &Sequence{
		ID: df.Counter,
		Time: t,
		dataframe: dataframe,
	}
	df.Sequences[df.Counter] = seq
	df.Counter++
	return seq
}

func (d *InMemoryDriver) TerminateSequence(seq *Sequence, t time.Time) {
	seq.Terminated = new(time.Time)
	*seq.Terminated = t
}

func (d *InMemoryDriver) AddSequenceMember(seq *Sequence, detection *Detection, t time.Time) {
	member := &SequenceMember{
		Detection: detection,
		time: t,
	}
	seq.Members = append(seq.Members, member)
}

func (d *InMemoryDriver) GetSequenceMetadata(seq *Sequence) []string {
	var s []string
	df := d.ensure(seq.dataframe)
	for _, meta := range df.Metadata[seq.ID] {
		s = append(s, meta.v)
	}
	return s
}

func (d *InMemoryDriver) AddSequenceMetadata(seq *Sequence, metadata string, t time.Time) {
	df := d.ensure(seq.dataframe)
	seq.GetMetadata()
	*seq.metadata = append(*seq.metadata, metadata)
	df.Metadata[seq.ID] = append(df.Metadata[seq.ID], inMemoryMetadata{t, metadata})
}

func (d *InMemoryDriver) GetUnterminatedSequences(dataframe string) map[int]*Sequence {
	seqs := make(map[int]*Sequence)
	df := d.ensure(dataframe)
	for _, seq := range df.Sequences {
		if seq.Terminated != nil {
			continue
		}
		seqs[seq.ID] = seq
	}
	return seqs
}

func (d *InMemoryDriver) GetSequencesAfter(dataframe string, t time.Time) map[int]*Sequence {
	seqs := make(map[int]*Sequence)
	df := d.ensure(dataframe)
	for _, seq := range df.Sequences {
		if seq.Terminated != nil && seq.Terminated.Before(t) {
			continue
		}
		seqs[seq.ID] = seq
	}
	return seqs
}

func (d *InMemoryDriver) GetSequences(dataframe string) map[int]*Sequence {
	seqs := make(map[int]*Sequence)
	df := d.ensure(dataframe)
	for _, seq := range df.Sequences {
		seqs[seq.ID] = seq
	}
	return seqs
}

func (d *InMemoryDriver) UndoSequences(dataframe string, t time.Time) {
	df := d.ensure(dataframe)
	// remove sequences that started after the time, and reset terminated flag
	for id, seq := range df.Sequences {
		if !seq.Time.Before(t) {
			delete(df.Sequences, id)
			if df.Metadata[seq.ID] != nil {
				delete(df.Metadata, seq.ID)
			}
		} else if seq.Terminated != nil && !seq.Terminated.Before(t) {
			seq.Terminated = nil
		}
	}
	// remove members/metadata after the time
	for _, seq := range df.Sequences {
		for i, member := range seq.Members {
			if !member.time.Before(t) {
				seq.Members = seq.Members[:i]
				break
			}
		}
		for i, meta := range df.Metadata[seq.ID] {
			if !meta.t.Before(t) {
				df.Metadata[seq.ID] = df.Metadata[seq.ID][:i]
				break
			}
		}
	}
}
