package pipeline

import (
	"github.com/mitroadmaps/gomapinfer/common"

	"fmt"
	"time"
)

type SequenceMember struct {
	ID int
	Detection *Detection
	time time.Time
}

type Sequence struct {
	ID int
	Time time.Time
	Members []*SequenceMember
	Terminated *time.Time
	dataframe string
	metadata *[]string
}

func NewSequence(dataframe string, t time.Time) *Sequence {
	return driver.AddSequence(dataframe, t)
}

func (seq *Sequence) Terminate(t time.Time) {
	driver.TerminateSequence(seq, t)
}

func (seq *Sequence) AddMember(detection *Detection, t time.Time) {
	driver.AddSequenceMember(seq, detection, t)
}

func (seq *Sequence) GetMetadata() []string {
	if seq.metadata == nil {
		metadata := driver.GetSequenceMetadata(seq)
		seq.metadata = &metadata
	}
	return *seq.metadata
}

func (seq *Sequence) AddMetadata(metadata string, t time.Time) {
	driver.AddSequenceMetadata(seq, metadata, t)
}

// Returns location of this sequence at specified time,
// or nil if the time is before first member or after last member.
// To compute location, we take center-point of rectangle bound of
// detections before/after the time, and average those points based
// on the time difference.
func (seq *Sequence) LocationAt(t time.Time) *common.Point {
	if seq.Members[0].Detection.Time.After(t) || seq.Members[len(seq.Members)-1].Detection.Time.Before(t) {
		fmt.Printf("nil since start=%v end=%v t=%v\n", seq.Members[0].Detection.Time, seq.Members[len(seq.Members)-1].Detection.Time, t)
		return nil
	} else if len(seq.Members) == 1 {
		p := seq.Members[0].Detection.Polygon.Bounds().Center()
		return &p
	}
	var detection1, detection2 *Detection
	for i := 0; i < len(seq.Members) - 1; i++ {
		if seq.Members[i+1].Detection.Time.Before(t) {
			continue
		}
		detection1 = seq.Members[i].Detection
		detection2 = seq.Members[i+1].Detection
		break
	}
	p1 := detection1.Polygon.Bounds().Center()
	p2 := detection2.Polygon.Bounds().Center()
	t1 := t.Sub(detection1.Time).Seconds()
	t2 := detection2.Time.Sub(t).Seconds()
	if t1 == 0 {
		return &p1
	} else if t2 == 0 {
		return &p2
	}
	v := p2.Sub(p1)
	location := p1.Add(v.Scale(t1 / (t1 + t2)))
	return &location
}

func GetUnterminatedSequences(dataframe string) map[int]*Sequence {
	return driver.GetUnterminatedSequences(dataframe)
}

func GetSequencesAfter(dataframe string, t time.Time) map[int]*Sequence {
	return driver.GetSequencesAfter(dataframe, t)
}

func GetSequences(dataframe string) map[int]*Sequence {
	return driver.GetSequences(dataframe)
}
