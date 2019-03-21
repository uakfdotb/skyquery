package pipeline

import (
	"github.com/mitroadmaps/gomapinfer/common"

	"fmt"
	"time"
)

type SequenceMember struct {
	ID int
	Detection *Detection
}

type Sequence struct {
	ID int
	Time time.Time
	Members []*SequenceMember
	Terminated *time.Time
	metadata *[]string
}

func NewSequence(dataframe string, t time.Time) *Sequence {
	result := db.Exec("INSERT INTO sequences (dataframe, time) VALUES (?, ?)", dataframe, t)
	return &Sequence{
		ID: result.LastInsertId(),
		Time: t,
	}
}

func (seq *Sequence) Terminate(t time.Time) {
	seq.Terminated = new(time.Time)
	*seq.Terminated = t
	db.Exec("UPDATE sequences SET terminated_at = ? WHERE id = ?", t, seq.ID)
}

func (seq *Sequence) AddMember(detection *Detection, t time.Time) {
	member := &SequenceMember{
		Detection: detection,
	}
	seq.Members = append(seq.Members, member)
	result := db.Exec(
		"INSERT INTO sequence_members (sequence_id, detection_id, time) VALUES (?, ?, ?)",
		seq.ID, member.Detection.ID, t,
	)
	member.ID = result.LastInsertId()
}

func (seq *Sequence) GetMetadata() []string {
	if seq.metadata == nil {
		rows := db.Query("SELECT metadata FROM sequence_metadata WHERE sequence_id = ? ORDER BY time", seq.ID)
		var metadata []string
		for rows.Next() {
			var s string
			rows.Scan(&s)
			metadata = append(metadata, s)
		}
		seq.metadata = &metadata
	}
	return *seq.metadata
}

func (seq *Sequence) AddMetadata(metadata string, t time.Time) {
	seq.GetMetadata()
	*seq.metadata = append(*seq.metadata, metadata)
	db.Exec("INSERT INTO sequence_metadata (sequence_id, metadata, time) VALUES (?, ?, ?)", seq.ID, metadata, t)
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

func rowsToSequences(rows Rows) map[int]*Sequence {
	sequences := make(map[int]*Sequence)
	for rows.Next() {
		var sequenceID int
		var member SequenceMember
		var detection Detection
		var polygonStr string

		var seqTime time.Time
		var seqTerminated *time.Time

		rows.Scan(&member.ID, &sequenceID, &detection.ID, &detection.Time, &polygonStr, &detection.FrameID, &seqTime, &seqTerminated)
		detection.Polygon = ParsePolygon(polygonStr)

		member.Detection = &detection
		if sequences[sequenceID] == nil {
			sequences[sequenceID] = &Sequence{
				ID: sequenceID,
				Time: seqTime,
				Members: []*SequenceMember{&member},
				Terminated: seqTerminated,
			}
		} else {
			sequences[sequenceID].Members = append(sequences[sequenceID].Members, &member)
		}
	}
	return sequences
}

func GetUnterminatedSequences(dataframe string) map[int]*Sequence {
	rows := db.Query(
		"SELECT sm.id, sm.sequence_id, sm.detection_id, d.time, d.polygon, d.frame_id, seqs.time, seqs.terminated_at " +
		"FROM sequences AS seqs, sequence_members AS sm, detections AS d " +
		"WHERE seqs.id = sm.sequence_id AND d.id = sm.detection_id AND " +
		"seqs.dataframe = ? AND seqs.terminated_at IS NULL " +
		"ORDER BY sm.id",
		dataframe,
	)
	return rowsToSequences(rows)
}

func GetSequencesAfter(dataframe string, t time.Time) map[int]*Sequence {
	rows := db.Query(
		"SELECT sm.id, sm.sequence_id, sm.detection_id, d.time, d.polygon, d.frame_id, seqs.time, seqs.terminated_at " +
		"FROM sequences AS seqs, sequence_members AS sm, detections AS d " +
		"WHERE seqs.id = sm.sequence_id AND d.id = sm.detection_id AND " +
		"sm.sequence_id IN (SELECT id FROM sequences AS subseqs WHERE subseqs.dataframe = ? AND (subseqs.terminated_at IS NULL OR subseqs.terminated_at >= ?))" +
		"ORDER BY d.time",
		dataframe, t,
	)
	return rowsToSequences(rows)
}

func GetSequences(dataframe string) map[int]*Sequence {
	rows := db.Query(
		"SELECT sm.id, sm.sequence_id, sm.detection_id, d.time, d.polygon, d.frame_id, seqs.time, seqs.terminated_at " +
		"FROM sequences AS seqs, sequence_members AS sm, detections AS d " +
		"WHERE seqs.id = sm.sequence_id AND d.id = sm.detection_id AND seqs.dataframe = ? " +
		"ORDER BY sm.id",
		dataframe,
	)
	return rowsToSequences(rows)
}
