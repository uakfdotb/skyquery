package main

import (
	"../golib"
	"github.com/mitroadmaps/gomapinfer/common"

	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const Debug = true

var db *golib.Database

type Frame struct {
	ID int
	VideoID int
	Idx int
	Time time.Time
}

type Detection struct {
	ID int
	Time time.Time
	Polygon common.Polygon
}

type SequenceMember struct {
	ID int
	Detection *Detection
	Metadata string
}

type Sequence struct {
	ID int
	Members []*SequenceMember
	Terminated bool
}

func NewSequence(dataframe string) *Sequence {
	result := db.Exec("INSERT INTO sequences (dataframe) VALUES (?)", dataframe)
	return &Sequence{
		ID: result.LastInsertId(),
	}
}

func (seq *Sequence) Terminate(t time.Time) {
	seq.Terminated = true
	db.Exec("UPDATE sequences SET terminated_at = ? WHERE id = ?", t, seq.ID)
}

func (seq *Sequence) AddMember(detection *Detection, metadata string, t time.Time) {
	member := &SequenceMember{
		Detection: detection,
		Metadata: metadata,
	}
	seq.Members = append(seq.Members, member)
	result := db.Exec(
		"INSERT INTO sequence_members (sequence_id, detection_id, metadata, created_at) VALUES (?, ?, ?, ?)",
		seq.ID, member.Detection.ID, member.Metadata, t,
	)
	member.ID = result.LastInsertId()
}

type Operator struct {
	Name string
	RootFunc func(frame *Frame)
	DetFunc func(frame *Frame, detections []*Detection)
	SeqFunc func(frame *Frame, sequences []*Sequence)
	Children []*Operator
}

func getUnterminatedSequences(dataframe string) map[int]*Sequence {
	rows := db.Query(
		"SELECT sm.id, sm.sequence_id, sm.detection_id, d.time, d.polygon, sm.metadata " +
		"FROM sequences AS seqs, sequence_members AS sm, detections AS d " +
		"WHERE seqs.id = sm.sequence_id AND d.id = sm.detection_id AND " +
		"seqs.dataframe = ? AND seqs.terminated_at IS NULL " +
		"ORDER BY sm.id",
		dataframe,
	)
	sequences := make(map[int]*Sequence)
	for rows.Next() {
		var sequenceID int
		var member SequenceMember
		var detection Detection
		var polygonStr string
		rows.Scan(&member.ID, &sequenceID, &detection.ID, &detection.Time, &polygonStr, &member.Metadata)
		detection.Polygon = golib.ParsePolygon(polygonStr)
		member.Detection = &detection
		if sequences[sequenceID] == nil {
			sequences[sequenceID] = &Sequence{
				ID: sequenceID,
				Members: []*SequenceMember{&member},
				Terminated: false,
			}
		} else {
			sequences[sequenceID].Members = append(sequences[sequenceID].Members, &member)
		}
	}
	return sequences
}

func getFrameDetections(dataframe string, frame *Frame) []*Detection {
	rows := db.Query("SELECT id, time, polygon FROM detections WHERE dataframe = ? AND frame_id = ? AND polygon IS NOT NULL AND polygon != ''", dataframe, frame.ID)
	var detections []*Detection
	for rows.Next() {
		var detection Detection
		var polygonStr string
		rows.Scan(&detection.ID, &detection.Time, &polygonStr)
		detection.Polygon = golib.ParsePolygon(polygonStr)
		detections = append(detections, &detection)
	}
	return detections
}

func main() {
	db = golib.NewDatabase()
	videoID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}

	// create pipeline graph
	rows := db.Query("SELECT name, parent, op_type, operands FROM seq_dataframes")
	type seqDataframe struct {
		name string
		parent string
		opType string
		operands []string
	}
	dataframes := make(map[string]seqDataframe)
	for rows.Next() {
		var dataframe seqDataframe
		var operands string
		rows.Scan(&dataframe.name, &dataframe.parent, &dataframe.opType, &operands)
		dataframe.operands = strings.Split(operands, ",")
		dataframes[dataframe.name] = dataframe
	}

	operators := make(map[string]*Operator)
	var roots []*Operator
	roots = append(roots, &Operator{
		Name: "cars",
	})
	MakeDetectionOperator(roots[0])
	for _, op := range roots {
		operators[op.Name] = op
	}
	for len(dataframes) > 0 {
		remaining := len(dataframes)
		for name, dataframe := range dataframes {
			if operators[dataframe.parent] == nil {
				continue
			}
			op := &Operator{
				Name: name,
			}
			operators[name] = op
			operators[dataframe.parent].Children = append(operators[dataframe.parent].Children, op)
			if dataframe.opType == "obj_track" {
				MakeObjTrackOperator(op)
			}
			delete(dataframes, name)
		}
		if len(dataframes) == remaining {
			// we didn't make any progress on this iteration
			panic(fmt.Errorf("got orphans when loading pipeline graph: %v", dataframes))
		}
	}

	fmt.Printf("created pipeline with %d operators\n", len(operators))

	// loop through frames
	rows = db.Query("SELECT id, video_id, idx, time FROM video_frames WHERE video_id = ? ORDER BY idx", videoID)
	for rows.Next() {
		var frame Frame
		rows.Scan(&frame.ID, &frame.VideoID, &frame.Idx, &frame.Time)
		fmt.Printf("pushing frame %d (video=%d, idx=%d) through the pipeline\n", frame.ID, frame.VideoID, frame.Idx)
		for _, op := range roots {
			op.RootFunc(&frame)
		}
	}
}
