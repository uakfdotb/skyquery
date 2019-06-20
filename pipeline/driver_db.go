package pipeline

import (
	"github.com/mitroadmaps/gomapinfer/common"

	"time"
)

type DatabaseDriver struct {
	db *Database
}

func NewDatabaseDriver(db *Database) Driver {
	return DatabaseDriver{db}
}

func (d DatabaseDriver) DeleteMatrixAfter(dataframe string, t time.Time) {
	d.db.Exec(
		"DELETE FROM matrix_data WHERE dataframe = ? AND time >= ?",
		dataframe, t,
	)
}

// Load the latest matrix data for every cell that has had at least one observation.
func (d DatabaseDriver) LoadMatrixBefore(dataframe string, t time.Time) map[[2]int]*MatrixData {
	m := make(map[[2]int]*MatrixData)
	rows := d.db.Query("SELECT DISTINCT i, j FROM matrix_data WHERE dataframe = ? AND time < ?", dataframe, t)
	for rows.Next() {
		var md MatrixData
		rows.Scan(&md.I, &md.J)
		row := d.db.QueryRow(
			"SELECT val, metadata FROM matrix_data WHERE dataframe = ? AND i = ? AND j = ? AND time < ? ORDER BY time DESC LIMIT 1",
			dataframe, md.I, md.J, t,
		)
		row.Scan(&md.Val, &md.Metadata)
		m[[2]int{md.I, md.J}] = &md
	}
	return m
}

func (d DatabaseDriver) AddMatrixData(dataframe string, md *MatrixData) {
	result := d.db.Exec(
		"INSERT INTO matrix_data (dataframe, i, j, val, metadata, time) VALUES (?, ?, ?, ?, ?, ?)",
		dataframe, md.I, md.J, md.Val, md.Metadata, md.Time,
	)
	md.ID = result.LastInsertId()
}

func rowsToMatrixDatas(rows Rows) []*MatrixData {
	var datas []*MatrixData
	for rows.Next() {
		var data MatrixData
		rows.Scan(&data.ID, &data.Time, &data.I, &data.J, &data.Val, &data.Metadata)
		datas = append(datas, &data)
	}
	return datas
}

func (d DatabaseDriver) GetLatestMatrixData(dataframe string, i int, j int) *MatrixData {
	rows := d.db.Query(
		"SELECT id, time, i, j, val, metadata FROM matrix_data WHERE dataframe = ? AND i = ? AND j = ? ORDER BY time DESC LIMIT 1",
		dataframe, i, j,
	)
	datas := rowsToMatrixDatas(rows)
	if len(datas) == 1 {
		return datas[0]
	} else {
		return nil
	}
}

func (d DatabaseDriver) GetMatrixDataBefore(dataframe string, i int, j int, t time.Time) *MatrixData {
	rows := d.db.Query(
		"SELECT id, time, i, j, val, metadata FROM matrix_data WHERE dataframe = ? AND i = ? AND j = ? AND time <= ? ORDER BY time DESC LIMIT 1",
		dataframe, i, j, t,
	)
	datas := rowsToMatrixDatas(rows)
	if len(datas) == 1 {
		return datas[0]
	} else {
		return nil
	}
}

func (d DatabaseDriver) GetMatrixDatasAfter(dataframe string, t time.Time) []*MatrixData {
	rows := d.db.Query("SELECT id, time, i, j, val, metadata FROM matrix_data WHERE dataframe = ? AND time >= ? ORDER BY id", dataframe, t)
	return rowsToMatrixDatas(rows)
}

func rowsToFrames(rows Rows) []*Frame {
	var frames []*Frame
	for rows.Next() {
		var frame Frame
		var polyStr *string
		rows.Scan(&frame.ID, &frame.VideoID, &frame.Idx, &frame.Time, &polyStr)
		if polyStr != nil {
			frame.Bounds = ParsePolygon(*polyStr)
		}
		frames = append(frames, &frame)
	}
	return frames
}

func (d DatabaseDriver) GetPredecessorFrames(t time.Time, count int) []*Frame {
	rows := d.db.Query("SELECT id, IFNULL(video_id, 0), idx, time, bounds FROM video_frames WHERE time < ? AND enabled = 1 ORDER BY time DESC LIMIT ?", t, count)
	frames := rowsToFrames(rows)
	orderedFrames := make([]*Frame, len(frames))
	for i := range orderedFrames {
		orderedFrames[i] = frames[len(frames) - i - 1]
	}
	return orderedFrames
}

func (d DatabaseDriver) AddFrame(idx int, t time.Time, bounds common.Polygon) *Frame {
	result := d.db.Exec("INSERT INTO video_frames (idx, time, bounds) VALUES (?, ?, ?)", idx, t, EncodePolygon(bounds))
	return &Frame{
		ID: result.LastInsertId(),
		Idx: idx,
		Time: t,
		Bounds: bounds,
	}
}

func (d DatabaseDriver) GetFramesStartingFrom(t time.Time) []*Frame {
	rows := d.db.Query("SELECT id, IFNULL(video_id, 0), idx, time, bounds FROM video_frames WHERE time >= ? AND enabled = 1 ORDER BY time", t)
	return rowsToFrames(rows)
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

func (d DatabaseDriver) AddSequence(dataframe string, t time.Time) *Sequence {
	result := db.Exec("INSERT INTO sequences (dataframe, time) VALUES (?, ?)", dataframe, t)
	return &Sequence{
		ID: result.LastInsertId(),
		Time: t,
	}
}

func (d DatabaseDriver) TerminateSequence(seq *Sequence, t time.Time) {
	seq.Terminated = new(time.Time)
	*seq.Terminated = t
	db.Exec("UPDATE sequences SET terminated_at = ? WHERE id = ?", t, seq.ID)
}

func (d DatabaseDriver) AddSequenceMember(seq *Sequence, detection *Detection, t time.Time) {
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

func (d DatabaseDriver) GetSequenceMetadata(seq *Sequence) []string {
	rows := db.Query("SELECT metadata FROM sequence_metadata WHERE sequence_id = ? ORDER BY time", seq.ID)
	var metadata []string
	for rows.Next() {
		var s string
		rows.Scan(&s)
		metadata = append(metadata, s)
	}
	return metadata
}

func (d DatabaseDriver) AddSequenceMetadata(seq *Sequence, metadata string, t time.Time) {
	seq.GetMetadata()
	*seq.metadata = append(*seq.metadata, metadata)
	db.Exec("INSERT INTO sequence_metadata (sequence_id, metadata, time) VALUES (?, ?, ?)", seq.ID, metadata, t)
}

func (d DatabaseDriver) GetUnterminatedSequences(dataframe string) map[int]*Sequence {
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

func (d DatabaseDriver) GetSequencesAfter(dataframe string, t time.Time) map[int]*Sequence {
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

func (d DatabaseDriver) GetSequences(dataframe string) map[int]*Sequence {
	rows := db.Query(
		"SELECT sm.id, sm.sequence_id, sm.detection_id, d.time, d.polygon, d.frame_id, seqs.time, seqs.terminated_at " +
		"FROM sequences AS seqs, sequence_members AS sm, detections AS d " +
		"WHERE seqs.id = sm.sequence_id AND d.id = sm.detection_id AND seqs.dataframe = ? " +
		"ORDER BY sm.id",
		dataframe,
	)
	return rowsToSequences(rows)
}

func (d DatabaseDriver) UndoSequences(dataframe string, t time.Time) {
	db.Exec(
		"DELETE sm FROM sequence_members AS sm " +
		"INNER JOIN sequences AS seqs ON seqs.id = sm.sequence_id " +
		"WHERE seqs.dataframe = ? AND sm.time >= ?",
		dataframe, t,
	)
	db.Exec(
		"DELETE smeta FROM sequence_metadata AS smeta " +
		"INNER JOIN sequences AS seqs ON seqs.id = smeta.sequence_id " +
		"WHERE seqs.dataframe = ? AND smeta.time >= ?",
		dataframe, t,
	)
	db.Exec("DELETE FROM sequences WHERE dataframe = ? AND time >= ?", dataframe, t)
	db.Exec("UPDATE sequences SET terminated_at = NULL WHERE dataframe = ? AND terminated_at >= ?", dataframe, t)
}
