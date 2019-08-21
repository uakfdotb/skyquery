package pipeline

import (
	"github.com/mitroadmaps/gomapinfer/common"

	"time"
)

type Driver interface {
	DeleteMatrixAfter(dataframe string, t time.Time)
	LoadMatrixBefore(dataframe string, t time.Time) map[[2]int]*MatrixData
	AddMatrixData(dataframe string, md *MatrixData)
	GetLatestMatrixData(dataframe string, i int, j int) *MatrixData
	GetMatrixDataBefore(dataframe string, i int, j int, t time.Time) *MatrixData
	GetMatrixDatasAfter(dataframe string, t time.Time) []*MatrixData
	GetPredecessorFrames(t time.Time, count int) []*Frame
	AddFrame(idx int, t time.Time, bounds common.Polygon) *Frame
	GetFramesStartingFrom(t time.Time) []*Frame
	AddSequence(dataframe string, t time.Time) *Sequence
	TerminateSequence(seq *Sequence, t time.Time)
	AddSequenceMember(seq *Sequence, detection *Detection, t time.Time)
	GetSequenceMetadata(seq *Sequence) []string
	AddSequenceMetadata(seq *Sequence, metadata string, t time.Time)
	GetUnterminatedSequences(dataframe string) map[int]*Sequence
	GetSequencesAfter(dataframe string, t time.Time) map[int]*Sequence
	GetSequences(dataframe string) map[int]*Sequence
	UndoSequences(dataframe string, t time.Time)
}

var driver = NewDatabaseDriver(db)
//var driver = NewInMemoryDriver()
//var driver2 = NewDatabaseDriver(db)

func GetDriver() Driver {
	return driver
}
