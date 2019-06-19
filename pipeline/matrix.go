package pipeline

import (
	"time"
)

type MatrixData struct {
	ID int
	Time time.Time
	I int
	J int
	Val int
	Metadata string
}

func AddMatrixData(dataframe string, i int, j int, val int, metadata string, t time.Time) *MatrixData {
	md := &MatrixData{
		Time: t,
		I: i,
		J: j,
		Val: val,
		Metadata: metadata,
	}
	driver.AddMatrixData(dataframe, md)
	return md
}

func LoadMatrix(dataframe string) map[[2]int]*MatrixData {
	return driver.LoadMatrixBefore(dataframe, time.Now().Add(9999*time.Hour))
}
