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

func rowsToMatrixDatas(rows Rows) []*MatrixData {
	var datas []*MatrixData
	for rows.Next() {
		var data MatrixData
		rows.Scan(&data.ID, &data.Time, &data.I, &data.J, &data.Val, &data.Metadata)
		datas = append(datas, &data)
	}
	return datas
}

func GetLatestMatrixData(dataframe string, i int, j int) *MatrixData {
	rows := db.Query(
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

func GetMatrixDataBefore(dataframe string, i int, j int, t time.Time) *MatrixData {
	rows := db.Query(
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

func GetMatrixDatasAfter(dataframe string, t time.Time) []*MatrixData {
	rows := db.Query("SELECT id, time, i, j, val, metadata FROM matrix_data WHERE dataframe = ? AND time >= ? ORDER BY id", dataframe, t)
	return rowsToMatrixDatas(rows)
}

func AddMatrixData(dataframe string, i int, j int, val int, metadata string, t time.Time) *MatrixData {
	result := db.Exec(
		"INSERT INTO matrix_data (dataframe, i, j, val, metadata, time) VALUES (?, ?, ?, ?, ?, ?)",
		dataframe, i, j, val, metadata, t,
	)
	return &MatrixData{
		ID: result.LastInsertId(),
		Time: t,
		I: i,
		J: j,
		Val: val,
		Metadata: metadata,
	}
}

// Load the latest matrix data for every cell that has had at least one observation.
func LoadMatrix(dataframe string) map[[2]int]*MatrixData {
	m := make(map[[2]int]*MatrixData)
	rows := db.Query("SELECT DISTINCT i, j FROM matrix_data WHERE dataframe = ?", dataframe)
	for rows.Next() {
		var md MatrixData
		rows.Scan(&md.I, &md.J)
		row := db.QueryRow(
			"SELECT val, metadata FROM matrix_data WHERE dataframe = ? AND i = ? AND j = ? ORDER BY time DESC LIMIT 1",
			dataframe, md.I, md.J,
		)
		row.Scan(&md.Val, &md.Metadata)
		m[[2]int{md.I, md.J}] = &md
	}
	return m
}
