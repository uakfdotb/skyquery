package pipeline

import (
	_ "github.com/go-sql-driver/mysql"

	"database/sql"
)

var db *Database = NewDatabase()

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

type Database struct {
	db *sql.DB
}

var dbName string = "skyql"

func SetDBName(name string) {
	dbName = name
	db = NewDatabase()
}

func NewDatabase() *Database {
	db := new(Database)
	//sqlDB, err := sql.Open("mysql", "skyql:skyql@/skyql?charset=utf8&parseTime=true")
	//sqlDB, err := sql.Open("mysql", "skyql:skyql@/sd_const_skyql?charset=utf8&parseTime=true")
	//sqlDB, err := sql.Open("mysql", "skyql:skyql@/sd_ttl_skyql?charset=utf8&parseTime=true")
	//sqlDB, err := sql.Open("mysql", "skyql:skyql@/sd_pattern_skyql?charset=utf8&parseTime=true")
	//sqlDB, err := sql.Open("mysql", "skyql:skyql@/sd_pattern2_skyql?charset=utf8&parseTime=true")
	//sqlDB, err := sql.Open("mysql", "skyql:skyql@/sd_pattern3_skyql?charset=utf8&parseTime=true")
	sqlDB, err := sql.Open("mysql", "skyql:skyql@/" + dbName + "?charset=utf8&parseTime=true")
	checkErr(err)
	db.db = sqlDB
	return db
}

func (db *Database) Query(q string, args ...interface{}) Rows {
	rows, err := db.db.Query(q, args...)
	checkErr(err)
	return Rows{rows}
}

func (db *Database) QueryRow(q string, args ...interface{}) Row {
	row := db.db.QueryRow(q, args...)
	return Row{row}
}

func (db *Database) Exec(q string, args ...interface{}) Result {
	result, err := db.db.Exec(q, args...)
	checkErr(err)
	return Result{result}
}

type Rows struct {
	rows *sql.Rows
}

func (r Rows) Close() {
	err := r.rows.Close()
	checkErr(err)
}

func (r Rows) Next() bool {
	return r.rows.Next()
}

func (r Rows) Scan(dest ...interface{}) {
	err := r.rows.Scan(dest...)
	checkErr(err)
}

type Row struct {
	row *sql.Row
}

func (r Row) Scan(dest ...interface{}) {
	err := r.row.Scan(dest...)
	checkErr(err)
}

type Result struct {
	result sql.Result
}

func (r Result) LastInsertId() int {
	id, err := r.result.LastInsertId()
	checkErr(err)
	return int(id)
}

func (r Result) RowsAffected() int {
	count, err := r.result.RowsAffected()
	checkErr(err)
	return int(count)
}
