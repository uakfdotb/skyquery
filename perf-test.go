package main

import (
	"./pipeline"

	"time"
)

func main() {
	db := pipeline.NewDatabase()

	db.Exec("UPDATE video_frames SET enabled = 0")
	for batch := 0; batch <= 60; batch++ {
		db.Exec("UPDATE video_frames SET enabled = 1 WHERE batch <= ?", batch)
		var t time.Time
		db.QueryRow("SELECT MIN(time) FROM video_frames WHERE batch = ?", batch).Scan(&t)
		db.Exec("UPDATE dataframes SET rerun_time = 0 WHERE name IN ('cars', 'pedestrians')")
		//db.Exec("UPDATE dataframes SET rerun_time = ? WHERE name IN ('cars', 'pedestrians')", t)
		pipeline.GetPipeline().RunAll()
	}
}
