package pipeline

import (
	"github.com/mitroadmaps/gomapinfer/common"

	"time"
)

type Detection struct {
	ID int
	Time time.Time
	Polygon common.Polygon
	FrameID int
}

func rowsToDetections(rows Rows) []*Detection {
	var detections []*Detection
	for rows.Next() {
		var detection Detection
		var polygonStr string
		rows.Scan(&detection.ID, &detection.Time, &polygonStr, &detection.FrameID)
		detection.Polygon = ParsePolygon(polygonStr)
		detections = append(detections, &detection)
	}
	return detections
}

func GetFrameDetections(dataframe string, frame *Frame) []*Detection {
	rows := db.Query("SELECT id, time, polygon, frame_id FROM detections WHERE dataframe = ? AND frame_id = ? AND polygon IS NOT NULL AND polygon != ''", dataframe, frame.ID)
	return rowsToDetections(rows)
}

func GetDetectionsAfter(dataframe string, t time.Time) []*Detection {
	rows := db.Query(
		"SELECT id, time, polygon, frame_id FROM detections WHERE dataframe = ? AND polygon IS NOT NULL AND polygon != '' AND time >= ? AND (SELECT enabled FROM video_frames WHERE video_frames.id = frame_id) = 1 ORDER BY time",
		dataframe, t,
	)
	return rowsToDetections(rows)
}

func GetDetections(dataframe string) []*Detection {
	rows := db.Query(
		"SELECT id, time, polygon, frame_id FROM detections WHERE dataframe = ? AND polygon IS NOT NULL AND polygon != '' ORDER BY time",
		dataframe,
	)
	return rowsToDetections(rows)
}
