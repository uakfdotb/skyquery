package pipeline

import (
	"github.com/mitroadmaps/gomapinfer/common"

	"time"
)

type Frame struct {
	ID int
	VideoID int
	Idx int
	Time time.Time
	Bounds common.Polygon
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

func GetPredecessorFrames(t time.Time, count int) []*Frame {
	rows := db.Query("SELECT id, video_id, idx, time, bounds FROM video_frames WHERE time < ? ORDER BY time DESC LIMIT ?", t, count)
	frames := rowsToFrames(rows)
	orderedFrames := make([]*Frame, len(frames))
	for i := range orderedFrames {
		orderedFrames[i] = frames[len(frames) - i - 1]
	}
	return orderedFrames
}
