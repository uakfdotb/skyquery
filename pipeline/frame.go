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

func GetFrame(id int) *Frame {
	rows := db.Query("SELECT id, IFNULL(video_id, 0), idx, time, bounds FROM video_frames WHERE id = ?", id)
	frames := rowsToFrames(rows)
	if len(frames) == 1 {
		return frames[0]
	} else {
		return nil
	}
}
