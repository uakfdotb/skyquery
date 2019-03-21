package main

import (
	"github.com/mitroadmaps/gomapinfer/common"
	"./golib"

	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	videoID, _ := strconv.Atoi(os.Args[1])
	db := golib.NewDatabase()
	var startTime time.Time
	db.QueryRow("SELECT start_time FROM videos WHERE id = ?", videoID).Scan(&startTime)

	c := exec.Command("./darknet", "detect", "../drone-car-data2/yolov3-test.cfg", "backup/yolov3.backup", "-thresh", "0.3")
	c.Dir = "darknet/"
	stdin, err := c.StdinPipe()
	if err != nil {
		panic(err)
	}
	stderr, err := c.StderrPipe()
	if err != nil {
		panic(err)
	}
	stdout, err := c.StdoutPipe()
	if err != nil {
		panic(err)
	}
	go func() {
		r := bufio.NewReader(stderr)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				panic(err)
			}
			line = strings.TrimSpace(line)
			fmt.Println("[yolo] [stderr] " + line)
		}
	}()
	r := bufio.NewReader(stdout)
	if err := c.Start(); err != nil {
		panic(err)
	}

	framePath := fmt.Sprintf("/home/ubuntu/skyql-subsystem/frames/%d/", videoID)
	files, err := ioutil.ReadDir(framePath)
	if err != nil {
		panic(err)
	}
	getLines := func() []string {
		var output string
		for {
			line, err := r.ReadString(':')
			if err != nil {
				panic(err)
			}
			fmt.Println("[yolo] [stdout] " + strings.TrimSpace(line))
			output += line
			if strings.Contains(line, "Enter") {
				break
			}
		}
		return strings.Split(output, "\n")
	}
	parseLines := func(lines []string) []common.Rectangle {
		var rects []common.Rectangle
		for i := 0; i < len(lines); i++ {
			if !strings.Contains(lines[i], "%") {
				continue
			}
			//parts := strings.Split(lines[i], ": ")
			//box.Class = parts[0]
			//box.Confidence, _ = strconv.Atoi(strings.Trim(parts[1], "%"))
			for !strings.Contains(lines[i], "Bounding Box:") {
				i++
			}
			parts := strings.Split(strings.Split(lines[i], ": ")[1], ", ")
			if len(parts) != 4 {
				panic(fmt.Errorf("bad bbox line %s", lines[i]))
			}
			var left, top, right, bottom int
			for _, part := range parts {
				kvsplit := strings.Split(part, "=")
				k := kvsplit[0]
				v, _ := strconv.Atoi(kvsplit[1])
				if k == "Left" {
					left = v
				} else if k == "Top" {
					top = v
				} else if k == "Right" {
					right = v
				} else if k == "Bottom" {
					bottom = v
				}
			}
			rects = append(rects, common.Rectangle{
				common.Point{float64(left), float64(top)},
				common.Point{float64(right), float64(bottom)},
			})
		}
		return rects
	}
	saveRects := func(frameIdx int, rects []common.Rectangle) {
		t := startTime.Add(time.Duration(frameIdx) * 200 * time.Millisecond)
		result := db.Exec("INSERT INTO video_frames (video_id, idx, time) VALUES (?, ?, ?)", videoID, frameIdx, t)
		frameID := result.LastInsertId()
		for _, rect := range rects {
			var points []string
			for _, p := range rect.ToPolygon() {
				points = append(points, fmt.Sprintf("%v,%v", p.X, p.Y))
			}
			polygon := strings.Join(points, " ")
			db.Exec("INSERT INTO detections (dataframe, time, frame_polygon, frame_id) VALUES ('cars', ?, ?, ?)", t, polygon, frameID)
		}
	}
	getFrameIdx := func(fname string) int {
		parts := strings.Split(fname, ".jpg")
		frameIdx, err := strconv.Atoi(parts[0])
		if err != nil {
			panic(err)
		}
		return frameIdx
	}
	sort.Slice(files, func(i, j int) bool {
		fname1 := files[i].Name()
		fname2 := files[j].Name()
		return getFrameIdx(fname1) < getFrameIdx(fname2)
	})
	var prevFrameIdx int = -1
	for _, fi := range files {
		frameIdx := getFrameIdx(fi.Name())
		fmt.Printf("[yolo] processing %s (%d)\n", fi.Name(), frameIdx)
		lines := getLines()
		if prevFrameIdx != -1 {
			rects := parseLines(lines)
			saveRects(prevFrameIdx, rects)
		}
		stdin.Write([]byte(framePath + fi.Name() + "\n"))
		prevFrameIdx = frameIdx
	}
	if prevFrameIdx != -1 {
		lines := getLines()
		rects := parseLines(lines)
		saveRects(prevFrameIdx, rects)
	}
}
