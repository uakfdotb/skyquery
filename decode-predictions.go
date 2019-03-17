package main

import (
	"../../../drones/dronevid/lib"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)

func getIstr(i int) string {
	istr := strconv.Itoa(i)
	for len(istr) < 6 {
		istr = "0" + istr
	}
	return istr
}

func main() {
	path := os.Args[1]
	numFrames, _ := strconv.Atoi(os.Args[2])
	frameBoxes := make([][]lib.BoundingBox, numFrames+1)
	for i := 2; i <= numFrames; i++ {
		fname := fmt.Sprintf("%s/predict%s.txt", path, getIstr(i))
		frameBoxes[i] = lib.ReadBoxes(fname)
	}
	func() {
		bytes, err := json.Marshal(frameBoxes)
		if err != nil {
			panic(err)
		}
		if err := ioutil.WriteFile("frame_boxes.json", bytes, 0644); err != nil {
			panic(err)
		}
	}()
}
