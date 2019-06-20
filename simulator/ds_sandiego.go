package simulator

import (
	"github.com/mitroadmaps/gomapinfer/common"
	"github.com/mitroadmaps/gomapinfer/googlemaps"
	"../pipeline"

	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

var SDOrigin = common.Point{-117.15, 32.7}
var SDTimeGridSize time.Duration = time.Hour

type SDTransaction struct {
	TransactionID string
	Start time.Time
	End time.Time
	Point common.Point
}

// Create 3D spacetime grid.
// Space grid aligns with the dataframe grid size.
// Time grid is hourly.
// Then for each query we can check the intersected cell (and match by time).
type SanDiego struct {
	Grid map[[3]int][]*SDTransaction

	// Transaction IDs that have been queried.
	// This is used for GetNew.
	Seen map[string]bool
}

func getSDMeterLocations() map[string]common.Point {
	bytes, err := ioutil.ReadFile("/data/discover-datasets/2019mar22-sandiego/meters.csv")
	if err != nil {
		panic(err)
	}
	meters := make(map[string]common.Point)
	for _, line := range strings.Split(string(bytes), "\n") {
		line = strings.Replace(strings.TrimSpace(line), "\"", "", -1)
		parts := strings.Split(line, ",")
		if len(parts) != 8 || parts[0] == "zone" {
			continue
		}
		pole := parts[3]
		if pole == "--" {
			continue
		}
		lon, _ := strconv.ParseFloat(parts[6], 64)
		lat, _ := strconv.ParseFloat(parts[7], 64)
		p := common.Point{lon, lat}
		if lon < -117.3 || lon > -117.0 || lat < 32.6 || lat > 32.9 {
			fmt.Printf("skip meter %v at %v\n", pole, p)
			continue
		}
		meters[pole] = googlemaps.LonLatToPixel(p, SDOrigin, 18)
	}
	return meters
}

func LoadSanDiego(start time.Time, end time.Time, rect common.Rectangle) SanDiego {
	fmt.Printf("reading san-diego transactions from %v to %v, in %v\n", start, end, rect)
	var transactions []*SDTransaction
	lastByPole := make(map[string]*SDTransaction)
	meters := getSDMeterLocations()
	for pole, location := range meters {
		if !rect.Contains(location) {
			delete(meters, pole)
		}
	}
	file, err := os.Open("/data/discover-datasets/2019mar22-sandiego/payments.csv")
	if err != nil {
		panic(err)
	}
	rd := bufio.NewReader(file)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}
		line = strings.Replace(strings.TrimSpace(line), "\"", "", -1)
		parts := strings.Split(line, ",")
		if len(parts) != 7 || parts[0] == "uuid" {
			continue
		}
		transactionID := parts[0]
		pole := parts[2]
		if _, ok := meters[pole]; !ok {
			continue
		}
		transStart, err := time.Parse("2006-01-02 15:04:05", parts[5])
		if err != nil {
			panic(err)
		}
		transEnd, err := time.Parse("2006-01-02 15:04:05", parts[6])
		if err != nil {
			panic(err)
		}
		if transEnd.Before(start) || transStart.After(end) {
			continue
		}
		if lastByPole[pole] != nil && transStart.Sub(lastByPole[pole].Start) < 2*time.Minute {
			prev := lastByPole[pole]
			if transEnd.After(prev.End) {
				prev.End = transEnd
			}
			continue
		}
		transaction := &SDTransaction{
			TransactionID: transactionID,
			Start: transStart,
			End: transEnd,
			Point: meters[pole],
		}
		transactions = append(transactions, transaction)
		lastByPole[pole] = transaction

		if len(transactions) % 10000 == 0 {
			fmt.Printf("... %d\n", len(transactions))
		}
	}

	grid := make(map[[3]int][]*SDTransaction)
	for _, transaction := range transactions {
		spaceCell := pipeline.ToCell(transaction.Point, GridSize)
		startTimeCell := int(transaction.Start.Unix() / int64(SDTimeGridSize/time.Second))
		endTimeCell := int(transaction.End.Unix() / int64(SDTimeGridSize/time.Second))
		for timeCell := startTimeCell; timeCell <= endTimeCell; timeCell++ {
			cell3d := [3]int{spaceCell[0], spaceCell[1], timeCell}
			grid[cell3d] = append(grid[cell3d], transaction)
		}
	}

	return SanDiego{
		Grid: grid,
		Seen: make(map[string]bool),
	}
}

func (sd SanDiego) GetMaxes() map[[2]int]int {
	maxes := make(map[[2]int]int)
	for cell3d, transactions := range sd.Grid {
		cell := [2]int{cell3d[0], cell3d[1]}
		timeCell := cell3d[2]
		t := time.Unix(int64(timeCell) * int64(SDTimeGridSize/time.Second), 0)
		var count int = 0
		for _, transaction := range transactions {
			if transaction.Start.After(t) || transaction.End.Before(t) {
				continue
			}
			count++
		}
		if count > maxes[cell] {
			maxes[cell] = count
		}
	}
	return maxes
}

func (sd SanDiego) getTransactions(cell [2]int, t time.Time) []*SDTransaction {
	timeCell := int(t.Unix() / int64(SDTimeGridSize/time.Second))
	var transactions []*SDTransaction
	for _, transaction := range sd.Grid[[3]int{cell[0], cell[1], timeCell}] {
		if transaction.Start.After(t) || transaction.End.Before(t) {
			continue
		}
		transactions = append(transactions, transaction)
	}
	return transactions
}

func (sd SanDiego) GetCount(cell [2]int, t time.Time) int {
	return len(sd.getTransactions(cell, t))
}

func (sd SanDiego) GetNew(cell [2]int, t time.Time) int {
	transactions := sd.getTransactions(cell, t)
	var countNew int
	for _, transaction := range transactions {
		if !sd.Seen[transaction.TransactionID] {
			sd.Seen[transaction.TransactionID] = true
			countNew++
		}
	}
	return countNew
}

func (sd SanDiego) Bounds() common.Rectangle {
	bounds := common.EmptyRectangle
	for _, transactions := range sd.Grid {
		for _, transaction := range transactions {
			bounds = bounds.Extend(transaction.Point)
		}
	}
	return bounds
}
