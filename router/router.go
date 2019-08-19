package router

import (
	"../pipeline"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
)

func abs(x int) int {
	if x < 0 {
		return -x
	} else {
		return x
	}
}

func distance(cell1 [2]int, cell2 [2]int) int {
	return abs(cell1[0] - cell2[0]) + abs(cell1[1] - cell2[1])
}

func isActive(battery int, location [2]int, base [2]int) bool {
	return battery > distance(location, base)
}

// Get cells from src to dst excluding src but including dst.
func getPathCells(src [2]int, dst [2]int) [][2]int {
	cur := src
	var cells [][2]int
	for cur != dst {
		if cur[0] < dst[0] {
			cur[0]++
		} else if cur[0] > dst[0] {
			cur[0]--
		} else if cur[1] < dst[1] {
			cur[1]++
		} else if cur[1] > dst[1] {
			cur[1]--
		}
		cells = append(cells, cur)
	}
	if len(cells) == 0 {
		// if cur == dst
		cells = append(cells, dst)
	}
	return cells
}

type DroneStatus struct {
	Cell [2]int `json:"cell"`

	// Remaining battery in terms of cell side lengths.
	Battery int `json:"battery"`
}

func (drone DroneStatus) IsActive(base [2]int) bool {
	return isActive(drone.Battery, drone.Cell, base)
}

type Router struct {
	Dataframe string
	Base [2]int
}

var idx int = 0

func (r Router) GetRoutes(ignoreCells map[[2]int]bool, drones []DroneStatus) [][][2]int {
	matrix := pipeline.LoadMatrix(r.Dataframe)
	cells := make(map[[2]int]int)
	var countNonzero int = 0
	for cell, md := range matrix {
		cells[cell] = md.Val
		if ignoreCells[cell] && md.Val > 1 {
			cells[cell] = 1
		}
		if md.Val > 0 {
			countNonzero++
		}
	}

	if false {
		r.WriteJSON(drones, cells, fmt.Sprintf("queries/%d.json", idx))
	}

	idx++
	if countNonzero > 200 {
		return r.getRoutes(drones, cells)
	}
	return r.getRoutesPython(drones, cells)
}

type QueryJson struct {
	Drones []DroneStatus `json:"drones"`
	Cells [][3]int `json:"cells"`
	Base [2]int `json:"base"`
}

func (r Router) WriteJSON(drones []DroneStatus, cells map[[2]int]int, fname string) {
	var cellJson [][3]int
	for cell, val := range cells {
		if val == 0 {
			continue
		}
		cellJson = append(cellJson, [3]int{cell[0], cell[1], val})
	}
	query := QueryJson{drones, cellJson, r.Base}
	bytes, err := json.Marshal(query)
	if err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(fname, bytes, 0644); err != nil {
		panic(err)
	}
}

func ReadJSON(fname string) ([]DroneStatus, map[[2]int]int, [2]int) {
	var query QueryJson
	bytes, err := ioutil.ReadFile(fname)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(bytes, &query); err != nil {
		panic(err)
	}
	cells := make(map[[2]int]int)
	for _, tuple := range query.Cells {
		cells[[2]int{tuple[0], tuple[1]}] = tuple[2]
	}
	return query.Drones, cells, query.Base
}

func Evaluate(fname string) {
	drones, cells, base := ReadJSON(fname)
	r := Router{"fake", base}
	routes := r.getRoutesPython(drones, cells)
	fmt.Println(drones)
	fmt.Println(cells)
	fmt.Println(base)
	fmt.Println(routes)
	var reward int
	visited := make(map[[2]int]bool)
	for i, route := range routes {
		position := drones[i].Cell
		var d int = 0
		for _, cell := range route {
			d += distance(position, cell)
			for _, p := range getPathCells(position, cell) {
				if visited[p] {
					continue
				}
				visited[p] = true
				reward += cells[p]
			}
			position = cell
		}
		fmt.Printf("drone %d battery=%d distance=%d\n", i, drones[i].Battery, d)
	}
	fmt.Printf("evaluated reward: %d\n", reward)
}

func (r Router) getRoutesPython(drones []DroneStatus, cells map[[2]int]int) [][][2]int {
	queryFname := fmt.Sprintf("query_%d.json", os.Getpid())
	r.WriteJSON(drones, cells, queryFname)
	bytes, err := exec.Command("python", "router.py", queryFname).CombinedOutput()
	if err != nil {
		fmt.Println(string(bytes))
		panic(err)
	}
	os.Remove(queryFname)
	var routes [][][2]int
	if err := json.Unmarshal(bytes, &routes); err != nil {
		panic(err)
	}
	return routes
}

// Given current drone locations, returns route for each drone.
// A route is a sequence of cells.
func (r Router) getRoutes(drones []DroneStatus, cells map[[2]int]int) [][][2]int {
	// greedy priority-over-battery implementation:
	// repeatedly assign cells to drones maximizing priority-over-battery along the path
	var curDrones []DroneStatus
	unseenCells := make(map[[2]int]int)
	for _, drone := range drones {
		curDrones = append(curDrones, drone)
	}
	for cell, val := range cells {
		unseenCells[cell] = val
	}

	routes := make([][][2]int, len(drones))
	returnedDrones := make(map[int]bool)
	for len(unseenCells) > 0 && len(returnedDrones) < len(drones) {
		for i := range curDrones {
			if returnedDrones[i] {
				continue
			}
			var bestCell [2]int
			var bestScore float64 = -1
			for cell := range unseenCells {
				d := distance(curDrones[i].Cell, cell)
				if !isActive(curDrones[i].Battery - d, cell, r.Base) || d < 5 {
					continue
				}
				var score float64 = 0
				for _, p := range getPathCells(curDrones[i].Cell, cell) {
					score += float64(unseenCells[p])
				}
				score /= float64(d)
				if bestScore == -1 || score > bestScore {
					bestCell = cell
					bestScore = score
				}
			}
			if bestScore == -1 {
				// we cannot assign any more cells to this drone
				// so we return it to base
				routes[i] = append(routes[i], r.Base)
				returnedDrones[i] = true
				continue
			}
			routes[i] = append(routes[i], bestCell)
			curDrones[i].Battery -= distance(curDrones[i].Cell, bestCell)
			for _, p := range getPathCells(curDrones[i].Cell, bestCell) {
				delete(unseenCells, p)
			}
			curDrones[i].Cell = bestCell
		}
	}
	return routes
}

// Given current drone locations, returns route for each drone.
// A route is a sequence of cells.
func (r Router) GetRoutes3(drones []DroneStatus) [][][2]int {
	// greedy implementation:
	// 1) find maximum # cells that drones could visit given remaining battery life
	//    (i.e., add up the battery life)
	// 2) divide by a custom factor
	// 3) find that many top-priority cells
	// 4) repeatedly assign those cells to closest drones
	matrix := pipeline.LoadMatrix(r.Dataframe)
	var batterySum int = 0
	for _, drone := range drones {
		if drone.IsActive(r.Base) {
			batterySum += drone.Battery
		}
	}

	// get cells ordered by error descending
	var cells [][2]int
	for cell := range matrix {
		cells = append(cells, cell)
	}
	sort.Slice(cells, func(i, j int) bool {
		return matrix[cells[i]].Val > matrix[cells[j]].Val
	})
	fmt.Printf("router: got %d cells\n", len(cells))

	assignCells := func(numCells int) ([][][2]int, bool) {
		cellSet := make(map[int][2]int)
		for i, cell := range cells[0:numCells] {
			cellSet[i] = cell
		}
		routes := make([][][2]int, len(drones))
		batteries := make([]int, len(drones))
		locations := make([][2]int, len(drones))
		for i := range drones {
			batteries[i] = drones[i].Battery
			locations[i] = drones[i].Cell
		}
		returnedDrones := make(map[int]bool)
		for len(cellSet) > 0 {
			var numScheduled int = 0
			for i := range drones {
				if returnedDrones[i] {
					continue
				}
				var bestCellIdx int = -1
				var bestDistance int
				for cellIdx, cell := range cellSet {
					d := distance(locations[i], cell)
					if !isActive(batteries[i] - d, cell, r.Base) {
						continue
					}
					if bestCellIdx == -1 || d < bestDistance {
						bestCellIdx = cellIdx
						bestDistance = d
					}
				}
				if bestCellIdx == -1 {
					// we cannot assign any more cells to this drone
					// so we return it to base
					routes[i] = append(routes[i], r.Base)
					returnedDrones[i] = true
					continue
				}
				routes[i] = append(routes[i], cellSet[bestCellIdx])
				batteries[i] -= distance(locations[i], cellSet[bestCellIdx])
				locations[i] = cellSet[bestCellIdx]
				delete(cellSet, bestCellIdx)
				numScheduled++
			}
			if numScheduled == 0 {
				return routes, false
			}
		}
		return routes, true
	}

	var numCells int
	for numCells = batterySum + 1; numCells >= 8; numCells /= 2 {
		routes, ok := assignCells(numCells)
		if ok {
			return routes
		}
	}
	routes, _ := assignCells(numCells)
	return routes
}

func (r Router) GetRoutes2(drones []DroneStatus) [][][2]int {
	// simple greedy implementation, assigning K cells to each drone:
	// 1) find (# drones) highest priority cells
	// 2) for each cell, assign to route of closest drone that hasn't been scheduled yet
	// 3) repeat
	matrix := pipeline.LoadMatrix(r.Dataframe)
	k := 20
	routes := make([][][2]int, len(drones))
	for i, drone := range drones {
		if !drone.IsActive(r.Base) {
			routes[i] = append(routes[i], r.Base)
		}
	}

	// get cells ordered by error descending
	var cells [][2]int
	for cell := range matrix {
		cells = append(cells, cell)
	}
	sort.Slice(cells, func(i, j int) bool {
		return matrix[cells[i]].Val > matrix[cells[j]].Val
	})
	fmt.Printf("router: got %d cells\n", len(cells))

	// assignments
	for lenIdx := 0; lenIdx < k; lenIdx++ {
		scheduledDrones := make(map[int]bool)
		for {
			var bestDroneIdx int = -1
			var bestDistance int
			for i, drone := range drones {
				if scheduledDrones[i] {
					continue
				}
				d := distance(drone.Cell, cells[0])
				if bestDroneIdx == -1 || d < bestDistance {
					bestDroneIdx = i
					bestDistance = d
				}
			}
			if bestDroneIdx == -1 {
				// scheduled all drones this round
				break
			}
			routes[bestDroneIdx] = append(routes[bestDroneIdx], cells[0])
			scheduledDrones[bestDroneIdx] = true
			cells = cells[1:]
		}
	}

	return routes
}
