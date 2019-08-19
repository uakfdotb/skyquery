package router

import (
	"sort"
)

func (r Router) getRoutesB(drones []DroneStatus, cells map[[2]int]int) [][][2]int {
	// binary search + greedy implementation
	// for single drone:
	// * select N highest priority cells
	// * greedily find a path, ignoring the priority
	// * increase N until it violates battery constraint (binary search)
	// for multiple drones, assign route to first drone first, then second, etc.

	// rank cells by value
	type rankedCell struct {
		cell [2]int
		val int
	}
	var rankedCells []rankedCell
	for cell, val := range cells {
		rankedCells = append(rankedCells, rankedCell{cell, val})
	}
	sort.Slice(rankedCells, func(i, j int) bool {
		return rankedCells[i].val > rankedCells[j].val
	})

	assign := func(cells map[[2]int]bool) ([][][2]int, bool) {
		routes := make([][][2]int, len(drones))
		for i := range drones {
			position := drones[i].Cell
			battery := drones[i].Battery
			for len(cells) > 0 {
				var bestCell [2]int
				var bestDistance int = -1
				for cell := range cells {
					d := distance(position, cell)
					if !isActive(battery - d, cell, r.Base) {
						continue
					}
					if bestDistance == -1 || d < bestDistance {
						bestCell = cell
						bestDistance = d
					}
				}
				if bestDistance == -1 {
					break
				}
				routes[i] = append(routes[i], bestCell)
				battery -= distance(position, bestCell)
				for _, cell := range getPathCells(position, bestCell) {
					delete(cells, cell)
				}
				position = bestCell
			}
			routes[i] = append(routes[i], r.Base)
		}
		return routes, len(cells) == 0
	}

	getTop := func(n int) map[[2]int]bool {
		cells := make(map[[2]int]bool)
		for _, rc := range rankedCells[0:n] {
			cells[rc.cell] = true
		}
		return cells
	}

	// binary search
	n := 1
	for {
		_, satisfiable := assign(getTop(n))
		if !satisfiable || n == len(rankedCells) {
			break
		}
		n *= 2
		if n > len(rankedCells) {
			n = len(rankedCells)
		}
	}
	for {
		routes, satisfiable := assign(getTop(n))
		if !satisfiable {
			n--
			continue
		}
		return routes
	}
}
