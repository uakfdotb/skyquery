package simulator

import (
	"../pipeline"
	"../router"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"
)

// 256 cells, 85 of them positive
// need to travel 8+8 x2 = 32 to reach furthest cell
// next things to try: higher time step again, and also max(...) instead of avg(...) for deltas

// Time step duration, which should be equal to how long it takes
// to travel from one cell to another.
const TimeStep time.Duration = 60*time.Second //30*time.Second
const GridSize float64 = 512 //256

// Fully charged battery level.
const DefaultBattery int = 60 //80

//const TimeStep time.Duration = 180*time.Second
//const GridSize float64 = 1024
//const DefaultBattery int = 30

type DataSource func(cell [2]int, t time.Time) int

func SaveGTData(ds DataSource, cells [][2]int, start time.Time, end time.Time, recordInterval time.Duration, fname string) map[string]map[int]int {
	intervals := int(end.Sub(start) / recordInterval)
	m := make(map[string]map[int]int)
	for _, cell := range cells {
		data := make(map[int]int)
		for interval := 0; interval < intervals; interval++ {
			t := start.Add(time.Duration(interval) * recordInterval)
			data[interval] = ds(cell, t)
		}
		m[fmt.Sprintf("%d %d", cell[0], cell[1])] = data
	}
	bytes, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(fname, bytes, 0644); err != nil {
		panic(err)
	}
	return m
}

type Drone struct {
	Location [2]int
	Battery int
	Route [][2]int
}

func (drone *Drone) Step(s *Simulation) {
	if len(drone.Route) == 0 {
		return
	}

	// update state
	dst := drone.Route[0]
	if drone.Location[0] < dst[0] {
		drone.Location[0]++
	} else if drone.Location[0] > dst[0] {
		drone.Location[0]--
	} else if drone.Location[1] < dst[1] {
		drone.Location[1]++
	} else if drone.Location[1] > dst[1] {
		drone.Location[1]--
	}
	if drone.Location == dst {
		drone.Route = drone.Route[1:]
	}
	if drone.Location == s.Base {
		drone.Battery = DefaultBattery
	} else {
		drone.Battery--
	}

	// collect observation
	for dataframe, ds := range s.DataSources {
		val := ds(drone.Location, s.Time)
		cellBounds := pipeline.GetCellRect(drone.Location, GridSize).AddTol(GridSize/10)
		frame := pipeline.GetDriver().AddFrame(0, s.Time, cellBounds.ToPolygon())
		md := pipeline.AddMatrixData(dataframe, drone.Location[0], drone.Location[1], val, "", s.Time)
		if false {
			fmt.Printf("[drone] insert a frame at %v (frame_id=%d, md_id=%d)\n", drone.Location, frame.ID, md.ID)
		}
	}
}

type Simulation struct {
	// Map fram dataframe name to DataSource function.
	DataSources map[string]DataSource

	Drones []*Drone
	Time time.Time
	Router router.Router
	Base [2]int
}

func (s *Simulation) AddDrone() *Drone {
	drone := &Drone{
		Location: s.Base,
		Battery: DefaultBattery,
	}
	s.Drones = append(s.Drones, drone)
	return drone
}

// Update drone positions and collect observations.
func (s *Simulation) Step() {
	for _, drone := range s.Drones {
		drone.Step(s)
	}
	s.Time = s.Time.Add(TimeStep)
}

// Run simulation for the specified number of timesteps.
func (s *Simulation) Run(duration int) {
	/*var droneStatuses []router.DroneStatus
	for _, drone := range s.Drones {
		droneStatuses = append(droneStatuses, router.DroneStatus{
			Cell: drone.Location,
			Battery: drone.Battery,
		})
	}
	routes := s.Router.GetRoutes(droneStatuses)
	for i, route := range routes {
		s.Drones[i].Route = route
	}*/
	for t := 0; t < duration; t++ {
		s.Step()
		needReroute := false
		for _, drone := range s.Drones {
			if len(drone.Route) == 0 {
				needReroute = true
			}
		}
		if needReroute {
			var droneStatuses []router.DroneStatus
			for _, drone := range s.Drones {
				droneStatuses = append(droneStatuses, router.DroneStatus{
					Cell: drone.Location,
					Battery: drone.Battery,
				})
			}
			routes := s.Router.GetRoutes(droneStatuses)
			for i, route := range routes {
				s.Drones[i].Route = route
			}
		}
	}
}
