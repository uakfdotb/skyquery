package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
)

func abs(x int) int {
	if x < 0 {
		return -x
	} else {
		return x
	}
}

func intSliceAvg(a []int) int {
	var sum int = 0
	for _, x := range a {
		sum += x
	}
	return sum / len(a)
}

func getMean(a []float64) float64 {
	var sum float64 = 0
	for _, x := range a {
		sum += x
	}
	return sum / float64(len(a))
}

func makebetter(m map[string]map[int]int) {
	// inputs a sparse m and fills it in with dense values
	// some lines must be commented out in sim-sandiego.go save function to get this sparse m
	// because here we assume m only contains sampled values, not copied over values
	for _, vals := range m {
		// samples for each 15-minute period over the day (i.e., interval % 96)
		cyclicSamples := make(map[int][]int)
		var prevValue int = 0
		var prevInterval int = -1
		for i := 0; i <= 5376; i++ {
			if val, ok := vals[i]; ok {
				// perform linear interpolation from the previous sample to update cyclicSamples
				if (i / 96) % 7 < 5 {
					if prevInterval == -1 {
						cycle := i % 96
						cyclicSamples[cycle] = append(cyclicSamples[cycle], val)
					} else {
						l := i - prevInterval
						for j := 1; j <= l; j++ {
							cycle := (prevInterval + j) % 96
							cval := (j * val + (l - j) * prevValue) / l
							cyclicSamples[cycle] = append(cyclicSamples[cycle], cval)
						}
					}
				}
				prevValue = val
				prevInterval = i
				continue
			}
			// not enough data => just repeat prevValue
			if prevInterval == -1 || true {
				vals[i] = 0
				continue
			}
			cycle := i % 96
			prevCycle := prevInterval % 96
			if len(cyclicSamples[cycle]) < 1 || len(cyclicSamples[prevCycle]) < 1 || (i / 96) % 7 >= 5 || true {
				vals[i] = prevValue
				continue
			}
			// otherwise, let's update it with the value change
			// clip negative values to 0
			prevMean := intSliceAvg(cyclicSamples[prevCycle])
			curMean := intSliceAvg(cyclicSamples[cycle])
			/*if prevMean < 5 || curMean < 5 || abs(curMean - prevMean) < 2 {
				vals[i] = prevValue
				continue
			}*/
			val := prevValue + curMean - prevMean
			if val < 0 {
				val = 0
			}
			/*if prevValue > prevMean && prevValue > curMean && val > prevValue {
				val = prevValue
			} else if prevValue < prevMean && prevValue < curMean && val < prevValue {
				val = prevValue
			}*/
			vals[i] = val
		}
	}
}

func makebetter2(m map[string]map[int]int) {
	// inputs a sparse m and fills it in with dense values
	// some lines must be commented out in sim-sandiego.go save function to get this sparse m
	// because here we assume m only contains sampled values, not copied over values
	for _, vals := range m {
		// samples for each 15-minute period over the day (i.e., interval % 96)
		cyclicSamples := make(map[int][]float64)
		var prevValue int = 0
		var prevInterval int = -1
		for i := 0; i <= 5376; i++ {
			if val, ok := vals[i]; ok {
				// perform linear interpolation from the previous sample to update cyclicSamples
				if (i / 96) % 7 < 5 && prevInterval != -1 {
					l := i - prevInterval
					rate := float64(val - prevValue) / float64(l)
					for j := 1; j <= l; j++ {
						cycle := (prevInterval + j) % 96
						cyclicSamples[cycle] = append(cyclicSamples[cycle], rate)
					}
				}
				prevValue = val
				prevInterval = i
				continue
			}
			// not enough data => just repeat prevValue
			if prevInterval == -1 {
				vals[i] = 0
				continue
			}
			cycle := i % 96
			prevCycle := prevInterval % 96
			if len(cyclicSamples[cycle]) < 1 || len(cyclicSamples[prevCycle]) < 1 || (i / 96) % 7 >= 5 {
				vals[i] = prevValue
				continue
			}
			// otherwise, let's update it with the value change
			// clip negative values to 0
			var rateSum float64
			for j := prevInterval + 1; j <= i; j++ {
				rateSum += getMean(cyclicSamples[j % 96])
			}
			val := prevValue + int(rateSum)
			if val < 0 {
				val = 0
			}
			vals[i] = val
		}
	}
}

func aggregate(m map[string]map[int]int) map[string]map[int]int {
	out := make(map[string]map[int]int)
	for cellstr, vals := range m {
		parts := strings.Split(cellstr, " ")
		i, _ := strconv.Atoi(parts[0])
		j, _ := strconv.Atoi(parts[1])
		smcell := [2]int{
			int(math.Floor(float64(i) / 4)),
			int(math.Floor(float64(j) / 4)),
		}
		smstr := fmt.Sprintf("%d %d", smcell[0], smcell[1])
		if out[smstr] == nil {
			out[smstr] = make(map[int]int)
		}
		for t, val := range vals {
			out[smstr][t] += val
		}
	}
	return out
}

func main() {
	var m map[string]map[int]int
	var gt map[string]map[int]int
	bytes, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(bytes, &m); err != nil {
		panic(err)
	}
	bytes, err = ioutil.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(bytes, &gt); err != nil {
		panic(err)
	}
	makebetter(m)
	//m = aggregate(m)

	var errSum, sqErrSum, count int
	actualSums := make(map[string]int)
	predSums := make(map[string]int)
	for s := range gt {
		allzero := true
		allone := true
		for _, val := range gt[s] {
			if val != 0 {
				allzero = false
			}
			if val != 1 {
				allone = false
			}
		}
		if allzero || allone {
			continue
		}
		for interval := range gt[s] {
			if interval < 672 {
				continue
			}
			pred := m[s][interval]
			actual := gt[s][interval]
			err := abs(pred - actual)
			errSum += err
			sqErrSum += err * err
			count++
			predSums[s] += pred
			actualSums[s] += actual
		}
	}
	var sumErrSum, actualSum, predSum int
	for s := range actualSums {
		//pred := predSums[s]
		var pred int = 0
		for _, x := range m[s] {
			if x > pred {
				pred = x
			}
		}

		sumErrSum += abs(actualSums[s] - pred)
		actualSum += actualSums[s]
		predSum += pred
	}
	fmt.Printf("erravg=%v, sqerravg=%v, sumdiff=%v (pred=%v, actual=%v)\n", float64(errSum)/float64(count), float64(sqErrSum)/float64(count), float64(sumErrSum)/float64(len(actualSums)), predSum, actualSum)
}
