package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
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

func makebetter(m map[string]map[int]int) {
	// inputs a sparse m and fills it in with dense values
	// some lines must be commented out in sim-sandiego.go save function to get this sparse m
	// because here we assume m only contains sampled values, not copied over values
	for _, vals := range m {
		// samples for each 15-minute period over the day (i.e., interval % 96)
		cyclicSamples := make(map[int][]int)
		var prevValue int = 0
		var prevInterval int = -1
		for i := 0; i <= 1343; i++ {
			if val, ok := vals[i]; ok {
				// perform linear interpolation from the previous sample to update cyclicSamples
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
				prevValue = val
				prevInterval = i
				continue
			}
			// not enough data => just repeat prevValue
			if prevInterval == -1 {
				vals[i] = 1
				continue
			}
			cycle := i % 96
			prevCycle := prevInterval % 96
			if len(cyclicSamples[cycle]) < 1 || len(cyclicSamples[prevCycle]) < 1 || true {
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
			vals[i] = val
		}
	}
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

	var errSum, count int
	actualSums := make(map[string]int)
	predSums := make(map[string]int)
	for s := range gt {
		for interval := range gt[s] {
			pred := m[s][interval]
			actual := gt[s][interval]
			errSum += abs(pred - actual)
			count++
			predSums[s] += pred
			actualSums[s] += actual
		}
	}
	var sumErrSum int
	for s := range actualSums {
		sumErrSum += abs(actualSums[s] - predSums[s])
	}
	fmt.Printf("erravg=%v, sumdiff=%v\n", float64(errSum)/float64(count), float64(sumErrSum)/float64(len(actualSums)))
}
