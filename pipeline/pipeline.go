package pipeline

import (
	"fmt"
	"strings"
	"time"
)

const Debug = true

func RunPipeline() {
	// create pipeline graph
	rows := db.Query("SELECT name, parents, op_type, operands FROM dataframes")
	type seqDataframe struct {
		name string
		parents []string
		opType string
		operands []string
	}
	dataframes := make(map[string]seqDataframe)
	for rows.Next() {
		var dataframe seqDataframe
		var parents, operands string
		rows.Scan(&dataframe.name, &parents, &dataframe.opType, &operands)
		dataframe.parents = strings.Split(parents, ",")
		dataframe.operands = strings.Split(operands, ",")
		dataframes[dataframe.name] = dataframe
	}

	operators := make(map[string]*Operator)
	var roots []*Operator
	startTime, err := time.Parse("2006-01-02 15:04:05", "2019-03-16 14:20:12")
	if err != nil {
		panic(err)
	}
	startTime = time.Time{}
	fmt.Printf("set start time = %v\n", startTime)
	roots = append(roots, &Operator{
		Name: "cars",
		RerunTime: &startTime,
	})
	MakeDetectionOperator(roots[0])
	for _, op := range roots {
		operators[op.Name] = op
	}
	for len(dataframes) > 0 {
		remaining := len(dataframes)
		for name, dataframe := range dataframes {
			var parents []*Operator
			haveParents := true
			for _, parentName := range dataframe.parents {
				if operators[parentName] == nil {
					haveParents = false
					break
				}
				parents = append(parents, operators[parentName])
			}
			if !haveParents {
				continue
			}
			op := &Operator{
				Name: name,
				Parents: parents,
			}
			operators[name] = op
			for _, parent := range parents {
				parent.Children = append(parent.Children, op)
			}
			if dataframe.opType == "obj_track" {
				MakeObjTrackOperator(op)
			} else if dataframe.opType == "filter" {
				MakeFilterOperator(op, dataframe.operands)
			} else if dataframe.opType == "seq_merge" {
				MakeSeqMergeOperator(op)
			} else if dataframe.opType == "to_matrix" {
				MakeToMatrixOperator(op)
			} else if dataframe.opType == "intersect" {
				MakeIntersectOperator(op)
			} else if dataframe.opType == "err_ttl" {
				MakeTTLErrorRate(op)
			} else if dataframe.opType == "error" {
				MakeErrorOperator(op)
			} else {
				panic(fmt.Errorf("unknown operator type %s", dataframe.opType))
			}
			delete(dataframes, name)
		}
		if len(dataframes) == remaining {
			// we didn't make any progress on this iteration
			panic(fmt.Errorf("got orphans when loading pipeline graph: %v", dataframes))
		}
	}

	fmt.Printf("created pipeline with %d operators\n", len(operators))

	/*op := operators["error_rate"]
	for _, parent := range op.Parents {
		parent.RerunTime = &startTime
	}
	op.Execute()
	return*/

	// execute operators one at a time
	done := make(map[string]bool)
	for _, op := range roots {
		done[op.Name] = true
	}
	for len(done) < len(operators) {
		for _, op := range operators {
			if done[op.Name] {
				continue
			}

			parentsDone := true
			for _, parent := range op.Parents {
				if !done[parent.Name] {
					parentsDone = false
					break
				}
			}
			if !parentsDone {
				continue
			}

			fmt.Printf("[main] executing operator %s\n", op.Name)
			op.Execute()
			done[op.Name] = true
		}
	}
}
