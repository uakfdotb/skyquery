package pipeline

import (
	"fmt"
	"strings"
	"time"
)

const Debug = false
var Quiet bool = false

type Pipeline map[string]*Operator

func RunPipeline() {
	GetPipeline().RunAll()
}

type OperatorFactory func(op *Operator, operands map[string]string)
var OperatorFactories = map[string]OperatorFactory{
	"raw_detection": MakeDetectionOperator,
	"raw_matrix": MakeMatrixOperator,
	"obj_track": MakeObjTrackOperator,
	"filter": MakeFilterOperator,
	"seq_merge": MakeSeqMergeOperator,
	"to_matrix": MakeToMatrixOperator,
	"intersect": MakeIntersectOperator,
	"product": MakeProductOperator,
	"thin": MakeThinOperator,
	"time_shift": MakeTimeShiftOperator,
	"err_ttl": MakeTTLErrorRate,
	"err_const": MakeConstErrorRate,
	"err_pattern": MakePatternErrorRate,
	"err_normalize": MakeNormalizeErrorRate,
	"error": MakeErrorOperator,
	"open_parking": MakeOpenParkingOperator,
}

func GetPipeline() Pipeline {
	// create pipeline graph
	rows := db.Query("SELECT name, parents, op_type, operands, rerun_time FROM dataframes")
	type seqDataframe struct {
		name string
		parents []string
		opType string
		operands map[string]string
		rerunTime time.Time
	}
	dataframes := make(map[string]seqDataframe)
	for rows.Next() {
		var dataframe seqDataframe
		var parents, operands string
		rows.Scan(&dataframe.name, &parents, &dataframe.opType, &operands, &dataframe.rerunTime)
		if parents != "" {
			dataframe.parents = strings.Split(parents, ",")
		}
		if operands != "" {
			dataframe.operands = make(map[string]string)
			for _, part := range strings.Split(operands, ",") {
				kv := strings.Split(part, "=")
				dataframe.operands[kv[0]] = kv[1]
			}
		}
		dataframes[dataframe.name] = dataframe
	}

	operators := make(map[string]*Operator)
	//var roots []*Operator
	/*startTime, err := time.Parse("2006-01-02 15:04:05", "2019-03-16 14:20:12")
	if err != nil {
		panic(err)
	}
	startTime = time.Time{}
	fmt.Printf("set start time = %v\n", startTime)
	roots = append(roots, &Operator{
		Name: "cars",
		RerunTime: startTime,
	})
	MakeDetectionOperator(roots[0])
	for _, op := range roots {
		operators[op.Name] = op
	}*/
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
				RerunTime: dataframe.rerunTime,
				ChildRerunTime: dataframe.rerunTime,
			}
			operators[name] = op
			for _, parent := range parents {
				parent.Children = append(parent.Children, op)
			}
			factory := OperatorFactories[dataframe.opType]
			if factory == nil {
				panic(fmt.Errorf("unknown operator type %s", dataframe.opType))
			}
			factory(op, dataframe.operands)
			delete(dataframes, name)
		}
		if len(dataframes) == remaining {
			// we didn't make any progress on this iteration
			panic(fmt.Errorf("got orphans when loading pipeline graph: %v", dataframes))
		}
	}

	if !Quiet {
		fmt.Printf("created pipeline with %d operators\n", len(operators))
	}
	return Pipeline(operators)
}

/*op := operators["error_rate"]
for _, parent := range op.Parents {
	parent.RerunTime = &startTime
}
op.Execute()
return*/

func (pipeline Pipeline) RunAll() {
	// execute operators one at a time
	done := make(map[string]bool)
	for _, op := range pipeline {
		if len(op.Parents) > 0 {
			continue
		}
		op.PropogateRerunTime()
		done[op.Name] = true
	}
	for len(done) < len(pipeline) {
		for _, op := range pipeline {
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

			if !Quiet {
				fmt.Printf("[main] executing operator %s\n", op.Name)
			}
			op.Execute()
			done[op.Name] = true
		}
	}
}
