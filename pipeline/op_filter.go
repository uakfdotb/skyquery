package pipeline

import (
	"fmt"
	"strconv"
)

type FilterFunc func(*Sequence) float64
var FilterFuncs = map[string]FilterFunc{
	"displacement": func(seq *Sequence) float64 {
		start := seq.Members[0].Detection.Polygon.Bounds().Center()
		end := seq.Members[len(seq.Members)-1].Detection.Polygon.Bounds().Center()
		return start.Distance(end)
	},
	"length": func(seq *Sequence) float64 {
		return float64(len(seq.Members))
	},
	"duration": func(seq *Sequence) float64 {
		start := seq.Members[0].Detection.Time
		end := seq.Members[len(seq.Members)-1].Detection.Time
		return end.Sub(start).Seconds()
	},
}

func MakeFilterOperator(op *Operator, operands []string) {
	filterFunc := FilterFuncs[operands[0]]
	val, err := strconv.ParseFloat(operands[2], 64)
	if err != nil {
		panic(err)
	}
	evaluate := func(seq *Sequence) bool {
		v1 := filterFunc(seq)
		if operands[1] == "<" && v1 < val {
			return true
		} else if operands[1] == ">" && v1 > val {
			return true
		}
		return false
	}

	// map from parent sequence ID to our sequence
	sequences := make(map[int]*Sequence)
	op.InitFunc = func(frame *Frame) {
		// for filter sequences: seq.time = seq.terminated_at (if not null) = member.time for all members
		// so because the times are the same, we can simply delete all rows with time >= rerun-time
		db.Exec(
			"DELETE sm FROM sequence_members AS sm " +
			"INNER JOIN sequences AS seqs ON seqs.id = sm.sequence_id " +
			"WHERE seqs.dataframe = ? AND sm.time >= ?",
			op.Name, frame.Time,
		)
		db.Exec(
			"DELETE smeta FROM sequence_metadata AS smeta " +
			"INNER JOIN sequences AS seqs ON seqs.id = smeta.sequence_id " +
			"WHERE seqs.dataframe = ? AND smeta.time >= ?",
			op.Name, frame.Time,
		)
		db.Exec("DELETE FROM sequences WHERE dataframe = ? AND time >= ?", op.Name, frame.Time)

		for _, seq := range GetUnterminatedSequences(op.Name) {
			parentID, _ := strconv.Atoi(seq.GetMetadata()[0])
			sequences[parentID] = seq
		}

		rerunTime := frame.Time
		op.RerunTime = &rerunTime
	}

	op.SeqFunc = func(frame *Frame, seqs []*Sequence) {
		for _, seq := range seqs {
			//if sequences[seq.ID] != nil || seq.Terminated == nil || !evaluate(seq) {
			if sequences[seq.ID] != nil || !evaluate(seq) {
				continue
			}
			mySeq := NewSequence(op.Name, seq.Time)
			mySeq.AddMetadata(fmt.Sprintf("%d", seq.ID), seq.Time)
			for _, member := range seq.Members {
				mySeq.AddMember(member.Detection, seq.Time)
			}
			// TODO: maybe we should terminate at last member's time?
			// otherwise when rerunning, parent may give us sequences which
			// we already marked terminated, but we missed them when
			// loading unterminated sequences...
			mySeq.Terminate(seq.Time)
			sequences[seq.ID] = mySeq
		}
	}

	op.Loader = op.SequenceLoader
}
