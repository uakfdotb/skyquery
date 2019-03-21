package pipeline

import (
	"fmt"
	"strconv"
	"time"
)

// Minimum number of consecutive frames where sequence end is visible in the field of view
// to qualify for a gap (sequence termination).
const SeqMergeGapThreshold int = 5

// Maximum distance of next seq start poly from previous seq end poly.
const SeqMergeDistanceThreshold float64 = 40

// Merge two sequences together with some merging criteria.
// SPATIAL: merge if previous sequence ends where next sequence starts
// All methods terminate a merged sequence if the location where last member ends is
//   visited by a drone without any sequence at the time of the visit.
// In other words, sequences A and B at location X are not merged if the drone visits
//   X between A and B without any sequence during that visit.
// This functionality is disabled if ignore_gaps=false.
// But ignore_gaps results in a dataframe without any terminated sequences, so be careful...?
func MakeSeqMergeOperator(op *Operator) {
	op.LookBehind = 5*time.Second

	// map from parent sequence ID -> our merged sequence
	parentSeqMap := make(map[int]*Sequence)

	// number of frames that the sequence was in the field of view
	// resets to 0 if goes out of the view
	type seqStatus struct {
		frames int
		videoID int
	}
	seqStatuses := make(map[int]seqStatus)

	activeSequences := make(map[int]*Sequence)

	// return sequences that end before the specified frame
	// these are candidates for termination
	getCandidateSequences := func(frame *Frame) map[int]*Sequence {
		seqs := make(map[int]*Sequence)
		for _, seq := range activeSequences {
			endTime := seq.Members[len(seq.Members)-1].Detection.Time
			if !frame.Time.After(endTime) {
				continue
			}
			seqs[seq.ID] = seq
		}
		return seqs
	}

	getSequencesEndingInFrame := func(frame *Frame) map[int]*Sequence {
		matchSeqs := make(map[int]*Sequence)
		for _, seq := range activeSequences {
			p := seq.Members[len(seq.Members)-1].Detection.Polygon.Bounds().Center()
			if !frame.Bounds.Contains(p) {
				continue
			}
			var d float64 = -1
			for _, segment := range frame.Bounds.Segments() {
				segD := segment.Distance(p)
				if d == -1 || segD < d {
					d = segD
				}
			}
			if d < 50 {
				continue
			}
			if seq.Members[0].Detection.ID == 23379 {
				fmt.Printf("DEBUG DEBUG at frame idx=%d, d=%v, p=%v, bounds=%v, seqid=%d\n", frame.Idx, d, p, frame.Bounds, seq.ID)
			}
			matchSeqs[seq.ID] = seq
		}
		return matchSeqs
	}

	// update seqStatuses for this frame
	// returns list of sequences that got a gap
	updateSeqStatus := func(frame *Frame) []*Sequence {
		matchSeqs := getSequencesEndingInFrame(frame)

		// if seq in matchSeqs, increment frame counter
		// else, if videoID matches, reset status
		var gapSeqs []*Sequence
		for _, seq := range getCandidateSequences(frame) {
			if matchSeqs[seq.ID] != nil {
				status := seqStatus{
					frames: 1,
					videoID: frame.VideoID,
				}
				if frame.VideoID == seqStatuses[seq.ID].videoID {
					status.frames = seqStatuses[seq.ID].frames + 1
				}
				seqStatuses[seq.ID] = status
				continue
			}
			if matchSeqs[seq.ID] == nil && frame.VideoID == seqStatuses[seq.ID].videoID {
				status := seqStatuses[seq.ID]
				if status.frames >= SeqMergeGapThreshold {
					gapSeqs = append(gapSeqs, seq)
				}
				seqStatuses[seq.ID] = seqStatus{}
				continue
			}
		}

		return gapSeqs
	}

	op.InitFunc = func(firstFrame *Frame) {
		// We set members.time equal to the seq.time of the parent sequence from which the members came from.
		// Similarly, metadata about parent sequences is the same seq.time.
		// So we delete everythnig based on the time.
		db.Exec(
			"DELETE sm FROM sequence_members AS sm " +
			"INNER JOIN sequences AS seqs ON seqs.id = sm.sequence_id " +
			"WHERE seqs.dataframe = ? AND sm.time >= ?",
			op.Name, firstFrame.Time,
		)
		db.Exec(
			"DELETE smeta FROM sequence_metadata AS smeta " +
			"INNER JOIN sequences AS seqs ON seqs.id = smeta.sequence_id " +
			"WHERE seqs.dataframe = ? AND smeta.time >= ?",
			op.Name, firstFrame.Time,
		)
		db.Exec("DELETE FROM sequences WHERE dataframe = ? AND time >= ?", op.Name, firstFrame.Time)
		db.Exec("UPDATE sequences SET terminated_at = NULL WHERE dataframe = ? AND terminated_at >= ?", op.Name, firstFrame.Time)

		activeSequences = GetUnterminatedSequences(op.Name)
		for _, seq := range activeSequences {
			for _, metadata := range seq.GetMetadata() {
				parentID, _ := strconv.Atoi(metadata)
				parentSeqMap[parentID] = seq
			}
		}
	}

	// we rerun at the minimum of any frame processed from parent or start time of sequences that we modify
	updateRerunTime := func(t time.Time) {
		if op.RerunTime == nil || t.Before(*op.RerunTime) {
			op.RerunTime = new(time.Time)
			*op.RerunTime = t
		}
	}

	op.SeqFunc = func(frame *Frame, seqs []*Sequence) {
		updateRerunTime(frame.Time)

		// merge seqs into candidates
		for _, parentSeq := range seqs {
			if parentSeqMap[parentSeq.ID] != nil {
				continue
			}
			parentBegins := parentSeq.Members[0].Detection
			parentPoint := parentBegins.Polygon.Bounds().Center()

			var bestMergeSequence *Sequence
			var bestDistance float64

			for _, mySeq := range activeSequences {
				myEnds := mySeq.Members[len(mySeq.Members)-1].Detection
				myPoint := myEnds.Polygon.Bounds().Center()
				d := parentPoint.Distance(myPoint)
				if d > SeqMergeDistanceThreshold {
					continue
				} else if parentBegins.Time.Before(myEnds.Time) {
					continue
				}
				if bestMergeSequence == nil || d < bestDistance {
					bestMergeSequence = mySeq
					bestDistance = d
				}
			}

			if bestMergeSequence != nil {
				updateRerunTime(bestMergeSequence.Time)
				for _, member := range parentSeq.Members {
					bestMergeSequence.AddMember(member.Detection, frame.Time)
				}
				bestMergeSequence.AddMetadata(fmt.Sprintf("%d", parentSeq.ID), frame.Time)
				parentSeqMap[parentSeq.ID] = bestMergeSequence
				seqStatuses[bestMergeSequence.ID] = seqStatus{}
				break
			}
		}

		// create new sequences for unmerged
		for _, parentSeq := range seqs {
			if parentSeqMap[parentSeq.ID] != nil {
				continue
			}

			mySeq := NewSequence(op.Name, parentSeq.Time)
			for _, member := range parentSeq.Members {
				mySeq.AddMember(member.Detection, parentSeq.Time)
			}
			mySeq.AddMetadata(fmt.Sprintf("%d", parentSeq.ID), parentSeq.Time)
			parentSeqMap[parentSeq.ID] = mySeq
			activeSequences[mySeq.ID] = mySeq
		}

		// terminate sequences that were in the field of view and are no longer
		gapSeqs := updateSeqStatus(frame)
		for _, mySeq := range gapSeqs {
			mySeq.Terminate(frame.Time)
			delete(activeSequences, mySeq.ID)
		}
	}

	op.Loader = op.SequenceLoader
}
