package pipeline

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Minimum number of consecutive frames where sequence end is visible in the field of view
// to qualify for a gap (sequence termination).
const SeqMergeGapThreshold int = 10

// Minimum distance from edge of frame for counting gaps.
const SeqMergeGapPadding float64 = 50

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
func MakeSeqMergeOperator(op *Operator, operands map[string]string) {
	op.LookBehind = 5*time.Second
	mode := operands["mode"]
	cachedImageSimilarities := make(map[[2]int]float64)

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

	getDetectionDistanceToFrame := func(frame *Frame, detection *Detection) float64 {
		p := detection.Polygon.Bounds().Center()
		if !frame.Bounds.Contains(p) {
			return -1
		}
		var d float64 = -1
		for _, segment := range frame.Bounds.Segments() {
			segD := segment.Distance(p)
			if d == -1 || segD < d {
				d = segD
			}
		}
		return d
	}

	getSequencesEndingInFrame := func(frame *Frame) map[int]*Sequence {
		matchSeqs := make(map[int]*Sequence)
		for _, seq := range activeSequences {
			detection := seq.Members[len(seq.Members)-1].Detection
			d := getDetectionDistanceToFrame(frame, detection)
			if d == -1 || d < SeqMergeGapPadding {
				continue
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

	// return first or last detection at least SeqMergeGapPadding away from frame
	findPaddedDetection := func(seq *Sequence, first bool) *Detection {
		var detections []*Detection
		for _, member := range seq.Members {
			detections = append(detections, member.Detection)
		}
		if !first {
			ndetections := make([]*Detection, len(detections))
			for i := range detections {
				ndetections[i] = detections[len(detections) - i - 1]
			}
			detections = ndetections
		}
		for _, detection := range detections {
			frame := GetFrame(detection.FrameID)
			d := getDetectionDistanceToFrame(frame, detection)
			if d >= SeqMergeGapPadding {
				return detection
			}
		}
		return detections[0]
	}

	op.InitFunc = func(firstFrame *Frame) {
		// We set members.time equal to the seq.time of the parent sequence from which the members came from.
		// Similarly, metadata about parent sequences is the same seq.time.
		// So we delete everythnig based on the time.
		driver.UndoSequences(op.Name, firstFrame.Time)

		activeSequences = GetUnterminatedSequences(op.Name)
		for _, seq := range activeSequences {
			for _, metadata := range seq.GetMetadata() {
				parentID, _ := strconv.Atoi(metadata)
				parentSeqMap[parentID] = seq
			}
		}
	}

	// we rerun at the minimum of any frame processed from parent or start time of sequences that we modify

	op.SeqFunc = func(frame *Frame, seqs []*Sequence) {
		op.updateChildRerunTime(frame.Time)

		// merge seqs into candidates
		for _, parentSeq := range seqs {
			if parentSeqMap[parentSeq.ID] != nil {
				continue
			}
			parentBegins := parentSeq.Members[0].Detection
			if len(parentSeq.Members) >= 4 {
				parentBegins = parentSeq.Members[3].Detection
			}
			parentPoint := parentBegins.Polygon.Bounds().Center()

			var bestMergeSequence *Sequence
			var bestDistance float64

			for _, mySeq := range activeSequences {
				myEnds := mySeq.Members[len(mySeq.Members)-1].Detection

				// only for parked cars!
				if len(mySeq.Members) >= 4 {
					myEnds = mySeq.Members[len(mySeq.Members)-4].Detection
				}

				myPoint := myEnds.Polygon.Bounds().Center()
				d := parentPoint.Distance(myPoint)
				if d > SeqMergeDistanceThreshold {
					continue
				} else if parentBegins.Time.Before(myEnds.Time) {
					continue
				}

				if mode == "image_similarity" {
					// use external python script to verify that image similarity is close
					// first get last/first detections that are SeqMergeGapPadding away from their frames
					var similarity float64
					k := [2]int{parentSeq.ID, mySeq.ID}
					if _, ok := cachedImageSimilarities[k]; ok {
						similarity = cachedImageSimilarities[k]
					} else {
						similarity = getImageSimilarity(findPaddedDetection(parentSeq, true), findPaddedDetection(mySeq, false))
						cachedImageSimilarities[k] = similarity
						fmt.Printf("%v %v\n", k, similarity)
					}
					if similarity < 0.15 {
						continue
					}
				}

				if bestMergeSequence == nil || d < bestDistance {
					bestMergeSequence = mySeq
					bestDistance = d
				}
			}

			if bestMergeSequence != nil {
				op.updateChildRerunTime(bestMergeSequence.Time)
				for _, member := range parentSeq.Members {
					bestMergeSequence.AddMember(member.Detection, frame.Time)
				}
				bestMergeSequence.AddMetadata(fmt.Sprintf("%d", parentSeq.ID), frame.Time)
				parentSeqMap[parentSeq.ID] = bestMergeSequence
				seqStatuses[bestMergeSequence.ID] = seqStatus{}
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

func getImageSimilarity(detection1 *Detection, detection2 *Detection) float64 {
	var video1, video2, frame1, frame2 string
	db.QueryRow("SELECT video_id, idx FROM video_frames WHERE id = ?", detection1.FrameID).Scan(&video1, &frame1)
	db.QueryRow("SELECT video_id, idx FROM video_frames WHERE id = ?", detection2.FrameID).Scan(&video2, &frame2)
	var poly1, poly2 string
	db.QueryRow("SELECT frame_polygon FROM detections WHERE id = ?", detection1.ID).Scan(&poly1)
	db.QueryRow("SELECT frame_polygon FROM detections WHERE id = ?", detection2.ID).Scan(&poly2)
	cmd := exec.Command("python", "seq-merge-imagediff.py", video1, video2, frame1, frame2, poly1, poly2)
	bytes, err := cmd.Output()
	if err != nil {
		fmt.Println(string(bytes))
		fmt.Println("warning!! image similarity error")
		//panic(err)
		return 0
	}
	output := strings.TrimSpace(string(bytes))
	lines := strings.Split(output, "\n")
	lastLine := lines[len(lines)-1]
	if strings.Contains(lastLine, "bad") {
		return 0
	}
	similarity, err := strconv.ParseFloat(lastLine, 64)
	if err != nil {
		fmt.Println(output)
		panic(err)
	}
	return similarity
}
