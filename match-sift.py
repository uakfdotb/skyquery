from discoverlib import geom, grid_index
import get_db

import cv2
import json
import math
import multiprocessing
import numpy
import os
from PIL import Image
import scipy.ndimage
import sys

video_id = int(sys.argv[1])
db = get_db.get_db()

BASE_PATH = 'apr02-ortho-masked.jpg'
FRAME_PATH = 'frames/{}/'.format(video_id)
LK_PARAMETERS = dict(winSize=(21, 21), maxLevel=2, criteria=(cv2.TERM_CRITERIA_COUNT | cv2.TERM_CRITERIA_EPS, 30, 0.01))

# in ortho-imagery resolution units which was 2cm/pixel but resized 4cm/pixel
# and time units is framerate
MAX_SPEED = 75

sift = cv2.xfeatures2d.SIFT_create()
matcher = cv2.DescriptorMatcher_create(cv2.DESCRIPTOR_MATCHER_BRUTEFORCE_L1)
base_im = scipy.ndimage.imread(BASE_PATH)
base_keypoints, base_desc = sift.detectAndCompute(base_im, None)

index = grid_index.GridIndex(256)
for i, kp in enumerate(base_keypoints):
	p = geom.Point(kp.pt[0], kp.pt[1])
	index.insert(p, i)

def points_to_poly_str(points):
	strs = ['{},{}'.format(points[j, 0], points[j, 1]) for j in xrange(points.shape[0])]
	return ' '.join(strs)

def homography_from_flow(prev_homography, prev_gray, cur_gray):
	positions = []
	for i in xrange(0, prev_gray.shape[0]-50, 50):
		for j in xrange(0, prev_gray.shape[1]-50, 50):
			positions.append((i, j))
	positions_np = numpy.array(positions, dtype='float32').reshape(-1, 1, 2)

	def flip_pos(positions):
		return numpy.stack([positions[:, :, 1], positions[:, :, 0]], axis=2)

	next_positions, st, err = cv2.calcOpticalFlowPyrLK(prev_gray, cur_gray, flip_pos(positions_np), None, **LK_PARAMETERS)
	if next_positions is None:
		return None

	next_positions = flip_pos(next_positions)
	differences = next_positions[:, 0, :] - positions_np[:, 0, :]
	differences_okay = differences[numpy.where(st[:, 0] == 1)]
	median = [numpy.median(differences_okay[:, 0]), numpy.median(differences_okay[:, 1])]
	good = (numpy.square(differences[:, 0] - median[0]) + numpy.square(differences[:, 1] - median[1])) < 16

	if float(numpy.count_nonzero(good)) / differences.shape[0] < 0.7:
		return None

	# translate previous homography based on the flow result
	translation = [numpy.median(differences[:, 0]), numpy.median(differences[:, 1])]
	H_translation = numpy.array([[1, 0, -translation[1]], [0, 1, -translation[0]], [0,0,1]], dtype='float32')
	return prev_homography.dot(H_translation)

frame_idx_to_fname = {}
for fname in os.listdir(FRAME_PATH):
	if '.jpg' not in fname:
		continue
	frame_idx = int(fname.split('.jpg')[0])
	frame_idx_to_fname[frame_idx] = fname

prev_bounds = None
prev_frame, prev_gray = None, None
prev_homography = None
prev_counter = 0

#db.execute("SELECT id, idx FROM video_frames WHERE bounds IS NULL AND video_id = %s ORDER BY idx", [video_id])
db.execute("SELECT id, idx FROM video_frames WHERE video_id = %s ORDER BY idx", [video_id])
for row in db.fetchall():

#while True:
#	db.execute("SELECT id, idx FROM video_frames WHERE bounds IS NULL AND video_id = %s ORDER BY RAND() LIMIT 1", [video_id])
#	rows = db.fetchall()
#	if len(rows) != 1:
#		break
#	row = rows[0]

	frame_id, frame_idx = row
	frame_fname = frame_idx_to_fname[frame_idx]

	print 'process {}'.format(frame_idx)
	frame = scipy.ndimage.imread(FRAME_PATH + frame_fname)
	frame = cv2.resize(frame, (frame.shape[1]/2, frame.shape[0]/2))
	frame_gray = cv2.cvtColor(frame, cv2.COLOR_RGB2GRAY)

	H = None

	# delete me
	prev_bounds = None

	if prev_homography is not None and prev_counter < 5:
		#H = homography_from_flow(prev_homography, prev_gray, frame_gray)
		prev_counter += 1

	if H is None:
		keypoints, desc = sift.detectAndCompute(frame, None)

		if prev_bounds is None:
			query_keypoints, query_desc = base_keypoints, base_desc
		else:
			indices = index.search(prev_bounds.add_tol(2*MAX_SPEED))
			indices = numpy.array(list(indices), dtype='int32')
			query_keypoints = []
			for i in indices:
				query_keypoints.append(base_keypoints[i])
			query_desc = base_desc[indices]

		matches = matcher.knnMatch(queryDescriptors=query_desc, trainDescriptors=desc, k=2)
		good = []
		for m, n in matches:
			if m.distance < 0.6*n.distance:
				good.append(m)

		src_pts = numpy.float32([keypoints[m.trainIdx].pt for m in good]).reshape(-1,1,2)
		dst_pts = numpy.float32([query_keypoints[m.queryIdx].pt for m in good]).reshape(-1,1,2)

		try:
			H, _ = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 5.0)
		except Exception as e:
			print 'warning: exception on frame {}: {}'.format(frame_idx, e)
			db.execute("UPDATE video_frames SET bounds = '' WHERE id = %s", [frame_id])
			prev_bounds = None
			continue

		prev_counter = 0

	if H is None:
		db.execute("UPDATE video_frames SET bounds = '' WHERE id = %s", [frame_id])
		prev_bounds = None
		continue

	bound_points = numpy.array([
		[0, 0],
		[frame.shape[1], 0],
		[frame.shape[1], frame.shape[0]],
		[0, frame.shape[0]],
	], dtype='float32').reshape(-1, 1, 2)
	transformed_points = cv2.perspectiveTransform(bound_points, H)

	bounds = None
	for p in transformed_points[:, 0, :]:
		p = geom.Point(p[0], p[1])
		if bounds is None:
			bounds = p.bounds()
		else:
			bounds = bounds.extend(p)

	print bounds

	if prev_bounds is not None:
		intersection_area = float(bounds.intersection(prev_bounds).area())
		union_area = float(bounds.area() + prev_bounds.area()) - intersection_area
		iou = intersection_area / union_area
		if iou < 0.6:
			print 'iou failed! ({})'.format(iou)
			print bounds, prev_bounds
			db.execute("UPDATE video_frames SET bounds = '' WHERE id = %s", [frame_id])
			prev_bounds = None
			continue

	poly_str = points_to_poly_str(transformed_points[:, 0, :])
	db.execute("UPDATE video_frames SET bounds = %s WHERE id = %s", [poly_str, frame_id])
	prev_bounds, prev_frame, prev_gray, prev_homography = bounds, frame, frame_gray, H

	# transform detections
	db.execute(
		"SELECT id, frame_polygon FROM detections WHERE frame_id = %s AND polygon IS NULL",
		[frame_id]
	)
	points = []
	detections = []
	for row in db.fetchall():
		poly_parts = row[1].split(' ')
		poly_points = []
		for part in poly_parts:
			point_parts = part.split(',')
			poly_points.append((int(point_parts[0])/2, int(point_parts[1])/2))
		detections.append((int(row[0]), len(poly_points)))
		points.extend(poly_points)

	if len(points) > 0:
		points = numpy.array(points, dtype='float32').reshape(-1, 1, 2)
		transformed_points = cv2.perspectiveTransform(points, H)
		i = 0
		for detection_id, num_points in detections:
			poly_str = points_to_poly_str(transformed_points[i:i+num_points, 0, :])
			db.execute("UPDATE detections SET polygon = %s WHERE id = %s", [poly_str, detection_id])
			print poly_str, detection_id
			i += num_points
		assert i == transformed_points.shape[0]
