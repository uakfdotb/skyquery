from discoverlib import geom
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

BASE_PATH = 'ortho.png'
FRAME_PATH = 'frames/{}/'.format(video_id)

sift = cv2.xfeatures2d.SIFT_create()
matcher = cv2.DescriptorMatcher_create(cv2.DESCRIPTOR_MATCHER_BRUTEFORCE_L1)
base_im = scipy.ndimage.imread(BASE_PATH)
base_keypoints, base_desc = sift.detectAndCompute(base_im, None)

def points_to_poly_str(points):
	strs = ['{},{}'.format(points[j, 0], points[j, 1]) for j in xrange(points.shape[0])]
	return ' '.join(strs)

frame_idx_to_fname = {}
for fname in os.listdir(FRAME_PATH):
	if '.jpg' not in fname:
		continue
	frame_idx = int(fname.split('.jpg')[0])
	frame_idx_to_fname[frame_idx] = fname

#db.execute("SELECT id, idx FROM video_frames WHERE bounds IS NULL AND video_id = %s", [video_id])
#for row in db.fetchall():

while True:
	db.execute("SELECT id, idx FROM video_frames WHERE bounds IS NULL AND video_id = %s ORDER BY RAND() LIMIT 1", [video_id])
	rows = db.fetchall()
	if len(rows) != 1:
		break
	row = rows[0]

	frame_id, frame_idx = row
	frame_fname = frame_idx_to_fname[frame_idx]

	print 'process {}'.format(frame_idx)
	frame = scipy.ndimage.imread(FRAME_PATH + frame_fname)
	frame = cv2.resize(frame, (frame.shape[1]/2, frame.shape[0]/2))
	keypoints, desc = sift.detectAndCompute(frame, None)
	matches = matcher.knnMatch(queryDescriptors=base_desc, trainDescriptors=desc, k=2)
	good = []
	for m, n in matches:
		if m.distance < 0.6*n.distance:
			good.append(m)

	src_pts = numpy.float32([keypoints[m.trainIdx].pt for m in good]).reshape(-1,1,2)
	dst_pts = numpy.float32([base_keypoints[m.queryIdx].pt for m in good]).reshape(-1,1,2)

	try:
		H, _ = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 5.0)
	except Exception as e:
		print 'warning: exception on frame {}: {}'.format(frame_idx, e)
		db.execute("UPDATE video_frames SET bounds = '' WHERE id = %s", [frame_id])
		continue

	if H is None:
		db.execute("UPDATE video_frames SET bounds = '' WHERE id = %s", [frame_id])
		continue

	bound_points = numpy.array([
		[0, 0],
		[frame.shape[1], 0],
		[frame.shape[1], frame.shape[0]],
		[0, frame.shape[0]],
	], dtype='float32').reshape(-1, 1, 2)
	transformed_points = cv2.perspectiveTransform(bound_points, H)
	poly_str = points_to_poly_str(transformed_points[:, 0, :])
	db.execute("UPDATE video_frames SET bounds = %s WHERE id = %s", [poly_str, frame_id])

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
