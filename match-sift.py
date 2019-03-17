from discoverlib import geom
import get_db

import cv2
import json
import math
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

for fname in os.listdir(FRAME_PATH):
	if '.jpg' not in fname:
		continue
	frame_idx = int(fname.split('.jpg')[0])

	db.execute(
		"SELECT detections.id, frame_polygon FROM detections, video_frames " +
		"WHERE video_frames.video_id = %s AND video_frames.idx = %s AND video_frames.id = detections.frame_id AND detections.polygon IS NULL",
		[video_id, frame_idx]
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

	if not points:
		continue

	print 'process {}'.format(frame_idx)
	frame = scipy.ndimage.imread(FRAME_PATH + fname)
	frame = cv2.resize(frame, (frame.shape[1]/2, frame.shape[0]/2))
	keypoints, desc = sift.detectAndCompute(frame, None)
	matches = matcher.knnMatch(queryDescriptors=base_desc, trainDescriptors=desc, k=2)
	good = []
	for m, n in matches:
		if m.distance < 0.6*n.distance:
			good.append(m)

	src_pts = numpy.float32([keypoints[m.trainIdx].pt for m in good]).reshape(-1,1,2)
	dst_pts = numpy.float32([base_keypoints[m.queryIdx].pt for m in good]).reshape(-1,1,2)
	H, _ = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 5.0)

	if H is None:
		for detection_id, _ in detections:
			db.execute("UPDATE detections SET polygon = '' WHERE id = %s", [detection_id])
		continue

	points = numpy.array(points, dtype='float32').reshape(-1, 1, 2)
	transformed_points = cv2.perspectiveTransform(points, H)
	i = 0
	for detection_id, num_points in detections:
		poly_points = transformed_points[i:i+num_points, 0, :]
		poly_strs = ['{},{}'.format(poly_points[j, 0], poly_points[j, 1]) for j in xrange(poly_points.shape[0])]
		poly_str = ' '.join(poly_strs)
		db.execute("UPDATE detections SET polygon = %s WHERE id = %s", [poly_str, detection_id])
		print poly_str, detection_id
		i += num_points
	assert i == transformed_points.shape[0]
