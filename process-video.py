import get_db

import os
import subprocess
import sys

video_id = int(sys.argv[1])

db = get_db.get_db()
db.execute("SELECT filename FROM videos WHERE id = %s", [video_id])
video_fname = db.fetchone()[0]

# get video frames
try:
	os.mkdir('frames/{}'.format(video_id))
except:
	pass
subprocess.call(['ffmpeg', '-i', 'videos/' + video_fname, '-vf', 'fps=5', 'frames/{}/%06d.jpg'.format(video_id)])

# run object detector
subprocess.call(['./run-yolo', str(video_id)])

# run sift-based frame matcher
subprocess.call(['./match-sift.py', str(video_id)])

db.execute("UPDATE videos SET preprocessed = 1 WHERE id = %s", [video_id])
