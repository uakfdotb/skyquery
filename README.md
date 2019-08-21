SkyQuery
========

SkyQuery is a query processing system for UAV visual sensing applications such
as traffic analysis, precision agriculture, and wildlife population management.
Users express queries that extract application-specific insights from video
captured by UAVs. SkyQuery dynamically optimizes query execution through a
query-driven probabilistic modeling framework that routes UAVs to prioritize
flights in regions where sensing results are most unpredictable.

For example, a parking monitoring application may seek to monitor the number of
parked cars in different parts of a city. Use cases include analyzing parking
behavior and helping drivers locate available spots. SkyQuery processes video
data and assigns UAV routes so that UAVs prioritize flights in regions where
parking is highly unpredictable. For example, UAVs may fly more frequently over
parking around retail outlets than over residential street parking.

SkyQuery is a research project and not a complete software system.

The SkyQuery paper is at https://favyen.com/skyquery.pdf


Getting Started
---------------

SkyQuery supports two modes of operation: processing a video offline to extract
insights, and routing UAVs in real-time in a simulation environment. Routing
physical UAVs in real-time requires integrating these two modes with an API
that controls the UAVs, which is possible but not supported in this codebase.

The instructions below primarily cover processing a video query offline on
video data stored on the filesystem. For the second mode, with the simulator,
see the router and simulator directories.

Setting up SkyQuery involves the following steps:

1. Capture a series of images over an area, along with some video over the same area.
2. Constructing an orthoimage by stitching together the images using OpenDroneMap.
3. Obtaining object detections and aligning video frames to the orthoimage.
4. Write a query that extracts some insights from video data. For example, the
number of parked cars in different parts of a city.
5. Apply the code in pipeline directory to process the query.

The sections below cover each of these steps.


Capturing Images and Video
--------------------------

In the next section we will use OpenDroneMap to stitch together the images and
build an orthoimage. The orthoimage is a single image that spans the area
covered by all of the individual images, and is orthorectified so that each
pixel corresponds to a longitude-latitude position.

The process will require images that have GPS position and camera angle
metadata set. If you collect the images with a consumer drone then it should
have this metadata. You should also make sure to capture images with
substantial overlap, e.g., 60-70% of an image should overlap with adjacent
images. If you use a DJI drone, there is a timed photo mode which keeps taking
photos at some frequency; this mode makes it easier to capture the images. You
could also use a planning app that allows you to plan a route ahead of time.

There are less requirements on the video. You may want to collect video at
1920x1080 resolution since that is what we use. The framerate should not be
important since we anyway sample the video at 5 fps by default.


Constructing Orthoimage with OpenDroneMap
-----------------------------------------

1. Install WebODM (https://github.com/OpenDroneMap/WebODM).
2. Create an orthoimage and surface model from the images captured in the previous step.
3. Save the Orthophoto (GeoTIFF) as ortho.tiff and the Surface Model GeoTIFF as surface.tiff.
These can be downloaded in WebODM from Project -> Task -> Download Assets.

Finally, run get-ortho.py to extract PNG data and mask the orthoimage. You may
need to adjust the surface threshold to match the surface height. This
threshold is used to mask out buildings and other features above surface level.
It may not work well if the area is not flat. But if there are no tall
buildings then you can skip the mask step.

	python get-ortho.py ortho.tiff surface.tiff ortho.png ortho-masked.png
	convert ortho.png -format jpg ortho.jpg
	convert ortho-masked.png -format jpg ortho-masked.jpg

Note that `convert` command is from ImageMagick.


Object Detections, Alignment
----------------------------

Next, we will preprocess a video to obtain object detections and align each
frame to the orthoimage. This is done in `process-video.py`. Note that this
script just runs three other commands (ffmpeg, `run-yolo.go`, and
`match-sift.py`), so you could also run those commands separately if desired.

Before we can proceed, though, we need to initialize MySQL database:

	git clone https://github.com/uakfdotb/skyquery.git
	echo 'CREATE DATABASE skyquery;' | mysql -u root -p
	echo "GRANT ALL ON skyquery.* TO 'skyquery'@'localhost' IDENTIFIED BY 'skyquery';" | mysql -u root -p
	cat skyquery/schema.sql | mysql -u root -p skyquery
	echo 'INSERT INTO videos (filename, start_time, start_location) VALUES ("video.mov", "2019-01-01 00:00:00", "");' | mysql -u root -p skyquery

Then, save the video as `skyquery/videos/video.mov`. Make sure the orthoimage is at
`skyquery/ortho-masked.jpg`.

OK we also need to set up YOLO model.
[Follow the darknet installation instructions](https://pjreddie.com/darknet/install/)
and put the `darknet/` folder in `skyquery/darknet/`. But you must use the git
repository at https://github.com/uakfdotb/darknet when installing darknet for a
one-line change that prints the bounding box to stdout.

The `run-yolo.go` program applies a YOLO model using the darknet binary on all
frames of a video. It requires a configuration file `yolo.cfg` and a darknet
model backup `yolo.backup`, both stored in `skyquery/yolo/`. The configuration
and backup files for our car detector model used in the paper are hosted on
[LunaNode](https://www.lunanode.com) and can be [downloaded here](http://lunanode-skyquery.lndyn.com/yolo.zip).

Then:

	cd skyquery
	mkdir frames
	go build run-yolo.go
	python process-video.py 1

Here it assumes the ID of row in videos table is 1. Or you can run each step manually:

	ffmpeg -i videos/video.mov -vf fps=5 frames/1/%06d.jpg
	go run run-yolo.go 1
	python match-sift.py 1

You may need to adjust paths in `run-yolo.go` or `match-sift.py`.

Now the video_frames and detections tables in your database should be
populated with some data.

Note: you may need to fetch dependencies for the Golang and Python code:

	sudo apt install -y libmetis-dev libmysqlclient-dev libsm6 libxrender1 libfontconfig1 ffmpeg
	export GOPATH=~/go
	go get github.com/mitroadmaps/gomapinfer/common
	go get github.com/cpmech/gosl/graph
	go get github.com/go-sql-driver/mysql
	sudo pip install numpy pillow scipy mysql-connector opencv-contrib-python

The Python code also uses our discoverlib code from RoadTracer project:

	cd skyquery/
	git clone https://github.com/mitroadmaps/roadtracer.git /tmp/roadtracer/
	mv /tmp/roadtracer/lib/discoverlib ./discoverlib


Write a Query
-------------

Queries are written by adding rows to the skyquery.dataframes table.
Consider this query that counts the number of parked cars:

	car_traj = obj_track(cars, mode=iou)
	tmp1 = filter(car_traj, length > 5)
	stopped_cars = filter(tmp1, displacement < 75)
	merged_cars = seq_merge(stopped_cars)
	parked_cars = filter(merged_cars, duration > 120)
	parked_counts = to_matrix(parked_cars, ignore_zero=yes, func=count_sum)

We can express it like this:

	sudo mysql -u root skyquery
	> INSERT INTO dataframes (name, op_type, operands, parents) VALUES ('cars', 'raw_detection', '', '');
	> INSERT INTO dataframes (name, op_type, operands, parents) VALUES ('car_traj', 'obj_track', 'mode=iou', 'cars');
	> INSERT INTO dataframes (name, op_type, operands, parents) VALUES ('tmp1', 'filter', 'left=length,op=>,right=5', 'car_traj');
	> INSERT INTO dataframes (name, op_type, operands, parents) VALUES ('stopped_cars', 'filter', 'left=displacement,op=<,right=75', 'tmp1');
	> INSERT INTO dataframes (name, op_type, operands, parents) VALUES ('merged_cars', 'seq_merge', '', 'stopped_cars');
	> INSERT INTO dataframes (name, op_type, operands, parents) VALUES ('parked_cars', 'filter', 'left=duration,op=>,right=120', 'merged_cars');
	> INSERT INTO dataframes (name, op_type, operands, parents) VALUES ('parked_counts', 'to_matrix', 'ignore_zero=yes,func=count_sum', 'parked_cars');


Apply Data Processor
--------------------

Running the data processor is straightforward:

	go run run-pipeline.go
