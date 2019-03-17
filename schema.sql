CREATE TABLE videos (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	filename VARCHAR(255) NOT NULL,
	processed TINYINT(1) NOT NULL DEFAULT 0,
	start_location VARCHAR(2048) NOT NULL,
	start_time TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
	preprocessed TINYINT(1) NOT NULL DEFAULT 0,
);

CREATE TABLE video_frames (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	video_id INT NOT NULL,
	idx INT NOT NULL,
	time timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
	homography varchar(2048) DEFAULT NULL
);
CREATE INDEX video_id ON video_frames (video_id);

CREATE TABLE det_dataframes (
	name VARCHAR(16) NOT NULL PRIMARY KEY,
	region VARCHAR(2048) NOT NULL,
	model VARCHAR(16) NOT NULL
);

CREATE TABLE seq_dataframes (
	name VARCHAR(16) NOT NULL PRIMARY KEY,
	parent VARCHAR(16) NOT NULL,
	op_type VARCHAR(16) NOT NULL,
	operands VARCHAR(2048) NOT NULL
);

CREATE TABLE detections (
	id INT NOT NULL AUTO_INCREMENT,
	dataframe VARCHAR(16) NOT NULL,
	time TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
	frame_polygon VARCHAR(2048) NOT NULL,
	polygon VARCHAR(2048) DEFAULT NULL,
	frame_id INT NOT NULL,
);
CREATE INDEX frame_id ON detections (frame_id);
CREATE INDEX dataframe ON detections (dataframe);

CREATE TABLE sequences (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	dataframe VARCHAR(16) NOT NULL,
	terminated_at TIMESTAMP NULL DEFAULT NULL
);
CREATE INDEX dataframe ON sequences (dataframe);

CREATE TABLE sequence_members (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	sequence_id INT NOT NULL,
	detection_id INT NOT NULL,
	metadata VARCHAR(2048),
	created_at TIMESTAMP DEFAULT '0000-00-00 00:00:00'
);
CREATE INDEX sequence_id ON sequence_members (sequence_id);
