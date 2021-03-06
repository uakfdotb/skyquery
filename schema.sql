CREATE TABLE videos (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	filename VARCHAR(255) NOT NULL,
	processed TINYINT(1) NOT NULL DEFAULT 0,
	start_location VARCHAR(2048) NOT NULL,
	start_time TIMESTAMP NOT NULL,
	preprocessed TINYINT(1) NOT NULL DEFAULT 0
);

CREATE TABLE video_frames (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	video_id INT DEFAULT NULL,
	idx INT NOT NULL,
	time TIMESTAMP NOT NULL,
	homography VARCHAR(2048) DEFAULT NULL,
	bounds VARCHAR(2048) DEFAULT NULL,
	enabled TINYINT(1) DEFAULT 1
);
CREATE INDEX video_id ON video_frames (video_id);

CREATE TABLE dataframes (
	name VARCHAR(16) NOT NULL PRIMARY KEY,
	parents VARCHAR(255) NOT NULL,
	op_type VARCHAR(16) NOT NULL,
	operands VARCHAR(2048) NOT NULL,
	seq INT NOT NULL DEFAULT 0,
	rerun_time TIMESTAMP NOT NULL DEFAULT '1971-01-01 00:00:00'
);

CREATE TABLE detections (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	dataframe VARCHAR(16) NOT NULL,
	time TIMESTAMP NOT NULL,
	frame_polygon VARCHAR(2048) NOT NULL,
	polygon VARCHAR(2048) DEFAULT NULL,
	frame_id INT NOT NULL
);
CREATE INDEX frame_id ON detections (frame_id);
CREATE INDEX dataframe ON detections (dataframe);

CREATE TABLE sequences (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	dataframe VARCHAR(16) NOT NULL,
	time TIMESTAMP NOT NULL,
	terminated_at TIMESTAMP NULL DEFAULT NULL
);
CREATE INDEX dataframe ON sequences (dataframe);

CREATE TABLE sequence_metadata (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	sequence_id INT NOT NULL,
	time TIMESTAMP NOT NULL,
	metadata VARCHAR(2048) NOT NULL DEFAULT ''
);
CREATE INDEX sequence_id ON sequence_metadata (sequence_id);

CREATE TABLE sequence_members (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	sequence_id INT NOT NULL,
	detection_id INT NOT NULL,
	time TIMESTAMP NOT NULL
);
CREATE INDEX sequence_id ON sequence_members (sequence_id);

CREATE TABLE matrix_data (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	dataframe VARCHAR(16) NOT NULL,
	time TIMESTAMP NOT NULL,
	i INT NOT NULL,
	j INT NOT NULL,
	val INT NOT NULL,
	metadata VARCHAR(2048) NOT NULL DEFAULT ''
);
CREATE INDEX dataframe ON matrix_data (dataframe);
CREATE INDEX cell ON matrix_data (i, j);

CREATE TABLE pending_routes (
	id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
	drone_id INT NOT NULL,
	i INT NOT NULL,
	j INT NOT NULL
);
