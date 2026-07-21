--- for publication
-- create relational database and table
DROP DATABASE IF EXISTS rdb CASCADE; -- ok
CREATE DATABASE rdb; -- ok
CREATE TABLE rdb.sensor_data1 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
);
-- create ts database and tables
DROP DATABASE IF EXISTS tsdb1 CASCADE; -- ok
CREATE TS DATABASE tsdb1; -- ok
CREATE TABLE tsdb1.sensor_data1 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id); -- ok

CREATE TABLE tsdb1.sensor_data2 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id); -- ok

CREATE TABLE tsdb1.sensor_data3 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id); -- ok

CREATE TABLE tsdb1.sensor_data4 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id); -- ok

CREATE TABLE tsdb1.sensor_data5 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id); -- ok

CREATE TABLE tsdb1.sensor_data6 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30)
) PRIMARY TAGS (sensor_id); -- ok

CREATE TABLE tsdb1.sensor_data7 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30)
) PRIMARY TAGS (sensor_id); -- ok

CREATE TABLE tsdb1.sensor_data8 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30)
) PRIMARY TAGS (sensor_id); -- ok

DROP DATABASE IF EXISTS tsdb2 CASCADE; -- ok
CREATE TS DATABASE tsdb2; -- ok
CREATE TABLE tsdb2.sensor_data1 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id); -- ok

CREATE TABLE tsdb2.sensor_data2 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id); -- ok

INSERT INTO tsdb1.sensor_data1(k_timestamp,temperature,humidity,pressure,sensor_id,sensor_type) VALUES (now(), 35.1, 0.81, 1.001, 10, 'sensor'); -- ok
INSERT INTO tsdb1.sensor_data1(k_timestamp,temperature,humidity,pressure,sensor_id,sensor_type) VALUES (now(), 37.2, 0.82, 1.002, 10, 'sensor'); -- ok

SELECT count(*) FROM tsdb1.sensor_data1; -- ok. 2 rows

-- create publications on table
CREATE PUB pub_sensor_data FOR TABLE tsdb1.sensor_data1; -- ok
CREATE PUB pub_table_rdb FOR TABLE rdb.sensor_data1; -- error
CREATE PUB pub_table_filter FOR TABLE tsdb1.sensor_data1 WHERE temperature > 36.8; -- error, for table_list does not support WHERE.
CREATE PUB pub_table_option_publish FOR TABLE tsdb1.sensor_data1 WITH OPTIONS(publish='insert, update'); -- ok
CREATE PUB pub_table_option_buffer_size FOR TABLE tsdb1.sensor_data1 WITH OPTIONS(buffer_size='2'); -- ok
CREATE PUB pub_table_option_sub_timeout FOR TABLE tsdb1.sensor_data1 WITH OPTIONS(sub_timeout='5'); -- ok
CREATE PUB pub_table_option_retrieve_tags FOR TABLE tsdb1.sensor_data1 WITH OPTIONS(retrieve_tags='off'); -- ok
CREATE PUB pub_table_option_all FOR TABLE tsdb1.sensor_data1 WITH OPTIONS(publish='insert, update',buffer_size='2',sub_timeout='5',retrieve_tags='off'); -- ok
CREATE PUB pub_table_option_publish_error FOR TABLE tsdb1.sensor_data1 WITH OPTIONS(publish='insert, update, select '); -- error
CREATE PUB pub_table_multi FOR TABLE tsdb1.sensor_data1, tsdb1.sensor_data2; -- ok
CREATE PUB pub_table_multi_2 FOR TABLE tsdb1.sensor_data1, tsdb2.sensor_data1; -- ok
CREATE PUB pub_table_multi_filter FOR TABLE tsdb1.sensor_data1, tsdb1.sensor_data2 WHERE temperature > 36.8; -- error. not support WHERE for multiple tables
CREATE PUB pub_table_multi_option FOR TABLE tsdb1.sensor_data1, tsdb1.sensor_data2 WITH OPTIONS(publish='insert, update',buffer_size='2',sub_timeout='5',retrieve_tags='off'); -- ok

DROP DATABASE tsdb1;
ALTER TABLE tsdb1.sensor_data1 ADD COLUMN c1 FLOAT; -- error
DROP TABLE tsdb1.sensor_data1; -- error
ALTER TABLE tsdb1.sensor_data3 ADD COLUMN c1 FLOAT; -- ok
DROP TABLE tsdb1.sensor_data3; -- ok
CREATE TABLE tsdb1.sensor_data3 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id); -- ok

SELECT name, pub_objects, create_by FROM [SHOW PUB pub_sensor_data]; -- ok. 1 row.
ALTER PUB pub_sensor_data SET TABLE tsdb1.sensor_data2; -- ok
SELECT name, pub_objects, create_by FROM [SHOW PUB pub_sensor_data]; -- ok
ALTER PUB pub_sensor_data SET TABLE tsdb1.sensor_data1(k_timestamp,temperature); -- ok
SELECT name, pub_objects, create_by FROM [SHOW PUB pub_sensor_data]; -- ok
ALTER PUB pub_sensor_data SET TABLE tsdb1.sensor_data1(k_timestamp,temperature) WHERE temperature > 36.8; -- ok
SELECT name, pub_objects, create_by FROM [SHOW PUB pub_sensor_data]; -- ok
ALTER PUB pub_sensor_data SET OPTIONS(publish='insert, update'); -- ok
ALTER PUB pub_sensor_data SET OPTIONS(publish='insert, update, select '); -- error
ALTER PUB pub_sensor_data SET OPTIONS(buffer_size='2'); -- ok
ALTER PUB pub_sensor_data SET OPTIONS(sub_timeout='5'); -- ok
ALTER PUB pub_sensor_data SET OPTIONS(retrieve_tags='off'); -- ok
ALTER PUB pub_sensor_data SET OPTIONS(publish='insert',buffer_size='1',sub_timeout='3',retrieve_tags='on'); -- ok
ALTER PUB pub_table_multi SET TABLE tsdb1.sensor_data2; -- error
ALTER PUB pub_table_multi_2 SET TABLE tsdb1.sensor_data2; -- error

SELECT name, pub_objects, create_by FROM [SHOW PUBS] ORDER BY name; -- ok. 9 rows.

SELECT convert_from(pub_sub_get_info('pub_sensor_not'),'UTF8');
SELECT convert_from(pub_sub_get_info('pub_sensor_data'),'UTF8');
SELECT convert_from(pub_sub_get_info('pub_table_option_publish'),'UTF8');
SELECT convert_from(pub_sub_get_info('pub_table_multi'),'UTF8');
SELECT convert_from(pub_sub_get_info('pub_table_option_all'),'UTF8');

SELECT * FROM pub_sub_history('pub_sensor_not','{}');
SELECT * FROM pub_sub_history('pub_sensor_data','{}');
SELECT * FROM pub_sub_history('pub_sensor_data','"table_list":[{"database":"tsdb1","schema":"public","table":"sensor_data111"}]}');
SELECT * FROM pub_sub_history('pub_sensor_data','{"table_list":[{"database":"tsdb1","schema":"public","table":"sensor_data111"}]}');
SELECT * FROM pub_sub_history('pub_sensor_data','{"table_list":[{"database":"tsdb1","schema":"public","table":"sensor_data1"},{"database":"tsdb1","schema":"public","table":"sensor_data111"}]}');
SELECT "table_name", "data_type", "format", "row_number"
FROM pub_sub_history('pub_sensor_data', '{"cluster_id ":"","table_list":[{"database":"tsdb1","schema":"public","table":"sensor_data1","low_watermark":0}]}');

SELECT * FROM pub_sub_realtime('pub_sensor_not','{}');
SELECT * FROM pub_sub_realtime('pub_sensor_data','"table_list":[{"database":"tsdb1","schema":"public","table":"sensor_data111"}]}');

DROP PUB IF EXISTS pub_sensor_data;
DROP PUB IF EXISTS pub_table_option_publish;
DROP PUB IF EXISTS pub_table_option_buffer_size;
DROP PUB IF EXISTS pub_table_option_sub_timeout;
DROP PUB IF EXISTS pub_table_option_retrieve_tags;
DROP PUB IF EXISTS pub_table_option_all;
DROP PUB IF EXISTS pub_table_multi;
DROP PUB IF EXISTS pub_table_multi_2;
DROP PUB IF EXISTS pub_table_multi_option;
SELECT name, pub_objects, create_by FROM [SHOW PUBS] ORDER BY name; -- ok. 0 row.

-- create publications on column
CREATE PUB pub_column FOR TABLE tsdb1.sensor_data1(k_timestamp,temperature); -- ok
CREATE PUB pub_column_star FOR TABLE tsdb1.sensor_data1(*); -- ok
CREATE PUB pub_column_filter FOR TABLE tsdb1.sensor_data1(k_timestamp,temperature,humidity,pressure) WHERE temperature > 35.5; -- ok
CREATE PUB pub_column_option_publish FOR TABLE tsdb1.sensor_data1(k_timestamp,temperature,humidity,pressure) WITH OPTIONS(publish='insert, update'); -- ok
CREATE PUB pub_column_option_buffer_size FOR TABLE tsdb1.sensor_data1(k_timestamp,temperature,humidity,pressure) WITH OPTIONS(buffer_size='2'); -- ok
CREATE PUB pub_column_option_sub_timeout FOR TABLE tsdb1.sensor_data1(k_timestamp,temperature,humidity,pressure) WITH OPTIONS(sub_timeout='5'); -- ok
CREATE PUB pub_column_option_retrieve_tags FOR TABLE tsdb1.sensor_data1(k_timestamp,temperature,humidity,pressure) WITH OPTIONS(retrieve_tags='off'); -- ok
CREATE PUB pub_column_option_all FOR TABLE tsdb1.sensor_data1(k_timestamp,temperature,humidity,pressure) WITH OPTIONS(publish='insert, update',buffer_size='2',sub_timeout='5',retrieve_tags='off'); -- ok
CREATE PUB pub_column_option_publish_error FOR TABLE tsdb1.sensor_data1(k_timestamp,temperature,humidity,pressure) WITH OPTIONS(publish='insert, update, select '); -- error
CREATE PUB pub_table_option_publish_error FOR TABLE tsdb1.sensor_data7(*) WITH OPTIONS(publish='insert, update, delete, import'); -- error

SELECT name, pub_objects, create_by FROM [SHOW PUB pub_column]; -- ok. 1 row.
SELECT name, pub_objects, create_by FROM [SHOW PUBS] ORDER BY name; -- ok. 8 rows.
DROP PUB IF EXISTS pub_column;
DROP PUB IF EXISTS pub_column_star;
DROP PUB IF EXISTS pub_column_filter;
DROP PUB IF EXISTS pub_column_option_publish;
DROP PUB IF EXISTS pub_column_option_buffer_size;
DROP PUB IF EXISTS pub_column_option_sub_timeout;
DROP PUB IF EXISTS pub_column_option_retrieve_tags;
DROP PUB IF EXISTS pub_column_option_all;
SELECT name, pub_objects, create_by FROM [SHOW PUBS] ORDER BY name; -- ok. 0 row.

-- create publications on database
CREATE PUB pub_db FOR DATABASE tsdb1; -- ok
CREATE PUB pub_db_filter FOR DATABASE tsdb1 WHERE k_timestamp > '2025-07-17 02:08:19.302+00:00'; -- error. not support WHERE for DATABSE
CREATE PUB pub_db_option FOR DATABASE tsdb1 WITH OPTIONS(publish='insert, update',buffer_size='2',sub_timeout='5',retrieve_tags='off'); -- ok
ALTER PUB pub_db SET TABLE tsdb2.sensor_data1; -- error
ALTER PUB pub_db_option SET OPTIONS(publish='insert',buffer_size='1',sub_timeout='3',retrieve_tags='on'); -- ok

ALTER TABLE tsdb1.sensor_data3 ADD COLUMN c2 FLOAT; -- error
DROP TABLE tsdb1.sensor_data3; -- error
CREATE TABLE tsdb1.sensor_data9 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id); -- error

SELECT name, pub_objects, create_by FROM [SHOW PUB pub_db]; -- ok. 1 row.
SELECT name, pub_objects, create_by FROM [SHOW PUBS] ORDER BY name; -- ok. 2 rows.
DROP PUB IF EXISTS pub_db;
DROP PUB IF EXISTS pub_db_option;
SELECT name, pub_objects, create_by FROM [SHOW PUBS] ORDER BY name; -- ok. 0 row.

CREATE TABLE tsdb1.sensor_data9 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id); -- ok
-- create role/user and grant privilege
CREATE USER IF NOT EXISTS user_sub; -- ok
GRANT SELECT ON TABLE tsdb1.sensor_data1 TO user_sub; -- ok
GRANT SELECT ON TABLE tsdb1.sensor_data2 TO user_sub; -- ok
GRANT SELECT ON TABLE tsdb1.sensor_data3 TO user_sub; -- ok
GRANT SELECT ON TABLE tsdb1.sensor_data4 TO user_sub; -- ok
GRANT SELECT ON TABLE tsdb1.sensor_data5 TO user_sub; -- ok
GRANT SELECT ON TABLE tsdb1.sensor_data6 TO user_sub; -- ok
GRANT SELECT ON TABLE tsdb1.sensor_data7 TO user_sub; -- ok
GRANT SELECT ON TABLE tsdb1.sensor_data8 TO user_sub; -- ok
GRANT SELECT ON TABLE tsdb2.sensor_data1 TO user_sub; -- ok
GRANT SELECT ON TABLE tsdb2.sensor_data2 TO user_sub; -- ok

-- create publication for subscription
CREATE PUB pub_1 FOR TABLE tsdb1.sensor_data1; -- ok
CREATE PUB pub_1_star FOR TABLE tsdb1.sensor_data1(*); -- ok
CREATE PUB pub_2 FOR TABLE tsdb1.sensor_data2(k_timestamp,temperature,humidity,sensor_id,sensor_type) WITH OPTIONS(retrieve_tags='on'); -- ok
CREATE PUB pub_3 FOR TABLE tsdb1.sensor_data3,tsdb1.sensor_data4; -- ok
CREATE PUB pub_4 FOR DATABASE tsdb2; -- ok
CREATE PUB pub_5 FOR TABLE tsdb1.sensor_data5(k_timestamp,temperature,humidity,sensor_id,sensor_type) WHERE temperature > 37.2 AND humidity > 0.95 WITH OPTIONS(retrieve_tags='on'); -- ok
CREATE PUB pub_6 FOR TABLE tsdb1.sensor_data6(k_timestamp,temperature,sensor_id,sensor_type) WITH OPTIONS(retrieve_tags='on'); -- ok
CREATE PUB pub_7 FOR TABLE tsdb1.sensor_data6(k_timestamp,temperature,sensor_id) WHERE sensor_type='sensor' WITH OPTIONS(retrieve_tags='on'); -- ok
CREATE PUB pub_8 FOR TABLE tsdb1.sensor_data7(*) WITH OPTIONS(publish='insert, update, delete'); -- ok
SELECT name, pub_objects, create_by FROM [SHOW PUBS] ORDER BY name; -- ok. 8 rows.

DROP PUB IF EXISTS pub_1; -- ok
DROP PUB IF EXISTS pub_1_star; -- ok
DROP PUB IF EXISTS pub_2; -- ok
DROP PUB IF EXISTS pub_3; -- ok
DROP PUB IF EXISTS pub_4; -- ok
DROP PUB IF EXISTS pub_5; -- ok
DROP PUB IF EXISTS pub_6; -- ok
DROP PUB IF EXISTS pub_7; -- ok
DROP PUB IF EXISTS pub_8; -- ok
DROP PUB IF EXISTS pub_11; -- ok

DROP DATABASE IF EXISTS rdb CASCADE;
DROP DATABASE IF EXISTS tsdb1 CASCADE;
DROP DATABASE IF EXISTS tsdb2 CASCADE;

DROP ROLE IF EXISTS user_sub;