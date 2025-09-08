CREATE TS DATABASE db_pipec;
CREATE TABLE db_pipec.t_point(k_timestamp TIMESTAMP NOT NULL,measure_value DOUBLE) ATTRIBUTES (point_sn VARCHAR(64) NOT NULL,sub_com_sn VARCHAR(32),work_area_sn VARCHAR(16), station_sn VARCHAR(16),pipeline_sn VARCHAR(16),measure_type SMALLINT, measure_location varchar(64)) PRIMARY TAGS (point_sn) ACTIVETIME 0s;
use db_pipec;

explain SELECT t.point_sn, count(*) FROM t_point t WHERE t.work_area_sn = 'work_area_sn_1' GROUP BY t.point_sn ORDER BY t.point_sn;
explain SELECT t.point_sn, last_row(t.measure_value) FROM t_point t WHERE t.work_area_sn = 'work_area_sn_1' GROUP BY t.point_sn ORDER BY t.point_sn;

drop database db_pipec cascade;
