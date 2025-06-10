CREATE TS DATABASE db_pipec;

-- 时序表
CREATE TABLE db_pipec.t_point (
k_timestamp timestamp NOT NULL,
measure_value double
) ATTRIBUTES (
point_sn varchar(64) NOT NULL,
sub_com_sn varchar(32),
work_area_sn varchar(16),
station_sn varchar(16),
pipeline_sn varchar(16) not null,
measure_type smallint,
measure_location varchar(64))
PRIMARY TAGS(point_sn)
ACTIVETIME 3h;
insert into db_pipec.t_point values('2024-08-27 11:00:00',10.5,'a','b','c','d','e',1,'f');
insert into db_pipec.t_point values('2024-08-27 12:00:00',11.5,'a1','b1','c1','d1','e1',1,'f1');
insert into db_pipec.t_point values('2024-08-27 10:00:00',12.5,'a2','b2','c2','d2','e2',2,'f2');
insert into db_pipec.t_point values('2024-08-26 10:00:00',13.5,'a3','b3','c3','d3','e3',2,'f3');
insert into db_pipec.t_point values('2024-08-28 10:00:00',14.5,'a4','b4','c4','d4','e4',3,'f4');
insert into db_pipec.t_point values('2024-08-29 10:00:00',15.5,'a5','b5','c5','d5','e5',3,'f5');
insert into db_pipec.t_point values('2024-08-28 11:00:00',10.5,'a6','b1','c1','d1','e1',4,'f1');
insert into db_pipec.t_point values('2024-08-28 12:00:00',11.5,'a7','b1','c1','d1','e1',4,'f1');

-- 关系表
CREATE DATABASE pipec_r;
CREATE TABLE pipec_r.station_info (
station_sn varchar(16) PRIMARY KEY,
station_name varchar(80),
work_area_sn varchar(16),
workarea_name varchar(80),
sub_company_sn varchar(32),
sub_company_name varchar(50));
CREATE INDEX station_sn_index ON pipec_r.station_info(work_area_sn);
CREATE INDEX station_name_index ON pipec_r.station_info(workarea_name);

insert into pipec_r.station_info values('d','dd','a','aa','b','bb');
insert into pipec_r.station_info values('d1','dd','a1','aa','b','bb');
insert into pipec_r.station_info values('d2','dd','a2','aa','b','bb');
insert into pipec_r.station_info values('d3','dd','a3','aa','b','bb');
insert into pipec_r.station_info values('d4','dd','a4','aa','b','bb');
insert into pipec_r.station_info values('d5','dd','a5','aa','b','bb');


CREATE TABLE pipec_r.pipeline_info (
pipeline_sn varchar(16) PRIMARY KEY,
pipeline_name varchar(60),
pipe_start varchar(80),
pipe_end varchar(80),
pipe_properties varchar(30));
CREATE INDEX pipeline_sn_index ON pipec_r.pipeline_info (pipeline_sn);
CREATE INDEX pipeline_name_index ON pipec_r.pipeline_info (pipeline_name);
insert into pipec_r.pipeline_info values('e','ee','a','aa','b');
insert into pipec_r.pipeline_info values('e1','ee','a','aa','b');
insert into pipec_r.pipeline_info values('e2','ee','a','aa','b');
insert into pipec_r.pipeline_info values('e3','ee','a','aa','b');
insert into pipec_r.pipeline_info values('e4','ee','a','aa','b');
insert into pipec_r.pipeline_info values('e5','ee','a','aa','b');

CREATE TABLE pipec_r.point_info (
point_sn varchar(64) PRIMARY KEY,
signal_code varchar(120),
signal_description varchar(200),
signal_type varchar(50),
station_sn varchar(16),
pipeline_sn varchar(16));
insert into pipec_r.point_info values('a','ee','a','aa','d','e');
insert into pipec_r.point_info values('a1','ee','a','aa','d','e');
insert into pipec_r.point_info values('a2','ee','a','aa','d','e');
insert into pipec_r.point_info values('a3','ee','a','aa','d','e');
insert into pipec_r.point_info values('a4','ee','a','aa','d','e');
insert into pipec_r.point_info values('a5','ee','a','aa','d','e');

SELECT
station_sn,
COUNT(DISTINCT point_sn) AS abnormal_point_count
FROM
db_pipec.t_point
WHERE
pipeline_sn = 'e1'
AND measure_type = 4
AND k_timestamp >= '2024-08-25 10:00:00'
AND k_timestamp <= '2024-08-30 10:00:00'
AND measure_value < 1* (
SELECT AVG(measure_value)
FROM db_pipec.t_point
WHERE pipeline_sn = 'e1'
AND measure_type = 4)
GROUP BY
station_sn
ORDER BY
abnormal_point_count DESC
LIMIT 1;

explain SELECT
            station_sn,
            COUNT(DISTINCT point_sn) AS abnormal_point_count
        FROM
            db_pipec.t_point
        WHERE
                pipeline_sn = 'e1'
          AND measure_type = 4
          AND k_timestamp >= '2024-08-25 10:00:00'
          AND k_timestamp <= '2024-08-30 10:00:00'
          AND measure_value < 1* (
            SELECT AVG(measure_value)
            FROM db_pipec.t_point
            WHERE pipeline_sn = 'e1'
              AND measure_type = 4)
        GROUP BY
            station_sn
        ORDER BY
            abnormal_point_count DESC
            LIMIT 1;

SELECT COUNT(*) AS max_sample_count,
AVG(t.measure_value) AS avg_value,
t.measure_value AS first_n_max_value
FROM db_pipec.t_point t
WHERE t.point_sn = (
SELECT t.point_sn AS point_sn
FROM db_pipec.t_point t
WHERE k_timestamp > '2024-01-01 00:00:00'
AND   k_timestamp < '2025-04-01 00:00:00'
AND  t.work_area_sn = 'c1'
GROUP BY t.point_sn
ORDER BY point_sn DESC
LIMIT 1
) AND t.work_area_sn = 'c1'
GROUP BY t.measure_value
ORDER BY t.measure_value DESC LIMIT 10;

explain SELECT COUNT(*) AS max_sample_count,
               AVG(t.measure_value) AS avg_value,
               t.measure_value AS first_n_max_value
        FROM db_pipec.t_point t
        WHERE t.point_sn = (
            SELECT t.point_sn AS point_sn
            FROM db_pipec.t_point t
            WHERE k_timestamp > '2024-01-01 00:00:00'
              AND   k_timestamp < '2025-04-01 00:00:00'
              AND  t.work_area_sn = 'c1'
            GROUP BY t.point_sn
            ORDER BY COUNT(*) DESC
            LIMIT 1
            ) AND t.work_area_sn = 'c1'
        GROUP BY t.measure_value
        ORDER BY t.measure_value DESC LIMIT 10;

-- subquery in render
select measure_value, point_sn,point_sn in (select max(point_sn) from db_pipec.t_point ) from db_pipec.t_point order by point_sn;
explain select measure_value, point_sn,point_sn in (select max(point_sn) from db_pipec.t_point ) from db_pipec.t_point order by point_sn;

select measure_value, point_sn, exists (select max(point_sn) from db_pipec.t_point ) from db_pipec.t_point order by point_sn;
explain select measure_value, point_sn, exists (select max(point_sn) from db_pipec.t_point ) from db_pipec.t_point order by point_sn;

select measure_value, point_sn, (select max(point_sn) from db_pipec.t_point ) from db_pipec.t_point order by point_sn;
explain select measure_value, point_sn,  (select max(point_sn) from db_pipec.t_point ) from db_pipec.t_point order by point_sn;

-- array can not push
select measure_value,point_sn, array (select point_sn from db_pipec.t_point order by point_sn) from db_pipec.t_point order by point_sn;
explain select measure_value,point_sn, array (select point_sn from db_pipec.t_point order by point_sn) from db_pipec.t_point order by point_sn;

-- subquery in filter
select measure_value, point_sn from db_pipec.t_point where point_sn = (select max(point_sn) from db_pipec.t_point) order by point_sn;
explain select measure_value, point_sn from db_pipec.t_point where point_sn = (select max(point_sn) from db_pipec.t_point) order by point_sn;

select measure_value, point_sn from db_pipec.t_point where  exists (select max(point_sn) from db_pipec.t_point) order by point_sn;
explain select measure_value, point_sn from db_pipec.t_point where  exists (select max(point_sn) from db_pipec.t_point) order by point_sn;

select measure_value, t.point_sn from db_pipec.t_point t, pipec_r.point_info p where t.point_sn = (select max(point_sn) from pipec_r.point_info) order by point_sn;
explain select measure_value, t.point_sn from db_pipec.t_point t, pipec_r.point_info p where t.point_sn = (select max(point_sn) from pipec_r.point_info) order by point_sn;

select max(measure_value), min(t.point_sn) from db_pipec.t_point t  group by  t.point_sn having (select max(point_sn) from pipec_r.point_info)>'a' order by point_sn;
explain select max(measure_value), min(t.point_sn) from db_pipec.t_point t  group by  t.point_sn having (select max(point_sn) from pipec_r.point_info)>'a' order by point_sn;

-- grouping will be opted
select max(measure_value), min(t.point_sn) from db_pipec.t_point t  group by  (select max(point_sn) from pipec_r.point_info);
explain select max(measure_value), min(t.point_sn) from db_pipec.t_point t  group by  (select max(point_sn) from pipec_r.point_info);

select max(measure_value), min(t.point_sn) from db_pipec.t_point t  group by  (select max(point_sn) from pipec_r.point_info) order by (select max(point_sn) from pipec_r.point_info);
explain select max(measure_value), min(t.point_sn) from db_pipec.t_point t  group by  (select max(point_sn) from pipec_r.point_info) order by (select max(point_sn) from pipec_r.point_info);

drop database pipec_r cascade;
drop database db_pipec cascade;

CREATE TS DATABASE tsdb;

CREATE TABLE tsdb.transformersuper (
                                       ts TIMESTAMPTZ NOT NULL,
                                       current FLOAT8 NULL,
                                       voltage FLOAT8 NULL,
                                       frequency FLOAT8 NULL,
                                       temperature FLOAT8 NULL
) TAGS (
    deviceid INT4 NOT NULL,
    modelid INT4 NOT NULL ) PRIMARY TAGS(deviceid, modelid);

CREATE DATABASE rdb;

CREATE TABLE rdb.devicemodel (
                                 modelid INT4 NOT NULL,
                                 typeid INT4 NULL,
                                 modelname VARCHAR(100) NULL,
                                 typename VARCHAR(100) NULL
);
SELECT ts FROM tsdb.transformersuper WHERE modelid = (SELECT max(modelid) FROM rdb.devicemodel);
explain SELECT ts FROM tsdb.transformersuper WHERE modelid = (SELECT max(modelid) FROM rdb.devicemodel);

SELECT ts FROM tsdb.transformersuper WHERE modelid = (SELECT max(modelid) FROM rdb.devicemodel) and deviceid = (SELECT max(modelid) FROM rdb.devicemodel);
explain SELECT ts FROM tsdb.transformersuper WHERE modelid = (SELECT max(modelid) FROM rdb.devicemodel) and deviceid = (SELECT max(modelid) FROM rdb.devicemodel);

drop database tsdb cascade;
drop database rdb cascade;
