set enable_multimodel=off;
CREATE TS DATABASE db_pipec;
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

CREATE TABLE pipec_r.pipeline_info (
                                       pipeline_sn varchar(16) PRIMARY KEY,
                                       pipeline_name varchar(60),
                                       pipe_start varchar(80),
                                       pipe_end varchar(80),
                                       pipe_properties varchar(30));
CREATE INDEX pipeline_sn_index ON pipec_r.pipeline_info (pipeline_sn);
CREATE INDEX pipeline_name_index ON pipec_r.pipeline_info (pipeline_name);

CREATE TABLE pipec_r.point_info (
                                    point_sn varchar(64) PRIMARY KEY,
                                    signal_code varchar(120),
                                    signal_description varchar(200),
                                    signal_type varchar(50),
                                    station_sn varchar(16),
                                    pipeline_sn varchar(16));

CREATE INDEX point_station_sn_index ON pipec_r.point_info(station_sn);
CREATE INDEX point_pipeline_sn_index ON pipec_r.point_info(pipeline_sn);

CREATE TABLE pipec_r.workarea_info (
                                       work_area_sn varchar(16) PRIMARY KEY,
                                       work_area_name varchar(80),
                                       work_area_location varchar(64),
                                       work_area_description varchar(128));
CREATE INDEX workarea_name_index ON pipec_r.workarea_info(work_area_name);

CREATE TABLE pipec_r.company_info (
                                      sub_company_sn varchar(32) PRIMARY KEY,
                                      sub_company_name varchar(50),
                                      sub_compnay_description varchar(128));
CREATE INDEX sub_company_name_index ON pipec_r.company_info(sub_company_name);

set cluster setting ts.sql.query_opt_mode=11111;

SELECT si.workarea_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     pipec_r.pipeline_info li,
     pipec_r.point_info pi,
     db_pipec.t_point t
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'
GROUP BY si.workarea_name,
         si.station_name,
         t.measure_type,
         timebucket;

explain SELECT si.workarea_name,
               si.station_name,
               t.measure_type,
               time_bucket(t.k_timestamp, '10s') as timebucket,
               AVG(t.measure_value) AS avg_value,
               MAX(t.measure_value) AS max_value,
               MIN(t.measure_value) AS min_value,
               COUNT(t.measure_value) AS number_of_values
        FROM pipec_r.station_info si,
             pipec_r.workarea_info wi,
             pipec_r.pipeline_info li,
             pipec_r.point_info pi,
             db_pipec.t_point t
        WHERE li.pipeline_sn = pi.pipeline_sn
          AND pi.station_sn = si.station_sn
          AND si.work_area_sn = wi.work_area_sn
          AND t.point_sn = pi.point_sn
          AND li.pipeline_name = 'pipeline_1'
          AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
          AND t.k_timestamp >= '2023-08-01 01:00:00'
        GROUP BY si.workarea_name,
                 si.station_name,
                 t.measure_type,
                 timebucket;

set cluster setting ts.sql.query_opt_mode=01110;
drop DATABASE db_pipec cascade;
drop DATABASE pipec_r cascade;
set enable_multimodel=on;