CREATE TS DATABASE db_pipec;
use db_pipec;
set cluster setting sql.distsql.temp_storage.workmem='4096Mib';
set cluster setting sql.all_push_down.enabled=true;
set cluster setting sql.pg_encode_short_circuit.enabled = true;
set cluster setting ts.parallel_degree=8;
set cluster setting server.tsinsert_direct.enabled = true; 
set CLUSTER SETTING server.sql_connections.max_limit = 1000;
set cluster setting ts.dedup.rule="keep";

CREATE TABLE db_pipec.t_point (
  k_timestamp timestamp NOT NULL,
  measure_value double
 ) ATTRIBUTES (
    point_sn varchar(64) NOT NULL,
    sub_com_sn varchar(32),
    work_area_sn varchar(16),
    station_sn varchar(16),
    pipeline_sn varchar(16),
    measure_type smallint,
    measure_location varchar(64))
  PRIMARY TAGS(point_sn) 
  ACTIVETIME 3h;

CREATE DATABASE pipec_r;
CREATE TABLE pipec_r.station_info (
  station_sn varchar(16) PRIMARY KEY, 
  station_name varchar(80), 
  work_area_sn varchar(16), 
  sub_company_sn varchar(32), 
  station_location varchar(64),
  station_description varchar(128));
CREATE INDEX station_work_area_sn_index ON pipec_r.station_info(work_area_sn);
CREATE INDEX station_name_index ON pipec_r.station_info(station_name);

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