create database test_rel;
create table test_rel.rel_t1(c1 UUID NOT NULL DEFAULT gen_random_uuid(), c2 BIT, c3 INET, c4 JSONB, c5 INT ARRAY, c6 INT, c7 FLOAT, c8 TIMESTAMP);
                    
insert into test_rel.rel_t1(c2, c3, c4, c5, c6, c7, c8) values (B'1', '192.168.0.1', '{"type": "account creation", "username": "harvestboy93"}', ARRAY[10,20,30], 5,
75.12, TIMESTAMP '2024-01-04 14:32:01');
insert into test_rel.rel_t1(c2, c3, c4, c5, c6, c7, c8) values (B'0', '192.168.0.2', '{"type": "account creation", "username": "hungrygame"}', ARRAY[15,25,35], 4,
65.31, TIMESTAMP '2024-01-04 14:33:01');

create table if not exists runba_tra.channel_info (
channel_id varchar(20) not null,
companyid int not null
);

insert into runba_tra.channel_info select distinct channel, companyid  from runba.opcdata449600;

-- guanwang small ddl
CREATE TS DATABASE db_pipec_small;

-- ts table
CREATE TABLE db_pipec_small.t_point(
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

-- relation table
CREATE DATABASE pipec_r_small;
CREATE TABLE pipec_r_small.station_info (
  station_sn varchar(16) PRIMARY KEY,
  station_name varchar(80),
  work_area_sn varchar(16),
  sub_company_sn varchar(32),
  station_location varchar(64),
  station_description varchar(128));
CREATE INDEX station_work_area_sn_index ON pipec_r_small.station_info(work_area_sn);
CREATE INDEX station_name_index_small ON pipec_r_small.station_info(station_name);

CREATE TABLE pipec_r_small.workarea_info (
  work_area_sn varchar(16) PRIMARY KEY,
  work_area_name varchar(80),
  work_area_location varchar(64),
  work_area_description varchar(128));
CREATE INDEX workarea_name_index ON pipec_r_small.workarea_info(work_area_name);

CREATE TABLE pipec_r_small.company_info(
  sub_company_sn varchar(32) PRIMARY KEY,
  sub_company_name varchar(50),
  sub_compnay_description varchar(128));
CREATE INDEX sub_company_name_index ON pipec_r_small.company_info(sub_company_name);

CREATE TABLE pipec_r_small.pipeline_info (
  pipeline_sn varchar(16) PRIMARY KEY,
  pipeline_name varchar(60),
  pipe_start varchar(80),
  pipe_end varchar(80),
  pipe_properties varchar(30));
CREATE INDEX pipeline_sn_index ON pipec_r_small.pipeline_info (pipeline_sn);
CREATE INDEX pipeline_name_index ON pipec_r_small.pipeline_info (pipeline_name);

CREATE TABLE pipec_r_small.point_info (
  point_sn varchar(64) PRIMARY KEY,
  signal_code varchar(120),
  signal_description varchar(200),
  signal_type varchar(50),
  station_sn varchar(16),
  pipeline_sn varchar(16));
CREATE INDEX point_station_sn_index ON pipec_r_small.point_info(station_sn);
CREATE INDEX point_pipeline_sn_index ON pipec_r_small.point_info(pipeline_sn);

-- t_point_small
insert into db_pipec_small.t_point values('2024-08-27 11:00:00',10.5,'a0','b0','c0','d0','e0',1,'f0');
insert into db_pipec_small.t_point values('2024-08-27 12:00:00',11.5,'a1','b1','c1','d1','e1',1,'f1');
insert into db_pipec_small.t_point values('2024-08-27 13:00:00',11.8,'a1','b1','c1','d1','e1',1,'f1');
insert into db_pipec_small.t_point values('2024-08-27 10:00:00',12.5,'a2','b2','c2','d2','e2',2,'f2');
insert into db_pipec_small.t_point values('2024-08-26 10:00:00',13.5,'a3','b3','c3','d3','e3',2,'f3');
insert into db_pipec_small.t_point values('2024-08-28 10:00:00',14.5,'a4','b4','c4','d4','e4',3,'f4');
insert into db_pipec_small.t_point values('2024-08-29 10:00:00',15.5,'a5','b5','c5','d5','e5',3,'f5');
insert into db_pipec_small.t_point values('2024-08-28 11:00:00',10.5,'a6','b6','c6','d6','e6',4,'f6');
insert into db_pipec_small.t_point values('2024-08-28 12:00:00',11.5,'a7','b7','c7','d7','e7',4,'f7');

-- station_info
insert into pipec_r_small.station_info values('d0','dd','c0','b0','aa','bb');
insert into pipec_r_small.station_info values('d1','dd','c1','b1','aa','bb');
insert into pipec_r_small.station_info values('d2','dd','c2','b2','aa','bb');
insert into pipec_r_small.station_info values('d3','dd','c3','b3','aa','bb');
insert into pipec_r_small.station_info values('d4','dd','c4','b4','aa','bb');
insert into pipec_r_small.station_info values('d5','dd','c5','b5','aa','bb');

-- company_info
insert into pipec_r_small.company_info values('b0','bb','aa');
insert into pipec_r_small.company_info values('b1','bb','aa');
insert into pipec_r_small.company_info values('b2','bb','aa');
insert into pipec_r_small.company_info values('b3','bb','aa');
insert into pipec_r_small.company_info values('b4','bb','aa');
insert into pipec_r_small.company_info values('b5','bb','aa');

-- workarea_info
insert into pipec_r_small.workarea_info values('c0','work_area_0','l0','aa');
insert into pipec_r_small.workarea_info values('c1','work_area_1','l1','aa');
insert into pipec_r_small.workarea_info values('c2','work_area_2','l2','aa');
insert into pipec_r_small.workarea_info values('c3','work_area_3','l3','aa');
insert into pipec_r_small.workarea_info values('c4','work_area_4','l4','aa');
insert into pipec_r_small.workarea_info values('c5','work_area_5','l5','aa');

-- pipeline_info
insert into pipec_r_small.pipeline_info values('e0','pipeline_0','a','aa','b');
insert into pipec_r_small.pipeline_info values('e1','pipeline_1','a','aa','b');
insert into pipec_r_small.pipeline_info values('e2','pipeline_2','a','aa','b');
insert into pipec_r_small.pipeline_info values('e3','pipeline_3','a','aa','b');
insert into pipec_r_small.pipeline_info values('e4','pipeline_4','a','aa','b');
insert into pipec_r_small.pipeline_info values('e5','pipeline_5','a','aa','b');

-- point_info
insert into pipec_r_small.point_info values('a0','ee','a','aa','d0','e0');
insert into pipec_r_small.point_info values('a1','ee','a','aa','d1','e1');
insert into pipec_r_small.point_info values('a2','ee','a','aa','d2','e2');
insert into pipec_r_small.point_info values('a3','ee','a','aa','d3','e3');
insert into pipec_r_small.point_info values('a4','ee','a','aa','d4','e4');
insert into pipec_r_small.point_info values('a5','ee','a','aa','d5','e5');

-- create stats
CREATE STATISTICS _stats_ FROM db_pipec_small.t_point;
CREATE STATISTICS _stats_ FROM pipec_r_small.station_info;
CREATE STATISTICS _stats_ FROM pipec_r_small.pipeline_info;
CREATE STATISTICS _stats_ FROM pipec_r_small.point_info;
CREATE STATISTICS _stats_ FROM pipec_r_small.workarea_info;
CREATE STATISTICS _stats_ FROM pipec_r_small.workarea_info;

-- insideout q7, loosenlimit q5
create view piview as (
      SELECT pi.point_sn    -- 436, 436
      FROM pipec_r.pipeline_info li,         -- 26
           pipec_r.point_info pi,            -- 150K
           pipec_r.station_info si,          -- 436
           pipec_r.workarea_info wi         -- 41
      WHERE si.work_area_sn = wi.work_area_sn -- 41, 41
        AND pi.station_sn = si.station_sn
        AND pi.pipeline_sn = li.pipeline_sn  -- 26, 22
        AND wi.work_area_name like '%3'  -- 3/41
        AND li.pipeline_name like '%7'  -- 1/26
);

-- insideout q21 
-- insideout q22, q23, q25, q26, q27, q28, q29, q30, q31
-- loosenlimit q22, q24, q25, q26, q27, q28, q29, q30
insert into  runba_tra.cd_behavior_area(id, area_name, area_type, company_id, company_name, coordinate, source, update_user_id, if_sync_position,
 style, "3d_model_link", create_time, update_time, if_display_datav, coordinate_wgs84, coordinate_wgs84_lng, coordinate_wgs84_lat, created_at, updated_at, remark) values(1, 'tongji_area_1', 1, 449600, 'company_name_1', 'coordinate_1', 4, 10001, 10, 'style1', 'model_link_1', now(), now(), 4, '', '', '', now(), now(), '')
;

-- extra
CREATE TS DATABASE mtagdb;
-- ts table
CREATE TABLE mtagdb.measurepoints (
  k_timestamp timestamp NOT NULL,
  measure_value double
 ) ATTRIBUTES (
    measure_tag varchar(16) NOT NULL,
    measure_type smallint NOT NULL,
    measure_position varchar(16) NOT NULL,
    measure_style int NOT NULL,
    measure_unit varchar(16),
    measure_location varchar(64))
  PRIMARY TAGS(measure_position, measure_tag, measure_style, measure_type) 
  ACTIVETIME 3h;
  
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:01', 2.5, 'pipeline_1', 1, 'pipeline_sn_1', 2, 'mm', 'locatin1');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:02', 3.5, 'pipeline_1', 1, 'pipeline_sn_1', 2, 'mm', 'locatin1');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:01', 2.5, 'pipeline_1', 1, 'pipeline_sn_1', 3, 'dB', 'locatin2');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:02', 3.1, 'pipeline_1', 1, 'pipeline_sn_1', 3, 'dB', 'locatin2');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:02', 3.6, 'pipeline_1', 1, 'pipeline_sn_1', 3, 'dB', 'locatin2');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:02', 4.6, 'pipeline_1', 1, 'pipeline_sn_1', 3, 'dB', 'locatin2');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:01', 2.5, 'pipeline_2', 2, 'pipeline_sn_2', 2, 'mm', 'locatin1');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:03', 4.5, 'pipeline_2', 2, 'pipeline_sn_2', 2, 'mm', 'locatin1');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:01', 2.5, 'pipeline_2', 2, 'pipeline_sn_3', 4, 'dB', 'locatin2');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:04', 6.5, 'pipeline_2', 2, 'pipeline_sn_3', 4, 'dB', 'locatin2');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:01', 2.5, 'pipeline_2', 3, 'pipeline_sn_4', 5, 'mm', 'locatin1');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:07', 5.5, 'pipeline_2', 3, 'pipeline_sn_4', 5, 'mm', 'locatin1');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:01', 2.5, 'pipeline_3', 1, 'pipeline_sn_1', 3, 'dB', 'locatin2');
  insert into mtagdb.measurepoints values ('2025-01-01 01:01:03', 12.5, 'pipeline_3', 1, 'pipeline_sn_1', 3, 'dB', 'locatin2');

-- create stats for new tables
create statistics _stats_ from runba_tra.channel_info;
create statistics _stats_ from test_rel.rel_t1;
create statistics _stats_ from runba_tra.cd_behavior_area;
create statistics _stats_ from mtagdb.measurepoints;
