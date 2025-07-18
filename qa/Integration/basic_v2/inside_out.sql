CREATE TS DATABASE db_pipec;
set cluster setting ts.sql.query_opt_mode=1110;
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

set inside_out_row_ratio = 0.9;

-- ts node under Inner-Join is tsscan
select max(measure_value) from db_pipec.t_point p,pipec_r.station_info s where p.station_sn=s.station_sn;
explain select max(measure_value) from db_pipec.t_point p,pipec_r.station_info s where p.station_sn=s.station_sn;

select max(measure_value) from db_pipec.t_point p,pipec_r.station_info s where p.station_sn=s.station_sn group by pipeline_sn order by pipeline_sn;
explain select max(measure_value) from db_pipec.t_point p,pipec_r.station_info s where p.station_sn=s.station_sn group by pipeline_sn order by pipeline_sn;

-- ts node under Inner-Join is selectExpr
select max(measure_value) from (select * from db_pipec.t_point where station_sn='d1') p,pipec_r.station_info s where p.station_sn=s.station_sn;
explain select max(measure_value) from (select * from db_pipec.t_point where station_sn='d1') p,pipec_r.station_info s where p.station_sn=s.station_sn;

select max(measure_value) from (select * from db_pipec.t_point where station_sn='d1' limit 10) p,pipec_r.station_info s where p.station_sn=s.station_sn;
explain select max(measure_value) from (select * from db_pipec.t_point where station_sn='d1' limit 10) p,pipec_r.station_info s where p.station_sn=s.station_sn;

-- ts node under Inner-Join is distinctOnExpr , can not opt
select max(measure_value) from (select measure_value,station_sn from db_pipec.t_point where station_sn='d1' group by sub_com_sn,measure_value,station_sn order by sub_com_sn ) p,pipec_r.station_info s where p.station_sn=s.station_sn;
explain select max(measure_value) from (select measure_value,station_sn from db_pipec.t_point where station_sn='d1' group by sub_com_sn,measure_value,station_sn order by sub_com_sn ) p,pipec_r.station_info s where p.station_sn=s.station_sn;

-- ts node under Inner-Join is Inner-Join
select max(measure_value) from (select * from db_pipec.t_point p,pipec_r.pipeline_info pi where p.pipeline_sn=pi.pipeline_sn) p,pipec_r.station_info s where p.station_sn=s.station_sn;
explain select max(measure_value) from (select * from db_pipec.t_point p,pipec_r.pipeline_info pi where p.pipeline_sn=pi.pipeline_sn) p,pipec_r.station_info s where p.station_sn=s.station_sn;

-- 4 tables join
select max(measure_value) from  pipec_r.pipeline_info pi,pipec_r.station_info s,db_pipec.t_point p,pipec_r.point_info pf
where p.station_sn=s.station_sn and p.pipeline_sn=pi.pipeline_sn and p.point_sn=pf.point_sn;
explain select max(measure_value) from  pipec_r.pipeline_info pi,pipec_r.station_info s,db_pipec.t_point p,pipec_r.point_info pf
        where p.station_sn=s.station_sn and p.pipeline_sn=pi.pipeline_sn and p.point_sn=pf.point_sn;


-- multi ts node under Inner-Join
select max(p.measure_value) from  (select * from db_pipec.t_point p,pipec_r.pipeline_info pi where p.pipeline_sn=pi.pipeline_sn ) p,
(select * from db_pipec.t_point p,pipec_r.point_info pf where p.point_sn=pf.point_sn) p1;
explain select max(p.measure_value) from  (select * from db_pipec.t_point p,pipec_r.pipeline_info pi where p.pipeline_sn=pi.pipeline_sn ) p,
                                          (select * from db_pipec.t_point p,pipec_r.point_info pf where p.point_sn=pf.point_sn) p1;


SELECT li.pipeline_name,
       li.pipe_start,
       li.pipe_end,
       station_name,
       COUNT(t.measure_value)
FROM pipec_r.pipeline_info li,
     pipec_r.station_info si,
     db_pipec.t_point t
WHERE t.pipeline_sn = li.pipeline_sn
  AND t.station_sn = si.station_sn
  AND t.measure_value > 2
  AND t.measure_type = 5
  AND k_timestamp >= '2023-08-01 01:00:00'
GROUP BY pipeline_name, pipe_start, pipe_end, station_name
HAVING COUNT(t.measure_value) > 20
ORDER BY COUNT(t.measure_value) DESC;

explain SELECT li.pipeline_name,
              li.pipe_start,
              li.pipe_end,
              station_name,
              COUNT(t.measure_value)
       FROM pipec_r.pipeline_info li,
            pipec_r.station_info si,
            db_pipec.t_point t
       WHERE t.pipeline_sn = li.pipeline_sn
         AND t.station_sn = si.station_sn
         AND t.measure_value > 2
         AND t.measure_type = 5
         AND k_timestamp >= '2023-08-01 01:00:00'
       GROUP BY pipeline_name, pipe_start, pipe_end, station_name
       HAVING COUNT(t.measure_value) > 20
       ORDER BY COUNT(t.measure_value) DESC;

set cluster setting ts.sql.query_opt_mode=1111;
SELECT li.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,
     db_pipec.t_point t
WHERE li.pipeline_sn = t.pipeline_sn
GROUP BY
    li.pipeline_name,
    t.measure_type,
    timebucket
order by timebucket;

explain SELECT li.pipeline_name,
               t.measure_type,
               time_bucket(t.k_timestamp, '10s') as timebucket,
               AVG(t.measure_value) AS avg_value,
               MAX(t.measure_value) AS max_value,
               MIN(t.measure_value) AS min_value,
               COUNT(t.measure_value) AS number_of_values
        FROM pipec_r.pipeline_info li,
             db_pipec.t_point t
        WHERE li.pipeline_sn = t.pipeline_sn
        GROUP BY
            li.pipeline_name,
            t.measure_type,
            timebucket
        order by timebucket;
set cluster setting ts.sql.query_opt_mode=1110;
use defaultdb;
drop database pipec_r cascade;
drop database db_pipec cascade;

-- ZDP-45642
CREATE TS DATABASE runba;
CREATE TABLE runba.opcdata449600 (
                                     "time" TIMESTAMPTZ NOT NULL,
                                     "value" FLOAT8 NULL
) TAGS (
  adr VARCHAR(20),
  channel VARCHAR(20) NOT NULL,
  companyid int NOT NULL,
  companyname VARCHAR(100),
  datatype VARCHAR(20),
  device VARCHAR(100),
  deviceid VARCHAR(20),
  highmorewarn VARCHAR(20),
  highwarn VARCHAR(20),
  lowmorewarn VARCHAR(20),
  lowwarn VARCHAR(20),
  "name" VARCHAR(20),
  region VARCHAR(20),
  slaveid VARCHAR(100),
  "tag" VARCHAR(20),
  unit VARCHAR(20)
) PRIMARY TAGS (
  channel,
  companyid
);

CREATE DATABASE IF NOT EXISTS runba_tra;
CREATE TABLE IF NOT EXISTS runba_tra.cd_behavior_area (
                                                          id INT4 NOT NULL,
                                                          area_name VARCHAR(255) NOT NULL,
    company_id INT4 NOT NULL,
    company_name VARCHAR(255) NULL,
    area_type INT4 NULL,
    coordinate VARCHAR(255) NOT NULL,
    is_select INT4 NULL,
    source INT4 NOT NULL,
    update_user_id INT4 NOT NULL,
    if_sync_position INT4 NOT NULL,
    style STRING NOT NULL,
    "3d_model_link" VARCHAR(500) NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    if_display_datav INT4 NOT NULL,
    coordinate_wgs84 VARCHAR(255) NULL,
    coordinate_wgs84_lng VARCHAR(100) NULL,
    coordinate_wgs84_lat VARCHAR(45) NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    remark VARCHAR(255) NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    FAMILY "primary" (
                         id,
                         area_name,
                         company_id,
                         company_name,
                         area_type,
                         coordinate,
                         is_select,
                         source,
                         update_user_id,
                         if_sync_position,
                         style,
                         "3d_model_link",
                         create_time,
                         update_time,
                         if_display_datav,
                         coordinate_wgs84,
                         coordinate_wgs84_lng,
                         coordinate_wgs84_lat,
                         created_at,
                         updated_at,
                         remark
                     )
    );
CREATE TABLE IF NOT EXISTS runba_tra.cd_device_point (
                                                         id INT4 NOT NULL,
                                                         point_name VARCHAR(500) NULL,
    adr VARCHAR(255) NULL,
    device_name VARCHAR(500) NULL,
    device_id INT4 NOT NULL,
    index_id INT4 NULL,
    index_upper_value DECIMAL(15, 6) NULL,
    index_lower_value DECIMAL(15, 6) NULL,
    company_id INT4 NULL,
    create_time TIMESTAMP NULL,
    update_time TIMESTAMP NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    FAMILY "primary" (
                         id,
                         point_name,
                         adr,
                         device_name,
                         device_id,
                         index_id,
                         index_upper_value,
                         index_lower_value,
                         company_id,
                         create_time,
                         update_time
                     )
    );

CREATE TABLE IF NOT EXISTS runba_tra.cd_security_device (
                                                            id INT4 NOT NULL,
                                                            manufacturer_id INT4 NOT NULL,
                                                            "type" VARCHAR(255) NOT NULL,
    device_no VARCHAR(255) NOT NULL,
    device_name VARCHAR(255) NOT NULL,
    "status" INT4 NOT NULL,
    preview_url VARCHAR(255) NOT NULL,
    rec_url VARCHAR(255) NOT NULL,
    company_id INT4 NOT NULL,
    company_name VARCHAR(255) NULL,
    area_id INT4 NOT NULL,
    category INT4 NOT NULL,
    has_alert INT4 NOT NULL,
    gas_device_info VARCHAR(255) NOT NULL,
    channel_code VARCHAR(255) NOT NULL,
    realtime_play INT4 NOT NULL,
    realtime_category INT4 NOT NULL,
    ai_device_id INT4 NULL,
    style STRING NOT NULL,
    "3d_model_link" STRING NULL,
    coordinate STRING NOT NULL,
    display_type VARCHAR(255) NOT NULL,
    device_info_id INT4 NOT NULL,
    behavior_apply_status INT4 NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    FAMILY "primary" (
                         id,
                         manufacturer_id,
                         "type",
                         device_no,
                         device_name,
                         "status",
                         preview_url,
                         rec_url,
                         company_id,
                         company_name,
                         area_id,
                         category,
                         has_alert,
                         gas_device_info,
                         channel_code,
                         realtime_play,
                         realtime_category,
                         ai_device_id,
                         style,
                         "3d_model_link",
                         coordinate,
                         display_type,
                         device_info_id,
                         behavior_apply_status
                     )
    );

CREATE TABLE IF NOT EXISTS runba_tra.plat_tijian_item_index (
                                                                id INT4 NOT NULL,
                                                                index_name VARCHAR(255) NOT NULL,
    index_unit VARCHAR(255) NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    FAMILY "primary" (
                         id,
                         index_name,
                         index_unit,
                         create_time,
                         update_time
                     )
    );

CREATE TABLE IF NOT EXISTS runba_tra.plat_risk_analyse_objects (
                                                                   id INT4 NOT NULL,
                                                                   company_id INT4 NOT NULL,
                                                                   obj_name VARCHAR(50) NOT NULL,
    obj_code VARCHAR(45) NULL,
    "level" VARCHAR(50) NOT NULL,
    category INT4 NULL,
    create_user_id INT4 NOT NULL,
    create_user_nickname VARCHAR(255) NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    danger_source VARCHAR(255) NOT NULL,
    danger_code VARCHAR(255) NOT NULL,
    danger_level VARCHAR(255) NOT NULL,
    chemical_name VARCHAR(255) NOT NULL,
    chemical_attribute VARCHAR(255) NOT NULL,
    chemical_cas VARCHAR(255) NOT NULL,
    chemical_technique VARCHAR(255) NOT NULL,
    chemical_storage VARCHAR(255) NOT NULL,
    technique_name VARCHAR(255) NOT NULL,
    technique_code VARCHAR(255) NOT NULL,
    technique_device VARCHAR(255) NOT NULL,
    technique_address VARCHAR(255) NOT NULL,
    location_name VARCHAR(255) NOT NULL,
    location_code VARCHAR(255) NOT NULL,
    location_category VARCHAR(255) NOT NULL,
    location_company_category VARCHAR(255) NOT NULL,
    location_technique VARCHAR(255) NOT NULL,
    risk_status INT4 NULL,
    risk_type INT4 NULL,
    is_access INT4 NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC),
    FAMILY "primary" (
                         id,
                         company_id,
                         obj_name,
                         obj_code,
                         "level",
                         category,
                         create_user_id,
                         create_user_nickname,
                         create_time,
                         update_time,
                         danger_source,
                         danger_code,
                         danger_level,
                         chemical_name,
                         chemical_attribute,
                         chemical_cas,
                         chemical_technique,
                         chemical_storage,
                         technique_name,
                         technique_code,
                         technique_device,
                         technique_address,
                         location_name,
                         location_code,
                         location_category,
                         location_company_category,
                         location_technique,
                         risk_status,
                         risk_type,
                         is_access
                     )
    );

SELECT
    a.area_name,
    d.device_name,
    s.device_no,
    t.index_name,
    r.obj_name,
    COUNT(o.value) AS value_count,
    AVG(o.value) AS avg_value,
    MAX(o.value) AS max_value,
    MIN(o.value) AS min_value
FROM
    runba.opcdata449600 o,
    runba_tra.cd_behavior_area a,
    runba_tra.cd_device_point d,
    runba_tra.cd_security_device s,
    runba_tra.plat_tijian_item_index t,
    runba_tra.plat_risk_analyse_objects r
WHERE
        o.companyid = a.company_id
  AND o.companyid = d.company_id
  AND o.companyid = s.company_id
  AND d.index_id = t.id
  AND o.companyid = r.company_id
  AND o.time >= '2023-01-01'
  AND o.time < '2024-01-02'
  AND a.area_type = 1
  AND d.index_upper_value > 1.0
  AND s.device_name = '1002'
  AND t.index_unit = 'MPA'
  AND r.risk_status = 1
GROUP BY
    a.area_name,
    d.device_name,
    s.device_no,
    t.index_name,
    r.obj_name
HAVING
        COUNT(o.value) > 1
order BY
    a.area_name,
    d.device_name,
    s.device_no,
    t.index_name,
    r.obj_name;
set cluster setting ts.sql.query_opt_mode=DEFAULT;
use defaultdb;
drop database runba cascade;
drop database runba_tra cascade;