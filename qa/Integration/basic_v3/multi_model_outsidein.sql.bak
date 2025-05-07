set cluster setting ts.sql.query_opt_mode = 1100;
set cluster setting sql.stats.tag_automatic_collection.enabled = false;
CREATE TS DATABASE db_pipec;
-- TS table
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

-- Populate sample data
insert into db_pipec.t_point values('2024-08-27 11:00:00',10.5,'a0','b0','c0','d0','e0',1,'f0');
insert into db_pipec.t_point values('2024-08-27 12:00:00',11.5,'a1','b1','c1','d1','e1',1,'f1');
insert into db_pipec.t_point values('2024-08-27 13:00:00',11.8,'a1','b1','c1','d1','e1',1,'f1');
insert into db_pipec.t_point values('2024-08-27 10:00:00',12.5,'a2','b2','c2','d2','e2',2,'f2');
insert into db_pipec.t_point values('2024-08-26 10:00:00',13.5,'a3','b3','c3','d3','e3',2,'f3');
insert into db_pipec.t_point values('2024-08-28 10:00:00',14.5,'a4','b4','c4','d4','e4',3,'f4');
insert into db_pipec.t_point values('2024-08-29 10:00:00',15.5,'a5','b5','c5','d5','e5',3,'f5');
insert into db_pipec.t_point values('2024-08-28 11:00:00',10.5,'a6','b6','c6','d6','e6',4,'f6');
insert into db_pipec.t_point values('2024-08-28 12:00:00',11.5,'a7','b7','c7','d7','e7',4,'f7');

-- create stats
CREATE STATISTICS _stats_ FROM db_pipec.t_point;

-- relational table 
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

insert into pipec_r.station_info values('d0','dd','c0','aa','b','bb');
insert into pipec_r.station_info values('d1','dd','c1','aa','b','bb');
insert into pipec_r.station_info values('d2','dd','c2','aa','b','bb');
insert into pipec_r.station_info values('d3','dd','c3','aa','b','bb');
insert into pipec_r.station_info values('d4','dd','c4','aa','b','bb');
insert into pipec_r.station_info values('d5','dd','c5','aa','b','bb');

-- create stats
CREATE STATISTICS _stats_ FROM pipec_r.station_info;

CREATE TABLE pipec_r.pipeline_info (
    pipeline_sn varchar(16) PRIMARY KEY,
    pipeline_name varchar(60),
    pipe_start varchar(80),
    pipe_end varchar(80),
    pipe_properties varchar(30));
CREATE INDEX pipeline_sn_index ON pipec_r.pipeline_info (pipeline_sn);
CREATE INDEX pipeline_name_index ON pipec_r.pipeline_info (pipeline_name);

insert into pipec_r.pipeline_info values('e0','pipeline_0','a','aa','b');
insert into pipec_r.pipeline_info values('e1','pipeline_1','a','aa','b');
insert into pipec_r.pipeline_info values('e2','pipeline_2','a','aa','b');
insert into pipec_r.pipeline_info values('e3','pipeline_3','a','aa','b');
insert into pipec_r.pipeline_info values('e4','pipeline_4','a','aa','b');
insert into pipec_r.pipeline_info values('e5','pipeline_5','a','aa','b');

-- create stats
CREATE STATISTICS _stats_ FROM pipec_r.pipeline_info;

CREATE TABLE pipec_r.point_info (
    point_sn varchar(64) PRIMARY KEY,
    signal_code varchar(120),
    signal_description varchar(200),
    signal_type varchar(50),
    station_sn varchar(16),
    pipeline_sn varchar(16));

insert into pipec_r.point_info values('a0','ee','a','aa','d0','e0');
insert into pipec_r.point_info values('a1','ee','a','aa','d1','e1');
insert into pipec_r.point_info values('a2','ee','a','aa','d2','e2');
insert into pipec_r.point_info values('a3','ee','a','aa','d3','e3');
insert into pipec_r.point_info values('a4','ee','a','aa','d4','e4');
insert into pipec_r.point_info values('a5','ee','a','aa','d5','e5');

-- create stats
CREATE STATISTICS _stats_ FROM pipec_r.point_info;

CREATE TABLE pipec_r.workarea_info (
  work_area_sn varchar(16) PRIMARY KEY,
  work_area_name varchar(80),
  work_area_location varchar(64), 
  work_area_description varchar(128));
CREATE INDEX workarea_name_index ON pipec_r.workarea_info(work_area_name);

insert into pipec_r.workarea_info values('c0','work_area_0','l0','aa');
insert into pipec_r.workarea_info values('c1','work_area_1','l1','aa');
insert into pipec_r.workarea_info values('c2','work_area_2','l2','aa');
insert into pipec_r.workarea_info values('c3','work_area_3','l3','aa');
insert into pipec_r.workarea_info values('c4','work_area_4','l4','aa');
insert into pipec_r.workarea_info values('c5','work_area_5','l5','aa');

-- create stats
CREATE STATISTICS _stats_ FROM pipec_r.workarea_info;

-- create a relation table with long varchar
CREATE TABLE pipec_r.pipeline_info_with_long_varchar (
    pipeline_sn varchar(70000) PRIMARY KEY,
    pipeline_name varchar(60),
    pipe_start varchar(80),
    pipe_end varchar(80),
    pipe_properties varchar(30));
CREATE INDEX pipeline_sn_index ON pipec_r.pipeline_info_with_long_varchar (pipeline_sn);
CREATE INDEX pipeline_name_index ON pipec_r.pipeline_info_with_long_varchar (pipeline_name);

insert into pipec_r.pipeline_info_with_long_varchar values('e0','pipeline_0','a','aa','b');
insert into pipec_r.pipeline_info_with_long_varchar values('e1','pipeline_1','a','aa','b');
insert into pipec_r.pipeline_info_with_long_varchar values('e2','pipeline_2','a','aa','b');
insert into pipec_r.pipeline_info_with_long_varchar values('e3','pipeline_3','a','aa','b');
insert into pipec_r.pipeline_info_with_long_varchar values('e4','pipeline_4','a','aa','b');
insert into pipec_r.pipeline_info_with_long_varchar values('e5','pipeline_5','a','aa','b');

-- create stats
CREATE STATISTICS _stats_ FROM pipec_r.pipeline_info_with_long_varchar;

-- long varchar pushdown bug
set enable_multimodel=true;

SELECT liwlv.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info_with_long_varchar liwlv,         -- 26
     db_pipec.t_point t                -- 45M
WHERE liwlv.pipeline_sn = t.pipeline_sn   -- 26, 21
GROUP BY
    liwlv.pipeline_name,
    t.measure_type,
    timebucket
ORDER BY
    liwlv.pipeline_name,
    t.measure_type,
    timebucket;


set enable_multimodel=false;

-- query 2
SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,           
     pipec_r.station_info si,
     pipec_r.workarea_info wi, 
     pipec_r.pipeline_info li,        
     pipec_r.point_info pi         
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;

explain SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,           
     pipec_r.station_info si,
     pipec_r.workarea_info wi, 
     pipec_r.pipeline_info li,        
     pipec_r.point_info pi         
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;

-- query 1
SELECT si.station_name,
       AVG(t.measure_value) AS avg_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,           
     pipec_r.station_info si,
     pipec_r.workarea_info wi, 
     pipec_r.pipeline_info li,        
     pipec_r.point_info pi         
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND wi.work_area_name = 'work_area_1'
  AND t.measure_type = 1                   -- 1/17
  AND t.point_sn = 'a1'
GROUP BY si.station_name
ORDER BY si.station_name;

explain SELECT si.station_name,
       AVG(t.measure_value) AS avg_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,           
     pipec_r.station_info si,
     pipec_r.workarea_info wi, 
     pipec_r.pipeline_info li,        
     pipec_r.point_info pi         
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND wi.work_area_name = 'work_area_1'
  AND t.measure_type = 1                   -- 1/17
  AND t.point_sn = 'a1'
GROUP BY si.station_name
ORDER BY si.station_name;

-- query 1'
-- original version of query 1
SELECT si.station_name,
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 1
  AND t.measure_value > 5
GROUP BY si.station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY si.station_name;

explain SELECT si.station_name,
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 1
  AND t.measure_value > 5
GROUP BY si.station_name
ORDER BY si.station_name;

-- query 4
-- fallback case, moved to fallback testcase

-- query 5
SELECT wi.work_area_name,
       t.measure_type,
       COUNT(DISTINCT t.point_sn) AS measure_point_count
FROM pipec_r.pipeline_info li,          -- 26
     pipec_r.station_info si,           -- 436
     pipec_r.workarea_info wi,          -- 41
     db_pipec.t_point t                 -- 45M
WHERE li.pipeline_sn = t.pipeline_sn    -- 26, 21
  AND si.work_area_sn = wi.work_area_sn -- 41, 41
  AND si.work_area_sn = t.work_area_sn  -- 41, 41
  AND li.pipeline_name = 'pipeline_1'   -- 1/26
GROUP BY
    wi.work_area_name, t.measure_type
ORDER BY
    wi.work_area_name, t.measure_type;

explain SELECT wi.work_area_name,
       t.measure_type,
       COUNT(DISTINCT t.point_sn) AS measure_point_count
FROM pipec_r.pipeline_info li,          -- 26
     pipec_r.station_info si,           -- 436
     pipec_r.workarea_info wi,          -- 41
     db_pipec.t_point t                 -- 45M
WHERE li.pipeline_sn = t.pipeline_sn    -- 26, 21
  AND si.work_area_sn = wi.work_area_sn -- 41, 41
  AND si.work_area_sn = t.work_area_sn  -- 41, 41
  AND li.pipeline_name = 'pipeline_1'   -- 1/26
GROUP BY
    wi.work_area_name, t.measure_type
ORDER BY
    wi.work_area_name, t.measure_type;

-- query 6
SELECT li.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,         -- 26
     db_pipec.t_point t                -- 45M
WHERE li.pipeline_sn = t.pipeline_sn   -- 26, 21
GROUP BY
    li.pipeline_name,
    t.measure_type,
    timebucket
ORDER BY
    li.pipeline_name,
    t.measure_type,
    timebucket;

explain SELECT li.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,         -- 26
     db_pipec.t_point t                -- 45M
WHERE li.pipeline_sn = t.pipeline_sn   -- 26, 21
GROUP BY
    li.pipeline_name,
    t.measure_type,
    timebucket
ORDER BY
    li.pipeline_name,
    t.measure_type,
    timebucket;

-- query library query 4
SELECT si.station_name,
       COUNT(sub_company_sn),COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 1
  AND t.measure_value > 1
GROUP BY si.station_name
ORDER BY si.station_name;

explain SELECT si.station_name,
       COUNT(sub_company_sn),COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 2
  AND t.measure_value > 10
GROUP BY si.station_name
ORDER BY si.station_name;

-- query library query 5
SELECT t.measure_type,
       COUNT(si.sub_company_sn),
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_value > 10
GROUP BY t.measure_type
ORDER BY t.measure_type;

explain SELECT t.measure_type,
       COUNT(si.sub_company_sn),
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_value > 10
GROUP BY t.measure_type
ORDER BY t.measure_type;

-- query library Query 9
SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     pipec_r.pipeline_info li,
     pipec_r.point_info pi,
     db_pipec.t_point t
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND t.point_sn in ('a2', 'a1')
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY timebucket;

explain SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     pipec_r.pipeline_info li,
     pipec_r.point_info pi,
     db_pipec.t_point t
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND t.point_sn in ('a2', 'a1')
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY timebucket;

-- query library Query 18
SELECT
    time_bucket(t.k_timestamp, '1h') AS timebucket,
    s.work_area_sn,
    w.work_area_name,
    pinfo.pipeline_name,
    COUNT(t.k_timestamp) AS measurement_count,
    SUM(t.measure_value) AS total_measure_value,
    AVG(t.measure_value) AS avg_measure_value
FROM
    db_pipec.t_point t,
    pipec_r.station_info s,
    pipec_r.workarea_info w,
    pipec_r.pipeline_info pinfo
WHERE
    t.station_sn = s.station_sn
    AND t.pipeline_sn = pinfo.pipeline_sn
    AND s.work_area_sn = w.work_area_sn
    AND t.k_timestamp BETWEEN '2024-08-27 1:30:00' AND '2024-08-28 1:31:00'
GROUP BY
    timebucket, s.work_area_sn, w.work_area_name, pinfo.pipeline_name
ORDER BY
    timebucket, s.work_area_sn;


explain SELECT
    time_bucket(t.k_timestamp, '1h') AS timebucket,
    s.work_area_sn,
    w.work_area_name,
    pinfo.pipeline_name,
    COUNT(t.k_timestamp) AS measurement_count,
    SUM(t.measure_value) AS total_measure_value,
    AVG(t.measure_value) AS avg_measure_value
FROM
    db_pipec.t_point t,
    pipec_r.station_info s,
    pipec_r.workarea_info w,
    pipec_r.pipeline_info pinfo
WHERE
    t.station_sn = s.station_sn
    AND t.pipeline_sn = pinfo.pipeline_sn
    AND s.work_area_sn = w.work_area_sn
    AND t.k_timestamp BETWEEN '2024-08-27 1:30:00' AND '2024-08-28 1:31:00'
GROUP BY
    timebucket, s.work_area_sn, w.work_area_name, pinfo.pipeline_name
ORDER BY
    timebucket, s.work_area_sn;

-- query library query 23
-- partial fallback case, multimodel crash fixed
SELECT COUNT(*),
       AVG(t.measure_value)
FROM db_pipec.t_point t,
     pipec_r.station_info si
WHERE si.station_sn = t.station_sn
AND t.k_timestamp >= '2023-08-01 01:00:00'
UNION ALL
SELECT COUNT(*),
       AVG(t.measure_value)
FROM db_pipec.t_point t,
     pipec_r.station_info si
WHERE si.station_sn = t.station_sn
AND t.k_timestamp >= '2023-07-01 01:00:00' ;

explain SELECT COUNT(*),
       AVG(t.measure_value)
FROM db_pipec.t_point t,
     pipec_r.station_info si
WHERE si.station_sn = t.station_sn
AND t.k_timestamp >= '2023-08-01 01:00:00'
UNION ALL
SELECT COUNT(*),
       AVG(t.measure_value)
FROM db_pipec.t_point t,
     pipec_r.station_info si
WHERE si.station_sn = t.station_sn
AND t.k_timestamp >= '2023-07-01 01:00:00' ;

-- query library query 24
-- join on test case

SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.point_info pi        -- 26
     join pipec_r.pipeline_info li         -- 26
         on li.pipeline_sn = pi.pipeline_sn            -- 150K
     join db_pipec.t_point t 
          on t.point_sn = pi.point_sn
     join pipec_r.station_info si           -- 436
         on pi.station_sn = si.station_sn     
     join pipec_r.workarea_info wi 
         on si.work_area_sn = wi.work_area_sn        -- 41
WHERE li.pipeline_name = 'pipeline_1'  -- 1/26
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')  -- 3/41
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;

EXPLAIN SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.point_info pi        -- 26
     join pipec_r.pipeline_info li         -- 26
         on li.pipeline_sn = pi.pipeline_sn            -- 150K
     join db_pipec.t_point t 
          on t.point_sn = pi.point_sn
     join pipec_r.station_info si           -- 436
         on pi.station_sn = si.station_sn     
     join pipec_r.workarea_info wi 
         on si.work_area_sn = wi.work_area_sn        -- 41
WHERE li.pipeline_name = 'pipeline_1'  -- 1/26
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')  -- 3/41
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;

set enable_multimodel=true;

-- query 2
SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,           
     pipec_r.station_info si,
     pipec_r.workarea_info wi, 
     pipec_r.pipeline_info li,        
     pipec_r.point_info pi         
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;

explain SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,           
     pipec_r.station_info si,
     pipec_r.workarea_info wi, 
     pipec_r.pipeline_info li,        
     pipec_r.point_info pi         
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;

-- query 1
SELECT si.station_name,
       AVG(t.measure_value) AS avg_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,           
     pipec_r.station_info si,
     pipec_r.workarea_info wi, 
     pipec_r.pipeline_info li,        
     pipec_r.point_info pi         
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND wi.work_area_name = 'work_area_1'
  AND t.measure_type = 1                   -- 1/17
  AND t.point_sn = 'a1'
GROUP BY si.station_name
ORDER BY si.station_name;

explain SELECT si.station_name,
       AVG(t.measure_value) AS avg_value,
       COUNT(t.measure_value) AS number_of_values
FROM db_pipec.t_point t,           
     pipec_r.station_info si,
     pipec_r.workarea_info wi, 
     pipec_r.pipeline_info li,        
     pipec_r.point_info pi         
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND wi.work_area_name = 'work_area_1'
  AND t.measure_type = 1                   -- 1/17
  AND t.point_sn = 'a1'
GROUP BY si.station_name
ORDER BY si.station_name;

-- query 1'
-- original version of query 1
SELECT si.station_name,
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 1
  AND t.measure_value > 5
GROUP BY si.station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY si.station_name;

explain SELECT si.station_name,
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 1
  AND t.measure_value > 5
GROUP BY si.station_name
ORDER BY si.station_name;

-- query 4
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
  AND t.measure_type = 2
  AND k_timestamp >= '2023-08-01 01:00:00'
GROUP BY pipeline_name, pipe_start, pipe_end, station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY pipeline_name DESC;

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
  AND t.measure_type = 2
  AND k_timestamp >= '2023-08-01 01:00:00'
GROUP BY pipeline_name, pipe_start, pipe_end, station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY pipeline_name DESC;

-- query 5
SELECT wi.work_area_name,
       t.measure_type,
       COUNT(DISTINCT t.point_sn) AS measure_point_count
FROM pipec_r.pipeline_info li,          -- 26
     pipec_r.station_info si,           -- 436
     pipec_r.workarea_info wi,          -- 41
     db_pipec.t_point t                 -- 45M
WHERE li.pipeline_sn = t.pipeline_sn    -- 26, 21
  AND si.work_area_sn = wi.work_area_sn -- 41, 41
  AND si.work_area_sn = t.work_area_sn  -- 41, 41
  AND li.pipeline_name = 'pipeline_1'   -- 1/26
GROUP BY
    wi.work_area_name, t.measure_type
ORDER BY
    wi.work_area_name, t.measure_type;

explain SELECT wi.work_area_name,
       t.measure_type,
       COUNT(DISTINCT t.point_sn) AS measure_point_count
FROM pipec_r.pipeline_info li,          -- 26
     pipec_r.station_info si,           -- 436
     pipec_r.workarea_info wi,          -- 41
     db_pipec.t_point t                 -- 45M
WHERE li.pipeline_sn = t.pipeline_sn    -- 26, 21
  AND si.work_area_sn = wi.work_area_sn -- 41, 41
  AND si.work_area_sn = t.work_area_sn  -- 41, 41
  AND li.pipeline_name = 'pipeline_1'   -- 1/26
GROUP BY
    wi.work_area_name, t.measure_type
ORDER BY
    wi.work_area_name, t.measure_type;

-- query 6
SELECT li.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,         -- 26
     db_pipec.t_point t                -- 45M
WHERE li.pipeline_sn = t.pipeline_sn   -- 26, 21
GROUP BY
    li.pipeline_name,
    t.measure_type,
    timebucket
ORDER BY
    li.pipeline_name,
    t.measure_type,
    timebucket;

explain SELECT li.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,         -- 26
     db_pipec.t_point t                -- 45M
WHERE li.pipeline_sn = t.pipeline_sn   -- 26, 21
GROUP BY
    li.pipeline_name,
    t.measure_type,
    timebucket
ORDER BY
    li.pipeline_name,
    t.measure_type,
    timebucket;

-- query library query 4
SELECT si.station_name,
       COUNT(sub_company_sn),COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 1
  AND t.measure_value > 10
GROUP BY si.station_name
ORDER BY si.station_name;

explain SELECT si.station_name,
       COUNT(sub_company_sn),COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 1
  AND t.measure_value > 1
GROUP BY si.station_name
ORDER BY si.station_name;

-- query library query 5
SELECT t.measure_type,
       COUNT(si.sub_company_sn),
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_value > 10
GROUP BY t.measure_type
ORDER BY t.measure_type;

explain SELECT t.measure_type,
       COUNT(si.sub_company_sn),
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_value > 10
GROUP BY t.measure_type
ORDER BY t.measure_type;

-- query library Query 9
SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     pipec_r.pipeline_info li,
     pipec_r.point_info pi,
     db_pipec.t_point t
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND t.point_sn in ('a2', 'a1')
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         time_bucket
ORDER BY timebucket;

explain SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     pipec_r.pipeline_info li,
     pipec_r.point_info pi,
     db_pipec.t_point t
WHERE li.pipeline_sn = pi.pipeline_sn
  AND pi.station_sn = si.station_sn
  AND si.work_area_sn = wi.work_area_sn
  AND t.point_sn = pi.point_sn
  AND t.point_sn in ('a2', 'a1')
  AND li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY timebucket;

-- query library Query 11
explain SELECT si.station_name,
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 1
  AND t.point_sn = 'a1'
  AND t.measure_value > 11
GROUP BY si.station_name
HAVING COUNT(t.measure_value) >= 1
ORDER BY si.station_name;

SELECT si.station_name,
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 1
  AND t.point_sn = 'a1'
  AND t.measure_value > 11
GROUP BY si.station_name
HAVING COUNT(t.measure_value) >= 1
ORDER BY si.station_name;

-- query library Query 18
SELECT
    time_bucket(t.k_timestamp, '1h') AS timebucket,
    s.work_area_sn,
    w.work_area_name,
    pinfo.pipeline_name,
    COUNT(t.k_timestamp) AS measurement_count,
    SUM(t.measure_value) AS total_measure_value,
    AVG(t.measure_value) AS avg_measure_value
FROM
    db_pipec.t_point t,
    pipec_r.station_info s,
    pipec_r.workarea_info w,
    pipec_r.pipeline_info pinfo
WHERE
    t.station_sn = s.station_sn
    AND t.pipeline_sn = pinfo.pipeline_sn
    AND s.work_area_sn = w.work_area_sn
    AND t.k_timestamp BETWEEN '2024-08-27 1:30:00' AND '2024-08-28 1:31:00'
GROUP BY
    timebucket, s.work_area_sn, w.work_area_name, pinfo.pipeline_name
ORDER BY
    timebucket, s.work_area_sn;

explain SELECT
    time_bucket(t.k_timestamp, '1h') AS timebucket,
    s.work_area_sn,
    w.work_area_name,
    pinfo.pipeline_name,
    COUNT(t.k_timestamp) AS measurement_count,
    SUM(t.measure_value) AS total_measure_value,
    AVG(t.measure_value) AS avg_measure_value
FROM
    db_pipec.t_point t,
    pipec_r.station_info s,
    pipec_r.workarea_info w,
    pipec_r.pipeline_info pinfo
WHERE
    t.station_sn = s.station_sn
    AND t.pipeline_sn = pinfo.pipeline_sn
    AND s.work_area_sn = w.work_area_sn
    AND t.k_timestamp BETWEEN '2024-08-27 1:30:00' AND '2024-08-28 1:31:00'
GROUP BY
    timebucket, s.work_area_sn, w.work_area_name, pinfo.pipeline_name
ORDER BY
    timebucket, s.work_area_sn;

-- query library query 23
-- partial fallback case, multimodel crash fixed
SELECT COUNT(*),
       AVG(t.measure_value)
FROM db_pipec.t_point t,
     pipec_r.station_info si
WHERE si.station_sn = t.station_sn
AND t.k_timestamp >= '2023-08-01 01:00:00'
UNION ALL
SELECT COUNT(*),
       AVG(t.measure_value)
FROM db_pipec.t_point t,
     pipec_r.station_info si
WHERE si.station_sn = t.station_sn
AND t.k_timestamp >= '2023-07-01 01:00:00' ;

explain SELECT COUNT(*),
       AVG(t.measure_value)
FROM db_pipec.t_point t,
     pipec_r.station_info si
WHERE si.station_sn = t.station_sn
AND t.k_timestamp >= '2023-08-01 01:00:00'
UNION ALL
SELECT COUNT(*),
       AVG(t.measure_value)
FROM db_pipec.t_point t,
     pipec_r.station_info si
WHERE si.station_sn = t.station_sn
AND t.k_timestamp >= '2023-07-01 01:00:00' ;

-- query library query 24
-- join on test case
SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.point_info pi        -- 26
     join pipec_r.pipeline_info li         -- 26
         on li.pipeline_sn = pi.pipeline_sn            -- 150K
     join db_pipec.t_point t 
          on t.point_sn = pi.point_sn
     join pipec_r.station_info si           -- 436
         on pi.station_sn = si.station_sn     
     join pipec_r.workarea_info wi 
         on si.work_area_sn = wi.work_area_sn        -- 41
WHERE li.pipeline_name = 'pipeline_1'  -- 1/26
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')  -- 3/41
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;

EXPLAIN SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.point_info pi        -- 26
     join pipec_r.pipeline_info li         -- 26
         on li.pipeline_sn = pi.pipeline_sn            -- 150K
     join db_pipec.t_point t 
          on t.point_sn = pi.point_sn
     join pipec_r.station_info si           -- 436
         on pi.station_sn = si.station_sn     
     join pipec_r.workarea_info wi 
         on si.work_area_sn = wi.work_area_sn        -- 41
WHERE li.pipeline_name = 'pipeline_1'  -- 1/26
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')  -- 3/41
  AND t.k_timestamp >= '2023-08-01 01:00:00'  -- 1/1 (all data passed)
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;

-- ZDP-44643 bug

insert into pipec_r.point_info values('a6','ee','a','aa','d0','e0');
insert into pipec_r.point_info values('a7','ee','a','aa','d1','e1');
insert into pipec_r.point_info values('a8','ee','a','aa','d2','e2');
insert into pipec_r.point_info values('a9','ee','a','aa','d3','e3');
insert into pipec_r.point_info values('a95','ee','a','aa','d4','e4');
insert into pipec_r.point_info values('a99','ee','a','aa','d5','e5');

-- create stats
CREATE STATISTICS _stats_ FROM pipec_r.point_info;

set cluster setting ts.parallel_degree=1;
SELECT
  pi.point_sn,
  t.measure_type,
  POWER(t.measure_value, 2) AS adjusted_value,
  t.k_timestamp
FROM
  pipec_r.point_info pi,
  db_pipec.t_point t
WHERE
  t.point_sn = pi.point_sn
ORDER BY
  pi.point_sn,
  t.k_timestamp;

set cluster setting ts.parallel_degree=0;

CREATE TS DATABASE mtagdb;

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

set hash_scan_mode = 2;
SELECT li.pipeline_name,
       li.pipe_start,
       li.pipe_end,
       COUNT(mt.measure_value),
       AVG(mt.measure_value)
FROM (select  pipe_start,
            pipe_end,
            pipeline_name,
            pipeline_sn,
            pipe_properties,
            (CASE WHEN substr(pipe_properties, 16, 1) = '1' THEN 2 ELSE 1 END) as ltype,
            (CASE WHEN substr(pipeline_sn, 13, 1) = '1' THEN 3 ELSE 2 END) as lstyle
      from pipec_r.pipeline_info) as li,
     mtagdb.measurepoints mt
WHERE mt.measure_tag = li.pipeline_name
  AND mt.measure_type = cast(li.ltype as int2)
  AND mt.measure_style = cast(li.lstyle as int)
  AND mt.measure_value >= 2.7
GROUP BY li.pipeline_name, li.pipe_start, li.pipe_end;
set hash_scan_mode = 0;

create table mtagdb.tb3(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2,t2 int not null,t3 int8 not null,t4 bool,t5 float4,t6 float8,t7 char,t8 char(100),t9 nchar,t10 nchar(254),t11 varchar not null,t12 varchar(128),t13 varbytes not null,t14 varbytes(100) not null,t15 varbytes,t16 varbytes(255)) primary tags(t3,t11,t2);
insert into mtagdb.tb3 values ('2024-05-10 23:23:23.783','2024-06-10 16:16:15.183',800,8000,80000,800000.808888,8000000.808088,true,'d','test测试！！！@TEST1 ','d','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,false,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into mtagdb.tb3 values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',8,800,8000,false,-20.123,800.578578,'d','test测试！！！@TEST1  ','d','ddd','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
select
    first(e22),first(t15),first(t13),first(t14)
from
    mtagdb.tb3 as tab1
    join (
        select
            first(e22),first(t15),first(t13),first(t14) as c14
        from
            mtagdb.tb3
    ) as tab2
on tab1.t14 = tab2.c14;


DROP DATABASE IF EXISTS test_SELECT_join cascade;
DROP DATABASE IF EXISTS test_SELECT_join_rel cascade;

CREATE ts DATABASE test_SELECT_join;
CREATE DATABASE test_SELECT_join_rel;

CREATE TABLE test_SELECT_join.t1(
                k_timestamp TIMESTAMPTZ NOT NULL,
                id INT NOT NULL,
                e1 INT2,
                e2 INT,
                e3 INT8,
                e4 FLOAT4,
                e5 FLOAT8,
                e6 BOOL,
                e7 TIMESTAMPTZ,
                e8 CHAR(1023),
                e9 NCHAR(255),
                e10 VARCHAR(4096),
                e11 CHAR,
                e12 CHAR(255),
                e13 NCHAR,
                e14 NVARCHAR(4096),
                e15 VARCHAR(1023), 
                e16 NVARCHAR(200),
                e17 NCHAR(255),
                e18 CHAR(200),           
                e19 VARBYTES,
                e20 varbytes(60),
                e21 VARCHAR,
                e22 NVARCHAR) 
ATTRIBUTES (
            code1 INT2 NOT NULL,code2 INT,code3 INT8,
            code4 FLOAT4 ,code5 FLOAT8,
            code6 BOOL,
            code7 VARCHAR,code8 VARCHAR(128) NOT NULL,
            code9 VARBYTES,code10 varbytes(60),
            code11 VARCHAR,code12 VARCHAR(60),
            code13 CHAR(2),code14 CHAR(1023) NOT NULL,
            code15 NCHAR,code16 NCHAR(254) NOT NULL) 
PRIMARY TAGS(code1,code14,code8,code16);

CREATE TABLE test_SELECT_join_rel.t1(
    k_timestamp TIMESTAMPTZ NOT NULL,
    id INT NOT NULL,
    e1 INT2,
    e2 INT,
    e3 INT8,
    e4 FLOAT4,
    e5 FLOAT8,
    e6 BOOL,
    e7 TIMESTAMPTZ,
    e8 CHAR(1023),
    e9 NCHAR(255),
    e10 VARCHAR(4096),
    e11 CHAR,
    e12 CHAR(255),
    e13 NCHAR,
    e14 NVARCHAR(4096),
    e15 VARCHAR(1023), 
    e16 NVARCHAR(200),
    e17 NCHAR(255),
    e18 CHAR(200),           
    e19 VARBYTES,
    e20 VARBYTES,
    e21 VARCHAR,
    e22 NVARCHAR,
    code1 INT2 NOT NULL,
    code2 INT,code3 INT8,
    code4 FLOAT4 ,code5 FLOAT8,
    code6 BOOL,
    code7 VARCHAR,code8 VARCHAR(128) NOT NULL,
    code9 VARBYTES,code10 VARBYTES,
    code11 VARCHAR,code12 VARCHAR(60),
    code13 CHAR(2),code14 CHAR(1023) NOT NULL,
    code15 NCHAR,code16 NCHAR(254) NOT NULL );

INSERT INTO test_select_join.t1 VALUES
(0,1,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','',''),
(1,2,0,0,0,0,0,true,999999,'          ','          ','          ',' ','          ',' ',' ','          ','          ','          ',' ',' ','          ','          ','          ',0,0,0,0,0,TRUE,'          ',' ',' ','          ','          ','          ','  ','          ',' ','          '),
('1976-10-20 12:00:12.123',3,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16'),
('1979-2-28 11:59:01.999',4,20002,20000002,200000000002,-20873209.0220322201,-22012110.113011921,false,123,'test数据库语法查询测试！！！@TEST4-8','test数据库语法查询测试！！！@TEST4-9','test数据库语法查询测试！！！@TEST4-10','t','test数据库语法查询测试！！！@TEST4-12','中','test数据库语法查询测试！！！@TEST4-14','test数据库语法查询测试！！！@TEST4-15','test数据库语法查询测试！TEST4-16xaa','test数据库语法查询测试！！！@TEST4-17','test数据库语法查询测试！！！@TEST4-18',b'\xca','test数据库语法查询测试！！！@TEST4-20','test数据库语法查询测试！！！@TEST4-21','test数据库语法查询测试！！！@TEST4-22',20002,-20000002,200000000002,-20873209.0220322201,22012110.113011921,true,'test数据库语法查询测试！！！@TEST4-7','test数据库语法查询测试！！！@TEST4-8',b'\xbb','test数据库语法查询测试！！！@TEST4-10','test数据库语法查询测试！！！@TEST4-11','test数据库语法查询测试！！！@TEST4-12','t3','test数据库语法查询测试！！！@TEST4-14','中','test数据库语法查询测试！！！@TEST4-16'),
(318193261000,5,30003,30000003,300000000003,-33472098.11312001,-39009810.333011921,true,'2015-3-12 10:00:00.234','test数据库语法查询测试！！！@TEST5-8','test数据库语法查询测试！！！@TEST5-9','test数据库语法查询测试！！！@TEST5-10','t','test数据库语法查询测试！！！@TEST5-12','中','test数据库语法查询测试！！！@TEST5-14','test数据库语法查询测试！！！@TEST5-15','test数据库语法查询测试！TEST5-16xaa','test数据库语法查询测试！！！@TEST5-17','test数据库语法查询测试！！！@TEST5-18',b'\xca','test数据库语法查询测试！！！@TEST5-20','test数据库语法查询测试！！！@TEST5-21','test数据库语法查询测试！！！@TEST5-22',-30003,30000003,-300000000003,33472098.11312001,-39009810.333011921,false,'test数据库语法查询测试！！！@TEST5-7','test数据库语法查询测试！！！@TEST5-8',b'\xcc','test数据库语法查询测试！！！@TEST5-10','test数据库语法查询测试！！！@TEST5-11','test数据库语法查询测试！！！@TEST5-12','t3','test数据库语法查询测试！！！@TEST5-14','中','test数据库语法查询测试！！！@TEST5-16'),
(318993291090,6,-10001,10000001,-100000000001,1047200.00312001,1109810.113011921,false,'2023-6-23 05:00:00.55','test数据库语法查询测试！！！@TEST6-8','test数据库语法查询测试！！！@TEST6-9','test数据库语法查询测试！！！@TEST6-10','t','test数据库语法查询测试！！！@TEST6-12','中','test数据库语法查询测试！！！@TEST6-14','test数据库语法查询测试！！！@TEST6-15','test数据库语法查询测试！TEST6-16xaa','test数据库语法查询测试！！！@TEST6-17','test数据库语法查询测试！！！@TEST6-18',b'\xca','test数据库语法查询测试！！！@TEST6-20','test数据库语法查询测试！！！@TEST6-21','test数据库语法查询测试！！！@TEST6-22',10001,-10000001,100000000001,424721.022311,4909810.11301191,true,'test数据库语法查询测试！！！@TEST6-7','test数据库语法查询测试！！！@TEST6-8',b'\xdd','test数据库语法查询测试！！！@TEST6-10','test数据库语法查询测试！！！@TEST6-11','test数据库语法查询测试！！！@TEST6-12','t3','test数据库语法查询测试！！！@TEST6-14','中','test数据库语法查询测试！！！@TEST6-16'),
(318995291029,7,-20002,20000002,-200000000002,20873209.0220322201,22012110.113011921,true,'2016-7-17 20:12:00.12','test数据库语法查询测试！！！@TEST7-8','test数据库语法查询测试！！！@TEST7-9','test数据库语法查询测试！！！@TEST7-10','t','test数据库语法查询测试！！！@TEST7-12','中','test数据库语法查询测试！！！@TEST7-14','test数据库语法查询测试！！！@TEST7-15','test数据库语法查询测试！TEST7-16xaa','test数据库语法查询测试！！！@TEST7-17','test数据库语法查询测试！！！@TEST7-18',b'\xca','test数据库语法查询测试！！！@TEST7-20','test数据库语法查询测试！！！@TEST7-21','test数据库语法查询测试！！！@TEST7-22',-20002,20000002,-200000000002,555500.0055505,55505532.553015321,false,'test数据库语法查询测试！！！@TEST7-7','test数据库语法查询测试！！！@TEST7-8',b'\xee','test数据库语法查询测试！！！@TEST7-10','test数据库语法查询测试！！！@TEST7-11','test数据库语法查询测试！！！@TEST7-12','t3','test数据库语法查询测试！！！@TEST7-14','中','test数据库语法查询测试！！！@TEST7-16'),
(318995302501,8,-30003,30000003,-300000000003,33472098.11312001,39009810.333011921,false,4565476,'test数据库语法查询测试！！！@TEST8-8','test数据库语法查询测试！！！@TEST8-9','test数据库语法查询测试！！！@TEST8-10','t','test数据库语法查询测试！！！@TEST8-12','中','test数据库语法查询测试！！！@TEST8-14','test数据库语法查询测试！！！@TEST8-15','test数据库语法查询测试！TEST8-16xaa','test数据库语法查询测试！！！@TEST8-17','test数据库语法查询测试！！！@TEST8-18',b'\xca','test数据库语法查询测试！！！@TEST8-20','test数据库语法查询测试！！！@TEST8-21','test数据库语法查询测试！！！@TEST8-22',30003,-30000003,300000000003,6900.0012345,6612.1215,true,'test数据库语法查询测试！！！@TEST8-7','test数据库语法查询测试！！！@TEST8-8',b'\xff','test数据库语法查询测试！！！@TEST8-10','test数据库语法查询测试！！！@TEST8-11','test数据库语法查询测试！！！@TEST8-12','t3','test数据库语法查询测试！！！@TEST8-14','中','test数据库语法查询测试！！！@TEST8-16'),
('2001-12-9 09:48:12.30',9,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,30000,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull'),
('2002-2-22 10:48:12.899',10,32767,-2147483648,9223372036854775807,-99999999991.9999999991,9999999999991.999999999991,true,'2020-10-1 12:00:01.0','test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t','test数据库语法查询测试！！！@TEST10-12','中','test数据库语法查询测试！！！@TEST10-14','test数据库语法查询测试！！！@TEST10-15','test数据库语法查询测试！TEST10-16xaa','test数据库语法查询测试！！！@TEST10-17','test数据库语法查询测试！！！@TEST10-18',b'\xca','test数据库语法查询测试！！！@TEST10-20','test数据库语法查询测试！！！@TEST10-21','test数据库语法查询测试！！！@TEST10-22',1,111,1111111,1472011.12345,1109810.113011921,false,'test数据库语法查询测试！！！@TEST10-7','test数据库语法查询测试！！！@TEST10-8',b'\xcc','test数据库语法查询测试！！！@TEST10-10','test数据库语法查询测试！！！@TEST10-11','test数据库语法查询测试！！！@TEST10-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16'),
('2003-10-1 11:48:12.1',11,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',2,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST11-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16'),
('2004-9-9 00:00:00.9',12,12000,12000000,120000000000,-12000021.003125,-122209810.1131921,true,'2129-3-1 12:00:00.011','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','t','aaaaaabbbbbbcccccc','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','c','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'aaaaaabbbbbbcccccc','b','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','ty','aaaaaabbbbbbcccccc','u','aaaaaabbbbbbcccccc'),
('2004-12-31 12:10:10.911',13,23000,23000000,230000000000,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK',20002,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK'),
('2008-2-29 2:10:10.111',14,32767,34000000,340000000000,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321'),
('2012-02-29 1:10:10.000',15,-32767,-34000000,-340000000000,43000079.07812032,122209810.1131921,true,'2099-9-1 11:01:00.111','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','1','数据库语法查询测试','2','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','9','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试',10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'数据库语法查询测试','8','7','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','65','数据库语法查询测试','4','数据库语法查询测试'),
(1344618710110,16,11111,-11111111,111111111111,-11111.11111,11111111.11111111,false,'2017-12-11 09:10:00.200',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',0,0,0,0,0,false,e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\''),
(1374618710110,17,-11111,11111111,-111111111111,11111.11111,-11111111.11111111,true,'2036-2-3 10:10:00.089',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',0,0,0,0,0,false,e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ '),
(1574618710110,18,22222,-22222222,222222222222,-22222.22222,22222222.22222222,false,'2012-1-1 12:12:00.049' ,e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',0,0,0,0,0,false,e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\'),
(1874618710110,19,-22222,22222222,-222222222222,22222.22222,-22222222.22222222,true,'1980-6-27 19:17:00.123','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,0,0,0,0,0,false,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\ ' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\'),
(9223372036000,20,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');

INSERT INTO test_select_join_rel.t1 VALUES
('1970-01-01 00:00:00+00:00',1,0,0,0,0,0,true,'1970-01-01 00:16:39.999+00:00','','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','',''),
('1970-1-1 8:00:00.001',2,0,0,0,0,0,true,'1970-01-01 00:16:39.999+00:00','          ','          ','          ',' ','          ',' ',' ','          ','          ','          ',' ',' ','          ','          ','          ',0,0,0,0,0,TRUE,'          ',' ',' ','          ','          ','          ','  ','          ',' ','          '),
('1976-10-20 12:00:12.123+00:00',3,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-03-01 12:00:00.909+00:00','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16'),
('1979-2-28 11:59:01.999',4,20002,20000002,200000000002,-20873209.0220322201,-22012110.113011921,false,'1970-01-01 00:00:00.123+00:00','test数据库语法查询测试！！！@TEST4-8','test数据库语法查询测试！！！@TEST4-9','test数据库语法查询测试！！！@TEST4-10','t','test数据库语法查询测试！！！@TEST4-12','中','test数据库语法查询测试！！！@TEST4-14','test数据库语法查询测试！！！@TEST4-15','test数据库语法查询测试！TEST4-16xaa','test数据库语法查询测试！！！@TEST4-17','test数据库语法查询测试！！！@TEST4-18',b'\xca','test数据库语法查询测试！！！@TEST4-20','test数据库语法查询测试！！！@TEST4-21','test数据库语法查询测试！！！@TEST4-22',20002,-20000002,200000000002,-20873209.0220322201,22012110.113011921,true,'test数据库语法查询测试！！！@TEST4-7','test数据库语法查询测试！！！@TEST4-8',b'\xbb','test数据库语法查询测试！！！@TEST4-10','test数据库语法查询测试！！！@TEST4-11','test数据库语法查询测试！！！@TEST4-12','t3','test数据库语法查询测试！！！@TEST4-14','中','test数据库语法查询测试！！！@TEST4-16'),
('1980-01-31 19:01:01+00:00',5,30003,30000003,300000000003,-33472098.11312001,-39009810.333011921,true,'2015-3-12 10:00:00.234','test数据库语法查询测试！！！@TEST5-8','test数据库语法查询测试！！！@TEST5-9','test数据库语法查询测试！！！@TEST5-10','t','test数据库语法查询测试！！！@TEST5-12','中','test数据库语法查询测试！！！@TEST5-14','test数据库语法查询测试！！！@TEST5-15','test数据库语法查询测试！TEST5-16xaa','test数据库语法查询测试！！！@TEST5-17','test数据库语法查询测试！！！@TEST5-18',b'\xca','test数据库语法查询测试！！！@TEST5-20','test数据库语法查询测试！！！@TEST5-21','test数据库语法查询测试！！！@TEST5-22',-30003,30000003,-300000000003,33472098.11312001,-39009810.333011921,false,'test数据库语法查询测试！！！@TEST5-7','test数据库语法查询测试！！！@TEST5-8',b'\xcc','test数据库语法查询测试！！！@TEST5-10','test数据库语法查询测试！！！@TEST5-11','test数据库语法查询测试！！！@TEST5-12','t3','test数据库语法查询测试！！！@TEST5-14','中','test数据库语法查询测试！！！@TEST5-16'),
('1980-02-10 01:14:51.09+00:00',6,-10001,10000001,-100000000001,1047200.00312001,1109810.113011921,false,'2023-6-23 05:00:00.55','test数据库语法查询测试！！！@TEST6-8','test数据库语法查询测试！！！@TEST6-9','test数据库语法查询测试！！！@TEST6-10','t','test数据库语法查询测试！！！@TEST6-12','中','test数据库语法查询测试！！！@TEST6-14','test数据库语法查询测试！！！@TEST6-15','test数据库语法查询测试！TEST6-16xaa','test数据库语法查询测试！！！@TEST6-17','test数据库语法查询测试！！！@TEST6-18',b'\xca','test数据库语法查询测试！！！@TEST6-20','test数据库语法查询测试！！！@TEST6-21','test数据库语法查询测试！！！@TEST6-22',10001,-10000001,100000000001,424721.022311,4909810.11301191,true,'test数据库语法查询测试！！！@TEST6-7','test数据库语法查询测试！！！@TEST6-8',b'\xdd','test数据库语法查询测试！！！@TEST6-10','test数据库语法查询测试！！！@TEST6-11','test数据库语法查询测试！！！@TEST6-12','t3','test数据库语法查询测试！！！@TEST6-14','中','test数据库语法查询测试！！！@TEST6-16'),
('1980-02-10 01:48:11.029+00:00',7,-20002,20000002,-200000000002,20873209.0220322201,22012110.113011921,true,'2016-07-17 20:12:00.12+00:00','test数据库语法查询测试！！！@TEST7-8','test数据库语法查询测试！！！@TEST7-9','test数据库语法查询测试！！！@TEST7-10','t','test数据库语法查询测试！！！@TEST7-12','中','test数据库语法查询测试！！！@TEST7-14','test数据库语法查询测试！！！@TEST7-15','test数据库语法查询测试！TEST7-16xaa','test数据库语法查询测试！！！@TEST7-17','test数据库语法查询测试！！！@TEST7-18',b'\xca','test数据库语法查询测试！！！@TEST7-20','test数据库语法查询测试！！！@TEST7-21','test数据库语法查询测试！！！@TEST7-22',-20002,20000002,-200000000002,555500.0055505,55505532.553015321,false,'test数据库语法查询测试！！！@TEST7-7','test数据库语法查询测试！！！@TEST7-8',b'\xee','test数据库语法查询测试！！！@TEST7-10','test数据库语法查询测试！！！@TEST7-11','test数据库语法查询测试！！！@TEST7-12','t3','test数据库语法查询测试！！！@TEST7-14','中','test数据库语法查询测试！！！@TEST7-16'),
('1980-02-10 01:48:22.501+00:00',8,-30003,30000003,-300000000003,33472098.11312001,39009810.333011921,false,'11970-01-01 01:16:05.476+00:00','test数据库语法查询测试！！！@TEST8-8','test数据库语法查询测试！！！@TEST8-9','test数据库语法查询测试！！！@TEST8-10','t','test数据库语法查询测试！！！@TEST8-12','中','test数据库语法查询测试！！！@TEST8-14','test数据库语法查询测试！！！@TEST8-15','test数据库语法查询测试！TEST8-16xaa','test数据库语法查询测试！！！@TEST8-17','test数据库语法查询测试！！！@TEST8-18',b'\xca','test数据库语法查询测试！！！@TEST8-20','test数据库语法查询测试！！！@TEST8-21','test数据库语法查询测试！！！@TEST8-22',30003,-30000003,300000000003,6900.0012345,6612.1215,true,'test数据库语法查询测试！！！@TEST8-7','test数据库语法查询测试！！！@TEST8-8',b'\xff','test数据库语法查询测试！！！@TEST8-10','test数据库语法查询测试！！！@TEST8-11','test数据库语法查询测试！！！@TEST8-12','t3','test数据库语法查询测试！！！@TEST8-14','中','test数据库语法查询测试！！！@TEST8-16'),
('2001-12-09 09:48:12.3+00:00',9,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,30000,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull'),
('2002-2-22 10:48:12.899',10,32767,-2147483648,9223372036854775807,-99999999991.9999999991,9999999999991.999999999991,true,'2020-10-01 12:00:01+00:00','test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t','test数据库语法查询测试！！！@TEST10-12','中','test数据库语法查询测试！！！@TEST10-14','test数据库语法查询测试！！！@TEST10-15','test数据库语法查询测试！TEST10-16xaa','test数据库语法查询测试！！！@TEST10-17','test数据库语法查询测试！！！@TEST10-18',b'\xca','test数据库语法查询测试！！！@TEST10-20','test数据库语法查询测试！！！@TEST10-21','test数据库语法查询测试！！！@TEST10-22',1,111,1111111,1472011.12345,1109810.113011921,false,'test数据库语法查询测试！！！@TEST10-7','test数据库语法查询测试！！！@TEST10-8',b'\xcc','test数据库语法查询测试！！！@TEST10-10','test数据库语法查询测试！！！@TEST10-11','test数据库语法查询测试！！！@TEST10-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16'),
('2003-10-1 11:48:12.1',11,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,'1970-11-25 09:23:07.421+00:00','test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',2,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST11-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16'),
('2004-09-09 00:00:00.9+00:00',12,12000,12000000,120000000000,-12000021.003125,-122209810.1131921,true,'2129-3-1 12:00:00.011','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','t','aaaaaabbbbbbcccccc','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','c','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'aaaaaabbbbbbcccccc','b','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','ty','aaaaaabbbbbbcccccc','u','aaaaaabbbbbbcccccc'),
('2004-12-31 12:10:10.911+00:00',13,23000,23000000,230000000000,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK',20002,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK'),
('2008-2-29 2:10:10.111',14,32767,34000000,340000000000,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321'),
('2012-02-29 1:10:10.000',15,-32767,-34000000,-340000000000,43000079.07812032,122209810.1131921,true,'2099-9-1 11:01:00.111','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','1','数据库语法查询测试','2','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','9','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试',10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'数据库语法查询测试','8','7','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','65','数据库语法查询测试','4','数据库语法查询测试'),
('2012-08-10 17:11:50.11+00:00',16,11111,-11111111,111111111111,-11111.11111,11111111.11111111,false,'2017-12-11 09:10:00.200',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',0,0,0,0,0,false,e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\''),
('2013-07-23 22:31:50.11+00:00',17,-11111,11111111,-111111111111,11111.11111,-11111111.11111111,true,'2036-02-03 10:10:00.089+00:00',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',0,0,0,0,0,false,e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ '),
('2019-11-24 18:05:10.11+00:00',18,22222,-22222222,222222222222,-22222.22222,22222222.22222222,false,'2012-01-01 12:12:00.049+00:00' ,e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',0,0,0,0,0,false,e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\'),
('2029-05-27 23:25:10.11+00:00',19,-22222,22222222,-222222222222,22222.22222,-22222222.22222222,true,'1980-06-27 19:17:00.123+00:00','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,0,0,0,0,0,false,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\ ' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\'),
('2262-04-11 23:47:16+00:00',20,-1,1,-1,1.125,-2.125,false,'2020-01-01 12:00:00+00:00','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');

-- add test case for nchar
SELECT t1.id,t11.id FROM test_select_join.t1 t1 JOIN test_select_join_rel.t1 t11 ON t1.code15 = t11.code15 ORDER BY t1.id,t11.id;

SELECT
    t1.id, t1.code9, t11.id, t11.code9
FROM
    test_select_join.t1 t1
    JOIN test_select_join_rel.t1 t11
    ON t1.code9 = t11.code9
order by
    t1.id,t11.id;

SELECT
    COUNT(*)
FROM
    test_select_join.t1 t1
    JOIN test_select_join_rel.t1 t11
    ON t1.code9 = t11.code9;

delete from system.table_statistics where name = '_stats_';
set enable_multimodel=false;
drop database mtagdb cascade;
drop database pipec_r cascade;
drop database db_pipec cascade;
DROP DATABASE IF EXISTS test_SELECT_join cascade;
DROP DATABASE IF EXISTS test_SELECT_join_rel cascade;
set cluster setting ts.sql.query_opt_mode = 1110;
set cluster setting sql.stats.tag_automatic_collection.enabled = true;
