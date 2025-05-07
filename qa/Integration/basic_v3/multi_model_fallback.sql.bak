set cluster setting ts.sql.query_opt_mode = 1100;
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

create database test_rel;
create table test_rel.rel_t1(c1 UUID NOT NULL DEFAULT gen_random_uuid(), 
                             c2 BIT, c3 INET, c4 JSONB, c5 INT ARRAY, 
                             c6 INT, c7 FLOAT, c8 TIMESTAMP);
                    
insert into test_rel.rel_t1(c2, c3, c4, c5, c6, c7, c8) values 
   (B'1', '192.168.0.1', '{"type": "account creation", "username": "harvestboy93"}', 
    ARRAY[10,20,30], 5, 75.1234567, TIMESTAMP '2024-01-04 14:32:01');
insert into test_rel.rel_t1(c2, c3, c4, c5, c6, c7, c8) values 
   (B'0', '192.168.0.2', '{"type": "account creation", "username": "hungrygame"}', 
    ARRAY[15,25,35], 4, 65.31, TIMESTAMP '2024-01-04 14:33:01');

set enable_multimodel=true;

-- query 1
-- cross join is not supported

explain SELECT si.station_name,
       COUNT(t.measure_value),
       AVG(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
GROUP BY si.station_name
HAVING COUNT(t.measure_value) > 0
ORDER BY si.station_name;

-- query 2
-- stddev is not supported
explain SELECT si.station_name,
       STDDEV(t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 5
  AND t.measure_value > 80
GROUP BY si.station_name
ORDER BY si.station_name;


-- query 3
-- corr is not supported
explain SELECT si.station_name,
       CORR(t.measure_type, t.measure_value)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 5
  AND t.measure_value > 80
GROUP BY si.station_name
ORDER BY si.station_name;

-- query 4
-- string_agg is not supported
explain SELECT si.station_name, string_agg(t.pipeline_sn, t.work_area_sn)
FROM pipec_r.station_info si,
     pipec_r.workarea_info wi,
     db_pipec.t_point t
WHERE wi.work_area_name = 'work_area_1'
  AND wi.work_area_sn = si.work_area_sn
  AND si.station_sn = t.station_sn
  AND t.measure_type = 5
  AND t.measure_value > 80
GROUP BY si.station_name
ORDER BY si.station_name;

-- query 5
-- unsupported data type
explain SELECT rt.c4,
       COUNT(si.station_sn),
       COUNT(rt.c2),
       AVG(t.measure_value)
FROM pipec_r.station_info si,              -- 436
     pipec_r.workarea_info wi,             -- 41
     db_pipec.t_point t,                   -- 45M
     test_rel.rel_t1 rt
WHERE wi.work_area_sn = si.work_area_sn    -- 41, 41
  AND si.station_sn = t.station_sn         -- 436, 401
  AND t.measure_type = rt.c6
  AND t.measure_value > 80                 -- est 1/3, act 8995243/45M = 0.19989
GROUP BY rt.c4;

-- query 6
-- cast on tag columns are not supported 
explain SELECT wi.work_area_name,
       t.measure_type,
       COUNT(DISTINCT t.point_sn) AS measure_point_count
FROM pipec_r.pipeline_info li,          -- 26
     pipec_r.station_info si,           -- 436
     pipec_r.workarea_info wi,          -- 41
     db_pipec.t_point t                 -- 45M
WHERE cast(li.pipeline_sn as string(9)) = cast(t.pipeline_sn as string(8))    -- 26, 21
  AND si.work_area_sn = wi.work_area_sn -- 41, 41
  AND si.work_area_sn = t.work_area_sn  -- 41, 41
  AND li.pipeline_name = 'pipeline_1'   -- 1/26
GROUP BY
    wi.work_area_name, t.measure_type
ORDER BY
    wi.work_area_name, t.measure_type;

-- query 7
-- mismatch in left join columns' positions with relationalInfo
explain SELECT wi.work_area_name,
       t.measure_type,
       COUNT(DISTINCT t.point_sn) AS measure_point_count
FROM pipec_r.pipeline_info li,          -- 26
     pipec_r.station_info si,           -- 436
     pipec_r.workarea_info wi,          -- 41
     db_pipec.t_point t                 -- 45M
WHERE cast(li.pipeline_sn as float) = t.measure_type
  AND si.work_area_sn = wi.work_area_sn
  AND si.work_area_sn = t.work_area_sn
  AND li.pipeline_name = 'pipeline_1'
GROUP BY
    wi.work_area_name, t.measure_type
ORDER BY
    wi.work_area_name, t.measure_type;

-- query 8
-- fall back: join between time-series tables
explain SELECT li.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info li,
     test_rel.rel_t1 t1,
     db_pipec.t_point t                -- 45M
WHERE t1.c7 = t.measure_value   -- 26, 21
GROUP BY
    li.pipeline_name,
    t.measure_type,
    timebucket
ORDER BY
    li.pipeline_name,
    t.measure_type,
    timebucket;

-- query 9
-- fall back: join on time-series metrics column
explain SELECT si.station_name,
       COUNT(DISTINCT point_sn) AS abnormal_point_count
FROM pipec_r.pipeline_info li,      
     pipec_r.station_info si,       
     db_pipec.t_point t             
WHERE li.pipeline_sn = t.pipeline_sn            
    AND t.station_sn = si.station_sn             
    AND li.pipeline_name = 'pipeline_1'         
    AND t.measure_type = 4                      
    AND t.k_timestamp >= '2023-08-01 00:00:00'
    AND t.k_timestamp <= '2024-08-01 01:00:00'   
    AND t.measure_value < 0.5 * (
        SELECT AVG(t1.measure_value) 
        FROM db_pipec.t_point t1               
        WHERE t1.pipeline_sn = li.pipeline_sn    
          AND t1.measure_type = 4)              
GROUP BY
    si.station_name
ORDER BY
    abnormal_point_count DESC;

-- query 10
-- fall back: mismatch in join columns' type or length
explain SELECT si.station_name
FROM pipec_r.station_info si,              
     pipec_r.workarea_info wi,             
     db_pipec.t_point t                   
WHERE wi.work_area_name = 'work_area_1'    
  AND wi.work_area_sn = si.work_area_sn        
  AND t.measure_type = cast(si.station_sn as int)                  
  AND t.measure_value > 80                
GROUP BY si.station_name
ORDER BY si.station_name;

-- query 11
-- fall back: unsupported aggregation function or expression
explain SELECT
    LOWER(wi.work_area_name) AS work_area_name,  
    CONCAT(si.station_name, ' Station') AS station_name,  
    t.measure_type + 1,
    time_bucket(t.k_timestamp, '10s') as timebucket,
    (EXTRACT(EPOCH FROM t.k_timestamp) / 600)::int * 600 AS timebucket_epoch,
    (CASE 
        WHEN t.measure_value > 100 THEN 'High'
        ELSE 'Low'
    END) AS value_range,
    AVG(t.measure_value) AS avg_value,
    MAX(t.measure_value) AS max_value,
    MIN(t.measure_value) AS min_value,
    COUNT(t.measure_value) AS number_of_values
FROM
    pipec_r.station_info si,
    pipec_r.workarea_info wi,
    pipec_r.pipeline_info li,
    pipec_r.point_info pi,
    db_pipec.t_point t
WHERE
    li.pipeline_sn = pi.pipeline_sn
    AND pi.station_sn = si.station_sn
    AND si.work_area_sn = wi.work_area_sn
    AND t.point_sn = pi.point_sn
    AND li.pipeline_name = 'pipeline_1'
    AND wi.work_area_name IN ('work_area_1', 'work_area_2', 'work_area_3')
    AND t.k_timestamp >= '2023-08-01 01:00:00'
GROUP BY
    LOWER(wi.work_area_name), 
    CONCAT(si.station_name, ' Station'), 
    t.measure_type + 1,
    time_bucket(t.k_timestamp, '10s'),
    (EXTRACT(EPOCH FROM t.k_timestamp) / 600)::int * 600, 
    (CASE 
        WHEN t.measure_value > 100 THEN 'High'
        ELSE 'Low'
    END);

-- haning bug:
set enable_multimodel=false;

SELECT si.station_name,
       COUNT(DISTINCT point_sn) AS abnormal_point_count
FROM pipec_r.pipeline_info li,
     pipec_r.station_info si,
     db_pipec.t_point t
WHERE li.pipeline_sn = t.pipeline_sn
    AND t.station_sn = si.station_sn
    AND li.pipeline_name = 'pipeline_1'
    AND t.k_timestamp >= '2023-08-01 00:00:00'
    AND t.k_timestamp <= '2025-08-01 01:00:00'
    AND t.measure_value < 2 * (
        SELECT AVG(t1.measure_value)
        FROM db_pipec.t_point t1
        WHERE t1.pipeline_sn = li.pipeline_sn)
GROUP BY
    si.station_name
ORDER BY
    abnormal_point_count DESC;

set enable_multimodel=true;

SELECT si.station_name,
       COUNT(DISTINCT point_sn) AS abnormal_point_count
FROM pipec_r.pipeline_info li,
     pipec_r.station_info si,
     db_pipec.t_point t
WHERE li.pipeline_sn = t.pipeline_sn
    AND t.station_sn = si.station_sn
    AND li.pipeline_name = 'pipeline_1'
    AND t.k_timestamp >= '2023-08-01 00:00:00'
    AND t.k_timestamp <= '2025-08-01 01:00:00'
    AND t.measure_value < 2 * (
        SELECT AVG(t1.measure_value)
        FROM db_pipec.t_point t1
        WHERE t1.pipeline_sn = li.pipeline_sn)
GROUP BY
    si.station_name
ORDER BY
    abnormal_point_count DESC;

DROP DATABASE IF EXISTS test_select_last_add cascade;
CREATE ts DATABASE test_select_last_add;
CREATE TABLE test_select_last_add.t1(
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
                e20 VARBYTES(60),
                e21 VARCHAR,
                e22 NVARCHAR)
ATTRIBUTES (
            code1 INT2 NOT NULL,code2 INT,code3 INT8,
            code4 FLOAT4 ,code5 FLOAT8,
            code6 BOOL,
            code7 VARCHAR,code8 VARCHAR(128) NOT NULL,
            code9 VARBYTES,code10 VARBYTES(60),
            code11 VARCHAR,code12 VARCHAR(60),
            code13 CHAR(2),code14 CHAR(1023) NOT NULL,
            code15 NCHAR,code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code1,code14,code8,code16);

SELECT
	e5,e21,code16,code8
FROM
	test_select_last_add.t1 as tab1
	JOIN (
		SELECT
			last(e5 ,'1980-01-31 19:01:01') LAST5,
			last(e21,'1970-01-01 00:00:00.001') AS LAST21,
			last(code16) AS LAST16,last(code8,'2970-01-01 00:00:00') AS LAST8
		FROM
			test_select_last_add.t1
	) as tab2
	ON
		tab1.e21 >= tab2.LAST21
		AND tab1.e8 = tab2.LAST8
ORDER BY e5,e21,code16,code8;

SELECT
    count(LE)
FROM (
    SELECT
        id, last(e1 ,'1970-01-01 00:00:00')  LE
    FROM
        test_select_last_add.t1 t1
    WHERE
        e11 != (
            SELECT
                max(e11)
            FROM
                test_select_last_add.t1 t2
            WHERE
                t1.e12 <t2.e12
                AND t1.code13 LIKE 't3'
                OR code1 IN (0)
            )
    GROUP BY id,e11 HAVING e11 >='t'
    ORDER BY id
    );

SELECT
	max(e1),min(e2),avg(e4),sum(e5)
FROM
	test_select_last_add.t1 as t1
	JOIN (
		SELECT last(e1 ,'1970-01-01 00:00:00'), last(e2 ,'1970-01-01 00:00:00.001'),last(e4 ,'1979-02-28 11:59:01.999'),last(e5 ,'1980-01-31 19:01:01')
		FROM test_select_last_add.t1 as t2
	)
	ON t1.code2 IN (SELECT t1.code2 FROM test_select_last_add.t1 WHERE e1>0)
GROUP BY e1,e2,e4,e5
HAVING max(e1)>1000
ORDER BY e1,e2,e4,e5;

set enable_multimodel=false;
drop database test_select_last_add cascade;
drop database pipec_r cascade;
drop database db_pipec cascade;
drop database test_rel cascade;
set cluster setting ts.sql.query_opt_mode = 1110;
