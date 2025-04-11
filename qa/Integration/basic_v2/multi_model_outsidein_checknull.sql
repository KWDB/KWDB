set cluster setting ts.sql.query_opt_mode = 1100;
set cluster setting sql.stats.tag_automatic_collection.enabled = false;
set cluster setting sql.stats.automatic_collection.enabled = false;
-- create and insert data into time-series table
CREATE TS DATABASE my_timeseries_db;

CREATE TABLE my_timeseries_db.firegases (
  "time" TIMESTAMPTZ NOT NULL,
  "value" FLOAT8 NULL
) TAGS (
  adr VARCHAR(20) NOT NULL,
  state VARCHAR(20) NULL,
  second_val FLOAT8 NULL
) PRIMARY TAGS (
  adr
);

INSERT INTO my_timeseries_db.firegases VALUES 
  ('2024-09-01 10:00:00+00', 22.5, 'adr1', 'active', 22.5),
  ('2024-09-01 10:05:00+00', NULL, 'adr2', 'inactive', NULL),
  ('2024-09-01 10:10:00+00', 30.0, 'adr3', NULL, 30.0),
  ('2024-09-01 10:15:00+00', 15.0, 'adr4', 'pending', 15.0);

-- create and insert data into relational table
CREATE DATABASE my_relation_db;

CREATE TABLE my_relation_db.station_info (
  station_sn VARCHAR(20) PRIMARY KEY NOT NULL,
  station_name VARCHAR(80) NULL,
  work_area_sn VARCHAR(20) NULL,
  expected_state VARCHAR(20) NULL,
  standard_val FLOAT8 NULL
);

INSERT INTO my_relation_db.station_info VALUES 
  ('sn1', 'Station One', 'adr1', 'active', 22.5),
  ('sn2', 'Station Two', NULL, 'inactive', 10.0),
  ('sn3', 'Station Three', 'adr3', 'pending', 35.0),
  ('sn4', 'Station Four', NULL, 'pending', NULL),
  ('sn5', 'Station Five', 'adr5', NULL, 15.5),
  ('sn6', 'Station Six', NULL, NULL, NULL);

-- create stats
CREATE STATISTICS _stats_firegases FROM my_timeseries_db.firegases;
CREATE STATISTICS _stats_station_info FROM my_relation_db.station_info;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = '_stats_firegases';
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = '_stats_station_info';

set enable_multimodel=true;

-- mode1: default
set hash_scan_mode = 0;

-- case1: 关系表过滤后无null值，时序表上有null值，join列为varchar类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
ORDER BY f."time", s.station_sn;

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
ORDER BY f."time", s.station_sn;

-- case2: 关系表上有null值，时序表通过过滤无null值，join列为varchar类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state != 'pending' and f.state = s.expected_state
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state != 'pending' and f.state = s.expected_state
ORDER BY f."time";

-- case3: 关系表、时序表join列上都有null值，join列为varchar类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
ORDER BY f."time", s.station_sn;

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
ORDER BY f."time", s.station_sn;

-- case4: 再次验证case1的情况，关系表过滤后无null值，时序表有null值，join列类型为float类型，在metric列 -- fallback
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.standard_val is not NULL AND f."value" = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.standard_val is not NULL AND f."value" = s.standard_val
ORDER BY f."time";

-- case5: 再次验证case2的情况，关系表有null值，时序表过滤后无null值，join列为float类型，在metric列 -- fallback
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" is not NULL AND f."value" = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" is not NULL AND f."value" = s.standard_val
ORDER BY f."time";

-- case6: 再次验证case3的情况， 关系表、时序表join列上都有null值，join列为float类型，在metric列 -- fallback
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" = s.standard_val
ORDER BY f."time";

-- case7: 再次验证case1的情况，关系表过滤后无null值，时序表有null值，join列类型为float类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.standard_val is not NULL AND f.second_val = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.standard_val is not NULL AND f.second_val = s.standard_val
ORDER BY f."time";

-- case8: 再次验证case2的情况，关系表有null值，时序表过滤后无null值，join列为float类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" is not NULL AND f.second_val = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" is not NULL AND f.second_val = s.standard_val
ORDER BY f."time";

-- case9: 再次验证case3的情况， 关系表、时序表join列上都有null值，join列为float类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.second_val = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.second_val = s.standard_val
ORDER BY f."time";

-- case10: with aggregation functions on NULL values
explain SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

-- case11: join on multiple columns with primary tag
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
      AND f.adr = s.work_area_sn
ORDER BY f."time", s.station_sn;

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
      AND f.adr = s.work_area_sn
ORDER BY f."time", s.station_sn;

-- case12: join on tag expression -- fallback
explain SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.second_val + 5 = s.standard_val
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.second_val + 5 = s.standard_val
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

-- case13: join on multiple columns without primary tag
explain SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.second_val = s.standard_val and f.state = s.expected_state
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.second_val = s.standard_val and f.state = s.expected_state
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

-- case 14: multiple joins on the same ts table, second join on varchar with null value
explain SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE s.expected_state is not NULL and f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.work_area_sn = ts.adr
ORDER BY ts."value";

SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE s.expected_state is not NULL and f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.work_area_sn = ts.adr
ORDER BY ts."value";

-- case 15: multiple joins on the same ts table, second join on float without null value
explain SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value") as value_avg,
      AVG(s.standard_val) as standard_avg,
      SUM(f.second_val) as second_avg,
      COUNT(s.standard_val) as standar_count,
      COUNT(f.state) as state_count,
      COUNT(s.station_name) as station_count
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.standard_avg= ts.second_val;

SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value") as value_avg,
      AVG(s.standard_val) as standard_avg,
      SUM(f.second_val) as second_avg,
      COUNT(s.standard_val) as standar_count,
      COUNT(f.state) as state_count,
      COUNT(s.station_name) as station_count
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.standard_avg= ts.second_val;

-- case 16: multiple joins on the same ts table, second join on float having expression
explain SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value") as value_avg,
      AVG(s.standard_val) as standard_avg,
      SUM(f.second_val) as second_avg,
      COUNT(s.standard_val) as standar_count,
      COUNT(f.state) as state_count,
      COUNT(s.station_name) as station_count
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.standard_avg + 5 = ts.second_val;

SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value") as value_avg,
      AVG(s.standard_val) as standard_avg,
      SUM(f.second_val) as second_avg,
      COUNT(s.standard_val) as standar_count,
      COUNT(f.state) as state_count,
      COUNT(s.station_name) as station_count
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.standard_avg + 5 = ts.second_val;

-- case 17: multiple joins on the same ts table with group by
explain SELECT *
FROM my_timeseries_db.firegases f,
    (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s,
        my_timeseries_db.firegases fire
    WHERE s.expected_state = fire.state
    GROUP BY expected_state) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

SELECT *
FROM my_timeseries_db.firegases f,
    (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s,
        my_timeseries_db.firegases fire
    WHERE s.expected_state = fire.state
    GROUP BY expected_state) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

-- case 18: multiple joins on the same ts table with UNION
explain SELECT *
FROM my_timeseries_db.firegases f,
    ((SELECT expected_state, standard_val as standard_avg
    FROM my_relation_db.station_info)
    UNION (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s,
        my_timeseries_db.firegases fire
    WHERE s.expected_state = fire.state
    GROUP BY expected_state)) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

SELECT *
FROM my_timeseries_db.firegases f,
    ((SELECT expected_state, standard_val as standard_avg
    FROM my_relation_db.station_info)
    UNION (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s,
        my_timeseries_db.firegases fire
    WHERE s.expected_state = fire.state
    GROUP BY expected_state)) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

-- case 19: multiple joins with one right join
explain SELECT *
FROM my_timeseries_db.firegases f,
    ((SELECT expected_state, standard_val as standard_avg
    FROM my_relation_db.station_info)
    UNION (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s
    RIGHT JOIN my_timeseries_db.firegases fire
    ON s.expected_state = fire.state
    GROUP BY expected_state)) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

SELECT *
FROM my_timeseries_db.firegases f,
    ((SELECT expected_state, standard_val as standard_avg
    FROM my_relation_db.station_info)
    UNION (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s
    RIGHT JOIN my_timeseries_db.firegases fire
    ON s.expected_state = fire.state
    GROUP BY expected_state)) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

-- create ts db2
CREATE TS DATABASE my_timeseries_db_v2;

-- create ts table
CREATE TABLE my_timeseries_db_v2.firegases (
  "time" TIMESTAMPTZ NOT NULL,
  second_val FLOAT8 NULL
) 
TAGS (
  adr INT NOT NULL,
  state VARCHAR(20) NULL,
  int2_val INT2 NULL,      
  int4_val INT4 NULL,      
  int_val INT NULL,        
  float4_val FLOAT4 NULL,  
  float_val FLOAT8 NULL,   
  bool_val BOOL NULL       
) 
PRIMARY TAGS (
  adr
);

INSERT INTO my_timeseries_db_v2.firegases ("time", int2_val, int4_val, int_val, float4_val, float_val, bool_val, adr, state, second_val) VALUES 
  ('2024-09-01 10:00:00+00', 10, 1000, 10000, 22.5, 22.5, TRUE, 1, 'active', 25.0),
  ('2024-09-01 10:05:00+00', 20, 2000, 20000, 18.5, NULL, FALSE, 2, 'inactive', NULL),
  ('2024-09-01 10:10:00+00', NULL, 3000, NULL, 30.0, 30.0, TRUE, 3, 'pending', 30.0),
  ('2024-09-01 10:15:00+00', 40, NULL, 40000, NULL, 15.0, NULL, 4, 'active', 15.0),
  ('2024-09-01 10:20:00+00', 50, 5000, NULL, 12.5, 12.5, TRUE, 5, NULL, NULL),
  ('2024-09-01 10:25:00+00', 60, 6000, 60000, 17.5, 17.5, FALSE, 6, 'inactive', NULL);

-- create rel db v2
CREATE DATABASE my_relation_db_v2;

-- create rel table 
CREATE TABLE my_relation_db_v2.station_info (
  station_sn INT PRIMARY KEY NOT NULL,   -- Using INT
  station_name VARCHAR(80) NULL,
  work_area_sn_int2 INT2 NULL,           -- Using INT2
  work_area_sn_int4 INT4 NULL,           -- Using INT4
  work_area_sn_int INT NULL,              -- Using INT
  expected_state VARCHAR(20) NULL,
  standard_val_float4 FLOAT4 NULL,       -- Using FLOAT4
  standard_val_float FLOAT NULL,          -- Using FLOAT
  bool_val BOOL NULL                      -- Using BOOL
);

INSERT INTO my_relation_db_v2.station_info VALUES 
  (1, 'Station One', 1, 10, 10000, 'active', 22.5, 20.0, TRUE),   -- 添加 float 列
  (2, 'Station Two', 2, NULL, 200, 'inactive', 10.0, 5.0, FALSE), 
  (3, 'Station Three', NULL, 3000, NULL, 'pending', 35.0, 30.0, TRUE),
  (4, 'Station Four', NULL, NULL, 40000, 'pending', NULL, 15.0, NULL),
  (5, 'Station Five', 40, 6000, 500, NULL, 15.5, 12.5, TRUE),
  (6, 'Station Six', NULL, NULL, NULL, NULL, NULL, NULL, FALSE);

-- create stats
CREATE STATISTICS _stats_ FROM my_timeseries_db_v2.firegases;
CREATE STATISTICS _stats_ FROM my_relation_db_v2.station_info;

-- JOIN on INT2 type (work_area_sn_int2)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.int2_val = s.work_area_sn_int2
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.int2_val = s.work_area_sn_int2
ORDER BY f."time";

-- JOIN on INT4 type (work_area_sn_int4)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.int4_val = s.work_area_sn_int4
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.int4_val = s.work_area_sn_int4
ORDER BY f."time";

-- JOIN on INT type (adr)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.adr = s.station_sn
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.adr = s.station_sn
ORDER BY f."time";

-- JOIN on FLOAT4 type (standard_val_float4)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.float4_val = s.standard_val_float4
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.float4_val = s.standard_val_float4
ORDER BY f."time";

-- JOIN on FLOAT type (standard_val_float)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.float_val = s.standard_val_float
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.float_val = s.standard_val_float
ORDER BY f."time";

-- JOIN on FLOAT8 type (second_val - metric col, should fallback)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f
JOIN my_relation_db_v2.station_info s ON f.second_val = s.standard_val_float
WHERE f.second_val IS NOT NULL AND s.standard_val_float IS NOT NULL
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f
JOIN my_relation_db_v2.station_info s ON f.second_val = s.standard_val_float
WHERE f.second_val IS NOT NULL AND s.standard_val_float IS NOT NULL
ORDER BY f."time";

-- JOIN on BOOL type (standard_val_bool)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.bool_val = s.bool_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.bool_val = s.bool_val
ORDER BY f."time", s.station_sn, f.float_val, f.int_val, f.adr;

-- mode1: hashTagScan
set hash_scan_mode = 1;

-- case1: 关系表过滤后无null值，时序表上有null值，join列为varchar类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
ORDER BY f."time", s.station_sn;

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
ORDER BY f."time", s.station_sn;

-- case2: 关系表上有null值，时序表通过过滤无null值，join列为varchar类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state != 'pending' and f.state = s.expected_state
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state != 'pending' and f.state = s.expected_state
ORDER BY f."time";

-- case3: 关系表、时序表join列上都有null值，join列为varchar类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
ORDER BY f."time", s.station_sn;

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
ORDER BY f."time", s.station_sn;

-- case4: 再次验证case1的情况，关系表过滤后无null值，时序表有null值，join列类型为float类型，在metric列 -- fallback
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.standard_val is not NULL AND f."value" = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.standard_val is not NULL AND f."value" = s.standard_val
ORDER BY f."time";

-- case5: 再次验证case2的情况，关系表有null值，时序表过滤后无null值，join列为float类型，在metric列 -- fallback
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" is not NULL AND f."value" = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" is not NULL AND f."value" = s.standard_val
ORDER BY f."time";

-- case6: 再次验证case3的情况， 关系表、时序表join列上都有null值，join列为float类型，在metric列 -- fallback
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" = s.standard_val
ORDER BY f."time";

-- case7: 再次验证case1的情况，关系表过滤后无null值，时序表有null值，join列类型为float类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.standard_val is not NULL AND f.second_val = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.standard_val is not NULL AND f.second_val = s.standard_val
ORDER BY f."time";

-- case8: 再次验证case2的情况，关系表有null值，时序表过滤后无null值，join列为float类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" is not NULL AND f.second_val = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" is not NULL AND f.second_val = s.standard_val
ORDER BY f."time";

-- case9: 再次验证case3的情况， 关系表、时序表join列上都有null值，join列为float类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.second_val = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.second_val = s.standard_val
ORDER BY f."time";

-- case10: with aggregation functions on NULL values
explain SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

-- case11: join on multiple columns with primary tag
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
      AND f.adr = s.work_area_sn
ORDER BY f."time", s.station_sn;

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
      AND f.adr = s.work_area_sn
ORDER BY f."time", s.station_sn;

-- case12: join on tag expression -- fallback
explain SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.second_val + 5 = s.standard_val
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.second_val + 5 = s.standard_val
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

-- case13: join on multiple columns withouot primary tag
explain SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.second_val = s.standard_val and f.state = s.expected_state
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.second_val = s.standard_val and f.state = s.expected_state
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

-- case 14: multiple joins on the same ts table, second join on varchar with null value
explain SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE s.expected_state is not NULL and f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.work_area_sn = ts.adr
ORDER BY ts."value";

SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE s.expected_state is not NULL and f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.work_area_sn = ts.adr
ORDER BY ts."value";

-- case 15: multiple joins on the same ts table, second join on float without null value
explain SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value") as value_avg,
      AVG(s.standard_val) as standard_avg,
      SUM(f.second_val) as second_avg,
      COUNT(s.standard_val) as standar_count,
      COUNT(f.state) as state_count,
      COUNT(s.station_name) as station_count
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.standard_avg= ts.second_val;

SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value") as value_avg,
      AVG(s.standard_val) as standard_avg,
      SUM(f.second_val) as second_avg,
      COUNT(s.standard_val) as standar_count,
      COUNT(f.state) as state_count,
      COUNT(s.station_name) as station_count
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.standard_avg= ts.second_val;

-- case 16: multiple joins on the same ts table, second join on float having expression
explain SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value") as value_avg,
      AVG(s.standard_val) as standard_avg,
      SUM(f.second_val) as second_avg,
      COUNT(s.standard_val) as standar_count,
      COUNT(f.state) as state_count,
      COUNT(s.station_name) as station_count
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.standard_avg + 5 = ts.second_val;

SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value") as value_avg,
      AVG(s.standard_val) as standard_avg,
      SUM(f.second_val) as second_avg,
      COUNT(s.standard_val) as standar_count,
      COUNT(f.state) as state_count,
      COUNT(s.station_name) as station_count
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.standard_avg + 5 = ts.second_val;

-- case 17: multiple joins on the same ts table with group by
explain SELECT *
FROM my_timeseries_db.firegases f,
    (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s,
        my_timeseries_db.firegases fire
    WHERE s.expected_state = fire.state
    GROUP BY expected_state) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

SELECT *
FROM my_timeseries_db.firegases f,
    (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s,
        my_timeseries_db.firegases fire
    WHERE s.expected_state = fire.state
    GROUP BY expected_state) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

-- case 18: multiple joins on the same ts table with UNION
explain SELECT *
FROM my_timeseries_db.firegases f,
    ((SELECT expected_state, standard_val as standard_avg
    FROM my_relation_db.station_info)
    UNION (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s,
        my_timeseries_db.firegases fire
    WHERE s.expected_state = fire.state
    GROUP BY expected_state)) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

SELECT *
FROM my_timeseries_db.firegases f,
    ((SELECT expected_state, standard_val as standard_avg
    FROM my_relation_db.station_info)
    UNION (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s,
        my_timeseries_db.firegases fire
    WHERE s.expected_state = fire.state
    GROUP BY expected_state)) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

-- case 19: multiple joins with one right join
explain SELECT *
FROM my_timeseries_db.firegases f,
    ((SELECT expected_state, standard_val as standard_avg
    FROM my_relation_db.station_info)
    UNION (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s
    RIGHT JOIN my_timeseries_db.firegases fire
    ON s.expected_state = fire.state
    GROUP BY expected_state)) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

SELECT *
FROM my_timeseries_db.firegases f,
    ((SELECT expected_state, standard_val as standard_avg
    FROM my_relation_db.station_info)
    UNION (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s
    RIGHT JOIN my_timeseries_db.firegases fire
    ON s.expected_state = fire.state
    GROUP BY expected_state)) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

-- JOIN on INT2 type (work_area_sn_int2)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.int2_val = s.work_area_sn_int2
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.int2_val = s.work_area_sn_int2
ORDER BY f."time";

-- JOIN on INT4 type (work_area_sn_int4)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.int4_val = s.work_area_sn_int4
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.int4_val = s.work_area_sn_int4
ORDER BY f."time";

-- JOIN on INT type (adr)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.adr = s.station_sn
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.adr = s.station_sn
ORDER BY f."time";

-- JOIN on FLOAT4 type (standard_val_float4)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.float4_val = s.standard_val_float4
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.float4_val = s.standard_val_float4
ORDER BY f."time";

-- JOIN on FLOAT type (standard_val_float)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.float_val = s.standard_val_float
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.float_val = s.standard_val_float
ORDER BY f."time";

-- JOIN on FLOAT8 type (second_val - metric col, should fallback)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f,
     my_relation_db_v2.station_info s
WHERE f.second_val IS NOT NULL
  AND s.standard_val_float IS NOT NULL
  AND f.second_val = s.standard_val_float
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f,
     my_relation_db_v2.station_info s
WHERE f.second_val IS NOT NULL
  AND s.standard_val_float IS NOT NULL
  AND f.second_val = s.standard_val_float
ORDER BY f."time";

-- JOIN on BOOL type (standard_val_bool)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.bool_val = s.bool_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.bool_val = s.bool_val
ORDER BY f."time", s.station_sn, f.float_val, f.int_val, f.adr;

-- mode3: primaryHashTagScan
set hash_scan_mode = 2;

-- case1: 关系表过滤后无null值，时序表上有null值，join列为varchar类型，无ptag join，预期为空；
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
ORDER BY f."time", s.station_sn;

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
ORDER BY f."time", s.station_sn;

-- case11: join on multiple columns with primary tag, expect to get results
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
      AND f.adr = s.work_area_sn
ORDER BY f."time", s.station_sn;

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
      AND f.adr = s.work_area_sn
ORDER BY f."time", s.station_sn;

-- JOIN on INT type (adr) also Ptag, expect to get results
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.adr = s.station_sn
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.adr = s.station_sn
ORDER BY f."time";

-- JOIN on VARCHAR type (adr in v1 ts table) also Ptag, expect to get results
EXPLAIN SELECT *
FROM my_timeseries_db.firegases f, my_relation_db.station_info s 
WHERE f.adr = s.work_area_sn
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f, my_relation_db.station_info s 
WHERE f.adr = s.work_area_sn
ORDER BY f."time";

-- mode4: hashRelScan
set hash_scan_mode = 3;

-- case1: 关系表过滤后无null值，时序表上有null值，join列为varchar类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
ORDER BY f."time", s.station_sn;

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
ORDER BY f."time", s.station_sn;

-- case2: 关系表上有null值，时序表通过过滤无null值，join列为varchar类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state != 'pending' and f.state = s.expected_state
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state != 'pending' and f.state = s.expected_state
ORDER BY f."time";

-- case3: 关系表、时序表join列上都有null值，join列为varchar类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
ORDER BY f."time", s.station_sn;

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
ORDER BY f."time", s.station_sn;

-- case4: 再次验证case1的情况，关系表过滤后无null值，时序表有null值，join列类型为float类型，在metric列 -- fallback
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.standard_val is not NULL AND f."value" = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.standard_val is not NULL AND f."value" = s.standard_val
ORDER BY f."time";

-- case5: 再次验证case2的情况，关系表有null值，时序表过滤后无null值，join列为float类型，在metric列 -- fallback
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" is not NULL AND f."value" = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" is not NULL AND f."value" = s.standard_val
ORDER BY f."time";

-- case6: 再次验证case3的情况， 关系表、时序表join列上都有null值，join列为float类型，在metric列 -- fallback
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" = s.standard_val
ORDER BY f."time";

-- case7: 再次验证case1的情况，关系表过滤后无null值，时序表有null值，join列类型为float类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.standard_val is not NULL AND f.second_val = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.standard_val is not NULL AND f.second_val = s.standard_val
ORDER BY f."time";

-- case8: 再次验证case2的情况，关系表有null值，时序表过滤后无null值，join列为float类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" is not NULL AND f.second_val = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f."value" is not NULL AND f.second_val = s.standard_val
ORDER BY f."time";

-- case9: 再次验证case3的情况， 关系表、时序表join列上都有null值，join列为float类型
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.second_val = s.standard_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.second_val = s.standard_val
ORDER BY f."time";

-- case10: with aggregation functions on NULL values
explain SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.state = s.expected_state
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

-- case11: join on multiple columns with primary tag
explain SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
      AND f.adr = s.work_area_sn
ORDER BY f."time", s.station_sn;

SELECT *
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE f.state = s.expected_state
      AND f.adr = s.work_area_sn
ORDER BY f."time", s.station_sn;

-- case12: join on tag expression -- fallback
explain SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.second_val + 5 = s.standard_val
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.second_val + 5 = s.standard_val
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

-- case13: join on multiple columns withouot primary tag
explain SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.second_val = s.standard_val and f.state = s.expected_state
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
FROM my_timeseries_db.firegases f,
     my_relation_db.station_info s
WHERE s.expected_state is not NULL and f.second_val = s.standard_val and f.state = s.expected_state
GROUP BY s.work_area_sn
ORDER BY s.work_area_sn;

-- case 14: multiple joins on the same ts table, second join on varchar with null value
explain SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE s.expected_state is not NULL and f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.work_area_sn = ts.adr
ORDER BY ts."value";

SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value"),
      AVG(s.standard_val),
      SUM(f.second_val),
      COUNT(s.standard_val),
      COUNT(f.state),
      COUNT(s.station_name)
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE s.expected_state is not NULL and f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.work_area_sn = ts.adr
ORDER BY ts."value";

-- case 15: multiple joins on the same ts table, second join on float without null value
explain SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value") as value_avg,
      AVG(s.standard_val) as standard_avg,
      SUM(f.second_val) as second_avg,
      COUNT(s.standard_val) as standar_count,
      COUNT(f.state) as state_count,
      COUNT(s.station_name) as station_count
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.standard_avg= ts.second_val;

SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value") as value_avg,
      AVG(s.standard_val) as standard_avg,
      SUM(f.second_val) as second_avg,
      COUNT(s.standard_val) as standar_count,
      COUNT(f.state) as state_count,
      COUNT(s.station_name) as station_count
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.standard_avg= ts.second_val;

-- case 16: multiple joins on the same ts table, second join on float having expression
explain SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value") as value_avg,
      AVG(s.standard_val) as standard_avg,
      SUM(f.second_val) as second_avg,
      COUNT(s.standard_val) as standar_count,
      COUNT(f.state) as state_count,
      COUNT(s.station_name) as station_count
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.standard_avg + 5 = ts.second_val;

SELECT *
FROM my_timeseries_db.firegases ts,
    (SELECT s.work_area_sn,
      AVG(f."value") as value_avg,
      AVG(s.standard_val) as standard_avg,
      SUM(f.second_val) as second_avg,
      COUNT(s.standard_val) as standar_count,
      COUNT(f.state) as state_count,
      COUNT(s.station_name) as station_count
    FROM my_timeseries_db.firegases f,
      my_relation_db.station_info s
    WHERE f.state = s.expected_state
    GROUP BY s.work_area_sn) as t
WHERE t.standard_avg + 5 = ts.second_val;

-- case 17: multiple joins on the same ts table with group by
explain SELECT *
FROM my_timeseries_db.firegases f,
    (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s,
        my_timeseries_db.firegases fire
    WHERE s.expected_state = fire.state
    GROUP BY expected_state) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

SELECT *
FROM my_timeseries_db.firegases f,
    (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s,
        my_timeseries_db.firegases fire
    WHERE s.expected_state = fire.state
    GROUP BY expected_state) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

-- case 18: multiple joins on the same ts table with UNION
explain SELECT *
FROM my_timeseries_db.firegases f,
    ((SELECT expected_state, standard_val as standard_avg
    FROM my_relation_db.station_info)
    UNION (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s,
        my_timeseries_db.firegases fire
    WHERE s.expected_state = fire.state
    GROUP BY expected_state)) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

SELECT *
FROM my_timeseries_db.firegases f,
    ((SELECT expected_state, standard_val as standard_avg
    FROM my_relation_db.station_info)
    UNION (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s,
        my_timeseries_db.firegases fire
    WHERE s.expected_state = fire.state
    GROUP BY expected_state)) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

-- case 19: multiple joins with one right join
explain SELECT *
FROM my_timeseries_db.firegases f,
    ((SELECT expected_state, standard_val as standard_avg
    FROM my_relation_db.station_info)
    UNION (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s
    RIGHT JOIN my_timeseries_db.firegases fire
    ON s.expected_state = fire.state
    GROUP BY expected_state)) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

SELECT *
FROM my_timeseries_db.firegases f,
    ((SELECT expected_state, standard_val as standard_avg
    FROM my_relation_db.station_info)
    UNION (SELECT expected_state, AVG(standard_val) as standard_avg
    FROM my_relation_db.station_info s
    RIGHT JOIN my_timeseries_db.firegases fire
    ON s.expected_state = fire.state
    GROUP BY expected_state)) as r
WHERE f.second_val = r.standard_avg
    AND f.state = r.expected_state;

-- JOIN on INT2 type (work_area_sn_int2)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.int2_val = s.work_area_sn_int2
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.int2_val = s.work_area_sn_int2
ORDER BY f."time";

-- JOIN on INT4 type (work_area_sn_int4)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.int4_val = s.work_area_sn_int4
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.int4_val = s.work_area_sn_int4
ORDER BY f."time";

-- JOIN on INT type (adr)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.adr = s.station_sn
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.adr = s.station_sn
ORDER BY f."time";

-- JOIN on FLOAT4 type (standard_val_float4)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.float4_val = s.standard_val_float4
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.float4_val = s.standard_val_float4
ORDER BY f."time";

-- JOIN on FLOAT type (standard_val_float)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.float_val = s.standard_val_float
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.float_val = s.standard_val_float
ORDER BY f."time";

-- JOIN on FLOAT8 type (second_val - metric col, should fallback)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f
JOIN my_relation_db_v2.station_info s ON f.second_val = s.standard_val_float
WHERE f.second_val IS NOT NULL AND s.standard_val_float IS NOT NULL
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f
JOIN my_relation_db_v2.station_info s ON f.second_val = s.standard_val_float
WHERE f.second_val IS NOT NULL AND s.standard_val_float IS NOT NULL
ORDER BY f."time";

-- JOIN on BOOL type (standard_val_bool)
EXPLAIN SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.bool_val = s.bool_val
ORDER BY f."time";

SELECT *
FROM my_timeseries_db_v2.firegases f, my_relation_db_v2.station_info s 
WHERE f.bool_val = s.bool_val
ORDER BY f."time", s.station_sn, f.float_val, f.int_val, f.adr;


set enable_multimodel=false;
set hash_scan_mode = 0;

DROP TABLE my_timeseries_db_v2.firegases;
DROP TABLE my_relation_db_v2.station_info;
DROP DATABASE my_timeseries_db_v2;
DROP DATABASE my_relation_db_v2;

DROP TABLE my_timeseries_db.firegases;
DROP TABLE my_relation_db.station_info;
DROP DATABASE my_timeseries_db;
DROP DATABASE my_relation_db;
delete  from system.table_statistics where name = '_stats_firegases' or name = '_stats_station_info' or name = '_stats_';

set cluster setting ts.sql.query_opt_mode = 1110;
set cluster setting sql.stats.tag_automatic_collection.enabled = true;
set cluster setting sql.stats.automatic_collection.enabled = true;