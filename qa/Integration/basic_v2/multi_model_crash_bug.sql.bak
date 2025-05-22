set cluster setting ts.sql.query_opt_mode = 1100;

-- relational table 
CREATE DATABASE pipec_r;

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
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = '_stats_';

set enable_multimodel=true;

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

-- random crash bug 44890, which could be reproduced through running many times

set hash_scan_mode = 2;
explain SELECT li.pipeline_name,
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

set enable_multimodel=false;
drop database pipec_r cascade;
drop database mtagdb cascade;
delete from system.table_statistics where name = '_stats_';
set cluster setting ts.sql.query_opt_mode = 1110;