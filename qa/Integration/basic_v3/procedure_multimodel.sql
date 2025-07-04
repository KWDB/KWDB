-- TEST multi model
set cluster setting ts.sql.query_opt_mode = 111100;
set cluster setting sql.stats.tag_automatic_collection.enabled = false;
set cluster setting sql.stats.automatic_collection.enabled = false;
DROP DATABASE IF EXISTS db_pipec cascade;
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

delimiter \\
CREATE PROCEDURE test_proc_multimodel1()
BEGIN
SELECT liwlv.pipeline_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.pipeline_info_with_long_varchar liwlv,
     db_pipec.t_point t
WHERE liwlv.pipeline_sn = t.pipeline_sn
GROUP BY
    liwlv.pipeline_name,
    t.measure_type,
    timebucket
ORDER BY
    liwlv.pipeline_name,
    t.measure_type,
    timebucket;
END \\
delimiter ;
call test_proc_multimodel1();
call test_proc_multimodel1();
drop procedure test_proc_multimodel1;

delimiter \\
CREATE PROCEDURE test_proc_multimodel2()
BEGIN
declare c1 varchar(20);
declare c2 int;
declare c3 float;
declare c5 float;
declare c6 float;
declare c7 int;
declare cur cursor for SELECT liwlv.pipeline_name,
                              t.measure_type,
                              AVG(t.measure_value) AS avg_value,
                              MAX(t.measure_value) AS max_value,
                              MIN(t.measure_value) AS min_value,
                              COUNT(t.measure_value) AS number_of_values
                       FROM pipec_r.pipeline_info_with_long_varchar liwlv,
                            db_pipec.t_point t
                       WHERE liwlv.pipeline_sn = t.pipeline_sn
                       GROUP BY
                           liwlv.pipeline_name,
                           t.measure_type
                       ORDER BY
                           liwlv.pipeline_name,
                           t.measure_type;
open cur;
fetch cur into c1,c2,c3,c5,c6,c7;
close cur;
select c1,c2,c3,c5,c6,c7;
END \\
delimiter ;
call test_proc_multimodel2();
call test_proc_multimodel2();
drop procedure test_proc_multimodel2;

set enable_multimodel=false;

delimiter \\
CREATE PROCEDURE test_proc_multimodel3()
BEGIN
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
  AND t.k_timestamp >= '2023-08-01 01:00:00'
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;
END \\
delimiter ;
call test_proc_multimodel3();
call test_proc_multimodel3();
drop procedure test_proc_multimodel3;

delimiter \\
CREATE PROCEDURE test_proc_multimodel4()
BEGIN
declare c1 varchar(20);
declare c2 float;
declare c3 int;
declare cur cursor for SELECT si.station_name,
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
                         AND t.measure_type = 1
                         AND t.point_sn = 'a1'
                       GROUP BY si.station_name
                       ORDER BY si.station_name;
open cur;
fetch cur into c1,c2,c3;
close cur;
select c1,c2,c3;
END \\
delimiter ;
call test_proc_multimodel4();
call test_proc_multimodel4();
drop procedure test_proc_multimodel4;

delimiter \\
CREATE PROCEDURE test_proc_multimodel5()
BEGIN
declare c1 varchar(20);
declare c2 int;
declare c3 float;
declare cur cursor for SELECT si.station_name,
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
open cur;
fetch cur into c1,c2,c3;
close cur;
select c1,c2,c3;
END \\
delimiter ;
call test_proc_multimodel5();
call test_proc_multimodel5();
drop procedure test_proc_multimodel5;

delimiter \\
CREATE PROCEDURE test_proc_multimodel6()
BEGIN
declare c1 varchar(20);
declare c2 int;
declare c3 int;
declare cur cursor for SELECT wi.work_area_name,
                              t.measure_type,
                              COUNT(DISTINCT t.point_sn) AS measure_point_count
                       FROM pipec_r.pipeline_info li,
                            pipec_r.station_info si,
                            pipec_r.workarea_info wi,
                            db_pipec.t_point t
                       WHERE li.pipeline_sn = t.pipeline_sn
                         AND si.work_area_sn = wi.work_area_sn
                         AND si.work_area_sn = t.work_area_sn
                         AND li.pipeline_name = 'pipeline_1'
                       GROUP BY
                           wi.work_area_name, t.measure_type
                       ORDER BY
                           wi.work_area_name, t.measure_type;
open cur;
fetch cur into c1,c2,c3;
close cur;
select c1,c2,c3;
END \\
delimiter ;
call test_proc_multimodel6();
call test_proc_multimodel6();
drop procedure test_proc_multimodel6;

delimiter \\
CREATE PROCEDURE test_proc_multimodel7()
BEGIN
declare c1 varchar(20);
declare c2 int;
declare c3 float;
declare c5 float;
declare c6 float;
declare c7 int;
declare cur cursor for SELECT li.pipeline_name,
                              t.measure_type,
                              AVG(t.measure_value) AS avg_value,
                              MAX(t.measure_value) AS max_value,
                              MIN(t.measure_value) AS min_value,
                              COUNT(t.measure_value) AS number_of_values
                       FROM pipec_r.pipeline_info li,
                            db_pipec.t_point t
                       WHERE li.pipeline_sn = t.pipeline_sn
                       GROUP BY
                           li.pipeline_name,
                           t.measure_type
                       ORDER BY
                           li.pipeline_name,
                           t.measure_type;
open cur;
fetch cur into c1,c2,c3,c5,c6,c7;
close cur;
select c1,c2,c3,c5,c6,c7;
END \\
delimiter ;
call test_proc_multimodel7();
call test_proc_multimodel7();
drop procedure test_proc_multimodel7;

delimiter \\
CREATE PROCEDURE test_proc_multimodel8()
BEGIN
declare c1 varchar(20);
declare c2 int;
declare c3 int;
declare c5 float;
declare cur cursor for SELECT si.station_name,
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
open cur;
fetch cur into c1,c2,c3,c5;
close cur;
select c1,c2,c3,c5;
END \\
delimiter ;
call test_proc_multimodel8();
call test_proc_multimodel8();
drop procedure test_proc_multimodel8;

delimiter \\
CREATE PROCEDURE test_proc_multimodel9()
BEGIN
declare c1 int;
declare c2 int;
declare c3 int;
declare c5 float;
declare cur cursor for SELECT t.measure_type,
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
open cur;
fetch cur into c1,c2,c3,c5;
close cur;
select c1,c2,c3,c5;
END \\
delimiter ;
call test_proc_multimodel9();
call test_proc_multimodel9();
drop procedure test_proc_multimodel9;

delimiter \\
CREATE PROCEDURE test_proc_multimodel10()
BEGIN
declare c1 varchar(20);
declare c2 varchar(20);
declare c3 int;
declare c5 float;
declare cur cursor for SELECT wi.work_area_name,
                              si.station_name,
                              t.measure_type,
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
                                t.measure_type;
open cur;
fetch cur into c1,c2,c3,c5;
close cur;
select c1,c2,c3,c5;
END \\
delimiter ;
call test_proc_multimodel10();
call test_proc_multimodel10();
drop procedure test_proc_multimodel10;

delimiter \\
CREATE PROCEDURE test_proc_multimodel11()
BEGIN
declare c1 varchar(20);
declare c2 varchar(20);
declare c3 varchar(20);
declare c5 int;
declare c6 float;
declare c7 float;
declare cur cursor for SELECT
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
                           s.work_area_sn, w.work_area_name, pinfo.pipeline_name
                       ORDER BY
                                                                           s.work_area_sn;
open cur;
fetch cur into c1,c2,c3,c5,c6,c7;
fetch cur into c1,c2,c3,c5,c6,c7;
close cur;
select c1,c2,c3,c5,c6,c7;
END \\
delimiter ;
call test_proc_multimodel11();
call test_proc_multimodel11();
drop procedure test_proc_multimodel11;

delimiter \\
CREATE PROCEDURE test_proc_multimodel12()
BEGIN
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
END \\
delimiter ;
call test_proc_multimodel12();
call test_proc_multimodel12();
drop procedure test_proc_multimodel12;

delimiter \\
CREATE PROCEDURE test_proc_multimodel13()
BEGIN
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
END \\
delimiter ;
call test_proc_multimodel13();
call test_proc_multimodel13();
drop procedure test_proc_multimodel13;

delimiter \\
CREATE PROCEDURE test_proc_multimodel14()
BEGIN
SELECT wi.work_area_name,
       si.station_name,
       t.measure_type,
       time_bucket(t.k_timestamp, '10s') as timebucket,
       AVG(t.measure_value) AS avg_value,
       MAX(t.measure_value) AS max_value,
       MIN(t.measure_value) AS min_value,
       COUNT(t.measure_value) AS number_of_values
FROM pipec_r.point_info pi
         join pipec_r.pipeline_info li
              on li.pipeline_sn = pi.pipeline_sn
         join db_pipec.t_point t
              on t.point_sn = pi.point_sn
         join pipec_r.station_info si
              on pi.station_sn = si.station_sn
         join pipec_r.workarea_info wi
              on si.work_area_sn = wi.work_area_sn
WHERE li.pipeline_name = 'pipeline_1'
  AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
  AND t.k_timestamp >= '2023-08-01 01:00:00'
GROUP BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket
ORDER BY wi.work_area_name,
         si.station_name,
         t.measure_type,
         timebucket;
END \\
delimiter ;
call test_proc_multimodel14();
call test_proc_multimodel14();
drop procedure test_proc_multimodel14;

delimiter \\
CREATE PROCEDURE test_proc_multimodel15()
BEGIN
declare c1 varchar(20);
declare c2 varchar(20);
declare c3 int;
declare c5 float;
declare c6 float;
declare c7 float;
declare c8 int;
declare cur cursor for SELECT wi.work_area_name,
                              si.station_name,
                              t.measure_type,
                              AVG(t.measure_value) AS avg_value,
                              MAX(t.measure_value) AS max_value,
                              MIN(t.measure_value) AS min_value,
                              COUNT(t.measure_value) AS number_of_values
                       FROM pipec_r.point_info pi
                                join pipec_r.pipeline_info li
                                     on li.pipeline_sn = pi.pipeline_sn
                                join db_pipec.t_point t
                                     on t.point_sn = pi.point_sn
                                join pipec_r.station_info si
                                     on pi.station_sn = si.station_sn
                                join pipec_r.workarea_info wi
                                     on si.work_area_sn = wi.work_area_sn
                       WHERE li.pipeline_name = 'pipeline_1'
                         AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
                         AND t.k_timestamp >= '2023-08-01 01:00:00'
                       GROUP BY wi.work_area_name,
                                si.station_name,
                                t.measure_type
                       ORDER BY wi.work_area_name,
                                si.station_name,
                                t.measure_type;
open cur;
fetch cur into c1,c2,c3,c5,c6,c7,c8;
select c1,c2,c3,c5,c6,c7,c8;
close cur;
END \\
delimiter ;
call test_proc_multimodel15();
call test_proc_multimodel15();
drop procedure test_proc_multimodel15;

set enable_multimodel=true;

delimiter \\
CREATE PROCEDURE test_proc_multimodel16()
BEGIN
declare c1 varchar(20);
declare c2 varchar(20);
declare c3 int;
declare c5 float;
declare c6 float;
declare c7 float;
declare c8 int;
declare cur cursor for SELECT wi.work_area_name,
                              si.station_name,
                              t.measure_type,
                              AVG(t.measure_value) AS avg_value,
                              MAX(t.measure_value) AS max_value,
                              MIN(t.measure_value) AS min_value,
                              COUNT(t.measure_value) AS number_of_values
                       FROM pipec_r.point_info pi
                                join pipec_r.pipeline_info li
                                     on li.pipeline_sn = pi.pipeline_sn
                                join db_pipec.t_point t
                                     on t.point_sn = pi.point_sn
                                join pipec_r.station_info si
                                     on pi.station_sn = si.station_sn
                                join pipec_r.workarea_info wi
                                     on si.work_area_sn = wi.work_area_sn
                       WHERE li.pipeline_name = 'pipeline_1'
                         AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
                         AND t.k_timestamp >= '2023-08-01 01:00:00'
                       GROUP BY wi.work_area_name,
                                si.station_name,
                                t.measure_type
                       ORDER BY wi.work_area_name,
                                si.station_name,
                                t.measure_type;
open cur;
fetch cur into c1,c2,c3,c5,c6,c7,c8;
select c1,c2,c3,c5,c6,c7,c8;
close cur;
END \\
delimiter ;
call test_proc_multimodel16();
call test_proc_multimodel16();
drop procedure test_proc_multimodel16;

delimiter \\
CREATE PROCEDURE test_proc_multimodel17()
BEGIN
declare c1 varchar(20);
declare c2 float;
declare c3 int;
declare cur cursor for SELECT si.station_name,
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
                         AND t.measure_type = 1
                         AND t.point_sn = 'a1'
                       GROUP BY si.station_name
                       ORDER BY si.station_name;
open cur;
fetch cur into c1,c2,c3;
select c1,c2,c3;
close cur;
END \\
delimiter ;
call test_proc_multimodel17();
call test_proc_multimodel17();
drop procedure test_proc_multimodel17;

delimiter \\
CREATE PROCEDURE test_proc_multimodel18()
BEGIN
declare c1 varchar(20);
declare c2 varchar(20);
declare c3 varchar(20);
declare c4 varchar(20);
declare c5 int;
declare cur cursor for SELECT li.pipeline_name,
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
open cur;
fetch cur into c1,c2,c3,c4,c5;
select c1,c2,c3,c4,c5;
close cur;
END \\
delimiter ;
call test_proc_multimodel18();
call test_proc_multimodel18();
drop procedure test_proc_multimodel18;

delimiter \\
CREATE PROCEDURE test_proc_multimodel19()
BEGIN
declare c1 varchar(20);
declare c2 varchar(20);
declare c3 varchar(20);
declare c4 varchar(20);
declare c5 int;
declare cur cursor for SELECT li.pipeline_name,
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
open cur;
fetch cur into c1,c2,c3,c4,c5;
select c1,c2,c3,c4,c5;
close cur;
END \\
delimiter ;
call test_proc_multimodel19();
call test_proc_multimodel19();
drop procedure test_proc_multimodel19;

delimiter \\
CREATE PROCEDURE test_proc_multimodel20()
BEGIN
declare c1 varchar(20);
declare c2 varchar(20);
declare c3 varchar(20);
declare c4 varchar(20);
declare c5 int;
declare cur cursor for SELECT li.pipeline_name,
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
open cur;
fetch cur into c1,c2,c3,c4,c5;
select c1,c2,c3,c4,c5;
close cur;
END \\
delimiter ;
call test_proc_multimodel20();
call test_proc_multimodel20();
drop procedure test_proc_multimodel20;

delimiter \\
CREATE PROCEDURE test_proc_multimodel21()
BEGIN
declare c1 varchar(20);
declare c2 varchar(20);
declare c3 varchar(20);
declare c4 varchar(20);
declare c5 int;
declare cur cursor for SELECT wi.work_area_name,
                              t.measure_type,
                              COUNT(DISTINCT t.point_sn) AS measure_point_count
                       FROM pipec_r.pipeline_info li,
                            pipec_r.station_info si,
                            pipec_r.workarea_info wi,
                            db_pipec.t_point t
                       WHERE li.pipeline_sn = t.pipeline_sn
                         AND si.work_area_sn = wi.work_area_sn
                         AND si.work_area_sn = t.work_area_sn
                         AND li.pipeline_name = 'pipeline_1'
                       GROUP BY
                           wi.work_area_name, t.measure_type
                       ORDER BY
                           wi.work_area_name, t.measure_type;
open cur;
fetch cur into c1,c2,c3,c4,c5;
select c1,c2,c3,c4,c5;
close cur;
END \\
delimiter ;
call test_proc_multimodel21();
call test_proc_multimodel21();
drop procedure test_proc_multimodel21;

delimiter \\
CREATE PROCEDURE test_proc_multimodel22()
BEGIN
declare c1 varchar(20);
declare c2 int;
declare c3 float;
declare c4 float;
declare c5 float;
declare c6 int;
declare cur cursor for SELECT li.pipeline_name,
                              t.measure_type,
                              AVG(t.measure_value) AS avg_value,
                              MAX(t.measure_value) AS max_value,
                              MIN(t.measure_value) AS min_value,
                              COUNT(t.measure_value) AS number_of_values
                       FROM pipec_r.pipeline_info li,
                            db_pipec.t_point t
                       WHERE li.pipeline_sn = t.pipeline_sn
                       GROUP BY
                           li.pipeline_name,
                           t.measure_type
                       ORDER BY
                           li.pipeline_name,
                           t.measure_type;
open cur;
fetch cur into c1,c2,c3,c4,c5,c6;
select c1,c2,c3,c4,c5,c6;
close cur;
END \\
delimiter ;
call test_proc_multimodel22();
call test_proc_multimodel22();
drop procedure test_proc_multimodel22;

delimiter \\
CREATE PROCEDURE test_proc_multimodel23()
BEGIN
declare c1 varchar(20);
declare c2 int;
declare c3 int;
declare c4 float;
declare cur cursor for SELECT si.station_name,
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
open cur;
fetch cur into c1,c2,c3,c4;
select c1,c2,c3,c4;
close cur;
END \\
delimiter ;
call test_proc_multimodel23();
call test_proc_multimodel23();
drop procedure test_proc_multimodel23;

delimiter \\
CREATE PROCEDURE test_proc_multimodel24()
BEGIN
declare c1 int;
declare c2 int;
declare c3 int;
declare c4 float;
declare cur cursor for SELECT t.measure_type,
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
open cur;
fetch cur into c1,c2,c3,c4;
select c1,c2,c3,c4;
close cur;
END \\
delimiter ;
call test_proc_multimodel24();
call test_proc_multimodel24();
drop procedure test_proc_multimodel24;

delimiter \\
CREATE PROCEDURE test_proc_multimodel25()
BEGIN
declare c1 varchar(20);
declare c2 varchar(20);
declare c3 int;
declare c4 float;
declare cur cursor for SELECT wi.work_area_name,
                              si.station_name,
                              t.measure_type,
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
                                t.measure_type;
open cur;
fetch cur into c1,c2,c3,c4;
select c1,c2,c3,c4;
close cur;
END \\
delimiter ;
call test_proc_multimodel25();
call test_proc_multimodel25();
drop procedure test_proc_multimodel25;

delimiter \\
CREATE PROCEDURE test_proc_multimodel26()
BEGIN
declare c1 varchar(20);
declare c2 int;
declare c3 float;
declare cur cursor for SELECT si.station_name,
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
open cur;
fetch cur into c1,c2,c3;
select c1,c2,c3;
close cur;
END \\
delimiter ;
call test_proc_multimodel26();
call test_proc_multimodel26();
drop procedure test_proc_multimodel26;

delimiter \\
CREATE PROCEDURE test_proc_multimodel27()
BEGIN
declare c1 varchar(20);
declare c2 varchar(20);
declare c3 varchar(20);
declare c4 int;
declare c5 float;
declare c6 float;
DECLARE i INT DEFAULT 0;
declare cur cursor for SELECT
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
                           s.work_area_sn, w.work_area_name, pinfo.pipeline_name
                       ORDER BY
                                                                                   s.work_area_sn;
open cur;
label myloop: WHILE i < 3 DO
fetch cur into c1,c2,c3,c4,c5,c6;
select c1,c2,c3,c4,c5,c6;
SET i = i + 1;
ENDWHILE myloop;
close cur;
END \\
delimiter ;
call test_proc_multimodel27();
call test_proc_multimodel27();
drop procedure test_proc_multimodel27;

delimiter \\
CREATE PROCEDURE test_proc_multimodel28()
BEGIN
declare c2 int;
declare c3 float;
    DECLARE i INT DEFAULT 0;
declare cur cursor for SELECT COUNT(*),
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
open cur;
label myloop: WHILE i < 2 DO
fetch cur into c2,c3;
select c2,c3;
SET i = i + 1;
ENDWHILE myloop;
close cur;
END \\
delimiter ;
call test_proc_multimodel28();
call test_proc_multimodel28();
drop procedure test_proc_multimodel28;

delimiter \\
CREATE PROCEDURE test_proc_multimodel29()
BEGIN
declare c1 varchar(20);
declare c2 varchar(20);
declare c3 int;
declare c4 float;
declare c5 float;
declare c6 float;
    declare c7 int;
declare cur cursor for SELECT wi.work_area_name,
                              si.station_name,
                              t.measure_type,
                              AVG(t.measure_value) AS avg_value,
                              MAX(t.measure_value) AS max_value,
                              MIN(t.measure_value) AS min_value,
                              COUNT(t.measure_value) AS number_of_values
                       FROM pipec_r.point_info pi
                                join pipec_r.pipeline_info li
                                     on li.pipeline_sn = pi.pipeline_sn
                                join db_pipec.t_point t
                                     on t.point_sn = pi.point_sn
                                join pipec_r.station_info si
                                     on pi.station_sn = si.station_sn
                                join pipec_r.workarea_info wi
                                     on si.work_area_sn = wi.work_area_sn
                       WHERE li.pipeline_name = 'pipeline_1'
                         AND wi.work_area_name in ('work_area_1', 'work_area_2', 'work_area_3')
                         AND t.k_timestamp >= '2023-08-01 01:00:00'
                       GROUP BY wi.work_area_name,
                                si.station_name,
                                t.measure_type
                       ORDER BY wi.work_area_name,
                                si.station_name,
                                t.measure_type;
open cur;
fetch cur into c1,c2,c3,c4,c5,c6,c7;
select c1,c2,c3,c4,c5,c6,c7;
close cur;
END \\
delimiter ;
call test_proc_multimodel29();
call test_proc_multimodel29();
drop procedure test_proc_multimodel29;

create table db_pipec.t5(k_timestamp timestamp not null,a int, b int)tags(size int not null)primary tags(size);
select * from db_pipec.t5;
create procedure test_proc_multimodel30() begin declare d int; set d = 10; insert into db_pipec.t5  values ('2000-1-1',d,d,1);select * from db_pipec.t5; end;
call test_proc_multimodel30();
drop procedure  test_proc_multimodel30;

delete from system.table_statistics where name = '_stats_';
set enable_multimodel=false;
drop database mtagdb cascade;
drop database pipec_r cascade;
drop database db_pipec cascade;
DROP DATABASE IF EXISTS test_SELECT_join cascade;
DROP DATABASE IF EXISTS test_SELECT_join_rel cascade;
set cluster setting ts.sql.query_opt_mode = DEFAULT;
set cluster setting sql.stats.tag_automatic_collection.enabled = true;

set cluster setting ts.sql.query_opt_mode = 1100;
set cluster setting sql.stats.tag_automatic_collection.enabled = false;
CREATE TS DATABASE runba;

CREATE TABLE runba.firegases00001 (
                                      "time" TIMESTAMPTZ NOT NULL,
                                      "value" FLOAT8 NULL
) TAGS (
  adr VARCHAR(20) NOT NULL,
  channel VARCHAR(20),
  companyid VARCHAR(20),
  companyname VARCHAR(100),
  datatype VARCHAR(20),
  factory VARCHAR(20),
  msg VARCHAR(20),
  "name" VARCHAR(100),
  slaveid varchar(20),
  "state" VARCHAR(20),
  "type" VARCHAR(20),
  unit VARCHAR(20)
) PRIMARY TAGS (
  adr
);

CREATE TABLE runba.firegases450470 (
                                       "time" TIMESTAMPTZ NOT NULL,
                                       "value" FLOAT8 NULL
) TAGS (
  adr VARCHAR(20) NOT NULL,
  channel VARCHAR(20),
  companyid VARCHAR(20),
  companyname VARCHAR(100),
  datatype VARCHAR(20),
  factory VARCHAR(20),
  msg VARCHAR(20),
  "name" VARCHAR(100),
  slaveid varchar(20),
  "state" VARCHAR(20),
  "type" VARCHAR(20),
  unit VARCHAR(20)
) PRIMARY TAGS (
  adr
);


CREATE TABLE runba.firegases451909 (
                                       "time" TIMESTAMPTZ NOT NULL,
                                       "value" FLOAT8 NULL
) TAGS (
  adr VARCHAR(20) NOT NULL,
  channel VARCHAR(20),
  companyid VARCHAR(20),
  companyname VARCHAR(100),
  datatype VARCHAR(20),
  factory VARCHAR(20),
  msg VARCHAR(20),
  "name" VARCHAR(100),
  slaveid varchar(20),
  "state" VARCHAR(20),
  "type" VARCHAR(20),
  unit VARCHAR(20)
) PRIMARY TAGS (
  adr
);

CREATE TABLE runba.opcdata411 (
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
  "name" VARCHAR(100),
  region VARCHAR(50),
  slaveid VARCHAR(100),
  "tag" VARCHAR(20),
  unit VARCHAR(20)
) PRIMARY TAGS (
  channel,
  companyid
);

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
) ACTIVETIME 10m;

CREATE TABLE runba.opcdata449652 (
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

CREATE TABLE runba.opcdata450081 (
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

CREATE TABLE runba.opcdata451732 (
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
  "name" VARCHAR(100),
  region VARCHAR(50),
  slaveid VARCHAR(100),
  "tag" VARCHAR(20),
  unit VARCHAR(20)
) PRIMARY TAGS (
  channel,
  companyid
);


CREATE TABLE runba.t_399551 (
                                "time" TIMESTAMPTZ NOT NULL,
                                "value" FLOAT8 NULL
) TAGS (
  companyid VARCHAR(20) NOT NULL,
  companyname VARCHAR(100),
  slaveid VARCHAR(100)
) PRIMARY TAGS (
  companyid
);

--- Relational
CREATE DATABASE IF NOT EXISTS runba_tra;

CREATE TABLE IF NOT EXISTS runba_tra.aqsc_donggongka (
                                                         id INT4 NOT NULL,
                                                         "from" VARCHAR(255) NOT NULL,
    type_id INT4 NOT NULL,
    company_id INT4 NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    working_unit VARCHAR(255) NOT NULL,
    working_start INT4 NOT NULL,
    working_end INT4 NOT NULL,
    working_addr VARCHAR(255) NOT NULL,
    working_detail STRING NOT NULL,
    main_tools STRING NOT NULL,
    report_time INT4 NOT NULL,
    report_person_id VARCHAR(255) NULL,
    report_person VARCHAR(255) NOT NULL,
    "status" INT4 NOT NULL,
    cancel_status INT4 NOT NULL,
    current_users STRING NOT NULL,
    history_users STRING NOT NULL,
    is_reback INT4 NOT NULL,
    update_time INT4 NOT NULL,
    checked_status INT4 NOT NULL,
    is_third_part INT4 NOT NULL,
    safety_measure STRING NOT NULL,
    create_time INT4 NOT NULL,
    checked_user STRING NOT NULL,
    deal_status INT4 NOT NULL,
    user_id INT4 NOT NULL,
    qr_code VARCHAR(255) NOT NULL,
    check_current_users STRING NOT NULL,
    check_history_users STRING NOT NULL,
    second_type INT4 NOT NULL,
    is_verifying INT4 NOT NULL,
    examine_id INT4 NOT NULL,
    check_id INT4 NOT NULL,
    verification_type INT4 NOT NULL,
    qianyi_id INT4 NOT NULL,
    query_examine_id INT4 NOT NULL,
    has_interrupt INT4 NULL,
    has_resume INT4 NULL,
    has_delay INT4 NULL,
    is_report INT4 NULL,
    has_alert INT4 NOT NULL,
    history_alert INT4 NOT NULL,
    working_start_plan INT4 NULL,
    working_end_plan INT4 NULL,
    working_start_true INT4 NULL,
    working_end_true INT4 NULL,
    lng VARCHAR(255) NULL,
    lat VARCHAR(255) NULL,
    is_delete INT4 NOT NULL,
    cancel_reason VARCHAR(255) NOT NULL,
    uuid VARCHAR(50) NULL,
    access_pdf_url VARCHAR(255) NULL,
    pdf_url VARCHAR(255) NULL,
    has_command1 INT4 NOT NULL,
    has_command2 INT4 NOT NULL,
    has_command3 INT4 NOT NULL,
    has_command4 INT4 NOT NULL,
    check_device_name VARCHAR(100) NULL,
    check_work_name VARCHAR(100) NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    FAMILY "primary" (
                         id,
                         "from",
                         type_id,
                         company_id,
                         company_name,
                         working_unit,
                         working_start,
                         working_end,
                         working_addr,
                         working_detail,
                         main_tools,
                         report_time,
                         report_person_id,
                         report_person,
                         "status",
                         cancel_status,
                         current_users,
                         history_users,
                         is_reback,
                         update_time,
                         checked_status,
                         is_third_part,
                         safety_measure,
                         create_time,
                         checked_user,
                         deal_status,
                         user_id,
                         qr_code,
                         check_current_users,
                         check_history_users,
                         second_type,
                         is_verifying,
                         examine_id,
                         check_id,
                         verification_type,
                         qianyi_id,
                         query_examine_id,
                         has_interrupt,
                         has_resume,
                         has_delay,
                         is_report,
                         has_alert,
                         history_alert,
                         working_start_plan,
                         working_end_plan,
                         working_start_true,
                         working_end_true,
                         lng,
                         lat,
                         is_delete,
                         cancel_reason,
                         uuid,
                         access_pdf_url,
                         pdf_url,
                         has_command1,
                         has_command2,
                         has_command3,
                         has_command4,
                         check_device_name,
                         check_work_name
                     )
    );

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

CREATE TABLE IF NOT EXISTS runba_tra.cd_tijian_item_index (
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

CREATE TABLE IF NOT EXISTS runba_tra.plat_company (
                                                      company_id INT4 NOT NULL,
                                                      index_show INT4 NOT NULL,
                                                      account VARCHAR(50) NULL,
    code VARCHAR(25) NULL,
    qr_code BYTES NULL,
    "name" VARCHAR(150) NULL,
    short_name VARCHAR(150) NOT NULL,
    is_apply INT4 NOT NULL,
    "type" INT4 NOT NULL,
    "status" INT4 NOT NULL,
    status_update_time INT4 NOT NULL,
    license_src VARCHAR(250) NOT NULL,
    structure_src VARCHAR(250) NOT NULL,
    tax_src VARCHAR(250) NOT NULL,
    province INT4 NOT NULL,
    city INT4 NOT NULL,
    district INT4 NOT NULL,
    town INT4 NOT NULL,
    "address" VARCHAR(250) NOT NULL,
    office_call VARCHAR(20) NOT NULL,
    fax VARCHAR(20) NOT NULL,
    web_site VARCHAR(255) NOT NULL,
    zip_code INT4 NOT NULL,
    contact_person VARCHAR(50) NOT NULL,
    invite_company_id INT4 NOT NULL,
    create_time INT4 NOT NULL,
    create_user_id INT4 NOT NULL,
    update_time INT4 NOT NULL,
    update_user_id INT4 NOT NULL,
    logo VARCHAR(255) NOT NULL,
    sort INT4 NOT NULL,
    registration_code VARCHAR(40) NOT NULL,
    zhuce_code VARCHAR(255) NOT NULL,
    duty_company_type_id INT4 NOT NULL,
    residence VARCHAR(50) NOT NULL,
    registered_capital VARCHAR(50) NOT NULL,
    registered_capital_no INT4 NOT NULL,
    scope_business STRING NULL,
    registration_authority VARCHAR(50) NOT NULL,
    approved_date DECIMAL(10) NULL,
    registration_status INT4 NOT NULL,
    start_run_date DECIMAL(10) NULL,
    end_run_date DECIMAL(10) NULL,
    information_sources_id INT4 NOT NULL,
    information_update_date DECIMAL(10) NULL,
    company_source INT4 NOT NULL,
    is_register INT4 NOT NULL,
    zone_company_id INT4 NOT NULL,
    registration_time INT4 NOT NULL,
    registration_unit VARCHAR(50) NOT NULL,
    company_status INT4 NOT NULL,
    company_provider_type INT4 NOT NULL,
    revocation_time INT4 NOT NULL,
    annual_inspection_status INT4 NULL,
    total_amount_capital_contribution VARCHAR(50) NOT NULL,
    first_tax_year_month INT4 NULL,
    establishment_date INT4 NOT NULL,
    syn_from VARCHAR(255) NULL,
    syn_uid INT4 NOT NULL,
    industry_code varchar(20) NULL,
    industry_name varchar(20) NULL,
    aoic_id INT4 NULL,
    town_id INT4 NULL,
    ipo_id INT4 NULL,
    complete_time INT4 NOT NULL,
    is_complete INT4 NOT NULL,
    organization_code VARCHAR(250) NOT NULL,
    approve_applicant_status INT4 NOT NULL,
    approve_applicant_time INT4 NOT NULL,
    approve_applicant_remark VARCHAR(200) NOT NULL,
    "identity-font-card" VARCHAR(250) NOT NULL,
    "identity-back-card" VARCHAR(250) NOT NULL,
    apply_applicant_time INT4 NOT NULL,
    "version" INT4 NOT NULL,
    taxpayer_code VARCHAR(200) NOT NULL,
    tax VARCHAR(200) NOT NULL,
    vat_tax_type INT4 NOT NULL,
    is_empty_company INT4 NOT NULL,
    is_delete INT4 NOT NULL,
    delete_date INT4 NOT NULL,
    ser_processing_time INT4 NULL,
    ser_volume INT4 NULL,
    service_tag VARCHAR(255) NOT NULL,
    service_detail STRING NULL,
    has_ower INT4 NOT NULL,
    company_user_relation INT4 NOT NULL,
    inviter_user_id INT4 NOT NULL,
    active_code varchar(20) NOT NULL,
    tenant_id INT4 NULL,
    tenant_name VARCHAR(255) NULL,
    tenant_admin_email VARCHAR(255) NULL,
    tenant_admin_phone VARCHAR(255) NULL,
    tenant_disabled INT4 NULL,
    approve_admin_time INT4 NOT NULL,
    approve_admin_remark VARCHAR(200) NOT NULL,
    contact VARCHAR(50) NOT NULL,
    contact_phone VARCHAR(25) NOT NULL,
    contact_email VARCHAR(250) NULL,
    contact_qq VARCHAR(25) NULL,
    contact_wechat VARCHAR(250) NULL,
    pid INT4 NULL,
    is_zone_company INT4 NULL,
    manual_audit_status INT4 NOT NULL,
    old_name VARCHAR(150) NULL,
    modify_time INT4 NULL,
    modify_user_id INT4 NULL,
    verify_status INT4 NOT NULL,
    verify_time INT4 NULL,
    verify_user_id INT4 NULL,
    business_information_complete INT4 NOT NULL,
    create_zone_company_id INT4 NOT NULL,
    has_requirement INT4 NOT NULL,
    add_reason INT4 NOT NULL,
    residence_mobile VARCHAR(25) NULL,
    administrator_mobile VARCHAR(25) NOT NULL,
    has_oa INT4 NOT NULL,
    "level" INT4 NOT NULL,
    tax_import_type INT4 NOT NULL,
    company_belong VARCHAR(255) NOT NULL,
    company_score INT4 NULL,
    success_service_count INT4 NULL,
    before_name VARCHAR(255) NOT NULL,
    taxpayer_num VARCHAR(255) NOT NULL,
    if_verified INT4 NOT NULL,
    verify_name VARCHAR(255) NOT NULL,
    hx_configid VARCHAR(255) NULL,
    industry_score INT4 NULL,
    percentile_score INT4 NULL,
    legal_type INT4 NULL,
    reg_number VARCHAR(50) NULL,
    industry VARCHAR(255) NULL,
    en_name VARCHAR(255) NULL,
    "tags" VARCHAR(255) NULL,
    org_approved_institute VARCHAR(255) NULL,
    province_short_name VARCHAR(50) NULL,
    org_number VARCHAR(50) NULL,
    gongshang_zhucecode VARCHAR(50) NULL,
    tax_code VARCHAR(255) NOT NULL,
    residence_id VARCHAR(255) NOT NULL,
    accountant VARCHAR(255) NOT NULL,
    accountant_mobile VARCHAR(255) NOT NULL,
    function_menu_url VARCHAR(255) NOT NULL,
    issue_date INT4 NOT NULL,
    min_pinpoint INT4 NULL,
    show_settle_type INT4 NULL,
    passport_id VARCHAR(50) NOT NULL,
    import_qua_area VARCHAR(255) NOT NULL,
    is_institution INT4 NOT NULL,
    tax_init INT4 NOT NULL,
    aqsc_update_time INT4 NOT NULL,
    balance DECIMAL(20, 2) NOT NULL,
    freezing_money DECIMAL(10, 2) NOT NULL,
    gs_caiji_time INT4 NULL,
    simple_desc STRING NOT NULL,
    register_time INT4 NOT NULL,
    id_num VARCHAR(50) NOT NULL,
    can_delete INT4 NOT NULL,
    can_not_delete_reason STRING NULL,
    update_business_info INT4 NULL,
    register_in_zone INT4 NOT NULL,
    stock_code VARCHAR(255) NOT NULL,
    has_system_role INT4 NOT NULL,
    user_create_default_role STRING NOT NULL,
    polyv_catid VARCHAR(255) NOT NULL,
    has_iot INT4 NULL,
    qianyi_id INT4 NOT NULL,
    threat_num_status INT4 NULL,
    central_point VARCHAR(255) NOT NULL,
    datav_logo VARCHAR(255) NOT NULL,
    datav_tilt INT4 NOT NULL,
    datav_redirect_host VARCHAR(255) NOT NULL,
    datav_screen_no INT4 NOT NULL,
    map_url VARCHAR(255) NOT NULL,
    rotation VARCHAR(255) NOT NULL,
    modelrotation VARCHAR(255) NOT NULL,
    pitch VARCHAR(255) NOT NULL,
    scale VARCHAR(255) NOT NULL,
    zoom VARCHAR(255) NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    company_id ASC
                                     ),
    FAMILY "primary" (
                         company_id,
                         index_show,
                         account,
                         code,
                         qr_code,
                         "name",
                         short_name,
                         is_apply,
                         "type",
                         "status",
                         status_update_time,
                         license_src,
                         structure_src,
                         tax_src,
                         province,
                         city,
                         district,
                         town,
                         "address",
                         office_call,
                         fax,
                         web_site,
                         zip_code,
                         contact_person,
                         invite_company_id,
                         create_time,
                         create_user_id,
                         update_time,
                         update_user_id,
                         logo,
                         sort,
                         registration_code,
                         zhuce_code,
                         duty_company_type_id,
                         residence,
                         registered_capital,
                         registered_capital_no,
                         scope_business,
                         registration_authority,
                         approved_date,
                         registration_status,
                         start_run_date,
                         end_run_date,
                         information_sources_id,
                         information_update_date,
                         company_source,
                         is_register,
                         zone_company_id,
                         registration_time,
                         registration_unit,
                         company_status,
                         company_provider_type,
                         revocation_time,
                         annual_inspection_status,
                         total_amount_capital_contribution,
                         first_tax_year_month,
                         establishment_date,
                         syn_from,
                         syn_uid,
                         industry_code,
                         industry_name,
                         aoic_id,
                         town_id,
                         ipo_id,
                         complete_time,
                         is_complete,
                         organization_code,
                         approve_applicant_status,
                         approve_applicant_time,
                         approve_applicant_remark,
                         "identity-font-card",
                         "identity-back-card",
                         apply_applicant_time,
                         "version",
                         taxpayer_code,
                         tax,
                         vat_tax_type,
                         is_empty_company,
                         is_delete,
                         delete_date,
                         ser_processing_time,
                         ser_volume,
                         service_tag,
                         service_detail,
                         has_ower,
                         company_user_relation,
                         inviter_user_id,
                         active_code,
                         tenant_id,
                         tenant_name,
                         tenant_admin_email,
                         tenant_admin_phone,
                         tenant_disabled,
                         approve_admin_time,
                         approve_admin_remark,
                         contact,
                         contact_phone,
                         contact_email,
                         contact_qq,
                         contact_wechat,
                         pid,
                         is_zone_company,
                         manual_audit_status,
                         old_name,
                         modify_time,
                         modify_user_id,
                         verify_status,
                         verify_time,
                         verify_user_id,
                         business_information_complete,
                         create_zone_company_id,
                         has_requirement,
                         add_reason,
                         residence_mobile,
                         administrator_mobile,
                         has_oa,
                         "level",
                         tax_import_type,
                         company_belong,
                         company_score,
                         success_service_count,
                         before_name,
                         taxpayer_num,
                         if_verified,
                         verify_name,
                         hx_configid,
                         industry_score,
                         percentile_score,
                         legal_type,
                         reg_number,
                         industry,
                         en_name,
                         "tags",
                         org_approved_institute,
                         province_short_name,
                         org_number,
                         gongshang_zhucecode,
                         tax_code,
                         residence_id,
                         accountant,
                         accountant_mobile,
                         function_menu_url,
                         issue_date,
                         min_pinpoint,
                         show_settle_type,
                         passport_id,
                         import_qua_area,
                         is_institution,
                         tax_init,
                         aqsc_update_time,
                         balance,
                         freezing_money,
                         gs_caiji_time,
                         simple_desc,
                         register_time,
                         id_num,
                         can_delete,
                         can_not_delete_reason,
                         update_business_info,
                         register_in_zone,
                         stock_code,
                         has_system_role,
                         user_create_default_role,
                         polyv_catid,
                         has_iot,
                         qianyi_id,
                         threat_num_status,
                         central_point,
                         datav_logo,
                         datav_tilt,
                         datav_redirect_host,
                         datav_screen_no,
                         map_url,
                         rotation,
                         modelrotation,
                         pitch,
                         scale,
                         zoom
                     )
    );

CREATE TABLE IF NOT EXISTS runba_tra.plat_datav_riskpoint_type (
                                                                   id INT4 NOT NULL,
                                                                   "level" INT4 NOT NULL,
                                                                   "type" INT4 NOT NULL,
                                                                   c INT4 NOT NULL,
                                                                   company_id INT4 NOT NULL,
                                                                   CONSTRAINT "primary" PRIMARY KEY (
                                                                   id ASC
),
    FAMILY "primary" (
                         id,
                         "level",
                         "type",
                         c,
                         company_id
                     )
    );

CREATE TABLE IF NOT EXISTS runba_tra.plat_device_info (
                                                          id INT4 NOT NULL,
                                                          devicenumber VARCHAR(255) NOT NULL,
    usecernum VARCHAR(50) NOT NULL,
    "name" VARCHAR(255) NOT NULL,
    areacode VARCHAR(50) NOT NULL,
    townname VARCHAR(255) NOT NULL,
    companyname VARCHAR(255) NOT NULL,
    makecomname VARCHAR(255) NOT NULL,
    usestatus VARCHAR(50) NOT NULL,
    "address" VARCHAR(255) NOT NULL,
    dangerflag VARCHAR(50) NOT NULL,
    jycomname VARCHAR(50) NOT NULL,
    result VARCHAR(50) NOT NULL,
    nextdate VARCHAR(50) NOT NULL,
    nextselfdate VARCHAR(50) NOT NULL,
    inspusestatus VARCHAR(50) NOT NULL,
    maintaincomname VARCHAR(255) NOT NULL,
    item_pk VARCHAR(255) NOT NULL,
    remark VARCHAR(255) NOT NULL,
    category1 VARCHAR(255) NOT NULL,
    category2 VARCHAR(255) NOT NULL,
    category3 VARCHAR(255) NOT NULL,
    company_id INT4 NOT NULL,
    qianyi_id INT4 NOT NULL,
    device_serial_number VARCHAR(255) NOT NULL,
    device_name_plate VARCHAR(255) NOT NULL,
    device_birth_number VARCHAR(255) NOT NULL,
    check_period INT4 NULL,
    check_period_unit VARCHAR(255) NULL,
    first_check_time VARCHAR(255) NULL,
    self_check_period INT4 NULL,
    self_check_period_unit VARCHAR(255) NULL,
    self_first_check_time VARCHAR(255) NULL,
    created_at INT4 NULL,
    updated_at INT4 NULL,
    area_id INT4 NOT NULL,
    if_sync_position INT4 NOT NULL,
    "3d_model_link" STRING NOT NULL,
    style STRING NOT NULL,
    nfc_id VARCHAR(255) NOT NULL,
    coordinate STRING NOT NULL,
    display_type VARCHAR(255) NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    FAMILY "primary" (
                         id,
                         devicenumber,
                         usecernum,
                         "name",
                         areacode,
                         townname,
                         companyname,
                         makecomname,
                         usestatus,
                         "address",
                         dangerflag,
                         jycomname,
                         result,
                         nextdate,
                         nextselfdate,
                         inspusestatus,
                         maintaincomname,
                         item_pk,
                         remark,
                         category1,
                         category2,
                         category3,
                         company_id,
                         qianyi_id,
                         device_serial_number,
                         device_name_plate,
                         device_birth_number,
                         check_period,
                         check_period_unit,
                         first_check_time,
                         self_check_period,
                         self_check_period_unit,
                         self_first_check_time,
                         created_at,
                         updated_at,
                         area_id,
                         if_sync_position,
                         "3d_model_link",
                         style,
                         nfc_id,
                         coordinate,
                         display_type
                     )
    );

CREATE TABLE IF NOT EXISTS runba_tra.plat_drill (
                                                    id INT4 NOT NULL,
                                                    company_id INT4 NOT NULL,
                                                    company_name VARCHAR(500) NOT NULL,
    "name" VARCHAR(500) NOT NULL,
    drill_plan_id INT4 NOT NULL,
    drill_plan_theme_id VARCHAR(500) NOT NULL,
    category INT4 NOT NULL,
    start_time INT4 NOT NULL,
    end_time INT4 NOT NULL,
    org_department VARCHAR(500) NOT NULL,
    attend_department VARCHAR(500) NOT NULL,
    place VARCHAR(500) NOT NULL,
    attend_person_num INT4 NOT NULL,
    process_description STRING NULL,
    general_director VARCHAR(500) NOT NULL,
    general_director_job VARCHAR(500) NOT NULL,
    general_planner VARCHAR(500) NOT NULL,
    general_planner_job VARCHAR(500) NOT NULL,
    is_assess INT4 NULL,
    is_del INT4 NULL,
    created_at TIMESTAMP NULL,
    updated_at TIMESTAMP NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    FAMILY "primary" (
                         id,
                         company_id,
                         company_name,
                         "name",
                         drill_plan_id,
                         drill_plan_theme_id,
                         category,
                         start_time,
                         end_time,
                         org_department,
                         attend_department,
                         place,
                         attend_person_num,
                         process_description,
                         general_director,
                         general_director_job,
                         general_planner,
                         general_planner_job,
                         is_assess,
                         is_del,
                         created_at,
                         updated_at
                     )
    );

CREATE TABLE IF NOT EXISTS runba_tra.plat_drill_plan (
                                                         id INT4 NOT NULL,
                                                         company_id INT4 NOT NULL,
                                                         company_name VARCHAR(255) NOT NULL,
    "name" VARCHAR(255) NOT NULL,
    plan_count INT4 NOT NULL,
    "status" INT4 NOT NULL,
    repeal_cause VARCHAR(500) NULL,
    repeal_user_id INT4 NOT NULL,
    repeal_user_name VARCHAR(500) NULL,
    repeal_time INT4 NULL,
    is_sign INT4 NOT NULL,
    copy_num INT4 NOT NULL,
    created_at TIMESTAMP NULL,
    updated_at TIMESTAMP NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    FAMILY "primary" (
                         id,
                         company_id,
                         company_name,
                         "name",
                         plan_count,
                         "status",
                         repeal_cause,
                         repeal_user_id,
                         repeal_user_name,
                         repeal_time,
                         is_sign,
                         copy_num,
                         created_at,
                         updated_at
                     )
    );

CREATE TABLE IF NOT EXISTS runba_tra.plat_drill_plan_theme (
                                                               id INT4 NOT NULL,
                                                               drill_plan_id INT4 NOT NULL,
                                                               theme VARCHAR(255) NOT NULL,
    "address" VARCHAR(255) NOT NULL,
    category INT4 NOT NULL,
    "year" VARCHAR(255) NOT NULL,
    "month" VARCHAR(255) NOT NULL,
    content STRING NULL,
    "sort" INT4 NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    FAMILY "primary" (
                         id,
                         drill_plan_id,
                         theme,
                         "address",
                         category,
                         "year",
                         "month",
                         content,
                         "sort",
                         created_at,
                         updated_at
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

CREATE TABLE IF NOT EXISTS runba_tra.plat_risk_area (
                                                        id INT4 NOT NULL,
                                                        risk_id INT4 NOT NULL,
                                                        area_id INT4 NOT NULL,
                                                        "desc" VARCHAR(255) NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    FAMILY "primary" (
                         id,
                         risk_id,
                         area_id,
                         "desc",
                         create_time,
                         update_time
                     )
    );

CREATE TABLE IF NOT EXISTS runba_tra.plat_risk_point (
                                                         id INT4 NOT NULL,
                                                         company_id INT4 NOT NULL,
                                                         user_id INT4 NOT NULL,
                                                         "name" VARCHAR(255) NOT NULL,
    "type" INT4 NOT NULL,
    activity_type INT4 NOT NULL,
    tszy_activity_type VARCHAR(255) NOT NULL,
    device_name VARCHAR(255) NOT NULL,
    "status" INT4 NOT NULL,
    "level" INT4 NOT NULL,
    create_time INT4 NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    FAMILY "primary" (
                         id,
                         company_id,
                         user_id,
                         "name",
                         "type",
                         activity_type,
                         tszy_activity_type,
                         device_name,
                         "status",
                         "level",
                         create_time
                     )
    );

CREATE TABLE IF NOT EXISTS runba_tra.plat_tijian_item (
                                                          id INT4 NOT NULL,
                                                          item VARCHAR(50) NOT NULL,
    content STRING NOT NULL,
    company_id INT4 NOT NULL,
    create_user_id INT4 NOT NULL,
    create_time INT4 NOT NULL,
    update_time INT4 NOT NULL,
    is_common INT4 NOT NULL,
    card_template_id INT4 NOT NULL,
    card_template_name VARCHAR(255) NOT NULL,
    period_id INT4 NULL,
    qianyi_id INT4 NOT NULL,
    check_type INT4 NOT NULL,
    index_id INT4 NOT NULL,
    measure STRING NOT NULL,
    source_from INT4 NOT NULL,
    template_type INT4 NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    FAMILY "primary" (
                         id,
                         item,
                         content,
                         company_id,
                         create_user_id,
                         create_time,
                         update_time,
                         is_common,
                         card_template_id,
                         card_template_name,
                         period_id,
                         qianyi_id,
                         check_type,
                         index_id,
                         measure,
                         source_from,
                         template_type
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

CREATE TABLE IF NOT EXISTS runba_tra.plat_tijiancard_item_relation (
                                                                       id INT4 NOT NULL,
                                                                       company_id INT4 NOT NULL,
                                                                       tijiancard_id VARCHAR(50) NOT NULL,
    item_ids STRING NOT NULL,
    obj_type INT4 NOT NULL,
    obj_id INT4 NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time INT4 NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    id ASC
                                     ),
    INDEX tijian_id_relation (
    tijiancard_id ASC
                             ),
    FAMILY "primary" (
                         id,
                         company_id,
                         tijiancard_id,
                         item_ids,
                         obj_type,
                         obj_id,
                         create_time,
                         update_time,
                         delete_time
                     )
    );

CREATE TABLE IF NOT EXISTS runba_tra.plat_zone_member (
                                                          member_id INT4 NOT NULL,
                                                          member_code VARCHAR(20) NOT NULL,
    member_name VARCHAR(50) NOT NULL,
    member_type INT4 NOT NULL,
    "contract" VARCHAR(50) NULL,
    "position" VARCHAR(50) NULL,
    contract_call VARCHAR(30) NULL,
    contract_mobile VARCHAR(20) NOT NULL,
    contract_email VARCHAR(100) NULL,
    invite_status INT4 NOT NULL,
    invite_operate_status INT4 NOT NULL,
    invite_operate_open_time INT4 NOT NULL,
    enterprise_status INT4 NULL,
    company_id INT4 NOT NULL,
    zone_company_id INT4 NOT NULL,
    rel_status INT4 NOT NULL,
    join_time INT4 NOT NULL,
    remove_time INT4 NOT NULL,
    create_time INT4 NOT NULL,
    create_user_id INT4 NULL,
    update_time INT4 NULL,
    update_user_id INT4 NULL,
    invite_date INT4 NOT NULL,
    invite_count INT4 NOT NULL,
    introducing_way INT4 NOT NULL,
    sys_investment_workplace_id INT4 NOT NULL,
    sys_investment_people_id INT4 NOT NULL,
    investment_type_id INT4 NULL,
    company_type_id INT4 NULL,
    investment_date INT4 NULL,
    expand_city_id INT4 NOT NULL,
    add_type INT4 NOT NULL,
    "status" INT4 NOT NULL,
    zhuce_code VARCHAR(50) NOT NULL,
    archives_code VARCHAR(50) NOT NULL,
    is_history_rel INT4 NOT NULL,
    agency_id INT4 NOT NULL,
    pic1_url VARCHAR(255) NOT NULL,
    pic1_down_times INT4 NOT NULL,
    pic1_upload_time TIMESTAMP NULL,
    pic2_url VARCHAR(255) NOT NULL,
    pic2_down_times INT4 NOT NULL,
    pic2_upload_time TIMESTAMP NULL,
    pic3_url VARCHAR(255) NOT NULL,
    pic3_down_times INT4 NOT NULL,
    pic3_upload_time TIMESTAMP NULL,
    pic4_url VARCHAR(255) NOT NULL,
    pic4_down_times INT4 NOT NULL,
    pic4_upload_time TIMESTAMP NULL,
    pic5_url VARCHAR(255) NOT NULL,
    pic5_down_times INT4 NOT NULL,
    pic5_upload_time TIMESTAMP NULL,
    new_taxpayer INT4 NOT NULL,
    effective_taxpayer INT4 NOT NULL,
    total_tax DECIMAL(20, 2) NOT NULL,
    company_type INT4 NOT NULL,
    is_virtual_member INT4 NOT NULL,
    new_taxpayer_month INT4 NULL,
    effective_taxpayer_month INT4 NULL,
    has_return_tax INT4 NULL,
    "level" INT4 NOT NULL,
    is_member INT4 NOT NULL,
    join_error_msg VARCHAR(255) NOT NULL,
    admin_user_id INT4 NOT NULL,
    return_tax_limit INT4 NOT NULL,
    leader_name VARCHAR(255) NOT NULL,
    leader_tel VARCHAR(255) NOT NULL,
    trade_type INT4 NOT NULL,
    move_in_type INT4 NOT NULL,
    expense_management_time INT4 NOT NULL,
    introduce_investment_people_id INT4 NOT NULL,
    company_belong VARCHAR(255) NOT NULL,
    residence_id VARCHAR(255) NOT NULL,
    passport_id VARCHAR(50) NOT NULL,
    accountant VARCHAR(255) NOT NULL,
    accountant_mobile VARCHAR(255) NOT NULL,
    administrator_mobile VARCHAR(25) NOT NULL,
    "tag" VARCHAR(255) NOT NULL,
    company_category VARCHAR(255) NOT NULL,
    register_in_zone INT4 NOT NULL,
    business_information_complete INT4 NOT NULL,
    qianyi_id INT4 NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (
    member_id ASC
                                     ),
    FAMILY "primary" (
                         member_id,
                         member_code,
                         member_name,
                         member_type,
                         "contract",
                         "position",
                         contract_call,
                         contract_mobile,
                         contract_email,
                         invite_status,
                         invite_operate_status,
                         invite_operate_open_time,
                         enterprise_status,
                         company_id,
                         zone_company_id,
                         rel_status,
                         join_time,
                         remove_time,
                         create_time,
                         create_user_id,
                         update_time,
                         update_user_id,
                         invite_date,
                         invite_count,
                         introducing_way,
                         sys_investment_workplace_id,
                         sys_investment_people_id,
                         investment_type_id,
                         company_type_id,
                         investment_date,
                         expand_city_id,
                         add_type,
                         "status",
                         zhuce_code,
                         archives_code,
                         is_history_rel,
                         agency_id,
                         pic1_url,
                         pic1_down_times,
                         pic1_upload_time,
                         pic2_url,
                         pic2_down_times,
                         pic2_upload_time,
                         pic3_url,
                         pic3_down_times,
                         pic3_upload_time,
                         pic4_url,
                         pic4_down_times,
                         pic4_upload_time,
                         pic5_url,
                         pic5_down_times,
                         pic5_upload_time,
                         new_taxpayer,
                         effective_taxpayer,
                         total_tax,
                         company_type,
                         is_virtual_member,
                         new_taxpayer_month,
                         effective_taxpayer_month,
                         has_return_tax,
                         "level",
                         is_member,
                         join_error_msg,
                         admin_user_id,
                         return_tax_limit,
                         leader_name,
                         leader_tel,
                         trade_type,
                         move_in_type,
                         expense_management_time,
                         introduce_investment_people_id,
                         company_belong,
                         residence_id,
                         passport_id,
                         accountant,
                         accountant_mobile,
                         administrator_mobile,
                         "tag",
                         company_category,
                         register_in_zone,
                         business_information_complete,
                         qianyi_id
                     )
    );

CREATE TABLE IF NOT EXISTS runba_tra.testa (
                                               a INT4 NULL,
                                               b VARCHAR(20) NULL,
    FAMILY "primary" (
                         a,
                         b,
                         rowid
                     )
    );

-- INSERT
-- 1
INSERT INTO runba.firegases450470 VALUES
                                      ('2023-11-25 17:32:33+00:00', 0, '40006', '4', '450470', '珠海保税区丽珠合成制药有限公司', '气体', '吉安达', '正常在线', '402岗位东罐区东北角', '2', '1', '可燃气体', '%'),
                                      ('2023-11-25 17:38:33+00:00', 2, '40006', '4', '450470', '珠海保税区丽珠合成制药有限公司', '气体', '吉安达', '正常在线', '402岗位东罐区东北角', '2', '1', '可燃气体', '%'),
                                      ('2023-11-25 17:40:33+00:00', 0, '40006', '4', '450470', '珠海保税区丽珠合成制药有限公司', '气体', '吉安达', '正常在线', '402岗位东罐区东北角', '2', '1', '可燃气体', '%'),
                                      ('2023-11-25 17:50:33+00:00', 0, '40006', '4', '450470', '珠海保税区丽珠合成制药有限公司', '气体', '吉安达', '正常在线', '402岗位东罐区东北角', '2', '1', '可燃气体', '%'),
                                      ('2023-11-25 17:56:33+00:00', 1, '40006', '4', '450470', '珠海保税区丽珠合成制药有限公司', '气体', '吉安达', '正常在线', '402岗位东罐区东北角', '2', '1', '可燃气体', '%');

-- 2
INSERT INTO runba.firegases451909 VALUES
                                      ('2024-04-25 13:55:22+00:00', 0, '40003', '3', '451909', '珠海中油油品配送中心有限公司', '气体', '山东瑶安', '正常在线', '1000M³罐出油口', '1', '1', '可燃气体', '%'),
                                      ('2024-04-25 13:56:07+00:00', 0, '40003', '3', '451909', '珠海中油油品配送中心有限公司', '气体', '山东瑶安', '正常在线', '1000M³罐出油口', '1', '1', '可燃气体', '%'),
                                      ('2024-04-25 13:56:53+00:00', 0, '40003', '3', '451909', '珠海中油油品配送中心有限公司', '气体', '山东瑶安', '正常在线', '1000M³罐出油口', '1', '1', '可燃气体', '%'),
                                      ('2024-04-25 13:57:38+00:00', 0, '40003', '3', '451909', '珠海中油油品配送中心有限公司', '气体', '山东瑶安', '正常在线', '1000M³罐出油口', '1', '1', '可燃气体', '%'),
                                      ('2024-04-25 13:58:23+00:00', 0, '40003', '3', '451909', '珠海中油油品配送中心有限公司', '气体', '山东瑶安', '正常在线', '1000M³罐出油口', '1', '1', '可燃气体', '%');

-- 3
INSERT INTO runba.opcdata411 VALUES
                                 ('2024-06-04 20:34:27+00:00', 23, '40039', 'CH39', 411, '222', '温度', '温度计', 'V-039', '0', '35', '0', '0', '戊二烯8#罐温度', '罐区间', '1', 'TQ_24', '℃'),
                                 ('2024-06-04 20:34:37+00:00', 23, '40039', 'CH39', 411, '222', '温度', '温度计', 'V-039', '0', '35', '0', '0', '戊二烯8#罐温度', '罐区间', '1', 'TQ_24', '℃'),
                                 ('2024-06-04 20:34:47+00:00', 23, '40039', 'CH39', 411, '222', '温度', '温度计', 'V-039', '0', '35', '0', '0', '戊二烯8#罐温度', '罐区间', '1', 'TQ_24', '℃'),
                                 ('2024-06-04 20:34:57+00:00', 23, '40039', 'CH39', 411, '222', '温度', '温度计', 'V-039', '0', '35', '0', '0', '戊二烯8#罐温度', '罐区间', '1', 'TQ_24', '℃'),
                                 ('2024-06-04 20:35:07+00:00', 23, '40039', 'CH39', 411, '222', '温度', '温度计', 'V-039', '0', '35', '0', '0', '戊二烯8#罐温度', '罐区间', '1', 'TQ_24', '℃');

-- 4
INSERT INTO runba.opcdata449600 VALUES
                                    ('2023-11-30 18:00:09+00:00', 1712.137, '400547', 'CH271', 449600, '珠海长炼石化设备有限公司', '温度', '温度计', 'V-223A', '2500', '2400', '500', '600', '-', '液化烃罐区', '1', 'MLT_223A', 'mm'),
                                    ('2023-11-30 18:00:39+00:00', 1714.28, '400547', 'CH272', 449600, '珠海长炼石化设备有限公司', '液位', '液位计', 'V-223A', '2500', '2400', '500', '600', '-', '液化烃罐区', '1', 'MLT_223A', 'mm'),
                                    ('2023-11-30 18:01:09+00:00', 1714.345, '400547', 'CH273', 449600, '珠海长炼石化设备有限公司', '温度', '温度计', 'V-223A', '2500', '2400', '500', '600', '-', '液化烃罐区', '1', 'MLT_223A', 'mm'),
                                    ('2023-11-30 18:01:39+00:00', 1714.28, '400547', 'CH274', 449600, '珠海长炼石化设备有限公司', '液位', '液位计', 'V-223A', '2500', '2400', '500', '600', '-', '液化烃罐区', '1', 'MLT_223A', 'mm'),
                                    ('2023-11-30 18:02:09+00:00', 1714.345, '400547', 'CH275', 449600, '珠海长炼石化设备有限公司', '液位', '液位计', 'V-223A', '2500', '2400', '500', '600', '-', '液化烃罐区', '1', 'MLT_223A', 'mm'),
                                    ('2023-11-30 18:03:09+00:00', 1715.137, '400547', 'CH271', 449600, '珠海长炼石化设备有限公司', '温度', '温度计', 'V-223A', '2500', '2400', '500', '600', '-', '液化烃罐区', '1', 'MLT_223A', 'mm'),
                                    ('2023-11-30 18:03:39+00:00', 1716.28, '400547', 'CH272', 449600, '珠海长炼石化设备有限公司', '液位', '液位计', 'V-223A', '2500', '2400', '500', '600', '-', '液化烃罐区', '1', 'MLT_223A', 'mm'),
                                    ('2023-11-30 18:04:09+00:00', 1716.345, '400547', 'CH273', 449600, '珠海长炼石化设备有限公司', '温度', '温度计', 'V-223A', '2500', '2400', '500', '600', '-', '液化烃罐区', '1', 'MLT_223A', 'mm'),
                                    ('2023-11-30 18:04:39+00:00', 1716.28, '400547', 'CH274', 449600, '珠海长炼石化设备有限公司', '液位', '液位计', 'V-223A', '2500', '2400', '500', '600', '-', '液化烃罐区', '1', 'MLT_223A', 'mm'),
                                    ('2023-11-30 18:05:09+00:00', 1716.345, '400547', 'CH275', 449600, '珠海长炼石化设备有限公司', '液位', '液位计', 'V-223A', '2500', '2400', '500', '600', '-', '液化烃罐区', '1', 'MLT_223A', 'mm');

-- 5
INSERT INTO runba.opcdata449652 VALUES
                                    ('2023-12-01 21:10:16+00:00', 14.40972, '40037', 'CH19', 449652, '珠海金鸡化工有限公司', '温度', '3#苯乙烯罐', 'V103C', '20', '19', '2', '5', '底部温度', '苯乙烯罐区', '1', 'TI10203', '℃'),
                                    ('2023-12-01 21:10:46+00:00', 14.40972, '40037', 'CH19', 449652, '珠海金鸡化工有限公司', '温度', '3#苯乙烯罐', 'V103C', '20', '19', '2', '5', '底部温度', '苯乙烯罐区', '1', 'TI10203', '℃'),
                                    ('2023-12-01 21:11:16+00:00', 14.40972, '40037', 'CH19', 449652, '珠海金鸡化工有限公司', '温度', '3#苯乙烯罐', 'V103C', '20', '19', '2', '5', '底部温度', '苯乙烯罐区', '1', 'TI10203', '℃'),
                                    ('2023-12-01 21:12:57+00:00', 14.40683, '40037', 'CH19', 449652, '珠海金鸡化工有限公司', '温度', '3#苯乙烯罐', 'V103C', '20', '19', '2', '5', '底部温度', '苯乙烯罐区', '1', 'TI10203', '℃'),
                                    ('2023-12-01 21:13:27+00:00', 14.39815, '40037', 'CH19', 449652, '珠海金鸡化工有限公司', '温度', '3#苯乙烯罐', 'V103C', '20', '19', '2', '5', '底部温度', '苯乙烯罐区', '1', 'TI10203', '℃');

-- 6
INSERT INTO runba.opcdata450081 VALUES
                                    ('2024-04-25 12:41:44+00:00', 0, '40030', 'CH30', 450081, '华创石油', '水高', '水高修正', 'V-6', '0', '0', '0', '0', '水高修正', '6号罐', '1', 'MTH6', 'm'),
                                    ('2024-04-25 12:41:46+00:00', 0, '40030', 'CH30', 450081, '华创石油', '水高', '水高修正', 'V-6', '0', '0', '0', '0', '水高修正', '6号罐', '1', 'MTH6', 'm'),
                                    ('2024-04-25 12:41:48+00:00', 0, '40030', 'CH30', 450081, '华创石油', '水高', '水高修正', 'V-6', '0', '0', '0', '0', '水高修正', '6号罐', '1', 'MTH6', 'm'),
                                    ('2024-04-25 12:41:50+00:00', 0, '40030', 'CH30', 450081, '华创石油', '水高', '水高修正', 'V-6', '0', '0', '0', '0', '水高修正', '6号罐', '1', 'MTH6', 'm'),
                                    ('2024-04-25 12:41:52+00:00', 0, '40030', 'CH30', 450081, '华创石油', '水高', '水高修正', 'V-6', '0', '0', '0', '0', '水高修正', '6号罐', '1', 'MTH6', 'm');

-- 7
INSERT INTO runba.opcdata451732 VALUES
                                    ('2024-06-04 20:46:18+00:00', 0.178, '40029', 'CH29', 451732, '惠州市聚辉环保材料有限公司', '压力', '压力表', 'V-029', '0', '0.2', '0', '0', '戊二烯6#罐压力', '罐区间', '1', NULL, 'MPa'),
                                    ('2024-06-04 20:46:28+00:00', 0.178, '40029', 'CH29', 451732, '惠州市聚辉环保材料有限公司', '压力', '压力表', 'V-029', '0', '0.2', '0', '0', '戊二烯6#罐压力', '罐区间', '1', NULL, 'MPa'),
                                    ('2024-06-04 20:46:38+00:00', 0.178, '40029', 'CH29', 451732, '惠州市聚辉环保材料有限公司', '压力', '压力表', 'V-029', '0', '0.2', '0', '0', '戊二烯6#罐压力', '罐区间', '1', NULL, 'MPa'),
                                    ('2024-06-04 20:46:48+00:00', 0.178, '40029', 'CH29', 451732, '惠州市聚辉环保材料有限公司', '压力', '压力表', 'V-029', '0', '0.2', '0', '0', '戊二烯6#罐压力', '罐区间', '1', NULL, 'MPa'),
                                    ('2024-06-04 20:46:58+00:00', 0.178, '40029', 'CH29', 451732, '惠州市聚辉环保材料有限公司', '压力', '压力表', 'V-029', '0', '0.2', '0', '0', '戊二烯6#罐压力', '罐区间', '1', NULL, 'MPa');

-- 8
INSERT INTO runba_tra.aqsc_donggongka VALUES
                                          (33, 'runbayun', 1, 398934, '上海妆佳电子商务有限公司', '上海妆佳电子商务有限公司', 1608206520, 1608217320, '', '', '', 1608134400, NULL, '', 0, 0, '', '', 0, 1608206763, 1, 1, 'a:0:{}', 1608206763, '', 1, 0, '', '', '', 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1608206520, 1608217320, 1608206520, 1608217320, '121.8614526', '31.48667343', 0, '', '8b9f8c47-4a4f-11ed-9c80-506b4b46e57c', NULL, NULL, 2, 2, 2, 2, NULL, NULL),
                                          (35, 'runbayun', 1, 398934, '上海妆佳电子商务有限公司', '上海妆佳电子商务有限公司', 1608206520, 1608217320, '哈哈', '哈哈', '哈哈', 1608134400, NULL, '哈哈', 1, 0, ';454823_0', ';', 0, 1608207495, 1, 1, 'a:1:{i:0;a:3:{s:6:"detail";s:36:"作业人员身体条件符合要求";s:6:"result";s:1:"1";s:6:"person";s:6:"哈哈";}}', 1608207495, '', 1, 0, '', '', '', 0, 0, 68, 0, 1, 0, 68, 0, 0, 0, 0, 0, 0, 1608206520, 1608217320, 1608206520, 1608217320, '121.8614526', '31.48667343', 0, '', '8b9f907c-4a4f-11ed-9c80-506b4b46e57c', NULL, NULL, 2, 2, 2, 2, NULL, NULL),
                                          (36, 'runbayun', 1, 398934, '上海妆佳电子商务有限公司', '上海妆佳电子商务有限公司', 1608206520, 1608217320, '哈哈', '哈哈', '哈哈', 1608134400, NULL, '哈哈', 1, 0, ';454823_0', ';', 0, 1608207643, 1, 1, 'a:1:{i:0;a:3:{s:6:"detail";s:36:"作业人员身体条件符合要求";s:6:"result";s:1:"1";s:6:"person";s:6:"哈哈";}}', 1608207643, '', 1, 0, '', '', '', 0, 0, 68, 0, 1, 0, 68, 0, 0, 0, 0, 0, 0, 1608206520, 1608217320, 1608206520, 1608217320, '121.8614526', '31.48667343', 0, '', '8b9f923c-4a4f-11ed-9c80-506b4b46e57c', NULL, NULL, 2, 2, 2, 2, NULL, NULL),
                                          (37, 'runbayun', 1, 398934, '上海妆佳电子商务有限公司', '上海妆佳电子商务有限公司', 1608206520, 1608217320, '哈哈', '哈哈', '哈哈', 1608134400, NULL, '哈哈', 0, 0, ';454823_0', ';', 0, 1608207701, 1, 1, 'a:1:{i:0;a:3:{s:6:"detail";s:36:"作业人员身体条件符合要求";s:6:"result";s:1:"1";s:6:"person";s:6:"哈哈";}}', 1608207701, '', 1, 0, '', '', '', 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1608206520, 1608217320, 1608206520, 1608217320, '121.8614526', '31.48667343', 0, '', '8b9f9330-4a4f-11ed-9c80-506b4b46e57c', NULL, NULL, 2, 2, 2, 2, NULL, NULL),
                                          (38, 'runbayun', 2, 398934, '上海妆佳电子商务有限公司', '上海妆佳电子商务有限公司', 1608206520, 1608217320, '哈哈', '哈哈', '哈哈', 1608134400, NULL, '哈哈', 1, 0, ';454821_3234', ';', 0, 1608208104, 1, 1, 'a:1:{i:0;a:3:{s:6:"detail";s:36:"作业人员身体条件符合要求";s:6:"result";s:1:"1";s:6:"person";s:6:"哈哈";}}', 1608208104, '', 1, 0, '', '', '', 0, 0, 69, 0, 1, 0, 69, 0, 0, 0, 0, 0, 0, 1608206520, 1608217320, 1608206520, 1608217320, '121.8614526', '31.48667343', 0, '', '8b9f9413-4a4f-11ed-9c80-506b4b46e57c', NULL, NULL, 2, 2, 2, 2, NULL, NULL);

-- 9
INSERT INTO runba_tra.cd_behavior_area VALUES
                                           (0, '', 0, NULL, 1, '', NULL, 0, 0, 0, '', '', '2024-03-30 20:58:27+00:00', '2024-03-30 20:58:43+00:00', 0, '', '', '', '2024-03-30 20:58:27+00:00', '2024-03-30 20:58:43+00:00', ''),
                                           (4, '空分罐区', 443118, '济宁协力能源有限公司', 1, '116.36784186901811,35.73399730926171', 1, 0, 0, 0, '', '', '2023-05-23 20:48:55+00:00', '2024-04-29 08:37:49+00:00', 0, '', '', '', '2024-03-30 14:18:46+00:00', '2024-04-29 08:37:49+00:00', ''),
                                           (5, '循环水泵房北', 443118, '济宁协力能源有限公司', 1, '116.3706619592664,35.73295615377787', 1, 0, 0, 0, '', '', '2023-05-23 20:48:55+00:00', '2024-02-19 09:03:11+00:00', 0, '', '', '', '2024-03-30 14:18:46+00:00', '2024-03-30 14:18:46+00:00', ''),
                                           (6, '1000立方罐罐顶', 443118, '济宁协力能源有限公司', 1, '16.36986366837378,35.73290085451418', 1, 0, 0, 0, '', '', '2023-05-23 20:48:55+00:00', '2023-12-28 13:01:50+00:00', 0, '', '', '', '2024-03-30 14:18:46+00:00', '2024-03-30 14:18:46+00:00', ''),
                                           (7, '冷箱球机-可移动', 443118, '济宁协力能源有限公司', 1, '116.36801882341574,35.73330927720075', 1, 0, 0, 0, '', '', '2023-05-23 20:48:55+00:00', '2023-12-28 13:01:39+00:00', 0, '', '', '', '2024-03-30 14:18:46+00:00', '2024-03-30 14:18:46+00:00', '');

-- 10
INSERT INTO runba_tra.cd_device_point VALUES
                                          (1, 'PI_01', NULL, '1#缓冲罐', 1, 2, '1.200000', '0.000000', 399551, '2023-06-12 17:55:44+00:00', '2023-11-19 09:14:57+00:00'),
                                          (2, 'PI_02', NULL, '2#缓冲罐', 2, 2, '1.200000', '0.000000', 399551, '2023-06-12 17:55:45+00:00', '2023-11-19 09:15:16+00:00'),
                                          (3, 'PI_03', NULL, '3#缓冲罐', 3, 2, '1.200000', '0.000000', 399551, '2023-06-12 17:55:45+00:00', '2023-11-19 09:15:13+00:00'),
                                          (4, 'LI_01', NULL, '2#溴储罐', 4, 7, '2.000000', '0.000000', 399551, '2023-06-12 17:55:50+00:00', '2023-11-19 09:15:38+00:00'),
                                          (5, 'LI_02', NULL, '1#溴储罐', 5, 7, '2.000000', '0.000000', 399551, '2023-06-12 17:55:50+00:00', '2023-11-19 09:16:01+00:00');

-- 11
INSERT INTO runba_tra.cd_security_device VALUES
                                             (1, 1, '2', 'MLT_223A', '1002', 0, '', '', 449600, '济宁协力能源有限公司', 2, 5, 0, '', '', 0, 0, 8, '', NULL, '', '', 1, 0),
                                             (2, 0, '1', 'MLT_223A', '1001', 0, '', '', 449600, '济宁协力能源有限公司', 1, 1, 0, '', '', 0, 0, 8, '', NULL, '', '', 2, 0),
                                             (3, 0, '3', 'MLT_223A', '1003', 0, '', '', 449600, '济宁协力能源有限公司', 3, 5, 0, '', '', 0, 0, 8, '', NULL, '', '', 2, 0),
                                             (4, 0, '3', 'MLT_223A', '1004', 0, '', '', 449600, '济宁协力能源有限公司', 4, 5, 0, '', '', 0, 0, 8, '', NULL, '', '', 3, 0),
                                             (5, 0, '3', 'MLT_223A', '1005', 0, '', '', 443118, '济宁协力能源有限公司', 5, 1, 0, '', '', 0, 0, 8, '', NULL, '', '', 1, 0);

-- 12
INSERT INTO runba_tra.cd_tijian_item_index VALUES
                                               (1, '温度', '℃', '2023-05-18 15:24:59+00:00', '2023-05-18 15:24:59+00:00'),
                                               (2, '压力', 'Kpa', '2023-06-12 11:07:10+00:00', '2024-01-29 10:17:06+00:00'),
                                               (3, '流量', 'm³/h', '2023-06-12 11:17:01+00:00', '2023-06-12 17:11:56+00:00'),
                                               (4, '重量', 'KG', '2023-06-12 11:39:27+00:00', '2023-06-12 11:39:27+00:00'),
                                               (5, '浓度', 'PPM', '2023-06-12 17:11:17+00:00', '2023-06-12 17:11:17+00:00'),
                                               (6, '开度', '%', '2023-06-12 17:11:32+00:00', '2023-06-12 17:11:32+00:00'),
                                               (7, '液位', 'M', '2023-06-12 17:12:41+00:00', '2023-06-12 17:12:41+00:00'),
                                               (8, '气体', '%LEL', '2023-11-25 18:27:41+00:00', '2023-11-30 18:53:57+00:00');

-- 13
INSERT INTO runba_tra.plat_company VALUES
                                       (6673, 0, NULL, NULL, NULL, NULL, '', 0, 1, 1, 0, '', '', '', 0, 0, 0, 0, '', '', '', '', 0, '', 0, 1485164305, 1371, 0, 0, '', 0, '', '', 0, '', '', 0, '', '', 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, '', 1, 0, 0, 1, '', NULL, 0, '0', 0, NULL, NULL, 0, 0, 0, 0, 0, '', 0, 0, '', '', '', 0, 0, '', '', 0, 1, 0, 0, NULL, NULL, '', '', 0, 0, 0, '', NULL, NULL, NULL, NULL, NULL, 0, '', '', '', NULL, NULL, NULL, NULL, 0, 0, NULL, NULL, NULL, 0, NULL, NULL, 0, 0, 0, 0, NULL, '', 0, 6, 1, '', 0, 0, '', '', 1, '', '', 0, 0, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', '', '', '', '', 0, NULL, NULL, '', '', 0, 0, 0, '0.00', '0.00', 0, '', 0, '', 0, NULL, 0, 0, '', 0, '', '', 0, 0, 1, '', '', 0, '', 0, '', '', '', '', '', ''),
                                       (6674, 0, NULL, NULL, NULL, NULL, '', 0, 1, 1, 0, '', '', '', 0, 0, 0, 0, '', '', '', '', 0, '', 0, 1485164477, 1372, 0, 0, '', 0, '', '', 0, '', '', 0, '', '', 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, '', 1, 0, 0, 1, '', NULL, 0, '0', 0, NULL, NULL, 0, 0, 0, 0, 0, '', 0, 0, '', '', '', 0, 0, '', '', 0, 1, 0, 0, NULL, NULL, '', '', 0, 0, 0, '', NULL, NULL, NULL, NULL, NULL, 0, '', '', '', NULL, NULL, NULL, NULL, 0, 0, NULL, NULL, NULL, 0, NULL, NULL, 0, 0, 0, 0, NULL, '', 0, 6, 1, '', 0, 0, '', '', 1, '', '', 0, 0, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', '', '', '', '', 0, NULL, NULL, '', '', 0, 0, 0, '0.00', '0.00', 0, '', 0, '', 0, NULL, 0, 0, '', 0, '', '', 0, 0, 1, '', '', 0, '', 0, '', '', '', '', '', ''),
                                       (6676, 0, NULL, NULL, NULL, NULL, '', 0, 1, 1, 0, '', '', '', 0, 0, 0, 0, '', '', '', '', 0, '', 0, 1485164589, 1373, 0, 0, '', 0, '', '', 0, '', '', 0, '', '', 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, '', 1, 0, 0, 1, '', NULL, 0, '0', 0, NULL, NULL, 0, 0, 0, 0, 0, '', 0, 0, '', '', '', 0, 0, '', '', 0, 1, 0, 0, NULL, NULL, '', '', 0, 0, 0, '', NULL, NULL, NULL, NULL, NULL, 0, '', '', '', NULL, NULL, NULL, NULL, 0, 0, NULL, NULL, NULL, 0, NULL, NULL, 0, 0, 0, 0, NULL, '', 0, 6, 1, '', 0, 0, '', '', 1, '', '', 0, 0, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', '', '', '', '', 0, NULL, NULL, '', '', 0, 0, 0, '0.00', '0.00', 0, '', 0, '', 0, NULL, 0, 0, '', 0, '', '', 0, 0, 1, '', '', 0, '', 0, '', '', '', '', '', ''),
                                       (6677, 0, NULL, NULL, NULL, NULL, '', 0, 1, 1, 0, '', '', '', 0, 0, 0, 0, '', '', '', '', 0, '', 0, 1485164751, 1374, 0, 0, '', 0, '', '', 0, '', '', 0, '', '', 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, '', 1, 0, 0, 1, '', NULL, 0, '0', 0, NULL, NULL, 0, 0, 0, 0, 0, '', 0, 0, '', '', '', 0, 0, '', '', 0, 1, 0, 0, NULL, NULL, '', '', 0, 0, 0, '', NULL, NULL, NULL, NULL, NULL, 0, '', '', '', NULL, NULL, NULL, NULL, 0, 0, NULL, NULL, NULL, 0, NULL, NULL, 0, 0, 0, 0, NULL, '', 0, 6, 1, '', 0, 0, '', '', 1, '', '', 0, 0, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', '', '', '', '', 0, NULL, NULL, '', '', 0, 0, 0, '0.00', '0.00', 0, '', 0, '', 0, NULL, 0, 0, '', 0, '', '', 0, 0, 1, '', '', 0, '', 0, '', '', '', '', '', ''),
                                       (6678, 0, NULL, NULL, NULL, NULL, '', 0, 1, 1, 0, '', '', '', 0, 0, 0, 0, '', '', '', '', 0, '', 0, 1485166243, 1375, 0, 0, '', 0, '', '', 0, '', '', 0, '', '', 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, '', 1, 0, 0, 1, '', NULL, 0, '0', 0, NULL, NULL, 0, 0, 0, 0, 0, '', 0, 0, '', '', '', 0, 0, '', '', 0, 1, 0, 0, NULL, NULL, '', '', 0, 0, 0, '', NULL, NULL, NULL, NULL, NULL, 0, '', '', '', NULL, NULL, NULL, NULL, 0, 0, NULL, NULL, NULL, 0, NULL, NULL, 0, 0, 0, 0, NULL, '', 0, 6, 1, '', 0, 0, '', '', 1, '', '', 0, 0, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '', '', '', '', '', 0, NULL, NULL, '', '', 0, 0, 0, '0.00', '0.00', 0, '', 0, '', 0, NULL, 0, 0, '', 0, '', '', 0, 0, 1, '', '', 0, '', 0, '', '', '', '', '', '');

-- 14
INSERT INTO runba_tra.plat_datav_riskpoint_type VALUES
                                                    (1, 1, 1, 1, 7981),
                                                    (2, 1, 1, 1, 80962),
                                                    (3, 1, 1, 5, 399013),
                                                    (4, 1, 1, 4, 399547),
                                                    (5, 1, 1, 1, 399551);

-- 15
INSERT INTO runba_tra.plat_device_info VALUES
                                           (1, 'DGFHR7688779', '', '变压器OERI02349', '', '', '上海徐汇测试有限公司', '乐山大佛进而为', '1', '乐山大佛进而为', '', '', '1', '', '2020-12-31', '', '', '', '乐山大佛进而为', '03', '03.01', '03.01.03', 449600, 0, '', '', '', 0, '', '', 0, '', '', 0, 0, 4, 0, '', '', '', '', ''),
                                           (2, 'EPRTIOTY398594', '', '云安电梯WOTI29', '', '', '湖南润吧信息技术有限公司', '云安集团有限公司', '1', '1号楼5层东侧', '', '', '1', '', '2020-10-31', '', '', '', '已检测', '06', '06.01', '06.01.01', 449600, 0, '', '', '', 0, '', '', 0, '', '', 0, 0, 5, 0, '', '', '', '', ''),
                                           (3, 'EPRTIOTY398594', '', 'SF9W8RW电梯', '', '', '上海海港航线发展有限公司', '云安集团有限公司', '1', '1号楼5层东侧', '', '', '1', '', '2020-12-31', '', '', '', '已检测', '06', '06.01', '06.01.02', 449600, 0, '', '', '', 0, '', '', 0, '', '', 0, 0, 6, 0, '', '', '', '', ''),
                                           (4, '311121', '', '温度计', '', '', '营口港务集团有限公司', '', '1', '', '', '', '', '', '2020-10-29', '', '', '', '', '01', '01.01', '01.01.01', 449600, 0, '', '', '', 0, '', '', 0, '', '', 0, 0, 7, 0, '', '', '', '', ''),
                                           (5, 'HDFR101202019050020', '', '电动单梁起重机', '', '', '润吧云产品用户体验版', '科尼起重机设备（上海）有限公司', '1', '上海市秋月路28弄', '', '', '1', '', '2020-12-31', '', '', '', '定期检测', '06', '06.02', '06.02.04', 449600, 0, '', '', '', 0, '', '', 0, '', '', 0, 0, 8, 0, '', '', '', '', '');

-- 16
INSERT INTO runba_tra.plat_drill VALUES
                                     (1, 1, '核对公司aa', '名称名称', 1, '22,23,24', 1, 1667557800, 1667561400, '4,5', '6,7', '地点哦', 33, '描述信息描述信息', '总指挥', '总指挥职业', '总策划', '总策划职业', 0, 0, '2022-11-14 12:58:22+00:00', '2022-11-14 12:58:22+00:00'),
                                     (10, 1001075, '1001075', '哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦', 42, '74,75', 1, 1668162180, 1668180180, '335', '335', '哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦', 50, '哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦', '哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦', '呢？哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦', '哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦', '哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦哦', 0, 0, '2022-11-14 15:09:55+00:00', '2022-11-14 15:09:55+00:00'),
                                     (11, 1, '核对公司', '名称名称', 1, '1', 1, 1667557800, 1667561400, '4,5', '6,7', '地点哦', 33, NULL, '总指挥', '总指挥职位', '演练总策划', '演练总策划职务', 0, 0, '2022-11-10 11:07:01+00:00', '2022-11-10 11:07:01+00:00'),
                                     (12, 1, '核对公司', '名称名称', 1, '1', 1, 1667557800, 1667561400, '4,5', '6,7', '地点哦', 33, NULL, '总指挥', '总指挥职位', '演练总策划', '演练总策划职务', 0, 0, '2022-11-10 11:07:46+00:00', '2022-11-10 11:07:46+00:00'),
                                     (13, 1, '核对公司aa', '名称名称', 1, '1', 1, 1667557800, 1667561400, '4,5', '6,7', '地点哦', 33, NULL, '总指挥', '总指挥职位', '演练总策划', '演练总策划职务', 0, 0, '2022-11-10 14:51:46+00:00', '2022-11-10 14:51:46+00:00');

-- 17
INSERT INTO runba_tra.plat_drill_plan VALUES
                                          (1, 422098, '企业一（正式）', '第二季度演练计划', 3, 1, '作废啦', 0, NULL, 1667642400, 1, 2, '2022-11-10 15:25:30+00:00', '2022-11-10 15:25:30+00:00'),
                                          (3, 422098, '企业一（正式）', '第四季度演练计划', 2, 1, '', 0, NULL, 0, 0, 0, '2022-11-07 17:26:30+00:00', '2022-11-07 17:26:30+00:00'),
                                          (4, 422098, '企业一（正式）', '第三季度演练计划', 2, 1, '', 0, NULL, 0, 0, 0, '2022-11-07 17:26:49+00:00', '2022-11-07 17:26:49+00:00'),
                                          (6, 422098, '企业一（正式）', '我给了', 1, 0, '，，', 540861, '李丰雨', 1668072581, 1, 3, '2022-11-10 17:29:25+00:00', '2022-11-10 17:29:25+00:00'),
                                          (8, 443143, '润吧云（山东）信息技术有限公司', '办公室火灾演练计划', 2, 1, '', 0, NULL, 0, 0, 0, '2022-11-24 22:29:21+00:00', '2022-11-24 22:29:21+00:00');

-- 18
INSERT INTO runba_tra.plat_drill_plan_theme VALUES
                                                (9, 3, '，，，', '，，，', 1, '2022', '09', '，，，', 1, '2022-11-23 19:00:55+00:00', '2022-11-07 17:26:30+00:00'),
                                                (10, 3, '2323', '底子地址', 1, '2022', '10', '大沙发电视', 2, '2022-11-08 11:07:53+00:00', '2022-11-07 17:26:30+00:00'),
                                                (11, 4, '，，，', '，，，', 1, '2022', '11', '，，，', 1, '2022-11-07 17:26:49+00:00', '2022-11-07 17:26:49+00:00'),
                                                (12, 4, '2323', '底子地址', 1, '2022', '12', '大沙发电视', 2, '2022-11-07 17:26:49+00:00', '2022-11-07 17:26:49+00:00'),
                                                (21, 1, '，，，', '，，，', 1, '2022', '09', '，，，', 1, '2022-11-23 19:00:59+00:00', '2022-11-10 15:25:30+00:00');

-- 19
INSERT INTO runba_tra.plat_risk_analyse_objects VALUES
                                                    (310, 449600, '4020生产装置', '', '', 1, 562001, '珞珈', '2022-08-28 11:55:50+00:00', '2024-01-23 11:03:24+00:00', '4020生产装置', '589640909', '一级', '', '', '', '', '', '', '', '', '', '', '', '一级', '', '', 1, 5, 0),
                                                    (311, 449600, '醚化装置', '', '', 1, 553458, '韩晗', '2022-08-29 12:35:01+00:00', '2023-08-19 17:48:00+00:00', '醚化装置', '370780002002', '三级', '', '', '', '', '', '', '', '', '', '', '', '生产单元', '', '', 1, 0, 0),
                                                    (312, 449600, '环氧乙烷罐区', '', '', 1, 553458, '韩晗', '2022-08-29 12:36:34+00:00', '2023-08-19 17:47:45+00:00', '环氧乙烷罐区', '370780002001', '三级', '', '', '', '', '', '', '', '', '', '', '', '储存单元', '', '', 1, 5, 0),
                                                    (313, 449600, '4020罐区', '', '', 1, 470462, '超级管理员', '2022-08-29 13:57:31+00:00', '2023-07-06 19:00:23+00:00', '4020罐区', '371700060005', '四级', '', '', '', '', '', '', '', '', '', '', '', '储存单元', '', '', 1, 5, 0),
                                                    (314, 449600, '液氯库', '', '', 1, 470462, '超级管理员', '2022-08-29 13:59:39+00:00', '2023-09-25 10:08:13+00:00', '液氯库', '371700060003', '一级', '', '', '', '', '', '', '', '', '', '', '', '储存单元', '', '', 1, 0, 0);

-- 20
INSERT INTO runba_tra.plat_risk_area VALUES
                                         (4, 313, 55, 'desc', '2023-12-08 13:53:31+00:00', '2023-12-08 13:53:31+00:00'),
                                         (5, 313, 64, 'desc', '2023-12-08 13:53:31+00:00', '2023-12-08 13:53:31+00:00'),
                                         (6, 313, 136, 'desc', '2023-12-08 13:53:31+00:00', '2023-12-08 13:53:31+00:00'),
                                         (7, 313, 137, 'desc', '2023-12-08 13:53:31+00:00', '2023-12-08 13:53:31+00:00'),
                                         (8, 313, 138, 'desc', '2023-12-08 13:53:31+00:00', '2023-12-08 13:53:31+00:00');

-- 21
INSERT INTO runba_tra.plat_risk_point VALUES
                                          (1, 399013, 479167, '熔炼作业风险点（仪表车间2）', 2, 0, '0', '熔炼作业', 1, 4, 1645596179),
                                          (2, 399013, 479167, '熔炼作业（第七车间）', 1, 2, '3', '熔炼作业', 1, 1, 1621342473),
                                          (3, 399013, 479167, '熔炼作业（第五车间）', 2, 0, '0', '熔炼作业', 1, 3, 1621342303),
                                          (4, 422098, 478054, 'iOS测试', 2, 0, '0', '步骤一', 1, 2, 1619423812),
                                          (5, 422098, 478054, '测试', 1, 2, '2', '测试', 1, 5, 1619423818);

-- 22
INSERT INTO runba_tra.plat_tijian_item VALUES
                                           (13298, '气瓶状况', '在检验周期内使用：', 399018, 1, 1568169029, 1568169029, 1, 1, '工业气瓶专业体检卡', 1, 0, 1, 0, '', 0, 1),
                                           (13299, '气瓶状况', '常用气瓶的检验周期为：一般气瓶（氧气、乙炔）每3年检验一次。惰性气体（氮气）每5年检验一次。超过30年的应按报废处理。', 399018, 1, 1568169029, 1568169029, 1, 1, '工业气瓶专业体检卡', 1, 0, 1, 0, '', 0, 1),
                                           (63, '气瓶状况', '外观无机械性损伤及严重腐蚀，表面漆色、字样和色环标记正确、明显；瓶阀、瓶帽、防震圈等安全附件齐全、完好。', 399018, 1, 1568169029, 1568169029, 1, 1, '工业气瓶专业体检卡', 1, 0, 1, 1, '', 0, 1),
                                           (13301, '气瓶状况', '漆色及标志正确、明显', 399018, 1, 1568169029, 1568169029, 1, 1, '工业气瓶专业体检卡', 1, 0, 1, 0, '', 0, 1),
                                           (13302, '气瓶状况', '安全附件齐全、完好', 399018, 1, 1568169029, 1568169029, 1, 1, '工业气瓶专业体检卡', 1, 0, 1, 0, '', 0, 1);

-- 23
INSERT INTO runba_tra.plat_tijian_item_index VALUES
                                                 (1, '温度', '℃', '2023-05-18 15:24:59+00:00', '2023-05-18 15:24:59+00:00'),
                                                 (2, '压力', 'MPA', '2023-06-12 11:07:10+00:00', '2023-06-12 17:11:04+00:00'),
                                                 (3, '液位', 'mm', '2023-06-12 11:17:01+00:00', '2023-12-28 15:39:20+00:00'),
                                                 (4, '声音', 'dB', '2023-06-12 11:39:27+00:00', '2023-12-28 15:39:30+00:00'),
                                                 (5, '震动', 'Hz', '2023-06-12 17:11:17+00:00', '2023-12-28 15:39:41+00:00'),
                                                 (6, '空间氧含量', 'kPa', '2023-06-12 17:11:32+00:00', '2023-12-28 15:40:10+00:00');

-- 24
INSERT INTO runba_tra.plat_tijiancard_item_relation VALUES
                                                        (1, 1001075, 'AYvsIbMtozZn7RfPYG6o', '10198', 1, 444, '2024-03-13 11:30:01+00:00', '2024-03-13 11:30:01+00:00', 0),
                                                        (2, 1000750, 'AXjOGU91B9Z3BEeOAJiC', '352,353', '-1', '-1', '2024-03-13 11:30:01+00:00', '2024-03-13 11:30:01+00:00', 0),
                                                        (3, 449600, 'AX5X4MOWmHcpSHXnzJWe', '6,63', '-1', '4', '2024-03-13 11:30:02+00:00', '2024-03-13 11:30:02+00:00', 0),
                                                        (4, 1000131, 'AX5X-4QMUEbf2HO14aOs', '63', '-1', '-1', '2024-03-13 11:30:02+00:00', '2024-03-13 11:30:02+00:00', 0),
                                                        (5, 451964, 'AY4dKHY5t11NteO2orMy', '7627246,7627247,7627248,7627249,7627250,7627251,7627252,7627253', '-1', '-1', '2024-03-13 11:30:02+00:00', '2024-03-13 11:30:02+00:00', 0);

-- 25
INSERT INTO runba_tra.plat_zone_member VALUES
                                           (12, 'M01000012', '', 0, '22', '', '', '', '', 0, 0, 0, NULL, 209, 202, 3, 1458635405, 1524218264, 1458635405, 462, NULL, NULL, 0, 0, 1, 0, 0, NULL, NULL, 1458635405, 1, 1, 1, '', '', 1, 0, '', 0, NULL, '', 0, NULL, '', 0, NULL, '', 0, NULL, '', 0, NULL, 0, 0, '0.00', 0, 0, NULL, NULL, 1, 6, 1, '', 0, 0, '', '', 0, 0, 0, 0, '', '', '', '', '', '', 'V-223A', '1', 2, 3, 0),
                                           (13, 'M01000013', '', 0, '银飞来了', '', '', '', 'ningmeng0823@hotmail.com', 0, 0, 0, NULL, 449600, 183, 2, 1452061598, 0, 1452061598, 245, NULL, NULL, 0, 0, 1, 0, 0, NULL, NULL, 1452061598, 1, 1, 1, '', '', 1, 0, '', 0, NULL, '', 0, NULL, '', 0, NULL, '', 0, NULL, '', 0, NULL, 0, 0, '0.00', 0, 0, NULL, NULL, 1, 6, 1, '', 0, 0, '', '', 0, 0, 0, 0, '', '', '', '', '', '', 'MLT_223A', '1', 2, 3, 0),
                                           (14, 'M01000014', '', 0, '李先进', '', '', '18621972267', 'tommy@gohoip.com', 0, 0, 0, NULL, 64, 183, 2, 1452063356, 0, 1452063356, 246, NULL, NULL, 0, 0, 1, 0, 0, NULL, NULL, 1452063356, 1, 1, 1, '', '', 1, 0, '', 0, NULL, '', 0, NULL, '', 0, NULL, '', 0, NULL, '', 0, NULL, 0, 0, '0.00', 0, 0, NULL, NULL, 1, 6, 1, '', 0, 0, '', '', 0, 0, 0, 0, '', '', '', '', '', '', 'MLT_223A', '1', 2, 3, 0),
                                           (15, 'M01000015', '', 0, '测试0311', '', '', '', '', 0, 0, 0, NULL, 189, 68, 2, 1457676379, 0, 1457676379, 412, NULL, NULL, 0, 0, 1, 0, 0, NULL, NULL, 1457676379, 1, 1, 1, '', '', 1, 0, '', 0, NULL, '', 0, NULL, '', 0, NULL, '', 0, NULL, '', 0, NULL, 0, 0, '0.00', 0, 0, NULL, NULL, 1, 6, 1, '', 0, 0, '', '', 0, 0, 0, 0, '', '', '', '', '', '', 'MLT_223A', '1', 2, 3, 0),
                                           (16, 'M01000016', '', 0, '测试03111', '', '', '', '', 0, 0, 0, NULL, 190, 68, 2, 1457677104, 0, 1457677104, 413, NULL, NULL, 0, 0, 1, 0, 0, NULL, NULL, 1457677104, 1, 1, 1, '', '', 1, 0, '', 0, NULL, '', 0, NULL, '', 0, NULL, '', 0, NULL, '', 0, NULL, 0, 0, '0.00', 0, 0, NULL, NULL, 1, 6, 1, '', 0, 0, '', '', 0, 0, 0, 0, '', '', '', '', '', '', 'V-223A', '1', 2, 3, 0);

-- create stats to avoid inconsistency when explain wrt stats
CREATE STATISTICS _stats_ FROM runba.firegases450470;
CREATE STATISTICS _stats_ FROM runba.firegases451909;
CREATE STATISTICS _stats_ FROM runba.opcdata411;
CREATE STATISTICS _stats_ FROM runba.opcdata449600;
CREATE STATISTICS _stats_ FROM runba.opcdata449652;
CREATE STATISTICS _stats_ FROM runba.opcdata450081;
CREATE STATISTICS _stats_ FROM runba.opcdata451732;
CREATE STATISTICS _stats_ FROM runba_tra.aqsc_donggongka;
CREATE STATISTICS _stats_ FROM runba_tra.cd_behavior_area;
CREATE STATISTICS _stats_ FROM runba_tra.cd_device_point;
CREATE STATISTICS _stats_ FROM runba_tra.cd_security_device;
CREATE STATISTICS _stats_ FROM runba_tra.cd_tijian_item_index;
CREATE STATISTICS _stats_ FROM runba_tra.plat_company;
CREATE STATISTICS _stats_ FROM runba_tra.plat_datav_riskpoint_type;
CREATE STATISTICS _stats_ FROM runba_tra.plat_device_info;
CREATE STATISTICS _stats_ FROM runba_tra.plat_drill;
CREATE STATISTICS _stats_ FROM runba_tra.plat_drill_plan;
CREATE STATISTICS _stats_ FROM runba_tra.plat_drill_plan_theme;
CREATE STATISTICS _stats_ FROM runba_tra.plat_risk_analyse_objects;
CREATE STATISTICS _stats_ FROM runba_tra.plat_risk_area;
CREATE STATISTICS _stats_ FROM runba_tra.plat_risk_point;
CREATE STATISTICS _stats_ FROM runba_tra.plat_tijian_item;
CREATE STATISTICS _stats_ FROM runba_tra.plat_tijian_item_index;
CREATE STATISTICS _stats_ FROM runba_tra.plat_tijiancard_item_relation;
CREATE STATISTICS _stats_ FROM runba_tra.plat_zone_member;

set enable_multimodel=true;

delimiter \\
CREATE PROCEDURE test_proc_multimodel31()
BEGIN
SELECT
    "time",
    "value",
    "device",
    "datatype"
FROM
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_tijian_item pti,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii
WHERE
        ptir."tijiancard_id" = 'AX5X4MOWmHcpSHXnzJWe'
  AND opcdata.companyid = ptir.company_id
  AND (cast(pti.id as string) = ANY string_to_array(ptir.item_ids, ','))
  AND ptir.obj_id = pdi.id
  AND pti.index_id = ptii.id
  AND opcdata.device = pdi.name
  AND opcdata.datatype = ptii.index_name
ORDER BY
    "time" DESC
    LIMIT 1;
END \\
delimiter ;
call test_proc_multimodel31();
call test_proc_multimodel31();
drop procedure test_proc_multimodel31;

delimiter \\
CREATE PROCEDURE test_proc_multimodel32()
BEGIN
SELECT time_bucket(o.time, '10s') as timebucket,
       AVG(o.value) as avg_value
FROM runba.opcdata449600 o,
     runba_tra.cd_security_device csd,
     runba_tra.cd_device_point cdp,
     runba_tra.cd_tijian_item_index ctii
WHERE o.companyid = csd.company_id
  AND o.tag = csd.device_no
  AND csd.id = cdp.device_id
  AND ctii.id = cdp.index_id
  AND csd.device_info_id in (
    SELECT id
    FROM runba_tra.plat_device_info pd
    WHERE pd.area_id in (
        SELECT id
        FROM runba_tra.plat_risk_area pra
        WHERE risk_id in (
            SELECT id
            FROM runba_tra.plat_risk_analyse_objects prab
            WHERE prab.company_id = pd.company_id
              AND prab.category = 1
              AND prab.danger_level = '四级'
              AND prab.risk_type = 5))
      AND pd.company_id = csd.company_id)
  AND csd.company_id = 449600
  AND csd.category = 5
GROUP BY timebucket
ORDER BY timebucket ASC;
END \\
delimiter ;
call test_proc_multimodel32();
call test_proc_multimodel32();
drop procedure test_proc_multimodel32;

delimiter \\
CREATE PROCEDURE test_proc_multimodel33()
BEGIN
SELECT
    last("time"), last("value"), last("device"), last("datatype")
FROM
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_tijian_item pti,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii
WHERE
    ptir."tijiancard_id" = 'AX5X4MOWmHcpSHXnzJWe'
  AND opcdata.companyid = ptir.company_id
  AND (cast(pti.id as char) = ANY string_to_array(ptir.item_ids, ','))
  AND pti.index_id = ptii.id
  AND opcdata.device = cast(pdi.name as string)
  AND pti.create_user_id > 0;
END \\
delimiter ;
call test_proc_multimodel33();
call test_proc_multimodel33();
drop procedure test_proc_multimodel33;


delimiter \\
CREATE PROCEDURE test_proc_multimodel34()
BEGIN
SELECT
    time_bucket(opcdata."time", '200s') AS timebucket,
    SUM(opcdata."value") as avg_val,
    "device",
    "datatype",
    ptir.company_id
FROM
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_tijian_item pti,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii
WHERE
        ptir."tijiancard_id" = 'AX5X4MOWmHcpSHXnzJWe'
  AND opcdata.companyid = ptir.company_id
  AND (cast(pti.id as string) = ANY string_to_array(ptir.item_ids, ','))
  AND ptir.obj_id = pdi.id
  AND pti.index_id = ptii.id
  AND opcdata.device = pdi.name
  AND opcdata.datatype = ptii.index_name
GROUP BY
    opcdata.device,
    opcdata.datatype,
    ptir.company_id,
    timebucket
ORDER BY
    opcdata.device,
    opcdata.datatype,
    timebucket;
END \\
delimiter ;
call test_proc_multimodel34();
call test_proc_multimodel34();
drop procedure test_proc_multimodel34;

delimiter \\
CREATE PROCEDURE test_proc_multimodel35()
BEGIN
SELECT
    COUNT(*),
    ROUND(AVG(opcdata."value"),2),
    last(opcdata."value")
FROM
    runba.opcdata449600 opcdata,
    runba_tra.plat_zone_member pzm
WHERE
    pzm."tag" = opcdata."tag"
  AND pzm.company_id = opcdata.companyid
  AND opcdata."time" > '2023-05-29 10:10:10'
  AND opcdata."time" <= '2024-05-29 16:10:10'
  AND pzm.contract_email = 'ningmeng0823@hotmail.com';
END \\
delimiter ;
call test_proc_multimodel35();
call test_proc_multimodel35();
drop procedure test_proc_multimodel35;


delimiter \\
CREATE PROCEDURE test_proc_multimodel36()
BEGIN
select * from (
                  SELECT
                      COUNT(*)
                  FROM
                      runba.opcdata449600 opcdata,
                      runba_tra.plat_zone_member pzm
                  where pzm."tag" = opcdata."tag"
                    AND pzm.company_id = opcdata.companyid
                    AND  opcdata."time" > '2023-05-29 10:10:10'
                    AND opcdata."time" <= '2024-05-29 16:10:10'
                    AND pzm.contract_email = 'ningmeng0823@hotmail.com');
END \\
delimiter ;
call test_proc_multimodel36();
call test_proc_multimodel36();
drop procedure test_proc_multimodel36;


delimiter \\
CREATE PROCEDURE test_proc_multimodel37()
BEGIN
select * from (SELECT
                   COUNT(*),
                   AVG(opcdata."value")
               FROM
                   runba.opcdata449600 opcdata,
                   runba_tra.plat_zone_member pzm
               where pzm."tag" = opcdata."tag"
                 AND pzm.company_id = opcdata.companyid
                 AND  opcdata."time" > '2023-05-29 10:10:10'
                 AND opcdata."time" <= '2024-05-29 16:10:10'
                 AND pzm.contract_email = 'ningmeng0823@hotmail.com'
                 AND pzm.contract_mobile = opcdata.channel);
END \\
delimiter ;
call test_proc_multimodel37();
call test_proc_multimodel37();
drop procedure test_proc_multimodel37;


delimiter \\
CREATE PROCEDURE test_proc_multimodel38()
BEGIN
SELECT
    *
FROM
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_tijian_item pti,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii
WHERE
    (ptir."tijiancard_id" = 'AYvsIbMtozZn7RfPYG6o'
        OR ptir."tijiancard_id" = 'AX5X4MOWmHcpSHXnzJWe')
  AND opcdata.companyid = ptir.company_id
  AND (cast(pti.id as string) = ANY string_to_array(ptir.item_ids, ','))
  AND ptir.obj_id = pdi.id
  AND pti.index_id = ptii.id
  AND opcdata.device = pdi.name
  AND opcdata.datatype = ptii.index_name
ORDER BY opcdata."time"
    LIMIT 100;
END \\
delimiter ;
call test_proc_multimodel38();
call test_proc_multimodel38();
drop procedure test_proc_multimodel38;

delimiter \\
CREATE PROCEDURE test_proc_multimodel39()
BEGIN
SELECT
    time_bucket(opcdata."time", '200s') AS timebucket,
    SUM(opcdata."value") / COUNT(opcdata."value") + MAX(opcdata."value") - MIN(opcdata."value") as avg_val,
    COUNT("device"),
    "datatype",
    COUNT(distinct ptir.company_id),
    AVG(pdi.check_period),
    MAX(pdi.area_id)
FROM
    runba.opcdata449600 opcdata,
    runba_tra.plat_tijiancard_item_relation ptir,
    runba_tra.plat_tijian_item pti,
    runba_tra.plat_device_info pdi,
    runba_tra.plat_tijian_item_index ptii
WHERE
        ptir."tijiancard_id" = 'AX5X4MOWmHcpSHXnzJWe'
  AND opcdata.companyid = ptir.company_id
  AND (cast(pti.id as string) = ANY string_to_array(ptir.item_ids, ','))
  AND ptir.obj_id = pdi.id
  AND pti.index_id = ptii.id
  AND opcdata.device = pdi.name
  AND opcdata.datatype = ptii.index_name
GROUP BY
    opcdata.device,
    opcdata.datatype,
    ptir.company_id,
    timebucket
HAVING
        AVG(opcdata."value") > 60.0
ORDER BY
    opcdata.device,
    opcdata.datatype,
    timebucket;
END \\
delimiter ;
call test_proc_multimodel39();
call test_proc_multimodel39();
drop procedure test_proc_multimodel39;

delimiter \\
CREATE PROCEDURE test_proc_multimodel40()
BEGIN
SELECT
    time_bucket(opcdata."time", '10s') AS timebucket,
    AVG(opcdata."value"),
    MIN(opcdata."value"),
    MAX(opcdata."value"),
    SUM(opcdata."value")
FROM
    runba.opcdata449600 opcdata,
    runba_tra.plat_zone_member pzm
WHERE
        pzm."tag" = opcdata."tag"
  AND pzm.company_id = opcdata.companyid
  AND opcdata."time" > '2023-05-29 10:10:10'
  AND opcdata."time" <= '2024-05-29 16:10:10'
  AND pzm.contract_email = 'ningmeng0823@hotmail.com'
GROUP BY
    timebucket
HAVING
        AVG(opcdata."value") > 65.0
   AND MAX(pzm.zone_company_id) > 100
ORDER BY
    timebucket desc;
END \\
delimiter ;
call test_proc_multimodel40();
call test_proc_multimodel40();
drop procedure test_proc_multimodel40;

delimiter \\
CREATE PROCEDURE test_proc_multimodel41()
BEGIN
SELECT
    time_bucket(opcdata."time", '10s') AS timebucket,
    AVG(opcdata."value"),
    MIN(opcdata."value"),
    MAX(opcdata."value"),
    SUM(opcdata."value")
FROM
    runba.opcdata449600 opcdata,
    (select c.company_id as company_id, p.company_id as c_id
     from runba_tra.plat_zone_member p
              left outer join runba_tra.cd_security_device c
                              on p.company_id = c.company_id limit 100
    ) as pzm
WHERE
        pzm.company_id = opcdata.companyid
  AND opcdata."time" > '2023-05-29 10:10:10'
  AND opcdata."time" <= '2024-05-29 11:10:10'
GROUP BY
    timebucket
ORDER BY
    timebucket;

SELECT
    time_bucket(opcdata."time", '10s') AS timebucket,
    AVG(opcdata."value"),
    MIN(opcdata."value"),
    MAX(opcdata."value"),
    SUM(opcdata."value")
FROM
    runba.opcdata449600 opcdata,
    (select c.company_id as company_id, p.company_id as c_id
     from runba_tra.plat_zone_member p
              left outer join runba_tra.cd_security_device c
                              on p.company_id = c.company_id limit 100
    ) as pzm
WHERE
        pzm.company_id = opcdata.companyid
  AND opcdata."time" > '2023-05-29 10:10:10'
  AND opcdata."time" <= '2024-05-29 11:10:10'
GROUP BY
    timebucket
ORDER BY
    timebucket;
END \\
delimiter ;
call test_proc_multimodel41();
call test_proc_multimodel41();
drop procedure test_proc_multimodel41;

set hash_scan_mode=2;

delimiter \\
CREATE PROCEDURE test_proc_multimodel42()
BEGIN
select
    sum(ts_t."value"),
    count(*),
    rel_t.company_id
from
    ((select
          t3.company_id as "company_id",
          t3.tijiancard_id as "tijiancard_id",
          t3.obj_type as "obj_type",
          t3.obj_id as "obj_id",
          t3.name as "name",
          t3.devicenumber as "devicenumber",
          t4.company_id as "company_id_1",
          t4.tijiancard_id as "tijiancard_id_1",
          t4.obj_type as "obj_type_1",
          t4.obj_id as "obj_id_1",
          'CH271' as "channel"
      from
          (select
               *
           from
               runba_tra.plat_tijiancard_item_relation t1,
               runba_tra.plat_device_info t2
           where
                   t1.obj_id = t2.id
          ) t3,
          (select * from runba_tra.plat_tijiancard_item_relation where company_id % 100 = 0 limit 1) t4
      where
              t3.company_id = t4.company_id
          limit 1)
     union
     (select
          t3.company_id as "company_id",
          t3.tijiancard_id as "tijiancard_id",
          t3.obj_type as "obj_type",
          t3.obj_id as "obj_id",
          t3.name as "name",
          t3.devicenumber as "devicenumber",
          t4.company_id as "company_id_1",
          t4.tijiancard_id as "tijiancard_id_1",
          t4.obj_type as "obj_type_1",
          t4.obj_id as "obj_id_1",
          'CH271' as "channel"
      from
          (select
               *
           from
               runba_tra.plat_tijiancard_item_relation t1,
               runba_tra.plat_device_info t2
           where
                   t1.obj_id = t2.id
          ) t3,
          (select * from runba_tra.plat_tijiancard_item_relation where company_id % 100 = 0 limit 1) t4
      where
              t3.company_id = t4.company_id
          limit 1)
    ) rel_t,
    runba.opcdata449600 ts_t
where
        rel_t.company_id = ts_t.companyid
  and rel_t.channel = ts_t.channel
group by
    rel_t.company_id;
END \\
delimiter ;
call test_proc_multimodel42();
call test_proc_multimodel42();
drop procedure test_proc_multimodel42;

set hash_scan_mode=0;

set enable_multimodel=false;

DROP TABLE runba.firegases00001;
DROP TABLE runba.firegases450470;
DROP TABLE runba.firegases451909;
DROP TABLE runba.opcdata411;
DROP TABLE runba.opcdata449600;
DROP TABLE runba.opcdata449652;
DROP TABLE runba.opcdata450081;
DROP TABLE runba.opcdata451732;
DROP TABLE runba.t_399551;

DROP TABLE runba_tra.aqsc_donggongka;
DROP TABLE runba_tra.cd_behavior_area;
DROP TABLE runba_tra.cd_device_point;
DROP TABLE runba_tra.cd_security_device;
DROP TABLE runba_tra.cd_tijian_item_index;
DROP TABLE runba_tra.plat_company;
DROP TABLE runba_tra.plat_datav_riskpoint_type;
DROP TABLE runba_tra.plat_device_info;
DROP TABLE runba_tra.plat_drill;
DROP TABLE runba_tra.plat_drill_plan;
DROP TABLE runba_tra.plat_drill_plan_theme;
DROP TABLE runba_tra.plat_risk_analyse_objects;
DROP TABLE runba_tra.plat_risk_area;
DROP TABLE runba_tra.plat_risk_point;
DROP TABLE runba_tra.plat_tijian_item;
DROP TABLE runba_tra.plat_tijian_item_index;
DROP TABLE runba_tra.plat_tijiancard_item_relation;
DROP TABLE runba_tra.plat_zone_member;
DROP TABLE runba_tra.testa;

DROP DATABASE runba_tra;
DROP DATABASE runba;
delete from system.table_statistics where name = '_stats_';
set cluster setting ts.sql.query_opt_mode = DEFAULT;
set cluster setting sql.stats.tag_automatic_collection.enabled = true;
set cluster setting sql.stats.automatic_collection.enabled = true;

DROP DATABASE IF EXISTS db cascade;