SET CLUSTER SETTING ts.rows_per_block.min_limit=100;
SET CLUSTER SETTING ts.rows_per_block.max_limit=100;
create ts database tsdb;
use tsdb;
create table t1(ts timestamp not null, status int, b int) tags (a int not null) primary tags(a);
Insert into t1 values('2025-01-10 06:14:47.298+00:00', 1, 2, 1);
Insert into t1 values('2025-01-10 06:14:48.298+00:00', 1, 3, 1);
Insert into t1 values('2025-01-11 06:14:47.298+00:00', 1, 4, 1);
Insert into t1 values('2025-01-12 06:14:47.298+00:00', 1, 5, 1);
Insert into t1 values('2025-01-15 06:14:47.298+00:00', 2, 6, 1);
Insert into t1 values('2025-01-16 06:14:47.298+00:00', 2, 1, 1);
Insert into t1 values('2025-01-17 06:14:47.298+00:00', 2, 2, 1);
Insert into t1 values('2025-01-20 06:14:47.298+00:00', 3, 3, 1);
Insert into t1 values('2025-01-21 06:14:47.298+00:00', 3, 4, 1);
Insert into t1 values('2025-01-22 06:14:47.298+00:00', 3, 5, 1);
Insert into t1 values('2025-01-28 06:14:47.298+00:00', 1, 6, 1);
Insert into t1 values('2025-01-29 06:14:47.298+00:00', 1, 7, 1);

-- state_window
select first(ts) as first ,count(ts) from tsdb.t1 group by state_window(status);
select first(ts) as first ,count(ts) from tsdb.t1 group by state_window(case when status=1 then 100 else 200 end);
select first(ts) as first ,count(ts) from tsdb.t1 group by a,state_window(status) order by a;
-- error
select first(ts) as first ,count(ts) from tsdb.t1 group by state_window(case when status=1 then 100.0 else 200.0 end);

-- session_window
select first(ts) as first ,count(ts) from tsdb.t1 group by session_window(ts, '1s');
select first(ts) as first ,count(ts) from tsdb.t1 group by session_window(ts, '1d');
select first(ts) as first ,count(ts) from tsdb.t1 group by session_window(ts, '3day');
select first(ts) as first ,count(ts) from tsdb.t1 group by a, session_window(ts, '1s') order by a;
-- error
select first(ts) as first ,count(ts) from tsdb.t1 group by session_window(ts, '3dayss');
select first(ts) as first ,count(ts) from tsdb.t1 group by session_window(ts, '-3days');

-- count_window
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(12);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(12,1);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(12,6);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(4,3);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(4,4);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(4);
-- error
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(-1,-1);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(0,0);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(12,0);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(12,'ds');


-- event_window
select first(ts) as first ,count(ts) from tsdb.t1 group by event_window(status>1, b>4);
select first(ts) as first ,count(ts) from tsdb.t1 group by event_window(status>2, b<3);
select first(ts) as first ,count(ts) from tsdb.t1 group by event_window(status>7, b<10);
select first(ts) as first ,count(ts) from tsdb.t1 group by event_window(status<0, b<1);
select first(ts) as first ,count(ts) from tsdb.t1 group by event_window(status<3, b<4);
-- error
select first(ts) as first ,count(ts) from tsdb.t1 group by event_window(sum(status)>100, b<10);

-- time_window
select first(ts) as _wstart,last(ts) as _wend, avg(b) from tsdb.t1 group by time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, avg(b) from tsdb.t1 group by time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, avg(b) from tsdb.t1 group by time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, avg(b) from tsdb.t1 group by time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, avg(b) from tsdb.t1 group by time_window(ts, '1mons', '1w');
-- error
select first(ts) as _wstart,last(ts) as _wend, avg(b) from tsdb.t1 group by time_window(ts, '1d', '1w');

-- time_window
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by time_window(ts, '1mons', '1w');
-- error
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by time_window(ts, '1d', '1w');

-- time_window
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by time_window(ts, '1mons', '1w');
-- error
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by time_window(ts, '1d', '1w');

select first_row(ts) as _wstart,last(ts) as _wend, min(b) from tsdb.t1 group by time_window(ts, '1w', '1d');
select first(ts) as _wstart,last_row(ts) as _wend, min(b) from tsdb.t1 group by time_window(ts, '1y', '1d');
select first_row(ts) as _wstart,last_row(ts) as _wend, min(b) from tsdb.t1 group by time_window(ts, '1y', '1w');
select first_row(ts) as _wstart,last_row(ts) as _wend, min(b) from tsdb.t1 group by time_window(ts, '2y', '1w');
select first_row(ts) as _wstart,last(ts) as _wend, min(b) from tsdb.t1 group by time_window(ts, '1mons', '1w');

drop database tsdb cascade;

drop database if exists t_time_w cascade;
create ts database t_time_w;
create table t_time_w.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int not null,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t2);
INSERT INTO t_time_w.tb VALUES
('2023-01-01 00:00:00', '2023-10-01 00:00:00', 1, 1, 1, 1.1, 1.1, true, 'a', 'char1', 'a', 'nchar1', 'varchar1', 'varchar1', 'varchar1', 'nvarchar1', 'nvarchar1', 'nvarchar1', 'varbytes1', 'varbytes1', 'varbytes1', 'varbytes1', 'varbytes1', 1, 1, 1, true, 1.1, 1.1, 'a', 'tag1', 'a', 'nchar1', 'varchar1', 'tag1', 'varbytes1', 'varbytes1', 'varbytes1', 'varbytes1'),
('2023-01-15 00:00:00', '2023-10-02 00:00:10', 2, 2, 2, 2.2, 2.2, false, 'b', 'char2', 'b', 'nchar2', 'varchar2', 'varchar2', 'varchar2', 'nvarchar2', 'nvarchar2', 'nvarchar2', 'varbytes2', 'varbytes2', 'varbytes2', 'varbytes2', 'varbytes2', 2, 2, 2, false, 1.2, 1.2, 'b', 'tag2', 'b', 'nchar2', 'varchar2', 'tag2', 'varbytes2', 'varbytes2', 'varbytes2', 'varbytes2'),
('2024-01-31 00:00:00', '2023-10-03 00:00:20', 3, 3, 3, 3.3, 3.3, true, 'c', 'char3', 'c', 'nchar3', 'varchar3', 'varchar3', 'varchar3', 'nvarchar3', 'nvarchar3', 'nvarchar3', 'varbytes3', 'varbytes3', 'varbytes3', 'varbytes3', 'varbytes3', 1, 2, 3, true, 1.3, 1.3, 'c', 'tag3', 'c', 'nchar3', 'varchar3', 'tag3', 'varbytes3', 'varbytes3', 'varbytes3', 'varbytes3'),
('2024-02-01 00:00:00', '2023-10-04 00:00:30', 4, 4, 4, 4.4, 4.4, false, 'd', 'char4', 'd', 'nchar4', 'varchar4', 'varchar4', 'varchar4', 'nvarchar4', 'nvarchar4', 'nvarchar4', 'varbytes4', 'varbytes4', 'varbytes4', 'varbytes4', 'varbytes4', 2, 2, 4, false, 1.4, 1.4, 'd', 'tag4', 'd', 'nchar4', 'varchar4', 'tag4', 'varbytes4', 'varbytes4', 'varbytes4', 'varbytes4'),
('2024-02-02 00:00:00', '2023-10-05 00:00:30', 5, 5, 5, 5.5, 5.5, false, 'e', 'char5', 'e', 'nchar5', 'varchar5', 'varchar5', 'varchar5', 'nvarchar5', 'nvarchar5', 'nvarchar5', 'varbytes5', 'varbytes5', 'varbytes5', 'varbytes5', 'varbytes5', 1, 2, 5, false, 1.5, 1.5, 'e', 'tag5', 'e', 'nchar5', 'varchar5', 'tag5', 'varbytes5', 'varbytes5', 'varbytes5', 'varbytes5'),
('2025-01-02 00:00:00', '2023-10-06 00:00:30', 6, 6, 6, 6.6, 6.6, false, 'e', 'char6', 'e', 'nchar6', 'varchar6', 'varchar6', 'varchar6', 'nvarchar6', 'nvarchar6', 'nvarchar6', 'varbytes6', 'varbytes6', 'varbytes6', 'varbytes6', 'varbytes6', 1, 1, 6, false, 1.6, 1.6, 'e', 'tag6', 'e', 'nchar6', 'varchar6', 'tag6', 'varbytes6', 'varbytes6', 'varbytes6', 'varbytes6');
SELECT first(k_timestamp),last(k_timestamp),count(*),first(e2) as first_e2,first(e3) as first_e3 FROM t_time_w.tb where t1=1 and t2=1 GROUP BY time_window(k_timestamp, '1y');
drop table t_time_w.tb;
drop database if exists t_time_w cascade;