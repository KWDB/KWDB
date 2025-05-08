SET CLUSTER SETTING ts.blocks_per_segment.max_limit=100;
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

drop database tsdb cascade;