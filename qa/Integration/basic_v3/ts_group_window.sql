SET CLUSTER SETTING ts.rows_per_block.min_limit=100;
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

create table t2(ts timestamp not null, status int, b int) tags (a int not null, c int not null) primary tags(a, c);
Insert into t2 values('2025-01-10 06:14:47.298+00:00', 1, 2, 1, 1);
Insert into t2 values('2025-01-10 06:14:48.298+00:00', 1, 3, 1, 1);
Insert into t2 values('2025-01-11 06:14:47.298+00:00', 1, 4, 1, 1);
Insert into t2 values('2025-01-12 06:14:47.298+00:00', 1, 5, 1, 2);
Insert into t2 values('2025-01-15 06:14:47.298+00:00', 2, 6, 1, 3);
Insert into t2 values('2025-01-16 06:14:47.298+00:00', 2, 1, 1, 3);
Insert into t2 values('2025-01-17 06:14:47.298+00:00', 2, 2, 1, 4);
Insert into t2 values('2025-01-20 06:14:47.298+00:00', 3, 3, 1, 5);
Insert into t2 values('2025-01-21 06:14:47.298+00:00', 3, 4, 1, 5);
Insert into t2 values('2025-01-22 06:14:47.298+00:00', 3, 5, 1, 5);
Insert into t2 values('2025-01-28 06:14:47.298+00:00', 1, 6, 1, 6);
Insert into t2 values('2025-01-29 06:14:47.298+00:00', 1, 7, 1, 6);

-- state_window
select first(ts) as first ,count(ts) from tsdb.t1 group by state_window(status);
select first(ts) as first ,count(ts) from tsdb.t1 group by state_window(case when status=1 then 100 else 200 end);
select first(ts) as first ,count(ts) from tsdb.t1 group by a,state_window(status) order by a;
select first(ts) as first ,count(ts) from tsdb.t1 where a = 1 group by state_window(status);

-- group window function optimizer
select first(ts) as first ,count(ts) from tsdb.t2 group by b, state_window(status);
select first(ts) as first ,count(ts) from tsdb.t2 group by b, state_window(status) order by b desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, state_window(status);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, state_window(status) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, state_window(status);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, state_window(status) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, state_window(status);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, state_window(status) order by a desc;

-- error
select first(ts) as first ,count(ts) from tsdb.t1 group by state_window(case when status=1 then 100.1 else 200.0 end);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, state_window(status) order by state_window(status);
select first(ts) as first ,count(ts) from tsdb.t1 where a = state_window(status) group by a, state_window(status);
select first(ts) as first ,count(ts), state_window(status) from tsdb.t1 group by a, state_window(status);
select first(ts) as first ,count(ts) from tsdb.t1 group by a+b, state_window(status);
with p1 as (select * from t1) select first(ts) as first ,count(ts) from p1 group by a, state_window(status);
select first(ts) as first ,count(ts) from tsdb.t1 group by a,state_window(status) union select first(ts) as first ,count(ts) from tsdb.t2 group by a,state_window(status);
select first(ts) as first ,count(ts) from tsdb.t1 group by a,state_window(status) union select first(ts) as first ,count(ts) from tsdb.t2 group by a;
select first(ts) as first ,count(ts) from tsdb.t1 where ts in (select first(ts) from tsdb.t2 group by state_window(status));
select first(ts) as first ,count(ts) from tsdb.t1 where ts = (select first(ts) from tsdb.t2) group by a, state_window(status);
select first(ts) as first ,count(ts) from (select * from tsdb.t1 group by a, state_window(status));
select a from (select * from tsdb.t1) group by a, state_window(status);

-- session_window
select first(ts) as first ,count(ts) from tsdb.t1 group by session_window(ts, '1s');
select first(ts) as first ,count(ts) from tsdb.t1 group by session_window(ts, '1d');
select first(ts) as first ,count(ts) from tsdb.t1 group by session_window(ts, '3day');
select first(ts) as first ,count(ts) from tsdb.t1 group by a, session_window(ts, '1s') order by a;
select first(ts) as first ,count(ts) from tsdb.t1 where a = 1 group by session_window(ts, '1s');

-- group window function optimizer
select first(ts) as first ,count(ts) from tsdb.t2 group by b, session_window(ts, '1s');
select first(ts) as first ,count(ts) from tsdb.t2 group by b, session_window(ts, '1s') order by b desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, session_window(ts, '1s');
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, session_window(ts, '1s') order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, session_window(ts, '1s');
select first(ts) as first ,count(ts) from tsdb.t2 group by a, session_window(ts, '1s') order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, session_window(ts, '1s');
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, session_window(ts, '1s') order by a desc;

-- error
select first(ts) as first ,count(ts) from tsdb.t1 group by session_window(ts, '3dayss');
select first(ts) as first ,count(ts) from tsdb.t1 group by session_window(ts, '-3days');
select first(ts) as first ,count(ts) from tsdb.t1 group by a, session_window(ts, '1s') order by session_window(ts, '1s');
select first(ts) as first ,count(ts) from tsdb.t1 where a = session_window(ts, '1s') group by a, session_window(ts, '1s');
select first(ts) as first ,count(ts), session_window(ts, '1s') from tsdb.t1 group by a, session_window(ts, '1s');
select first(ts) as first ,count(ts) from tsdb.t1 group by a+b, session_window(ts, '1s');
with p1 as (select * from t1) select first(ts) as first ,count(ts) from p1 group by a, session_window(ts, '1s');
select first(ts) as first ,count(ts) from tsdb.t1 group by a,session_window(ts, '1s') union select first(ts) as first ,count(ts) from tsdb.t2 group by a,session_window(ts, '1s');
select first(ts) as first ,count(ts) from tsdb.t1 group by a,session_window(ts, '1s') union select first(ts) as first ,count(ts) from tsdb.t2 group by a;
select first(ts) as first ,count(ts) from tsdb.t1 where ts in (select first(ts) from tsdb.t2 group by session_window(ts, '1s'));
select first(ts) as first ,count(ts) from tsdb.t1 where ts = (select first(ts) from tsdb.t2) group by a, session_window(ts, '1s');
select first(ts) as first ,count(ts) from (select * from tsdb.t1 group by a, session_window(ts, '1s'));
select a from (select * from tsdb.t1) group by a, session_window(ts, '1s');

-- count_window
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(12);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(12,1);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(12,6);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(4,3);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(4,4);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(4);

select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(12);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(12) order by a;
select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(12,1);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(12,1) order by a;
select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(12,6);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(12,6) order by a;
select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(4,3);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(4,3) order by a;
select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(4,4);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(4,4) order by a;
select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(4);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(4) order by a;

select first(ts) as first ,count(ts) from tsdb.t1 where a=1 group by count_window(12);
select first(ts) as first ,count(ts) from tsdb.t1 where a=1 group by count_window(12,1);
select first(ts) as first ,count(ts) from tsdb.t1 where a=1 group by count_window(12,6);
select first(ts) as first ,count(ts) from tsdb.t1 where a=1 group by count_window(4,3);
select first(ts) as first ,count(ts) from tsdb.t1 where a=1 group by count_window(4,4);
select first(ts) as first ,count(ts) from tsdb.t1 where a=1 group by count_window(4);

-- group window function optimizer
select first(ts) as first ,count(ts) from tsdb.t2 group by b, count_window(12);
select first(ts) as first ,count(ts) from tsdb.t2 group by b, count_window(12) order by b desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, count_window(12);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, count_window(12) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, count_window(12);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, count_window(12) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, count_window(12);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, count_window(12) order by a desc;

select first(ts) as first ,count(ts) from tsdb.t2 group by b, count_window(12,1);
select first(ts) as first ,count(ts) from tsdb.t2 group by b, count_window(12,1) order by b desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, count_window(12,1);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, count_window(12,1) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, count_window(12,1);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, count_window(12,1) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, count_window(12,1);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, count_window(12,1) order by a desc;

select first(ts) as first ,count(ts) from tsdb.t2 group by b, count_window(4,3);
select first(ts) as first ,count(ts) from tsdb.t2 group by b, count_window(4,3) order by b desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, count_window(4,3);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, count_window(4,3) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, count_window(4,3);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, count_window(4,3) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, count_window(4,3);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, count_window(4,3) order by a desc;

select first(ts) as first ,count(ts) from tsdb.t2 group by b, count_window(4);
select first(ts) as first ,count(ts) from tsdb.t2 group by b, count_window(4) order by b desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, count_window(4);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, count_window(4) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, count_window(4);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, count_window(4) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, count_window(4);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, count_window(4) order by a desc;

-- error
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(-1,-1);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(0,0);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(12,0);
select first(ts) as first ,count(ts) from tsdb.t1 group by count_window(12,'ds');
select first(ts) as first ,count(ts) from tsdb.t1 group by a, count_window(4) order by count_window(4);
select first(ts) as first ,count(ts) from tsdb.t1 where a = count_window(4) group by a, count_window(4);
select first(ts) as first ,count(ts), count_window(4) from tsdb.t1 group by a, count_window(4);
select first(ts) as first ,count(ts) from tsdb.t1 group by a+b, count_window(4);
with p1 as (select * from t1) select first(ts) as first ,count(ts) from p1 group by a, count_window(4);
select first(ts) as first ,count(ts) from tsdb.t1 group by a,count_window(4) union select first(ts) as first ,count(ts) from tsdb.t2 group by a,count_window(4);
select first(ts) as first ,count(ts) from tsdb.t1 group by a,count_window(4) union select first(ts) as first ,count(ts) from tsdb.t2 group by a;
select first(ts) as first ,count(ts) from tsdb.t1 where ts in (select first(ts) from tsdb.t2 group by count_window(4));
select first(ts) as first ,count(ts) from tsdb.t1 where ts = (select first(ts) from tsdb.t2) group by a, count_window(4);
select first(ts) as first ,count(ts) from (select * from tsdb.t1 group by a, count_window(4));
select a from (select * from tsdb.t1) group by a, count_window(4);

-- event_window
select first(ts) as first ,count(ts) from tsdb.t1 group by event_window(status>1, b>4);
select first(ts) as first ,count(ts) from tsdb.t1 group by event_window(status>2, b<3);
select first(ts) as first ,count(ts) from tsdb.t1 group by event_window(status>7, b<10);
select first(ts) as first ,count(ts) from tsdb.t1 group by event_window(status<0, b<1);
select first(ts) as first ,count(ts) from tsdb.t1 group by event_window(status<3, b<4);

select first(ts) as first ,count(ts) from tsdb.t1 group by a, event_window(status>1, b>4);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, event_window(status>1, b>4) order by a;
select first(ts) as first ,count(ts) from tsdb.t1 group by a, event_window(status>2, b<3);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, event_window(status>2, b<3) order by a;
select first(ts) as first ,count(ts) from tsdb.t1 group by a, event_window(status>7, b<10);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, event_window(status>7, b<10) order by a;
select first(ts) as first ,count(ts) from tsdb.t1 group by a, event_window(status<0, b<1);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, event_window(status<0, b<1) order by a;
select first(ts) as first ,count(ts) from tsdb.t1 group by a, event_window(status<3, b<4);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, event_window(status<3, b<4) order by a;

select first(ts) as first ,count(ts) from tsdb.t1 where a=1 group by event_window(status>1, b>4);
select first(ts) as first ,count(ts) from tsdb.t1 where a=1 group by event_window(status>2, b<3);
select first(ts) as first ,count(ts) from tsdb.t1 where a=1 group by event_window(status>7, b<10);
select first(ts) as first ,count(ts) from tsdb.t1 where a=1 group by event_window(status<0, b<1);
select first(ts) as first ,count(ts) from tsdb.t1 where a=1 group by event_window(status<3, b<4);

-- group window function optimizer
select first(ts) as first ,count(ts) from tsdb.t2 group by b, event_window(status>1, b>4);
select first(ts) as first ,count(ts) from tsdb.t2 group by b, event_window(status>1, b>4) order by b desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, event_window(status>1, b>4);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, event_window(status>1, b>4) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, event_window(status>1, b>4);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, event_window(status>1, b>4) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, event_window(status>1, b>4);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, event_window(status>1, b>4) order by a desc;

select first(ts) as first ,count(ts) from tsdb.t2 group by b, event_window(status>2, b<3);
select first(ts) as first ,count(ts) from tsdb.t2 group by b, event_window(status>2, b<3) order by b desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, event_window(status>2, b<3);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, event_window(status>2, b<3) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, event_window(status>2, b<3);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, event_window(status>2, b<3) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, event_window(status>2, b<3);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, event_window(status>2, b<3) order by a desc;

select first(ts) as first ,count(ts) from tsdb.t2 group by b, event_window(status<3, b<4);
select first(ts) as first ,count(ts) from tsdb.t2 group by b, event_window(status<3, b<4) order by b desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, event_window(status<3, b<4);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, c, b, event_window(status<3, b<4) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, event_window(status<3, b<4);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, event_window(status<3, b<4) order by a desc;
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, event_window(status<3, b<4);
select first(ts) as first ,count(ts) from tsdb.t2 group by a, b, event_window(status<3, b<4) order by a desc;

-- error
select first(ts) as first ,count(ts) from tsdb.t1 group by event_window(sum(status)>100, b<10);
select first(ts) as first ,count(ts) from tsdb.t1 group by a, event_window(status>2, b<3) order by event_window(status>2, b<3);
select first(ts) as first ,count(ts) from tsdb.t1 where a = event_window(status>2, b<3) group by a, event_window(status>2, b<3);
select first(ts) as first ,count(ts), event_window(status>2, b<3) from tsdb.t1 group by a, event_window(status>2, b<3);
select first(ts) as first ,count(ts) from tsdb.t1 group by a+b, event_window(status>2, b<3);
with p1 as (select * from t1) select first(ts) as first ,count(ts) from p1 group by a, event_window(status>2, b<3);
select first(ts) as first ,count(ts) from tsdb.t1 group by a,event_window(status>2, b<3) union select first(ts) as first ,count(ts) from tsdb.t2 group by a,event_window(status>2, b<3);
select first(ts) as first ,count(ts) from tsdb.t1 group by a,event_window(status>2, b<3) union select first(ts) as first ,count(ts) from tsdb.t2 group by a;
select first(ts) as first ,count(ts) from tsdb.t1 where ts in (select first(ts) from tsdb.t2 group by event_window(status>2, b<3));
select first(ts) as first ,count(ts) from tsdb.t1 where ts = (select first(ts) from tsdb.t2) group by a, event_window(status>2, b<3);
select first(ts) as first ,count(ts) from (select * from tsdb.t1 group by a, event_window(status>2, b<3));
select a from (select * from tsdb.t1) group by a, event_window(status>2, b<3);

-- time_window
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by time_window(ts, '1mons', '1w');

select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by a, time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by a, time_window(ts, '1w', '1d') order by a;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by a, time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by a, time_window(ts, '1y', '1d') order by a;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by a, time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by a, time_window(ts, '1y', '1w') order by a;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by a, time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by a, time_window(ts, '2y', '1w') order by a;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by a, time_window(ts, '1mons', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 group by a, time_window(ts, '1mons', '1w') order by a;

select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 where a=1 group by time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 where a=1 group by time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 where a=1 group by time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 where a=1 group by time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2) from tsdb.t1 where a=1 group by time_window(ts, '1mons', '1w');

-- time_window
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by time_window(ts, '1mons', '1w');

select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by a, time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by a, time_window(ts, '1w', '1d') order by a;
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by a, time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by a, time_window(ts, '1y', '1d') order by a;
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by a, time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by a, time_window(ts, '1y', '1w') order by a;
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by a, time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by a, time_window(ts, '2y', '1w') order by a;
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by a, time_window(ts, '1mons', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 group by a, time_window(ts, '1mons', '1w') order by a;

select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 where a=1 group by time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 where a=1 group by time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 where a=1 group by time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 where a=1 group by time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, max(b) from tsdb.t1 where a=1 group by time_window(ts, '1mons', '1w');

-- time_window
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by time_window(ts, '1mons', '1w');

select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by a, time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by a, time_window(ts, '1w', '1d') order by a;
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by a, time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by a, time_window(ts, '1y', '1d') order by a;
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by a, time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by a, time_window(ts, '1y', '1w') order by a;
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by a, time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by a, time_window(ts, '2y', '1w') order by a;
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by a, time_window(ts, '1mons', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by a, time_window(ts, '1mons', '1w') order by a;

select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 where a=1 group by time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 where a=1 group by time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 where a=1 group by time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 where a=1 group by time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 where a=1 group by time_window(ts, '1mons', '1w');

-- group window function optimizer
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by b, time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by b, time_window(ts, '1w', '1d') order by b desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, c, b, time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, c, b, time_window(ts, '1w', '1d') order by a desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, time_window(ts, '1w', '1d') order by a desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, b, time_window(ts, '1w', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, b, time_window(ts, '1w', '1d') order by a desc;

select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by b, time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by b, time_window(ts, '1y', '1d') order by b desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, c, b, time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, c, b, time_window(ts, '1y', '1d') order by a desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, time_window(ts, '1y', '1d') order by a desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, b, time_window(ts, '1y', '1d');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, b, time_window(ts, '1y', '1d') order by a desc;

select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by b, time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by b, time_window(ts, '1y', '1w') order by b desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, c, b, time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, c, b, time_window(ts, '1y', '1w') order by a desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, time_window(ts, '1y', '1w') order by a desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, b, time_window(ts, '1y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, b, time_window(ts, '1y', '1w') order by a desc;

select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by b, time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by b, time_window(ts, '2y', '1w') order by b desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, c, b, time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, c, b, time_window(ts, '2y', '1w') order by a desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, time_window(ts, '2y', '1w') order by a desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, b, time_window(ts, '2y', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, b, time_window(ts, '2y', '1w') order by a desc;

select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by b, time_window(ts, '1mons', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by b, time_window(ts, '1mons', '1w') order by b desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, c, b, time_window(ts, '1mons', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, c, b, time_window(ts, '1mons', '1w') order by a desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, time_window(ts, '1mons', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, time_window(ts, '1mons', '1w') order by a desc;
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, b, time_window(ts, '1mons', '1w');
select first(ts) as _wstart,last(ts) as _wend, round(avg(b),2), min(b), max(c) from tsdb.t2 group by a, b, time_window(ts, '1mons', '1w') order by a desc;

-- error
select first(ts) as _wstart,last(ts) as _wend, ts, min(b) from tsdb.t1 group by time_window(ts, '1d', '1w');
select first(ts) as first ,count(ts) from tsdb.t1 group by a, time_window(ts, '1mons', '1w') order by time_window(ts, '1mons', '1w');
select first(ts) as first ,count(ts) from tsdb.t1 where a = time_window(ts, '1mons', '1w') group by a, time_window(ts, '1mons', '1w');
select first(ts) as first ,count(ts), time_window(ts, '1mons', '1w') from tsdb.t1 group by a, time_window(ts, '1mons', '1w');
select first(ts) as first ,count(ts) from tsdb.t1 group by a+b, time_window(ts, '1mons', '1w');
with p1 as (select * from t1) select first(ts) as first ,count(ts) from p1 group by a, time_window(ts, '1mons', '1w');
select first(ts) as first ,count(ts) from tsdb.t1 group by a,time_window(ts, '1mons', '1w') union select first(ts) as first ,count(ts) from tsdb.t2 group by a,time_window(ts, '1mons', '1w');
select first(ts) as first ,count(ts) from tsdb.t1 group by a,time_window(ts, '1mons', '1w') union select first(ts) as first ,count(ts) from tsdb.t2 group by a;
select first(ts) as first ,count(ts) from tsdb.t1 where ts in (select first(ts) from tsdb.t2 group by time_window(ts, '1mons', '1w'));
select first(ts) as first ,count(ts) from tsdb.t1 where ts = (select first(ts) from tsdb.t2) group by a, time_window(ts, '1mons', '1w');
select first(ts) as first ,count(ts) from (select * from tsdb.t1 group by a, time_window(ts, '1mons', '1w'));
select a from (select * from tsdb.t1) group by a, time_window(ts, '1mons', '1w');


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

-- ZDP-51150
CREATE ts DATABASE test_procedure_opt_dxy_ts;
CREATE TABLE test_procedure_opt_dxy_ts.t1(
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
    code1 INT2,code2 INT,code3 INT8,
    code4 FLOAT4 ,code5 FLOAT8,
    code6 BOOL,
    code7 VARCHAR,code8 VARCHAR(128) NOT NULL,
    code9 VARBYTES,code10 VARBYTES(60),
    code11 VARCHAR,code12 VARCHAR(60),
    code13 CHAR(2),code14 CHAR(1023) NOT NULL,
    code15 NCHAR,code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code14,code8,code16);
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(0,1,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(1,2,0,0,0,0,0,true,999999,'          ','          ','          ',' ','          ',' ',' ','          ','          ','          ',' ',' ','          ','          ','          ',0,0,0,0,0,TRUE,'          ',' ',' ','          ','          ','          ','  ','          ',' ','          ');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('1976-10-20 12:00:12.123',3,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('1979-2-28 11:59:01.999',4,20002,20000002,200000000002,-20873209.0220322201,-22012110.113011921,false,123,'test数据库语法查询测试！！！@TEST4-8','test数据库语法查询测试！！！@TEST4-9','test数据库语法查询测试！！！@TEST4-10','t','test数据库语法查询测试！！！@TEST4-12','中','test数据库语法查询测试！！！@TEST4-14','test数据库语法查询测试！！！@TEST4-15','test数据库语法查询测试！TEST4-16xaa','test数据库语法查询测试！！！@TEST4-17','test数据库语法查询测试！！！@TEST4-18',b'\xca','test数据库语法查询测试！！！@TEST4-20','test数据库语法查询测试！！！@TEST4-21','test数据库语法查询测试！！！@TEST4-22',20002,-20000002,200000000002,-20873209.0220322201,22012110.113011921,true,'test数据库语法查询测试！！！@TEST4-7','test数据库语法查询测试！！！@TEST4-8',b'\xbb','test数据库语法查询测试！！！@TEST4-10','test数据库语法查询测试！！！@TEST4-11','test数据库语法查询测试！！！@TEST4-12','t3','test数据库语法查询测试！！！@TEST4-14','中','test数据库语法查询测试！！！@TEST4-16');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(318193261000,5,30003,30000003,300000000003,-33472098.11312001,-39009810.333011921,true,'2015-3-12 10:00:00.234','test数据库语法查询测试！！！@TEST5-8','test数据库语法查询测试！！！@TEST5-9','test数据库语法查询测试！！！@TEST5-10','t','test数据库语法查询测试！！！@TEST5-12','中','test数据库语法查询测试！！！@TEST5-14','test数据库语法查询测试！！！@TEST5-15','test数据库语法查询测试！TEST5-16xaa','test数据库语法查询测试！！！@TEST5-17','test数据库语法查询测试！！！@TEST5-18',b'\xca','test数据库语法查询测试！！！@TEST5-20','test数据库语法查询测试！！！@TEST5-21','test数据库语法查询测试！！！@TEST5-22',-30003,30000003,-300000000003,33472098.11312001,-39009810.333011921,false,'test数据库语法查询测试！！！@TEST5-7','test数据库语法查询测试！！！@TEST5-8',b'\xcc','test数据库语法查询测试！！！@TEST5-10','test数据库语法查询测试！！！@TEST5-11','test数据库语法查询测试！！！@TEST5-12','t3','test数据库语法查询测试！！！@TEST5-14','中','test数据库语法查询测试！！！@TEST5-16');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(318993291090,6,-10001,10000001,-100000000001,1047200.00312001,1109810.113011921,false,'2023-6-23 05:00:00.55','test数据库语法查询测试！！！@TEST6-8','test数据库语法查询测试！！！@TEST6-9','test数据库语法查询测试！！！@TEST6-10','t','test数据库语法查询测试！！！@TEST6-12','中','test数据库语法查询测试！！！@TEST6-14','test数据库语法查询测试！！！@TEST6-15','test数据库语法查询测试！TEST6-16xaa','test数据库语法查询测试！！！@TEST6-17','test数据库语法查询测试！！！@TEST6-18',b'\xca','test数据库语法查询测试！！！@TEST6-20','test数据库语法查询测试！！！@TEST6-21','test数据库语法查询测试！！！@TEST6-22',10001,-10000001,100000000001,424721.022311,4909810.11301191,true,'test数据库语法查询测试！！！@TEST6-7','test数据库语法查询测试！！！@TEST6-8',b'\xdd','test数据库语法查询测试！！！@TEST6-10','test数据库语法查询测试！！！@TEST6-11','test数据库语法查询测试！！！@TEST6-12','t3','test数据库语法查询测试！！！@TEST6-14','中','test数据库语法查询测试！！！@TEST6-16');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(318995291029,7,-20002,20000002,-200000000002,20873209.0220322201,22012110.113011921,true,'2016-7-17 20:12:00.12','test数据库语法查询测试！！！@TEST7-8','test数据库语法查询测试！！！@TEST7-9','test数据库语法查询测试！！！@TEST7-10','t','test数据库语法查询测试！！！@TEST7-12','中','test数据库语法查询测试！！！@TEST7-14','test数据库语法查询测试！！！@TEST7-15','test数据库语法查询测试！TEST7-16xaa','test数据库语法查询测试！！！@TEST7-17','test数据库语法查询测试！！！@TEST7-18',b'\xca','test数据库语法查询测试！！！@TEST7-20','test数据库语法查询测试！！！@TEST7-21','test数据库语法查询测试！！！@TEST7-22',-20002,20000002,-200000000002,555500.0055505,55505532.553015321,false,'test数据库语法查询测试！！！@TEST7-7','test数据库语法查询测试！！！@TEST7-8',b'\xee','test数据库语法查询测试！！！@TEST7-10','test数据库语法查询测试！！！@TEST7-11','test数据库语法查询测试！！！@TEST7-12','t3','test数据库语法查询测试！！！@TEST7-14','中','test数据库语法查询测试！！！@TEST7-16');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(318995302501,8,-30003,30000003,-300000000003,33472098.11312001,39009810.333011921,false,4565476,'test数据库语法查询测试！！！@TEST8-8','test数据库语法查询测试！！！@TEST8-9','test数据库语法查询测试！！！@TEST8-10','t','test数据库语法查询测试！！！@TEST8-12','中','test数据库语法查询测试！！！@TEST8-14','test数据库语法查询测试！！！@TEST8-15','test数据库语法查询测试！TEST8-16xaa','test数据库语法查询测试！！！@TEST8-17','test数据库语法查询测试！！！@TEST8-18',b'\xca','test数据库语法查询测试！！！@TEST8-20','test数据库语法查询测试！！！@TEST8-21','test数据库语法查询测试！！！@TEST8-22',30003,-30000003,300000000003,6900.0012345,6612.1215,true,'test数据库语法查询测试！！！@TEST8-7','test数据库语法查询测试！！！@TEST8-8',b'\xff','test数据库语法查询测试！！！@TEST8-10','test数据库语法查询测试！！！@TEST8-11','test数据库语法查询测试！！！@TEST8-12','t3','test数据库语法查询测试！！！@TEST8-14','中','test数据库语法查询测试！！！@TEST8-16');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('2001-12-9 09:48:12.30',9,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,30000,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('2002-2-22 10:48:12.899',10,32767,-2147483648,9223372036854775807,-99999999991.9999999991,9999999999991.999999999991,true,'2020-10-1 12:00:01.0','test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t','test数据库语法查询测试！！！@TEST10-12','中','test数据库语法查询测试！！！@TEST10-14','test数据库语法查询测试！！！@TEST10-15','test数据库语法查询测试！TEST10-16xaa','test数据库语法查询测试！！！@TEST10-17','test数据库语法查询测试！！！@TEST10-18',b'\xca','test数据库语法查询测试！！！@TEST10-20','test数据库语法查询测试！！！@TEST10-21','test数据库语法查询测试！！！@TEST10-22',1,111,1111111,1472011.12345,1109810.113011921,false,'test数据库语法查询测试！！！@TEST10-7','test数据库语法查询测试！！！@TEST10-8',b'\xcc','test数据库语法查询测试！！！@TEST10-10','test数据库语法查询测试！！！@TEST10-11','test数据库语法查询测试！！！@TEST10-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('2003-10-1 11:48:12.1',11,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',2,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST11-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('2004-9-9 00:00:00.9',12,12000,12000000,120000000000,-12000021.003125,-122209810.1131921,true,'2129-3-1 12:00:00.011','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','t','aaaaaabbbbbbcccccc','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','c','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'aaaaaabbbbbbcccccc','b','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','ty','aaaaaabbbbbbcccccc','u','aaaaaabbbbbbcccccc');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('2004-12-31 12:10:10.911',13,23000,23000000,230000000000,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK',20002,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('2008-2-29 2:10:10.111',14,32767,34000000,340000000000,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('2012-02-29 1:10:10.000',15,-32767,-34000000,-340000000000,43000079.07812032,122209810.1131921,true,'2099-9-1 11:01:00.111','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','1','数据库语法查询测试','2','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','9','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试',10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'数据库语法查询测试','8','7','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','65','数据库语法查询测试','4','数据库语法查询测试');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(1344618710110,16,11111,-11111111,111111111111,-11111.11111,11111111.11111111,false,'2017-12-11 09:10:00.200',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',0,0,0,0,0,false,e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(1374618710110,17,-11111,11111111,-111111111111,11111.11111,-11111111.11111111,true,'2036-2-3 10:10:00.089',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',0,0,0,0,0,false,e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(1574618710110,18,22222,-22222222,222222222222,-22222.22222,22222222.22222222,false,'2012-1-1 12:12:00.049' ,e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',0,0,0,0,0,false,e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(1874618710110,19,-22222,22222222,-222222222222,22222.22222,-22222222.22222222,true,'1980-6-27 19:17:00.123','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,0,0,0,0,0,false,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\ ' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(9223372036000,20,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES(-62167219200000,21,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('1900-1-1 12:12:12',22,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('0000-1-1 00:00:00',23,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');
INSERT INTO test_procedure_opt_dxy_ts.t1 VALUES('2970-1-1 00:00:00',24,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');

drop procedure test_procedure_opt_dxy_ts.pro1;
CREATE PROCEDURE test_procedure_opt_dxy_ts.pro1() $$BEGIN SET @a=0; SELECT max(e1) FROM test_procedure_opt_dxy_ts.t1 GROUP BY code1, event_window(e1>2,code2<10000) HAVING max(e1)>code1 ORDER BY code1; END $$;
call test_procedure_opt_dxy_ts.pro1();

drop procedure test_procedure_opt_dxy_ts.pro2;
CREATE PROCEDURE test_procedure_opt_dxy_ts.pro2() $$BEGIN SET @a=0;SET @b='\x64787968636b78'; SELECT max(code1) FROM test_procedure_opt_dxy_ts.t1 GROUP BY @b; SELECT max(code1) FROM test_procedure_opt_dxy_ts.t1 GROUP BY state_window(e1); END $$;

drop procedure test_procedure_opt_dxy_ts.pro3;
CREATE PROCEDURE test_procedure_opt_dxy_ts.pro3() $$BEGIN SET @a=0;SET @b='\x64787968636b78'; SELECT max(code1) FROM test_procedure_opt_dxy_ts.t1 GROUP BY state_window(e1); SELECT max(code1) FROM test_procedure_opt_dxy_ts.t1 GROUP BY @b; END $$;
drop database test_procedure_opt_dxy_ts cascade;

 -- group window expand
create ts database tsdb;
use tsdb;
create table t1(ts timestamp not null, status int, b int) tags (a int not null) primary tags(a);
Insert into t1 values('2025-01-09 06:14:47.298+00:00', 1, 1, 1);
Insert into t1 values('2025-01-10 06:14:48.298+00:00', 1, 1, 1);
Insert into t1 values('2025-01-11 06:14:47.298+00:00', 1, 1, 1);
Insert into t1 values('2025-01-12 06:14:47.298+00:00', 1, 2, 1);
Insert into t1 values('2025-01-15 06:14:47.298+00:00', 2, 3, 1);
Insert into t1 values('2025-01-16 06:14:47.298+00:00', 2, 3, 1);
Insert into t1 values('2025-01-17 06:14:47.298+00:00', 2, 4, 1);
Insert into t1 values('2025-01-20 06:14:47.298+00:00', 3, 5, 1);
Insert into t1 values('2025-01-21 06:14:47.298+00:00', 3, 5, 1);
Insert into t1 values('2025-01-22 06:14:47.298+00:00', 3, 5, 1);
Insert into t1 values('2025-01-28 06:14:47.298+00:00', 1, 6, 1);
Insert into t1 values('2025-01-29 06:14:47.298+00:00', 1, 6, 1);

Insert into t1 values('2025-01-30 06:14:47.298+00:00', 1, 6, 2);
Insert into t1 values('2025-01-31 06:14:47.298+00:00', 1, 7, 2);

-- state_window
select first(ts) as first ,count(ts) from tsdb.t1 group by b, state_window(status);
select first(ts) as first ,count(ts) from tsdb.t1 group by b,state_window(case when status=1 then 100 else 200 end);
select first(ts) as first ,count(ts) from tsdb.t1 group by b,a,state_window(status) order by a;
select first(ts) as first ,count(ts), b from tsdb.t1 group by a, b, state_window(status) order by b desc;
-- error
select first(ts) as first ,count(ts) from tsdb.t1 group by b,state_window(case when status=1 then 100.0 else 200.0 end);

-- session_window
select first(ts) as first ,count(ts) from tsdb.t1 group by b,session_window(ts, '1s');
select first(ts) as first ,count(ts) from tsdb.t1 group by b,session_window(ts, '1d');
select first(ts) as first ,count(ts) from tsdb.t1 group by b,session_window(ts, '3day');
select first(ts) as first ,count(ts) from tsdb.t1 group by b, a, session_window(ts, '1s') order by a;
select first(ts) as first ,count(ts), b from tsdb.t1 group by  a,b, session_window(ts, '1s') order by b desc;
-- error
select first(ts) as first ,count(ts) from tsdb.t1 group by b,session_window(ts, '3dayss');
select first(ts) as first ,count(ts) from tsdb.t1 group by b,session_window(ts, '-3days');

-- count_window
select first(ts) as first ,count(ts) from tsdb.t1 group by b,count_window(12);
select first(ts) as first ,count(ts) from tsdb.t1 group by b,count_window(12,1);
select first(ts) as first ,count(ts) from tsdb.t1 group by b,count_window(12,6);
select first(ts) as first ,count(ts) from tsdb.t1 group by b,count_window(4,3);
select first(ts) as first ,count(ts) from tsdb.t1 group by b,count_window(4,4);
select first(ts) as first ,count(ts) from tsdb.t1 group by b,count_window(4);
select first(ts) as first ,count(ts) from tsdb.t1 group by b,count_window(2,1);
select first(ts) as first ,count(ts), b from tsdb.t1 group by a, b,count_window(2,1) order by b desc, first;
-- error
select first(ts) as first ,count(ts) from tsdb.t1 group by b,count_window(-1,-1);
select first(ts) as first ,count(ts) from tsdb.t1 group by b,count_window(0,0);
select first(ts) as first ,count(ts) from tsdb.t1 group by b,count_window(12,0);
select first(ts) as first ,count(ts) from tsdb.t1 group by b,count_window(12,'ds');

drop database tsdb cascade;

use defaultdb;drop database if exists t_state_w cascade;
create ts database t_state_w;
create table t_state_w.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);
INSERT INTO t_state_w.tb VALUES ('2023-10-01 10:00:00', '2023-10-01 10:00:00', 1, 1, 1, 1.1, 1.1, TRUE, 'A', 'Test1', 'A', 'NChar1', 'Desc1', 'Varchar254', 'Varchar4096', 'Nvarchar', 'Nvarchar255', 'Nvarchar4096', 'Varbytes', 'Varbytes100', 'Varbytes', 'Varbytes254', 'Varbytes4096', 1, 1, 1, TRUE, 1.1, 1.1, 'A', 'Tag8_1', 'A', 'Nchar254', 'Varchar', 'Tag12_1', 'Varbytes', 'Varbytes100', 'Varbytes', 'Varbytes255'),('2023-10-01 10:01:00', '2023-10-01 10:00:00', 1, 1, 1, 1.1, 1.1, TRUE, 'A', 'Test1', 'A', 'NChar1', 'Desc1', 'Varchar254', 'Varchar4096', 'Nvarchar', 'Nvarchar255', 'Nvarchar4096', 'Varbytes', 'Varbytes100', 'Varbytes', 'Varbytes254', 'Varbytes4096', 1, 1, 1, TRUE, 1.1, 1.1, 'A', 'Tag8_1', 'A', 'Nchar254', 'Varchar', 'Tag12_1', 'Varbytes', 'Varbytes100', 'Varbytes', 'Varbytes255'),('2023-10-01 10:05:00', '2023-10-01 10:05:00', 2, 2, 2, 2.2, 2.2, FALSE, 'B', 'Test2', 'B', 'NChar2', 'Desc2', 'Varchar254_2', 'Varchar4096_2', 'Nvarchar2', 'Nvarchar255_2', 'Nvarchar4096_2', 'Varbytes2', 'Varbytes100_2', 'Varbytes2', 'Varbytes254_2', 'Varbytes4096_2', 1, 2, 2, TRUE, 2.2, 2.2, 'B', 'Tag8_1', 'B', 'Nchar254_2', 'Varchar2', 'Tag12_1', 'Varbytes2', 'Varbytes100_2', 'Varbytes2', 'Varbytes255_2'),('2023-10-02 10:05:00', '2023-10-01 10:05:00', 2, 2, 2, 2.2, 2.2, FALSE, 'B', 'Test2', 'B', 'NChar2', 'Desc2', 'Varchar254_2', 'Varchar4096_2', 'Nvarchar2', 'Nvarchar255_2', 'Nvarchar4096_2', 'Varbytes2', 'Varbytes100_2', 'Varbytes2', 'Varbytes254_2', 'Varbytes4096_2', 1, 2, 2, TRUE, 2.2, 2.2, 'B', 'Tag8_1', 'B', 'Nchar254_2', 'Varchar2', 'Tag12_1', 'Varbytes2', 'Varbytes100_2', 'Varbytes2', 'Varbytes255_2'),('2023-10-01 10:10:00', '2023-10-01 10:10:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, TRUE, NULL, NULL, NULL, 'Tag8_1', NULL, NULL, NULL, 'Tag12_1', NULL, NULL, NULL, NULL);
INSERT INTO t_state_w.tb VALUES ('2023-10-01 10:15:00', '2023-10-01 10:15:00', 3, 3, 3, 3.3, 3.3, TRUE, 'C', 'Test3', 'C', 'NChar3', 'Desc3', 'Varchar254_3', 'Varchar4096_3', 'Nvarchar3', 'Nvarchar255_3', 'Nvarchar4096_3', 'Varbytes3', 'Varbytes100_3', 'Varbytes3', 'Varbytes254_3', 'Varbytes4096_3', 2, 3, 3, FALSE, 3.3, 3.3, 'C', 'Tag8_2', 'C', 'Nchar254_3', 'Varchar3', 'Tag12_2', 'Varbytes3', 'Varbytes100_3', 'Varbytes3', 'Varbytes255_3'),('2023-10-01 10:16:00', '2023-10-01 10:15:00', 3, 3, 3, 3.3, 3.3, TRUE, 'C', 'Test3', 'C', 'NChar3', 'Desc3', 'Varchar254_3', 'Varchar4096_3', 'Nvarchar3', 'Nvarchar255_3', 'Nvarchar4096_3', 'Varbytes3', 'Varbytes100_3', 'Varbytes3', 'Varbytes254_3', 'Varbytes4096_3', 2, 3, 3, FALSE, 3.3, 3.3, 'C', 'Tag8_2', 'C', 'Nchar254_3', 'Varchar3', 'Tag12_2', 'Varbytes3', 'Varbytes100_3', 'Varbytes3', 'Varbytes255_3'),('2023-10-01 10:20:00', '2023-10-01 10:20:00', 4, 4, 4, 4.4, 4.4, FALSE, 'D', 'Test4', 'D', 'NChar4', 'Desc4', 'Varchar254_4', 'Varchar4096_4', 'Nvarchar4', 'Nvarchar255_4', 'Nvarchar4096_4', 'Varbytes4', 'Varbytes100_4', 'Varbytes4', 'Varbytes254_4', 'Varbytes4096_4', 2, 4, 4, FALSE, 4.4, 4.4, 'D', 'Tag8_2', 'D', 'Nchar254_4', 'Varchar4', 'Tag12_2', 'Varbytes4', 'Varbytes100_4', 'Varbytes4', 'Varbytes255_4'),('2023-10-02 10:20:00', '2023-10-01 10:20:00', 4, 4, 4, 4.4, 4.4, FALSE, 'D', 'Test4', 'D', 'NChar4', 'Desc4', 'Varchar254_4', 'Varchar4096_4', 'Nvarchar4', 'Nvarchar255_4', 'Nvarchar4096_4', 'Varbytes4', 'Varbytes100_4', 'Varbytes4', 'Varbytes254_4', 'Varbytes4096_4', 2, 4, 4, FALSE, 4.4, 4.4, 'D', 'Tag8_2', 'D', 'Nchar254_4', 'Varchar4', 'Tag12_2', 'Varbytes4', 'Varbytes100_4', 'Varbytes4', 'Varbytes255_4'),('2023-10-01 10:25:00', '2023-10-01 10:25:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 2, NULL, NULL, FALSE, NULL, NULL, NULL, 'Tag8_2', NULL, NULL, NULL, 'Tag12_2', NULL, NULL, NULL, NULL);
INSERT INTO t_state_w.tb VALUES ('2023-10-01 10:30:00', '2023-10-01 10:30:00', 5, 5, 5, 5.5, 5.5, TRUE, 'E', 'Test5', 'E', 'NChar5', 'Desc5', 'Varchar254_5', 'Varchar4096_5', 'Nvarchar5', 'Nvarchar255_5', 'Nvarchar4096_5', 'Varbytes5', 'Varbytes100_5', 'Varbytes5', 'Varbytes254_5', 'Varbytes4096_5', 3,5, 5, FALSE, 5.5, 5.5, 'E', 'Tag8_3', 'E', 'Nchar254_5', 'Varchar5', 'Tag12_3', 'Varbytes5', 'Varbytes100_5', 'Varbytes5', 'Varbytes255_5'),('2023-10-01 10:31:00', '2023-10-01 10:30:00', 5, 5, 5, 5.5, 5.5, TRUE, 'E', 'Test5', 'E', 'NChar5', 'Desc5', 'Varchar254_5', 'Varchar4096_5', 'Nvarchar5', 'Nvarchar255_5', 'Nvarchar4096_5', 'Varbytes5', 'Varbytes100_5', 'Varbytes5', 'Varbytes254_5', 'Varbytes4096_5', 3,5, 5, FALSE, 5.5, 5.5, 'E', 'Tag8_3', 'E', 'Nchar254_5', 'Varchar5', 'Tag12_3', 'Varbytes5', 'Varbytes100_5', 'Varbytes5', 'Varbytes255_5'),('2023-10-01 10:35:00', '2023-10-01 10:35:00', 6, 6, 6, 6.6, 6.6, FALSE, 'F', 'Test6', 'F', 'NChar6', 'Desc6', 'Varchar254_6', 'Varchar4096_6', 'Nvarchar6', 'Nvarchar255_6', 'Nvarchar4096_6', 'Varbytes6', 'Varbytes100_6', 'Varbytes6', 'Varbytes254_6', 'Varbytes4096_6', 3,6, 6, FALSE, 6.6, 6.6, 'F', 'Tag8_3', 'F', 'Nchar254_6', 'Varchar6', 'Tag12_3', 'Varbytes6', 'Varbytes100_6', 'Varbytes6', 'Varbytes255_6'),('2023-10-02 10:35:00', '2023-10-01 10:35:00', 6, 6, 6, 6.6, 6.6, FALSE, 'F', 'Test6', 'F', 'NChar6', 'Desc6', 'Varchar254_6', 'Varchar4096_6', 'Nvarchar6', 'Nvarchar255_6', 'Nvarchar4096_6', 'Varbytes6', 'Varbytes100_6', 'Varbytes6', 'Varbytes254_6', 'Varbytes4096_6', 3,6, 6, FALSE, 6.6, 6.6, 'F', 'Tag8_3', 'F', 'Nchar254_6', 'Varchar6', 'Tag12_3', 'Varbytes6', 'Varbytes100_6', 'Varbytes6', 'Varbytes255_6'),('2023-10-01 10:40:00', '2023-10-01 10:40:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 3, NULL, NULL, TRUE, NULL, NULL, NULL, 'Tag8_3', NULL, NULL, NULL, 'Tag12_3', NULL, NULL, NULL, NULL);
INSERT INTO t_state_w.tb VALUES ('2023-10-01 10:45:00', '2023-10-01 10:45:00', 7, 7, 7, 7.7, 7.7, TRUE, 'G', 'Test7', 'G', 'NChar7', 'Desc7', 'Varchar254_7', 'Varchar4096_7', 'Nvarchar7', 'Nvarchar255_7', 'Nvarchar4096_7', 'Varbytes7', 'Varbytes100_7', 'Varbytes7', 'Varbytes254_7', 'Varbytes4096_7', 4, 7, 7, TRUE, 7.7, 7.7, 'G', 'Tag8_4', 'G', 'Nchar254_7', 'Varchar7', 'Tag12_4', 'Varbytes7', 'Varbytes100_7', 'Varbytes7', 'Varbytes255_7'),('2023-10-01 10:46:00', '2023-10-01 10:45:00', 7, 7, 7, 7.7, 7.7, TRUE, 'G', 'Test7', 'G', 'NChar7', 'Desc7', 'Varchar254_7', 'Varchar4096_7', 'Nvarchar7', 'Nvarchar255_7', 'Nvarchar4096_7', 'Varbytes7', 'Varbytes100_7', 'Varbytes7', 'Varbytes254_7', 'Varbytes4096_7', 4, 7, 7, TRUE, 7.7, 7.7, 'G', 'Tag8_4', 'G', 'Nchar254_7', 'Varchar7', 'Tag12_4', 'Varbytes7', 'Varbytes100_7', 'Varbytes7', 'Varbytes255_7'),('2023-10-01 10:50:00', '2023-10-01 10:50:00', 8, 8, 8, 8.8, 8.8, FALSE, 'H', 'Test8', 'H', 'NChar8', 'Desc8', 'Varchar254_8', 'Varchar4096_8', 'Nvarchar8', 'Nvarchar255_8', 'Nvarchar4096_8', 'Varbytes8', 'Varbytes100_8', 'Varbytes8', 'Varbytes254_8', 'Varbytes4096_8', 4, 8, 8, TRUE, 8.8, 8.8, 'H', 'Tag8_4', 'H', 'Nchar254_8', 'Varchar8', 'Tag12_4', 'Varbytes8', 'Varbytes100_8', 'Varbytes8', 'Varbytes255_8'),('2023-10-02 10:50:00', '2023-10-01 10:50:00', 8, 8, 8, 8.8, 8.8, FALSE, 'H', 'Test8', 'H', 'NChar8', 'Desc8', 'Varchar254_8', 'Varchar4096_8', 'Nvarchar8', 'Nvarchar255_8', 'Nvarchar4096_8', 'Varbytes8', 'Varbytes100_8', 'Varbytes8', 'Varbytes254_8', 'Varbytes4096_8', 4, 8, 8, TRUE, 8.8, 8.8, 'H', 'Tag8_4', 'H', 'Nchar254_8', 'Varchar8', 'Tag12_4', 'Varbytes8', 'Varbytes100_8', 'Varbytes8', 'Varbytes255_8'),('2023-10-01 10:55:00', '2023-10-01 10:55:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 4, NULL, NULL, FALSE, NULL, NULL, NULL, 'Tag8_4', NULL, NULL, NULL, 'Tag12_4', NULL, NULL, NULL, NULL);
INSERT INTO t_state_w.tb VALUES ('2023-10-01 11:00:00', '2023-10-01 11:00:00', 9, 9, 9, 9.9, 9.9, TRUE, 'I', 'Test9', 'I', 'NChar9', 'Desc9', 'Varchar254_9', 'Varchar4096_9', 'Nvarchar9', 'Nvarchar255_9', 'Nvarchar4096_9', 'Varbytes9', 'Varbytes100_9', 'Varbytes9', 'Varbytes254_9', 'Varbytes4096_9', 5, 9, 9, FALSE, 9.9, 9.9, 'I', 'Tag8_5', 'I', 'Nchar254_9', 'Varchar9', 'Tag12_5', 'Varbytes9', 'Varbytes100_9', 'Varbytes9', 'Varbytes255_9'),('2023-10-01 11:01:00', '2023-10-01 11:00:00', 9, 9, 9, 9.9, 9.9, TRUE, 'I', 'Test9', 'I', 'NChar9', 'Desc9', 'Varchar254_9', 'Varchar4096_9', 'Nvarchar9', 'Nvarchar255_9', 'Nvarchar4096_9', 'Varbytes9', 'Varbytes100_9', 'Varbytes9', 'Varbytes254_9', 'Varbytes4096_9', 5, 9, 9, FALSE, 9.9, 9.9, 'I', 'Tag8_5', 'I', 'Nchar254_9', 'Varchar9', 'Tag12_5', 'Varbytes9', 'Varbytes100_9', 'Varbytes9', 'Varbytes255_9'),('2023-10-01 11:05:00', '2023-10-01 11:05:00', 10, 10, 10, 10.10, 10.10, FALSE, 'J', 'Test10', 'J', 'NChar10', 'Desc10', 'Varchar254_10', 'Varchar4096_10', 'Nvarchar10', 'Nvarchar255_10', 'Nvarchar4096_10', 'Varbytes10', 'Varbytes100_10', 'Varbytes10', 'Varbytes254_10', 'Varbytes4096_10', 5, 10, 10, FALSE, 10.10, 10.10, 'J', 'Tag8_5', 'J', 'Nchar254_10', 'Varchar10', 'Tag12_5', 'Varbytes10', 'Varbytes100_10', 'Varbytes10', 'Varbytes255_10'),('2023-10-02 11:05:00', '2023-10-01 11:05:00', 10, 10, 10, 10.10, 10.10, FALSE, 'J', 'Test10', 'J', 'NChar10', 'Desc10', 'Varchar254_10', 'Varchar4096_10', 'Nvarchar10', 'Nvarchar255_10', 'Nvarchar4096_10', 'Varbytes10', 'Varbytes100_10', 'Varbytes10', 'Varbytes254_10', 'Varbytes4096_10', 5, 10, 10, FALSE, 10.10, 10.10, 'J', 'Tag8_5', 'J', 'Nchar254_10', 'Varchar10', 'Tag12_5', 'Varbytes10', 'Varbytes100_10', 'Varbytes10', 'Varbytes255_10'),('2023-10-01 11:10:00', '2023-10-01 11:10:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 5, NULL, NULL, TRUE, NULL, NULL, NULL, 'Tag8_5', NULL, NULL, NULL, 'Tag12_5', NULL, NULL, NULL, NULL);
SELECT COUNT(*) FROM t_state_w.tb;
-- handle null status
SELECT first(k_timestamp) AS first_ts, last(k_timestamp) AS last_ts, count(*) AS record_count, first(e3) AS first_e3 FROM t_state_w.tb GROUP BY t1,t8,t12,state_window(e3) ORDER BY t1;
drop database t_state_w cascade;

use defaultdb;drop database if exists t_state_w cascade;
create ts database t_state_w;
create table t_state_w.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);
INSERT INTO t_state_w.tb VALUES ('2023-10-01 10:00:00', '2023-10-01 10:00:00', 1, 1, 1, 1.1, 1.1, TRUE, 'A', 'Test1', 'A', 'NChar1', 'Desc1', 'Varchar254', 'Varchar4096', 'Nvarchar', 'Nvarchar255', 'Nvarchar4096', 'Varbytes', 'Varbytes100', 'Varbytes', 'Varbytes254', 'Varbytes4096', 1, 1, 1, TRUE, 1.1, 1.1, 'A', 'Tag8', 'A', 'Nchar254', 'Varchar', 'Tag12', 'Varbytes', 'Varbytes100', 'Varbytes', 'Varbytes255');
INSERT INTO t_state_w.tb VALUES ('2023-10-01 10:05:00', '2023-10-01 10:05:00', 2, 2, 2, 2.2, 2.2, FALSE, 'B', 'Test2', 'B', 'NChar2', 'Desc2', 'Varchar254_2', 'Varchar4096_2', 'Nvarchar2', 'Nvarchar255_2', 'Nvarchar4096_2', 'Varbytes2', 'Varbytes100_2', 'Varbytes2', 'Varbytes254_2', 'Varbytes4096_2', 2, 2, 2, FALSE, 2.2, 2.2, 'B', 'Tag8_2', 'B', 'Nchar254_2', 'Varchar2', 'Tag12_2', 'Varbytes2', 'Varbytes100_2', 'Varbytes2', 'Varbytes255_2');
INSERT INTO t_state_w.tb VALUES ('2023-10-01 10:10:00', '2023-10-01 10:10:00', 3, 3, 3, 3.3, 3.3, TRUE, 'C', 'Test3', 'C', 'NChar3', 'Desc3', 'Varchar254_3', 'Varchar4096_3', 'Nvarchar3', 'Nvarchar255_3', 'Nvarchar4096_3', 'Varbytes3', 'Varbytes100_3', 'Varbytes3', 'Varbytes254_3', 'Varbytes4096_3', 3, 3, 3, TRUE, 3.3, 3.3, 'C', 'Tag8_3', 'C', 'Nchar254_3', 'Varchar3', 'Tag12_3', 'Varbytes3', 'Varbytes100_3', 'Varbytes3', 'Varbytes255_3');
INSERT INTO t_state_w.tb VALUES ('2023-10-01 10:15:00', '2023-10-01 10:15:00', 4, 4, 4, 4.4, 4.4, FALSE, 'D', 'Test4', 'D', 'NChar4', 'Desc4', 'Varchar254_4', 'Varchar4096_4', 'Nvarchar4', 'Nvarchar255_4', 'Nvarchar4096_4', 'Varbytes4', 'Varbytes100_4', 'Varbytes4', 'Varbytes254_4', 'Varbytes4096_4', 4, 4, 4, FALSE, 4.4, 4.4, 'D', 'Tag8_4', 'D', 'Nchar254_4', 'Varchar4', 'Tag12_4', 'Varbytes4', 'Varbytes100_4', 'Varbytes4', 'Varbytes255_4');
SELECT first(k_timestamp) AS start_time, last(k_timestamp) AS end_time, count(*) AS record_count, last(e1) AS last_e1, last(e2) AS last_e2, last(e3) AS last_e3, last(e4) AS last_e4, last(e5) AS last_e5, last(e6) AS last_e6, last(e7) AS last_e7, last(e8) AS last_e8, last(e9) AS last_e9, last(e10) AS last_e10, last(e11) AS last_e11, last(e12) AS last_e12, last(e13) AS last_e13, last(e14) AS last_e14, last(e15) AS last_e15, last(e16) AS last_e16, last(e17) AS last_e17, last(e18) AS last_e18, last(e19) AS last_e19, last(e20) AS last_e20, last(e21) AS last_e21, last(e22) AS last_e22, last(t1) AS last_t1, last(t2) AS last_t2, last(t3) AS last_t3, last(t4) AS last_t4, last(t5) AS last_t5, last(t6) AS last_t6, last(t7) AS last_t7, last(t8) AS last_t8, last(t9) AS last_t9, last(t10) AS last_t10, last(t11) AS last_t11, last(t12) AS last_t12, last(t13) AS last_t13, last(t14) AS last_t14, last(t15) AS last_t15, last(t16) AS last_t16 FROM t_state_w.tb GROUP BY state_window(case when e8 = 'B' then 1 when e8 = 'D' then 0 end);
drop database t_state_w cascade;