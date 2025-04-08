drop database if exists tsdb cascade;
drop database if exists redb cascade;
set cluster setting ts.sql.query_opt_mode=1110;
CREATE TS DATABASE tsdb;
CREATE TABLE tsdb.ts1 (
                          ts TIMESTAMPTZ NOT NULL,
                          e1 int,
                          e2 int,
                          e3 int
) TAGS (
    tag1 INT NOT NULL,
    tag2 int,
    tag3 int
) PRIMARY TAGS(tag1);
insert into tsdb.ts1 values('2024-08-27 11:00:00',1,2,3,10,20,30);

CREATE DATABASE redb;
CREATE TABLE redb.re1 (
                          e1 int,
                          e2 int,
                          e3 int
);
insert into redb.re1 values(10, 20, 30);


select * from tsdb.ts1 order by e1;
select * from redb.re1 order by e1;
select * from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 order by r.e1;

-- optimization is applicable
-- 1. non group by

-- 1.1 ts column

-- 1.1.1 non project
select max(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select max(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select min(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select min(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select count(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select count(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select sum(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select sum(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select avg(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select avg(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select count(*) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select count(*) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

-- 1.1.2 with project
select max(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select max(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select min(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select min(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select count(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select count(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select sum(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select sum(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select avg(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select avg(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;


-- 1.2 relation column

-- 1.2.1 non project
select max(r.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select max(r.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select min(r.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select min(r.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

-- 1.2.2 with project
select max(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select max(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select min(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select min(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;





-- 2. with group by

-- 1.1 ts column

-- 1.1.1 non project
select max(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;
explain select max(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;

select min(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;
explain select min(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;

select count(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1;
explain select count(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1;

select sum(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1;
explain select sum(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1;

select avg(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;
explain select avg(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;

select count(*) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;
explain select count(*) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;

-- 1.1.2 with project
select max(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;
explain select max(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;

select min(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;
explain select min(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;

select count(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1;
explain select count(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1;

select sum(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;
explain select sum(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;

select avg(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;
explain select avg(ts.e1+ts.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;


-- 1.2 relation column

-- 1.2.1 non project
select max(r.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;
explain select max(r.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;

select min(r.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;
explain select min(r.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;

-- 1.2.2 with project
select max(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;
explain select max(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;

select min(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;
explain select min(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by ts.tag1, r.e1;

-- 1.3 join
select max(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 and ts.tag2=r.e2 and ts.tag3=r.e3 group by ts.tag1, r.e1;
explain select max(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 and ts.tag2=r.e2 and ts.tag3=r.e3 group by ts.tag1, r.e1;

select min(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 and ts.tag2=r.e2 and ts.tag3=r.e3;
explain select min(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 and ts.tag2=r.e2 and ts.tag3=r.e3;

select min(ts.e1+ts.e2) from redb.re1 r join (select * from tsdb.ts1 where tsdb.ts1.e1 > 0) ts on ts.tag1=r.e1 and ts.tag2=r.e2 group by r.e1;
explain select min(ts.e1+ts.e2) from redb.re1 r join (select * from tsdb.ts1 where tsdb.ts1.e1 > 0) ts on ts.tag1=r.e1 and ts.tag2=r.e2 group by r.e1;

select min(ts.e1+ts.e2) from redb.re1 r1 join redb.re1 r2 join (select * from tsdb.ts1 where tsdb.ts1.e1 > 0) ts on ts.tag1=r2.e1 and ts.tag2=r2.e2 on ts.tag1=r1.e1 and ts.tag2=r1.e2 group by r1.e1;
explain select min(ts.e1+ts.e2) from redb.re1 r1 join redb.re1 r2 join (select * from tsdb.ts1 where tsdb.ts1.e1 > 0) ts on ts.tag1=r2.e1 and ts.tag2=r2.e2 on ts.tag1=r1.e1 and ts.tag2=r1.e2 group by r1.e1;

-- time_bucket
set cluster setting ts.sql.query_opt_mode= 1111;
select count(ts.e1), time_bucket(ts.ts, '10s') as tb from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by tb;
explain select count(ts.e1), time_bucket(ts.ts, '10s') as tb from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by tb;


set cluster setting ts.sql.query_opt_mode= 1110;

-- optimization is not applicable

--- count/sum/avg(relation column)
select count(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select count(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select sum(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select sum(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select avg(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;
explain select avg(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1;

select count(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;
explain select count(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;

select sum(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;
explain select sum(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;

select avg(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;
explain select avg(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 group by r.e1;

-- without on
select max(ts.e1) from tsdb.ts1 ts, redb.re1 r;
explain select max(ts.e1) from tsdb.ts1 ts, redb.re1 r;

-- on is not applicable
select max(ts.e1) from tsdb.ts1 ts join redb.re1 r on ts.e1=r.e1;
explain select max(ts.e1) from tsdb.ts1 ts join redb.re1 r on ts.e1=r.e1;

-- join with e1=e2+e3
select max(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1+r.e2-10;
explain select max(ts.e1) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1+r.e2-10;

-- multi timeseries table
select max(ts2.e1) from tsdb.ts1 ts2 join tsdb.ts1 ts3 on ts2.tag1=10 join redb.re1 r on ts2.tag1=r.e1;
explain select max(ts2.e1) from tsdb.ts1 ts2 join tsdb.ts1 ts3 on ts2.tag1=10 join redb.re1 r on ts2.tag1=r.e1;

--- join with non-and
select max(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 and ts.tag2=r.e2 or ts.tag3=r.e3 group by ts.tag1, r.e1;
explain select max(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 and ts.tag2=r.e2 or ts.tag3=r.e3 group by ts.tag1, r.e1;

select min(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 and ts.tag2=r.e2 or ts.tag3=r.e3;
explain select min(r.e1+r.e2) from tsdb.ts1 ts, redb.re1 r where ts.tag1=r.e1 and ts.tag2=r.e2 or ts.tag3=r.e3;

select min(ts.e1+ts.e2) from redb.re1 r join (select * from tsdb.ts1 where tsdb.ts1.e1 is not null) ts on ts.tag1=r.e1 and ts.tag2=r.e2 group by r.e1;
explain select min(ts.e1+ts.e2) from redb.re1 r join (select * from tsdb.ts1 where tsdb.ts1.e1 is not null) ts on ts.tag1=r.e1 and ts.tag2=r.e2 group by r.e1;

select min(ts.e1+ts.e2) from redb.re1 r1 join redb.re1 r2 join (select * from tsdb.ts1 where tsdb.ts1.e1 is not null) ts on ts.tag2=r2.e2 on ts.tag1=r1.e1 group by r1.e1;
explain select min(ts.e1+ts.e2) from redb.re1 r1 join redb.re1 r2 join (select * from tsdb.ts1 where tsdb.ts1.e1 is not null) ts on ts.tag2=r2.e2 on ts.tag1=r1.e1 group by r1.e1;

select min(ts.e1+ts.e2) from redb.re1 r1 join redb.re1 r2 join (select * from tsdb.ts1 where tsdb.ts1.e1 > 0) ts on ts.tag1=r2.e1 or ts.tag2=r2.e2 on ts.tag1=r1.e1 or ts.tag2=r1.e2 group by r1.e1;
explain select min(ts.e1+ts.e2) from redb.re1 r1 join redb.re1 r2 join (select * from tsdb.ts1 where tsdb.ts1.e1 > 0) ts on ts.tag1=r2.e1 or ts.tag2=r2.e2 on ts.tag1=r1.e1 or ts.tag2=r1.e2 group by r1.e1;
set cluster setting ts.sql.query_opt_mode=11110;
drop database if exists tsdb cascade;
drop database if exists redb cascade;
