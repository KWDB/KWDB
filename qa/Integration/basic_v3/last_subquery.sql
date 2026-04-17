drop database d1 cascade;
create ts database d1;
use d1;
create table t1(ts timestamptz not null, e1 int, e2 timestamptz) tags(tag1 int not null, tag2 int) primary tags(tag1);
insert into t1 values ('2026-1-1', 1, '2026-1-31', 10,     20);
insert into t1 values ('2026-1-2', 2, '2026-1-30', 100,    200);
insert into t1 values ('2026-1-3', 3, '2026-1-29', 1000,   2000);
insert into t1 values ('2026-1-4', 4, '2026-1-28', 10000,  20000);
insert into t1 values ('2026-1-5', 5, '2026-1-27', 100000, 200000);

create table t2(ts timestamptz not null, e1 int, e2 timestamptz) tags(tag1 int not null, tag2 int) primary tags(tag1);
insert into t2 select * from t1;

drop database d2 cascade;
create database d2;
use d2;
create table t1 (e1 int, e2 timestamptz);
insert into t1 values (1, '2026-1-1');
insert into t1 values (2, '2026-1-2');
insert into t1 values (3, '2026-1-3');

create view v1 as select t1.e1, t1.e2 from t1;

SELECT last(e1,'2020-1-1 00:00:00') FROM t1;
SELECT first(e1,'2020-1-1 00:00:00') FROM t1;
SELECT last(new_e1,'2020-1-1 00:00:00') FROM (SELECT e1 as new_e1 FROM t1);


use d1;
select last(e1) from t1 ;
select last(e1, '2026-1-3') from t1;
select last(e1, e2) from t1 where e1 is not null;
select last(e1, e2-1s) from t1 where e1 is not null;
select last(*, e2) from t1 where e1 is not null;

select lastts(e1) from t1;
select lastts(e1, '2026-1-3') from t1;
select lastts(e1, e2) from t1 where e1 is not null;
select lastts(e1, e2-1s) from t1 where e1 is not null;
select lastts(*, e2) from t1 where e1 is not null;

select last_row(e1) from t1;
select last_row(e1, '2026-1-3') from t1;
select last_row(e1, e2) from t1 where e1 is not null;
select last_row(e1, e2-1s) from t1 where e1 is not null;
select last_row(*, e2) from t1 where e1 is not null;

select last_row_ts(e1) from t1;
select last_row_ts(e1, '2026-1-3') from t1;
select last_row_ts(e1, e2) from t1 where e1 is not null;
select last_row_ts(e1, e2-1s) from t1 where e1 is not null;
select last_row_ts(*, e2) from t1 where e1 is not null;

select first(e1) from t1;
select first(e1, '2026-1-3') from t1;
select first(e1, e2) from t1 where e1 is not null;
select first(e1, e2-1s) from t1 where e1 is not null;
select first(*, e2) from t1 where e1 is not null;

select firstts(e1) from t1;
select firstts(e1, '2026-1-3') from t1;
select firstts(e1, e2) from t1 where e1 is not null;
select firstts(e1, e2-1s) from t1 where e1 is not null;
select firstts(*, e2) from t1 where e1 is not null;

select first_row(e1) from t1;
select first_row(e1, '2026-1-3') from t1;
select first_row(e1, e2) from t1 where e1 is not null;
select first_row(e1, e2-1s) from t1 where e1 is not null;
select first_row(*, e2) from t1 where e1 is not null;

select first_row_ts(e1) from t1;
select first_row_ts(e1, '2026-1-3') from t1;
select first_row_ts(e1, e2) from t1 where e1 is not null;
select first_row_ts(e1, e2-1s) from t1 where e1 is not null;
select first_row_ts(*, e2) from t1 where e1 is not null;

select last(new_e1, new_e2) from
    (
    select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select last(new_e1, new_e2, '2026-1-29') from
    (
    select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select last(e1, e2) from d2.v1;

with t3 as (select t1.e1, t1.e2 from t1 join t2 on t1.e1=t2.e1) select last(e1, e2) from t3;

select last(e1,e2) from t1 where exists (select e1 from t2);

select last(e1, e2) from t1 where exists (select e1 from t2 where e1 = t1.e1);

select last(t1.e1, t1.e2) from t1 join t2 on t1.e1=t2.e1;

select lastts(new_e1, new_e2) from
    (
        select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select lastts(new_e1, new_e2, '2026-1-29') from
    (
        select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select lastts(e1, e2) from d2.v1;

with t3 as (select t1.e1, t1.e2 from t1 join t2 on t1.e1=t2.e1) select lastts(e1, e2) from t3;

select lastts(e1,e2) from t1 where exists (select e1 from t2);

select lastts(e1, e2) from t1 where exists (select e1 from t2 where e1 = t1.e1);

select lastts(t1.e1, t1.e2) from t1 join t2 on t1.e1=t2.e1;

select last_row(new_e1, new_e2) from
    (
        select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select last_row(new_e1, new_e2, '2026-1-29') from
    (
        select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select last_row(e1, e2) from d2.v1;

with t3 as (select t1.e1, t1.e2 from t1 join t2 on t1.e1=t2.e1) select last_row(e1, e2) from t3;

select last_row(e1,e2) from t1 where exists (select e1 from t2);

select last_row(e1, e2) from t1 where exists (select e1 from t2 where e1 = t1.e1);

select last_row(t1.e1, t1.e2) from t1 join t2 on t1.e1=t2.e1;

select last_row_ts(new_e1, new_e2) from
    (
        select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select last_row_ts(new_e1, new_e2, '2026-1-29') from
    (
        select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select last_row_ts(e1, e2) from d2.v1;

with t3 as (select t1.e1, t1.e2 from t1 join t2 on t1.e1=t2.e1) select last_row_ts(e1, e2) from t3;

select last_row_ts(e1,e2) from t1 where exists (select e1 from t2);

select last_row_ts(e1, e2) from t1 where exists (select e1 from t2 where e1 = t1.e1);

select last_row_ts(t1.e1, t1.e2) from t1 join t2 on t1.e1=t2.e1;

select first(new_e1, new_e2) from
    (
    select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select first(new_e1, new_e2, '2026-1-29') from
    (
    select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select first(e1, e2) from d2.v1;

with t3 as (select t1.e1, t1.e2 from t1 join t2 on t1.e1=t2.e1) select first(e1, e2) from t3;

select first(e1,e2) from t1 where exists (select e1 from t2);

select first(e1, e2) from t1 where exists (select e1 from t2 where e1 = t1.e1);

select first(t1.e1, t1.e2) from t1 join t2 on t1.e1=t2.e1;

select firstts(new_e1, new_e2) from
    (
        select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select firstts(new_e1, new_e2, '2026-1-29') from
    (
        select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select firstts(e1, e2) from d2.v1;

with t3 as (select t1.e1, t1.e2 from t1 join t2 on t1.e1=t2.e1) select firstts(e1, e2) from t3;

select firstts(e1,e2) from t1 where exists (select e1 from t2);

select firstts(e1, e2) from t1 where exists (select e1 from t2 where e1 = t1.e1);

select firstts(t1.e1, t1.e2) from t1 join t2 on t1.e1=t2.e1;

select first_row(new_e1, new_e2) from
    (
        select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select first_row(new_e1, new_e2, '2026-1-29') from
    (
        select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select first_row(e1, e2) from d2.v1;

with t3 as (select t1.e1, t1.e2 from t1 join t2 on t1.e1=t2.e1) select first_row(e1, e2) from t3;

select first_row(e1,e2) from t1 where exists (select e1 from t2);

select first_row(e1, e2) from t1 where exists (select e1 from t2 where e1 = t1.e1);

select first_row(t1.e1, t1.e2) from t1 join t2 on t1.e1=t2.e1;

select first_row_ts(new_e1, new_e2) from
    (
        select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select first_row_ts(new_e1, new_e2, '2026-1-29') from
    (
        select t1.e1 as new_e1, t1.e2 as new_e2 from t1 join t2 on t1.e1=t2.e1
    );

select first_row_ts(e1, e2) from d2.v1;

with t3 as (select t1.e1, t1.e2 from t1 join t2 on t1.e1=t2.e1) select first_row_ts(e1, e2) from t3;

select first_row_ts(e1,e2) from t1 where exists (select e1 from t2);

select first_row_ts(e1, e2) from t1 where exists (select e1 from t2 where e1 = t1.e1);

select first_row_ts(t1.e1, t1.e2) from t1 join t2 on t1.e1=t2.e1;

select last(e1, e2) from t1;
select lastts(e1, e2) from t1;
select last_row(e1, e2) from t1;
select last_row_ts(e1, e2) from t1;

select first(e1, e2) from t1;
select firstts(e1, e2) from t1;
select first_row(e1, e2) from t1;
select first_row_ts(e1, e2) from t1;

SELECT last(e1,'2020-1-2 12:00:01.123'-1d) FROM t1 where e1=1;
SELECT last(e1,now()) FROM t1 where e1=1;
SELECT lastts(e1,'2020-1-2 12:00:01.123'-1d) FROM t1;
SELECT last_row(e1,'2020-1-2 12:00:01.123'-1d) FROM t1 where e1=1;
SELECT last_row(e1,now()) FROM t1 where e1=1;
SELECT last_row_ts(e1,'2020-1-2 12:00:01.123'-1d) FROM t1;

SELECT first(e1,'2020-1-2 12:00:01.123'-1d) FROM t1 where e1=1;
SELECT first(e1,now()) FROM t1 where e1=1;
SELECT firstts(e1,'2020-1-2 12:00:01.123'-1d) FROM t1;
SELECT first_row(e1,'2020-1-2 12:00:01.123'-1d) FROM t1 where e1=1;
SELECT first_row(e1,now()) FROM t1 where e1=1;
SELECT first_row_ts(e1,'2020-1-2 12:00:01.123'-1d) FROM t1;

SELECT last(last_e1,ts1) FROM (SELECT last(e1) as last_e1,ts as ts1 FROM t1 GROUP BY ts,event_window(e1>0,e1>0));
SELECT max(new_e1) FROM (SELECT max(e1) as new_e1 FROM t1 GROUP BY event_window(e1>0,e1>0));

drop database d1 cascade;
drop database d2 cascade;
