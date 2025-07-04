create ts database last_db;
use last_db;
create table t(k_timestamp timestamp not null, x float, y int, z varchar(32)) tags(a int not null) primary tags(a);
create table t1(k_timestamp timestamp not null, x float, y int, z varchar(32)) tags(a int not null) primary tags(a);

insert into t values('2023-07-29 03:11:59.688', 1.0, 1, 'a',1);
insert into t values('2023-07-29 03:12:59.688', 2.0, 2, 'b',1);
insert into t values('2023-07-29 03:15:59.688', 3.0, 1, 'a',1);
insert into t values('2023-07-29 03:18:59.688', 4.0, 2, 'b',1);
insert into t values('2023-07-29 03:25:59.688', 5.0, 5, 'e',1);
insert into t values('2023-07-29 03:35:59.688', 6.0, 6, 'e',1);
insert into t values('2023-07-29 03:36:59.688', null, 7, 'f',1);
insert into t values('2023-07-29 03:37:59.688', 8.0, null, 'g',1);
insert into t values('2023-07-29 03:38:59.688', 9.0, 9, null,1);
insert into t values('2023-07-29 03:39:59.688', null, null, null,1);


select last(x) from t;
select last(y) from t;
select last(z) from t;
select last(z), sum(x) from t;
select last(x), last(x), last(y) from t;
select last(*) from t;
select last(t.*) from t; -- throw error 
select last(*), avg(x) from t;
select last(k_timestamp), last(x), last(y), last(z) from t;
select last(k_timestamp), last(x), avg(x), last(y), last(z) from t;

select last_row(x) from t;
select last_row(y) from t;
select last_row(z) from t;
select last_row(z), sum(x) from t;
select last_row(x), last_row(x), last_row(y) from t;
select last_row(*) from t;
select last_row(t.*) from t; -- throw error 
select last_row(*), avg(x) from t;
select last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from t;
select last_row(k_timestamp), last_row(x), avg(x), last_row(y), last_row(z) from t;

select time_bucket(k_timestamp, '300s') bucket, last(x) from t group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last(y), sum(a) from t group by bucket order by bucket; -- throw error because cannot aggregate tag
select time_bucket(k_timestamp, '300s') bucket, last(y) from t group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last(z) from t group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last(x), last(x), last(y) from t group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last(*) from t group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last(*), count(*) from t group by bucket order by bucket; 
select time_bucket(k_timestamp, '300s') bucket, last(k_timestamp), last(x), last(y), last(z) from t group by bucket order by bucket;

select time_bucket(k_timestamp, '300s') bucket, last(x) from t where x > 1.0 group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last(y) from t where y > 2 group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last(y) from t where y in (1,3,5,6) order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last(z) from t where z in ('a', 'b', 'e') group by bucket order by bucket; 
select time_bucket(k_timestamp, '300s') bucket, count(distinct y), last(x), last(x), last(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last(x), last(x), last(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket  order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last(*) from t where y in (6,5,3,1) group by bucket  order by bucket;
select time_bucket(k_timestamp, '300s') bucket, sum(x), last(*), count(*) from t where y in (6,5,3,1) group by bucket order by bucket;  
select time_bucket(k_timestamp, '300s') bucket, last(k_timestamp), last(x), last(y), last(z) from t where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket order by bucket;

/* negative cases, error out */
-- select time_bucket(k_timestamp, '300s') bucket, last(*), count(*) from t group by bucket having last(*) > 1;
select last(t1.*), last(t2.*) from t t1, t t2 where t1.y = t2.y;
select last(x+1) from t;

insert into t1 values('2023-07-29 03:11:59.688', 1.0, NULL, 'a', 1);
insert into t1 values('2023-07-29 03:12:59.688', NULL, NULL, 'b', 1);
insert into t1 values('2023-07-29 03:15:59.688', NULL, NULL, NULL, 1);

select last(y) from t1;
select last(y), sum(y) from t1;
select last(x),last(y) from t1;
select last(x),last(y) from t1 where z is null;
select last(x),last(y) from t1 where z is not null;
select z, last(x),last(y) from t1 group by z order by z;
select z, last(x),last(y) from t1 group by z having last(x) is not null order by z;
select last(*) from t1;
select last(*), count(*) from t1;

select last_row(x),last_row(y) from t1;
select last_row(*),last(*) from t1;


drop table t cascade;
drop table t1 cascade;
drop database last_db cascade;
