create ts database last_db;
use last_db;

/* normal time-series table */ 
create table t(k_timestamp timestamp not null, x float, y int, z varchar(32)) tags(a int not null) primary tags (a);
insert into t values('2023-07-29 03:11:59.688', 1.0, 1, 'a', 1);
insert into t values('2023-07-29 03:12:59.688', 2.0, 2, 'b', 1);
insert into t values('2023-07-29 03:15:59.688', 3.0, 1, 'a', 1);
insert into t values('2023-07-29 03:18:59.688', 4.0, 2, 'b', 1);
insert into t values('2023-07-29 03:25:59.688', 5.0, 5, 'e', 1);
insert into t values('2023-07-29 03:35:59.688', 6.0, 6, 'e', 1);
insert into t values('2023-07-29 03:10:59.688', 0.1, 2, 'b', 1);
insert into t values('2023-07-29 03:26:59.688', 5.5, 5, 'e', 1);

/* last */
select last(x) from t;
select last(y) from t;
select last(z) from t;
select last(z), sum(x) from t;
select last(x), count(distinct y) from t;
select last(x), last(x), last(y) from t;
select last(*) from t;
select last(t.*) from t;
select last(*), avg(x) from t;
select last(k_timestamp), last(x), last(y), last(z) from t;
select last(k_timestamp), last(x), avg(x), last(y), last(z) from t;

select last(x) from t where x > 1.0;
select last(y) from t where y > 2;
select last(y), sum(x) from t where y > 2;
select last(z) from t where z in ('a', 'b', 'e'); 
select last(x), last(x), last(y) from t where k_timestamp > '2023-07-29 03:12:59.68';
select last(*) from t where y in (6,5,3,1); 
select last(t.*) from t where y in (6,5,3,1); -- throw error
select last(k_timestamp), last(x), last(y), last(z) from t where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688';

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
select time_bucket(k_timestamp, '300s') bucket, last(z) from t where z in ('a', 'b', 'e') group by bucket order by bucket; 
select time_bucket(k_timestamp, '300s') bucket, count(distinct y), last(x), last(x), last(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last(x), last(x), last(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last(*) from t where y in (6,5,3,1) group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, sum(x), last(*), count(*) from t where y in (6,5,3,1) group by bucket order by bucket;  
select time_bucket(k_timestamp, '300s') bucket, last(k_timestamp), last(x), last(y), last(z) from t where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket order by bucket;

select time_bucket(k_timestamp, '300s') bucket, last(x) from t where x > 1.0 group by bucket, y order by bucket, y;
select time_bucket(k_timestamp, '300s') bucket, last(y) from t where y > 2 group by bucket, z order by bucket, z;
select time_bucket(k_timestamp, '300s') bucket, last(z) from t where z in ('a', 'b', 'e') group by bucket, y order by bucket, y;
select time_bucket(k_timestamp, '300s') bucket, count(distinct y), last(x), last(x), last(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket, z having last(x) < 100 order by bucket, z;
select time_bucket(k_timestamp, '300s') bucket, last(x), last(x), last(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket, z having last(y) > 2 order by bucket, z;
select time_bucket(k_timestamp, '300s') bucket, last(*) from t where y in (6,5,3,1) group by bucket, y order by bucket, y;
select time_bucket(k_timestamp, '300s') bucket, sum(x), last(*), count(*) from t where y in (6,5,3,1) group by bucket, y order by bucket, y;
select time_bucket(k_timestamp, '300s') bucket, last(k_timestamp), last(x), last(y), last(z) from t where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, y having last(x) > 1.5 order by bucket, y;

select time_bucket(k_timestamp, '300s') bucket, last(x) from t where x > 1.0 group by bucket, y order by bucket, y limit 1;
select time_bucket(k_timestamp, '300s') bucket, last(y) from t where y > 2 group by bucket, z order by bucket, z limit 1;
select time_bucket(k_timestamp, '300s') bucket, last(z) from t where z in ('a', 'b', 'e') group by bucket, y order by bucket, y limit 1;
select time_bucket(k_timestamp, '300s') bucket, count(distinct y), last(x), last(x), last(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket, z having last(x) < 100 order by bucket, z limit 1;
select time_bucket(k_timestamp, '300s') bucket, last(x), last(x), last(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket, z having last(y) > 2 order by bucket, z limit 1;
select time_bucket(k_timestamp, '300s') bucket, last(*) from t where y in (6,5,3,1) group by bucket, y order by bucket, y limit 1;
select time_bucket(k_timestamp, '300s') bucket, sum(x), last(*), count(*) from t where y in (6,5,3,1) group by bucket, y order by bucket, y limit 1;
select time_bucket(k_timestamp, '300s') bucket, last(k_timestamp), last(x), last(y), last(z) from t where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, y having last(x) > 1.5 order by bucket, y limit 1;

/* negative cases, error out */
select time_bucket(k_timestamp, '300s') bucket, last(*), count(*) from t group by bucket having last(*) > 1;
select last(t1.*), last(t2.*) from t t1, t t2 where t1.y = t2.y;
select last(x+1) from t;
select last() from t;
select last_row() from t;
select last(x,y) from t;
select last_row(x,y) from t;

/* last_row*/
select last_row(x) from t;
select last_row(y) from t;
select last_row(z) from t;
select last_row(z), sum(x) from t;
select last_row(x), count(distinct y) from t;
select last_row(x), last_row(x), last_row(y) from t;
select last_row(*) from t;
select last_row(*), avg(x) from t;
select last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from t;
select last_row(k_timestamp), last_row(x), avg(x), last_row(y), last_row(z) from t;

select last_row(x) from t where x > 1.0;
select last_row(y) from t where y > 2;
select last_row(y), sum(x) from t where y > 2;
select last_row(z) from t where z in ('a', 'b', 'e');
select last_row(x), last_row(x), last_row(y) from t where k_timestamp > '2023-07-29 03:12:59.68';
select last_row(*) from t where y in (6,5,3,1); 
select last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from t where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688';

select time_bucket(k_timestamp, '300s') bucket, last_row(x) from t group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last_row(y), sum(a) from t group by bucket order by bucket; -- throw error because cannot aggregate tag
select time_bucket(k_timestamp, '300s') bucket, last_row(y) from t group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last_row(z) from t group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last_row(x), last_row(x), last_row(y) from t group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last_row(*) from t group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last_row(*), count(*) from t group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from t group by bucket order by bucket;

select time_bucket(k_timestamp, '300s') bucket, last_row(x) from t where x > 1.0 group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last_row(y) from t where y > 2 group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last_row(z) from t where z in ('a', 'b', 'e') group by bucket order by bucket; 
select time_bucket(k_timestamp, '300s') bucket, count(distinct y), last_row(x), last_row(x), last_row(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last_row(x), last_row(x), last_row(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last_row(*) from t where y in (6,5,3,1) group by bucket order by bucket; 
select time_bucket(k_timestamp, '300s') bucket, sum(x), last_row(*), count(*) from t where y in (6,5,3,1) group by bucket order by bucket;
select time_bucket(k_timestamp, '300s') bucket, last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from t where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket order by bucket;

select time_bucket(k_timestamp, '300s') bucket, last_row(x) from t where x > 1.0 group by bucket, y order by bucket, y;
select time_bucket(k_timestamp, '300s') bucket, last_row(y) from t where y > 2 group by bucket, z order by bucket, z;
select time_bucket(k_timestamp, '300s') bucket, last_row(z) from t where z in ('a', 'b', 'e') group by bucket, y order by bucket, y;
select time_bucket(k_timestamp, '300s') bucket, count(distinct y), last_row(x), last_row(x), last_row(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket, z having last_row(x) < 100 order by bucket, z;
select time_bucket(k_timestamp, '300s') bucket, last_row(x), last_row(x), last_row(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket, z having last_row(y) > 2 order by bucket, z;
select time_bucket(k_timestamp, '300s') bucket, last_row(*) from t where y in (6,5,3,1) group by bucket, y order by bucket, y;
select time_bucket(k_timestamp, '300s') bucket, sum(x), last_row(*), count(*) from t where y in (6,5,3,1) group by bucket, y order by bucket, y;
select time_bucket(k_timestamp, '300s') bucket, last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from t where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, y having last_row(x) > 1.5 order by bucket, y;

select time_bucket(k_timestamp, '300s') bucket, last_row(x) from t where x > 1.0 group by bucket, y order by bucket, y limit 1;
select time_bucket(k_timestamp, '300s') bucket, last_row(y) from t where y > 2 group by bucket, z order by bucket, z limit 1;
select time_bucket(k_timestamp, '300s') bucket, last_row(z) from t where z in ('a', 'b', 'e') group by bucket, y order by bucket, y limit 1;
select time_bucket(k_timestamp, '300s') bucket, count(distinct y), last_row(x), last_row(x), last_row(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket, z having last_row(x) < 100 order by bucket, z limit 1;
select time_bucket(k_timestamp, '300s') bucket, last_row(x), last_row(x), last_row(y) from t where k_timestamp > '2023-07-29 03:12:59.68' group by bucket, z having last_row(y) > 2 order by bucket, z limit 1;
select time_bucket(k_timestamp, '300s') bucket, last_row(*) from t where y in (6,5,3,1) group by bucket, y order by bucket, y limit 1;
select time_bucket(k_timestamp, '300s') bucket, sum(x), last_row(*), count(*) from t where y in (6,5,3,1) group by bucket, y order by bucket, y limit 1;
select time_bucket(k_timestamp, '300s') bucket, last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from t where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, y having last_row(x) > 1.5 order by bucket, y limit 1;

-- /* negative cases, error out */
select time_bucket(k_timestamp, '300s') bucket, last_row(*), count(*) from t group by bucket having last_row(*) > 1;
select last_row(t1.*), last_row(t2.*) from t t1, t t2 where t1.y = t2.y;
select last_row(x+1) from t;


/* test template tables */
create table ts(k_timestamp timestamp not null, x int, y float, z varchar(32), a char(1)) tags(tabName varchar(10) not null, location varchar(32), d_type smallint) primary tags(tabName);

/* no instance tables (return 0 row for all) */
select last(x) from ts;
select last(y) from ts;
select last(z) from ts;
select last(z), sum(x) from ts;
select last(x), last(x), last(y) from ts;
select time_bucket(k_timestamp, '300s') bucket, last(x) from ts where x > 1.0 group by bucket;

select last_row(x) from ts;
select last_row(y) from ts;
select last_row(z) from ts;
select last_row(z), sum(x) from ts;
select last_row(x), last_row(x), last_row(y) from ts;
select time_bucket(k_timestamp, '300s') bucket, last_row(x) from ts where x > 1.0 group by bucket;

/* add instance tables */
insert into ts values('2023-07-29 03:10:59.680', 1, 1.0, 'aaa', 'a', 'ts_1', 'Milpitas', 1);
insert into ts values('2023-07-29 03:12:59.680', 2, 2.0, 'bbb', 'b', 'ts_1', 'Milpitas', 1);
insert into ts values('2023-07-29 03:15:59.680', 2, 3.0, 'ccc', 'c', 'ts_1', 'Milpitas', 1);
insert into ts values('2023-07-29 03:17:59.680', 3, 4.0, 'ddd', 'd', 'ts_1', 'Milpitas', 1);
insert into ts values('2023-07-29 03:19:59.680', 5, 5.0, 'eee', 'e', 'ts_1', 'Milpitas', 1);
insert into ts values('2023-07-29 03:29:59.680', 2, 6.0, 'eee', 'e', 'ts_1', 'Milpitas', 1);
insert into ts values('2023-07-29 03:30:59.680', 8, 7.0, 'bbb', 'b', 'ts_1', 'Milpitas', 1);
insert into ts values('2023-07-29 03:32:59.680', 8, 8.0, 'ccc', 'c', 'ts_1', 'Milpitas', 1);

insert into ts values('2023-07-29 03:10:59.680', 1, 1.0, 'aaa', 'a', 'ts_2', 'JiNan', 2);
insert into ts values('2023-07-29 03:12:59.680', 2, 2.0, 'bbb', 'b', 'ts_2', 'JiNan', 2);
insert into ts values('2023-07-29 03:15:59.680', 2, 3.0, 'ccc', 'c', 'ts_2', 'JiNan', 2);
insert into ts values('2023-07-29 03:17:59.680', 3, 4.0, 'ddd', 'd', 'ts_2', 'JiNan', 2);
insert into ts values('2023-07-29 03:19:59.680', 5, 5.0, 'eee', 'e', 'ts_2', 'JiNan', 2);
insert into ts values('2023-07-29 03:29:59.680', 2, 6.0, 'eee', 'e', 'ts_2', 'JiNan', 2);
insert into ts values('2023-07-29 03:30:59.680', 8, 7.0, 'bbb', 'b', 'ts_2', 'JiNan', 2);
insert into ts values('2023-07-29 03:32:59.680', 8, 8.0, 'ccc', 'c', 'ts_2', 'JiNan', 2);

insert into ts values('2023-07-29 03:11:59.680', 1, 1.0, 'aaa', 'a', 'ts_3', 'ShangHai', 3);
insert into ts values('2023-07-29 03:14:59.680', 2, 2.0, 'bbb', 'b', 'ts_3', 'ShangHai', 3);
insert into ts values('2023-07-29 03:15:59.680', 2, 3.0, 'ccc', 'c', 'ts_3', 'ShangHai', 3);
insert into ts values('2023-07-29 03:17:59.680', 3, 4.0, 'ddd', 'd', 'ts_3', 'ShangHai', 3);
insert into ts values('2023-07-29 03:25:59.680', 5, 5.0, 'eee', 'e', 'ts_3', 'ShangHai', 3);
insert into ts values('2023-07-29 03:29:59.680', 2, 6.0, 'eee', 'e', 'ts_3', 'ShangHai', 3);
insert into ts values('2023-07-29 03:30:59.680', 8, 7.0, 'bbb', 'b', 'ts_3', 'ShangHai', 3);
insert into ts values('2023-07-29 03:32:59.680', 8, 8.0, 'ccc', 'c', 'ts_3', 'ShangHai', 3);

insert into ts values('2023-07-29 03:11:59.680', 1, 1.0, 'aaa', 'a','ts_4','TianJin', 4);
insert into ts values('2023-07-29 03:14:59.680', 2, 2.0, 'bbb', 'b','ts_4','TianJin', 4);
insert into ts values('2023-07-29 03:15:59.680', 2, 3.0, 'ccc', 'c','ts_4','TianJin', 4);
insert into ts values('2023-07-29 03:17:59.680', 3, 4.0, 'ddd', 'd','ts_4','TianJin', 4);
insert into ts values('2023-07-29 03:25:59.680', 5, 5.0, 'eee', 'e','ts_4','TianJin', 4);
insert into ts values('2023-07-29 03:29:59.680', 2, 6.0, 'eee', 'e','ts_4','TianJin', 4);
insert into ts values('2023-07-29 03:30:59.680', 8, 7.0, 'bbb', 'b','ts_4','TianJin', 4);
insert into ts values('2023-07-29 03:32:59.680', 8, 8.0, 'ccc', 'c','ts_4','TianJin', 4);

/* last */ 
select last(x) from ts group by location order by location;
select last(y) from ts group by location order by location;
select last(z) from ts group by location order by location;
select last(z), sum(x) from ts group by location order by location;
select last(x), last(x), last(y) from ts group by location order by location;
select last(*) from ts group by location order by location;
select last(*), avg(x) from ts group by location order by location;
select last(k_timestamp), last(x), last(y), last(z) from ts group by location order by location;
select last(k_timestamp), last(x), avg(x), last(y), last(z) from ts group by location order by location;

select last(x) from ts where x > 1.0 group by location order by location;
select last(y) from ts where y > 2 group by location order by location;
select last(y), sum(x) from ts where y > 2 group by location order by location;
select last(z) from ts where z in ('aaa', 'bbb', 'eee') group by location order by location;
select last(x), last(x), last(y) from ts where k_timestamp > '2023-07-29 03:12:59.68' group by location order by location;
select last(*) from ts where y in (6,5,3,1) group by location order by location;
select last(k_timestamp), last(x), last(y), last(z) from ts where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by location order by location;

select location, time_bucket(k_timestamp, '300s') bucket, last(x) from ts group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last(y), sum(a) from ts group by bucket, location order by bucket, location; -- error because a is an attribute
select location, time_bucket(k_timestamp, '300s') bucket, last(y) from ts group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last(z) from ts group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last(x), last(x), last(y) from ts group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last(*) from ts group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last(*), count(*) from ts group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last(k_timestamp), last(x), last(y), last(z) from ts group by bucket, location order by bucket, location;

select location, time_bucket(k_timestamp, '300s') bucket, last(x) from ts where x > 1.0 group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last(y) from ts where y > 2 group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last(z) from ts where z in ('aaa', 'bbb', 'eee') group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, count(distinct y), last(x), last(x), last(y) from ts where k_timestamp > '2023-07-29 03:12:59.68' group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last(x), last(x), last(y) from ts where k_timestamp > '2023-07-29 03:12:59.68' group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last(*) from ts where y in (6,5,3,1) group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, sum(x), last(*), count(*) from ts where y in (6,5,3,1) group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last(k_timestamp), last(x), last(y), last(z) from ts where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, location order by bucket, location;

select location, time_bucket(k_timestamp, '300s') bucket, last(*) from ts where y in (6,5,3,1) group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, sum(x), last(*), count(*) from ts where y in (6,5,3,1) group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last(k_timestamp), last(x), last(y), last(z) from ts where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, location having last(x) > 1.5 order by bucket, location;
select time_bucket(k_timestamp, '300s') bucket, location, last(k_timestamp), last(x), last(y), last(z) from ts where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, location having last(x) > 1.5 order by bucket, location;

select location, time_bucket(k_timestamp, '300s') bucket, last(*) from ts where y in (6,5,3,1) group by bucket, location, y order by bucket, location, y limit 1;
select location, time_bucket(k_timestamp, '300s') bucket, sum(x), last(*), count(*) from ts where y in (6,5,3,1) group by bucket, location,y order by bucket, location, y limit 1;
select location, time_bucket(k_timestamp, '300s') bucket, last(k_timestamp), last(x), last(y), last(z) from ts where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, location having last(x) > 1.5 order by bucket, location limit 1;
select time_bucket(k_timestamp, '300s') bucket, location, last(k_timestamp), last(x), last(y), last(z) from ts where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, location having last(x) > 1.5 order by bucket, location limit 1;

/* last_row */ 
select last_row(x) from ts group by location order by location;
select last_row(y) from ts group by location order by location;
select last_row(z) from ts group by location order by location;
select last_row(z), sum(x) from ts group by location order by location;
select last_row(x), last_row(x), last_row(y) from ts group by location order by location;
select last_row(*) from ts group by location order by location;
select last_row(*), avg(x) from ts group by location order by location;
select last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from ts group by location order by location;
select last_row(k_timestamp), last_row(x), avg(x), last_row(y), last_row(z) from ts group by location order by location;

select last_row(x) from ts where x > 1.0 group by location order by location;
select last_row(y) from ts where y > 2 group by location order by location;
select last_row(y), sum(x) from ts where y > 2 group by location order by location;
select last_row(z) from ts where z in ('aaa', 'bbb', 'eee') group by location order by location;
select last_row(x), last_row(x), last_row(y) from ts where k_timestamp > '2023-07-29 03:12:59.68' group by location order by location;
select last_row(*) from ts where y in (6,5,3,1) group by location order by location;
select last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from ts where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by location order by location;

select location, time_bucket(k_timestamp, '300s') bucket, last_row(x) from ts group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(y), sum(x) from ts group by bucket, location order by bucket, location; -- error because a is an attribute
select location, time_bucket(k_timestamp, '300s') bucket, last_row(y) from ts group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(z) from ts group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(x), last_row(x), last_row(y) from ts group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(*) from ts group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(*), count(*) from ts group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from ts group by bucket, location order by bucket, location;

select location, time_bucket(k_timestamp, '300s') bucket, last_row(x) from ts where x > 1.0 group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(y) from ts where y > 2 group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(z) from ts where z in ('aaa', 'bbb', 'eee') group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, count(distinct y), last_row(x), last_row(x), last_row(y) from ts where k_timestamp > '2023-07-29 03:12:59.68' group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(x), last_row(x), last_row(y) from ts where k_timestamp > '2023-07-29 03:12:59.68' group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(*) from ts where y in (6,5,3,1) group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, sum(x), last_row(*), count(*) from ts where y in (6,5,3,1) group by bucket, location order by bucket, location;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from ts where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, location order by bucket, location;

select location, time_bucket(k_timestamp, '300s') bucket, last_row(*) from ts where y in (6,5,3,1) group by bucket, location, y order by bucket, location, y;
select location, time_bucket(k_timestamp, '300s') bucket, sum(x), last_row(*), count(*) from ts where y in (6,5,3,1) group by bucket, location, y order by bucket, location, y;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from ts where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, location having last_row(x) > 1.5 order by bucket, location;
select time_bucket(k_timestamp, '300s') bucket, location, last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from ts where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, location having last_row(x) > 1.5 order by bucket, location;

select location, time_bucket(k_timestamp, '300s') bucket, last_row(*) from ts where y in (6,5,3,1) group by bucket, location, y order by bucket, location, y limit 1;
select location, time_bucket(k_timestamp, '300s') bucket, sum(x), last_row(*), count(*) from ts where y in (6,5,3,1) group by bucket, location, y order by bucket, location, y limit 1;
select location, time_bucket(k_timestamp, '300s') bucket, last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from ts where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, location having last_row(x) > 1.5 order by bucket, location limit 1;
select time_bucket(k_timestamp, '300s') bucket, location, last_row(k_timestamp), last_row(x), last_row(y), last_row(z) from ts where k_timestamp between '2023-07-29 03:12:59.680' and '2023-07-29 03:35:59.688' group by bucket, location having last_row(x) > 1.5 order by bucket, location limit 1;

--- ZDP-25189 ---
select last(x) from ts group by location order by location limit 1;
select last_row(x) from ts group by location order by location limit 1;

drop table t;
drop table ts cascade;
use defaultdb;
drop database last_db cascade;

--- ZDP-25295 ---
create ts database ts_db;
use ts_db;
create table test(ts timestamp not null, ts1 timestamp, a int) tags (size int not null) primary tags(size);
insert into test values (1695139200000, 1695139200000, 1, 1);
select ts,ts1 from test group by ts,ts1 having max(ts)-last(ts1) = '0h';
select ts,ts1 from test group by ts,ts1 having max(ts)-max(ts1) = '0h';
select ts,ts1 from test group by ts,ts1 having max(ts)-last(ts1) = 0;
select ts,ts1 from test group by ts,ts1 having max(ts)-max(ts1) = 0;
drop table test;
use defaultdb;
drop database ts_db cascade;