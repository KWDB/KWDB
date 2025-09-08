drop database  if EXISTS  test1 cascade;
create ts database test1;
CREATE TABLE test1.t1(k_timestamp TIMESTAMP not null, a int, b double, c int2, d int8) TAGS (tag1 int not null,tag2 int) PRIMARY TAGS (tag1);
insert into test1.t1 values('2023-07-29 03:11:59.688', 1,  1,1,-3,1,2);
insert into test1.t1 values('2023-07-29 03:12:59.688', 2,  2,2,3,2,2);
insert into test1.t1 values('2023-07-29 03:15:59.688', 21,  21,3,3,31,2);
insert into test1.t1 values('2023-07-29 03:16:59.688', 21,  21,32767,3,31,2);
insert into test1.t1 values('2023-07-29 03:17:59.688', 2147483641, 2147483643,32767,2,31,2);
insert into test1.t1 values('2023-07-29 03:19:59.688', 2147483642, 2147483642,32767,2,31,2);
insert into test1.t1 values('2023-07-29 03:21:59.688', 2147483643, 2147483641,32767,2,31,2);
insert into test1.t1 values('2023-07-29 03:23:59.688', 21,  21,3,9223372036854775807,30,1);
insert into test1.t1 values('2023-07-29 03:25:59.688', 21,  21,3,9223372036854775807,30,1);
insert into test1.t1 values('2023-07-29 03:27:59.688', 21,  21,3,9223372036854775807,30,1);
insert into test1.t1 values('2023-07-29 03:29:59.688', 21,  21,3,9223372036854775807,33,3);
insert into test1.t1 values('2023-07-29 03:31:59.688', 20,  21,3,9223372036854775807,33,3);
insert into test1.t1 values('2023-07-29 03:33:59.688', 20,  21,3,9223372036854775807,33,3);
insert into test1.t1 values('2023-07-29 03:35:59.688', 20,  21,3,323232,34,4);

select sum(a),sum(b),sum(c),sum(d)-sum(d)%100000,sum(tag1),sum(tag2) from test1.t1;

CREATE TABLE test1.t2(k_timestamp TIMESTAMP not null, a int, b double, c int2, d int8) TAGS (tag1 int8 not null,tag2 int8) PRIMARY TAGS (tag1);
insert into test1.t2 values('2023-07-29 03:11:59.688', 11,  12,13,14,15,16);
insert into test1.t2 values('2023-07-29 03:12:59.688', 12,  13,13,14,15,17);
insert into test1.t2 values('2023-07-29 03:15:59.688', 2,  3,3,4,15,17);
insert into test1.t2 values('2023-07-29 03:16:59.688', 21,  22,23,24,25,26);
insert into test1.t2 values('2023-07-29 03:19:59.688', 31,  32,33,34,35,36);
insert into test1.t2 values('2023-07-29 03:31:59.688', 41,  42,43,44,45,46);
select min(a) from test1.t2;
select min(tag1) from test1.t2;
select max(tag1) from test1.t2;
select sum(tag1) from test1.t2;
select min(tag1), sum(tag1), count(tag1), max(tag1),min(tag2) from test1.t2;
select max(a), max(b), min(tag1), sum(tag1), max(d), count(tag1), max(tag1), sum(d),last(tag1), last(tag2),last(a) from test1.t2;
select first(a),firstts(b),first_row(c), first_row_ts(tag1),last(a), last(c) ,last(tag1) ,last_row(b) from test1.t2;
select first(a),last_row_ts(tag1),last_row(tag2), last(d),last(a), last(c) ,last(tag1) ,last_row(b) from test1.t2;


CREATE TABLE test1.t3(k_timestamp TIMESTAMP not null, a int, c int8) TAGS (tag1 int not null,tag2 int) PRIMARY TAGS (tag1);
insert into test1.t3 values('2023-07-29 03:31:59.688', -2147483648, -2147483648,1,2);
insert into test1.t3 values('2023-07-29 03:35:59.688', 2147483647, 2147483647,2,2);
insert into test1.t3 values('2023-07-29 03:38:59.688', 21, 200,31,2);
select min(a) from test1.t3;
select sum(tag1),sum(a)  from test1.t3;

CREATE TABLE test1.t4(k_timestamp TIMESTAMP not null, a int, b double, c int2, d int8) TAGS (tag1 int not null,tag2 int) PRIMARY TAGS (tag1);
insert into test1.t4 values('2023-07-29 03:31:59.688', 22,  22,31,323236,2147483647,2147483646);
insert into test1.t4 values('2023-07-29 03:32:59.688', 23,  23,32,323237,2147483646,2147483647);
insert into test1.t4 values('2023-07-29 03:34:59.688', 20,  21,3,9223372036854775807,33,3);
insert into test1.t4 (k_timestamp, a,b,c,d,tag1) values(now(), 20,  21,3,9223372036854775807,34);
select count(a) from test1.t4;
select count(tag1) from test1.t4;
select count(tag2) from test1.t4;
select count_rows() from test1.t4;

CREATE TABLE test1.t5(k_timestamp TIMESTAMP not null, a int, b double, c int2, d int8) TAGS (tag1 int8 not null,tag2 int8) PRIMARY TAGS (tag1);
insert into test1.t5 values('2023-07-29 03:31:59.688', 22,  22,31,323236,9223372036854775807,2147483646);
insert into test1.t5 values('2023-07-29 03:32:59.688', 23,  23,32,323237,9223372036854775807,2147483647);
insert into test1.t5 values('2023-07-29 03:36:59.688', 3,  3,3,323231,9223372036854775807,2147483644);
insert into test1.t5 values('2023-07-29 03:37:59.688', 20,  21,3,9223372036854775807,33,3);
-- the expected result is wrong, need to do overflow
select sum(a),sum(b),sum(c),ROUND((sum(d)/100000), 0),ROUND((sum(tag1)/100000),0),sum(tag2) from test1.t5;


CREATE TABLE test1.t6(k_timestamp TIMESTAMP not null, a int, c int8) TAGS (tag1 int not null,tag2 int) PRIMARY TAGS (tag1);
insert into test1.t6 values('2023-07-29 03:31:59.688', -2147483648, -2147483648,2147483641,2);
insert into test1.t6 values('2023-07-29 03:36:59.688', 2147483647, 2147483647,-2147483641,2);
insert into test1.t6 values('2023-07-29 03:37:59.688', 21, 200,31,2);
select min(a) from test1.t6;
select sum(tag1),sum(tag2),sum(a) ,sum(c) from test1.t6;

CREATE TABLE test1.t7(k_timestamp TIMESTAMP not null, a int not null, b double, c int2, d int8) TAGS (tag1 int not null,tag2 int) PRIMARY TAGS (tag1);
insert into test1.t7 values('2024-07-25 06:42:01.922', 1,  1,1,-3,1,2);
insert into test1.t7 values('2024-07-25 06:45:50.57', 21,  21,32767,3,31,2);
select count(*),first(a) from test1.t7  where k_timestamp > '2024-7-25 06:43:00' and k_timestamp < '2024-7-25 06:45:00';
select count(*),last(a) from test1.t7  where k_timestamp > '2024-7-25 06:43:00' and k_timestamp < '2024-7-25 06:45:00';
select count(*),last(k_timestamp) from test1.t7  where k_timestamp > '2024-7-25 06:43:00' and k_timestamp < '2024-7-25 06:45:00';

use defaultdb;
drop database test1 CASCADE;

create ts database test;
use test;
create table t2(ts timestamp not null, e1 int) tags(t int not null) primary tags(t);
insert into t2 values(1,1,1);
insert into t2 values(2,2,2);
insert into t2 values(3,3,3);
select *  from t2 order by ts;
select count(t),max(t) from t2 where ts<='1970-01-01 00:00:00.002';
select count(t),max(t),first(t) from t2 where ts<='1970-01-01 00:00:00.002';
select count(t),max(t) from t2 where ts<='1970-01-01 00:00:00.002' group by t,ts order by ts;
select count(t),max(t),first(t) from t2 where ts<='1970-01-01 00:00:00.002' group by t,ts order by ts;
select count(e1),max(t),first(t),t from t2 where ts<='1970-01-01 00:00:00.002' group by t,ts order by ts;
select count(e1),max(t),t from t2 where ts<='1970-01-01 00:00:00.002' group by t,ts order by ts;
select count(e1),max(t),last(t),t from t2 where ts<='1970-01-01 00:00:00.002' group by t,ts order by ts;
select count(e1),max(t),last(t) from t2;
select count(e1),max(t),first_row(t),t from t2 where ts<='1970-01-01 00:00:00.002' group by t,ts order by ts;
select count(e1),max(t),first(t) from t2 where ts<='1970-01-01 00:00:00.002' group by t,ts order by ts;
select count(e1),first(t) from t2 where ts<='1970-01-01 00:00:00.002';
select max(t) from t2 where ts<='1970-01-01 00:00:00.002' group by t,ts order by ts;
select count(e1),max(t),first(t)  from t2 where ts<='1970-01-01 00:00:00.002' group by t,ts order by ts;
select count(t),max(t) from t2 where ts<='2024-01-01 00:00:00.002';
select max(t),first(t) from t2 where ts<='1970-01-01 00:00:00.002';
select first(t),last(t) from t2 where ts<='1970-01-01 00:00:00.002';
select first(t),last(e1) from t2 where ts<='1970-01-01 00:00:00.002';
select first(t),lastts(e1) from t2 where ts<='1970-01-01 00:00:00.002';

create table t3(ts timestamp not null, e1 int) tags(t int not null,t2 int ) primary tags(t);
insert into t3 values('2024-01-01 00:00:00.001',1,1,2);
insert into t3 values('2024-01-01 00:00:00.002',2,2,3);
insert into t3 values('2024-01-01 00:00:00.003',3,3,4);
select * from t3 order by ts;
select count(t),max(t) from t3 where ts<='1970-01-01 00:00:00.002';
select count(t),max(t),first(t) from t3 where ts<='2024-01-01 00:00:00.002';
select count(t2),max(t2),first(t2) from t3 where ts<='2024-01-01 00:00:00.002';
select count(e1),max(e1),first(t2),t from t3 where ts<='2024-01-01 00:00:00.002' group by t,ts order by ts;
select count(e1),max(e1),sum(t2),t from t3 where ts<='2024-01-01 00:00:00.004' group by t,ts order by ts;
select min(t2),max(t2),first(t2)  from t3 where ts<=' 2024-01-01 00:00:00.002';
drop database test cascade;


