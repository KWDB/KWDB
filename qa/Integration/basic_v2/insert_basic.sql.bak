-----Part 1: insert basic data type testing
-----testcase0001 insert int2

CREATE ts DATABASE test_savedata1;
CREATE TABLE test_savedata1.d1(k_timestamp timestamp not null,e1 int2 not null)
attributes (t1_attribute int not null) primary tags(t1_attribute);

INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:00', -1, 1);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:01', 0, 2);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:02', 32768, 3);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:03', 'test', 4);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:04', true, 5);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:05', 123.679, 6);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:06', -32768, 7);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:07', 32767, 8);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:08', 32767, 9);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:09', '中文', 10);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:10', 32768, 11);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:11', -32769, 12);


--------
select e1, t1_attribute from test_savedata1.d1 order by e1,t1_attribute;

--------Clean test data
DROP DATABASE test_savedata1 cascade;

-----testcase0002 int4 insert
CREATE ts DATABASE test_savedata1;

create TABLE test_savedata1.d1(k_timestamp timestamp not null,e1 int4 not null)
attributes (t1_attribute int not null) primary tags(t1_attribute);

INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:00', -1, 1);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:01', 0, 2);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:02', 2147483648, 3);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:03', 'test', 4);

INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:04', true, 5);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:05', 2147483647, 6);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:06', 2147483648, 7);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:07', -2147483648, 8);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:08', 123.679, 9);

INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:09', '中文', 10);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:10', -12147483649, 11);

--------Verify data correctness
select e1, t1_attribute from test_savedata1.d1 order by e1,t1_attribute;

--------Clean test data
DROP DATABASE test_savedata1 cascade;

-----testcase0003 int8
CREATE ts DATABASE test_savedata1;
create TABLE test_savedata1.d1(k_timestamp timestamp not null,e1 int8  not null)
attributes (t1_attribute int not null) primary tags(t1_attribute);

INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:00', -1, 1);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:01', 0, 2);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:02', 9223372036854775807, 3);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:03', 'test', 4);

INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:04', true, 5);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:05', 9223372036854775807, 6);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:06', 9223372036854775807, 7);

INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:07', -9223372036854775808, 8);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:08', 123.679, 9);

INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:09', '中文', 10);

--------Verify data correctness
select e1, t1_attribute from test_savedata1.d1 order by e1,t1_attribute;

--------Clean test data
DROP DATABASE test_savedata1 cascade;

-----testcase0004 int64 insert
CREATE ts DATABASE test_savedata1;
create TABLE test_savedata1.d1(k_timestamp timestamp not null,e1 int64  not null)
attributes (t1_attribute int not null) primary tags(t1_attribute);

INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:00', -1, 1);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:01', 0, 2);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:02', 9223372036854775807, 3);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:03', 'test', 4);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:04', true, 5);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:05', 9223372036854775807, 6);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:06', 9223372036854775807, 7);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:07', 123.679, 8);
INSERT INTO test_savedata1.d1 VALUES ('2018-10-10 10:00:08', '中文', 9);


--------Verify data correctness
select e1, t1_attribute from test_savedata1.d1 order by e1,t1_attribute;

--------Clean test data
DROP DATABASE test_savedata1 cascade;
-----testcase0005 FLOAT4 insert

CREATE ts DATABASE test_savedata1;
create TABLE test_savedata1.d1(k_timestamp timestamp not null,e1 float4  not null)
attributes (t1_attribute int not null) primary tags(t1_attribute);

INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:00', 0, 1);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:01', 32768.123, 2);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:02', 'test', 3);

INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:03', true, 4);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:04', 9223372036854775807.12345, 5);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:05', 9223372036854775807.54321, 6);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:06', -9223372036854775808.12345, 7);

INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:07', 99999999, 8);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:08', '9223372036854775808.12345', 9);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:09', '中文', 10);

--------Verify data correctness
select e1, t1_attribute from test_savedata1.d1 order by e1,t1_attribute;

--------Clean test data
DROP DATABASE test_savedata1 cascade;

-----testcase0006 FLOAT8 insert
CREATE ts DATABASE test_savedata1;
create TABLE test_savedata1.d1(k_timestamp timestamp not null,e1 float8  not null)
attributes (t1_attribute int not null) primary tags(t1_attribute);

INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:01', 0, 1);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:02', 32768.123, 2);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:03', 'test', 3);

INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:04', true, 4);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:05', 9223372036854775807.12345, 5);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:06', 9223372036854775807.54321, 6);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:07', -9223372036854775808.12345, 7);

INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:08', 99999999, 8);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:09', '9223372036854775808.12345', 9);
INSERT INTO test_savedata1.d1  VALUES ('2018-10-10 10:00:10', '中文', 10);

--------Verify data correctness
select e1, t1_attribute from test_savedata1.d1 order by e1,t1_attribute;

--------Clean test data
DROP DATABASE test_savedata1 cascade;

--------ZDP-20730
CREATE ts DATABASE test_savedata1;
--instance table
-- create table test_savedata1.st1(k_timestamp timestamp not null, e1 int) attributes (color1 varchar);
-- create table test_savedata1.ct_1 using test_savedata1.st1(color1) attributes('blue1');
-- insert into test_savedata1.ct_1 values('2018-10-10 10:00:00', 111);
-- insert into test_savedata1.ct_1 values('2018-10-10 10:00:01', 111);

--time-series table
create TABLE test_savedata1.nst1(k_timestamp timestamp not null,e1 int not null) attributes (t1_attribute int not null) primary tags(t1_attribute);
insert into test_savedata1.nst1 values('2018-10-10 10:00:00', 222, 1);
insert into test_savedata1.nst1 values('2018-10-10 10:00:01', 222, 2);

--Check the value of the now function twice, expected: 1;
--select count(*) from (select first(k_timestamp), last(k_timestamp) from test_savedata1.ct_1) where first != last;
--select count(*) from (select first(k_timestamp), last(k_timestamp) from test_savedata1.nst1) where first != last;

--------Clean test data
DROP DATABASE test_savedata1 cascade;

--ZDP-20727
CREATE ts DATABASE test_savedata1;

--time-series table
create TABLE test_savedata1.nst1(ts timestamp not null, e1 int, e2 int not null) attributes (tag1 int not null) primary tags(tag1);
--expected: error
insert into test_savedata1.nst1 values('2018-10-10 10:00:00', 111, 1);
insert into test_savedata1.nst1(ts, e1, tag1) values('2018-10-10 10:00:01', 111, 2);
insert into test_savedata1.nst1(ts, e1, e2, tag1) values('2018-10-10 10:00:02', 111, 222, 333, 3);
insert into test_savedata1.nst1(ts, e1, tag1) values('2018-10-10 10:00:03', 111, 222, 4);
--expected: succeed
insert into test_savedata1.nst1(ts, e1, e2, tag1) values('2018-10-10 10:00:04', 111, 222, 5);
insert into test_savedata1.nst1(ts, e2, tag1) values('2018-10-10 10:00:05', 222, 6);
--------Clean test data
DROP DATABASE test_savedata1 cascade;

--ZDP-21650
create ts database test_nullable;
create table test_nullable.p3(ts timestamp not null, e2 char(30), e3 varbytes(10)) tags(l int not null) primary tags(l);

--insert byte
insert into test_nullable.p3 values('2018-10-10 10:00:00', b'\xaa', b'\xaa', 1);

-- select e2, e3, l from test_nullable.p3;

--------Clean test data
DROP DATABASE test_nullable cascade;

--ZDP-23974
-- CREATE ts DATABASE tsdb1;
-- use tsdb1;

-- --create template table
-- create table tsdb1.st1(ts timestamp not null, a int) attributes (color1 int);
-- --create instance table
-- create table tsdb1.ct1 using tsdb1.st1(color1) attributes(123);
-- insert into public.ct1 values('2023-9-5 12:13:14', 1234);
-- select count(*) from tsdb1.ct1;

-- use defaultdb;
-- insert into public.ct1 values('2023-9-5 12:13:14', 1234);

--------Clean test data
-- DROP DATABASE tsdb1 cascade;

--expected error
CREATE ts DATABASE public;
--expected succeed
CREATE DATABASE public;
DROP DATABASE public cascade;

drop database if exists tsdb cascade;
create ts database tsdb;

--ts/e2/e5/e8 is not null
create table tsdb.nst1(ts timestamp not null, e1 int, e2 float not null, e3 bool, e4 int8, e5 char(10) not null, e6 nchar(10),
    e7 char(10), e8 char(10) not null) attributes (tag1 int not null) primary tags(tag1);

--expected failed
insert into tsdb.nst1 values('2023-8-23 12:13:14', 1, 2.2, true, 4, 'five', 'six', 'seven', 1);
insert into tsdb.nst1(ts,e8,e6,e4,e2,e5,e3,e1,e7,e9, tag1) values('2023-8-23 12:13:14', 'eight', 'six', 4, 2.2, 'five', true, 1, 'seven',9, 2);
insert into tsdb.nst1(ts,e8,e6,e4,e2,e5,e3,e1, tag1) values('2023-8-23 12:13:14', 'eight', 'six', 4, 2.2, 'five', true, 3);
insert into tsdb.nst1(ts,e8,e6,e4,e2,e5,e3, tag1) values('2023-8-23 12:13:14', 'eight', 'six', 4, 2.2, 'five', true, 1, 4);
insert into tsdb.nst1(ts,e8,e6,e4,e2, tag1) values('2023-8-23 12:13:14', 'eight', 'six', 4, 2.2, 5);

select * from tsdb.nst1 order by ts;

--expected succeed
insert into tsdb.nst1 values
('2023-8-23 12:13:14', 1, 2.2, true, 4, 'five1', 'six', 'seven', 'eight', 1),
('2023-8-23 12:13:15', 11, 2.2, true, 4, 'five1', 'six', 'seven', 'eight', 1);

insert into tsdb.nst1(ts,e8,e6,e4,e2,e5,e3,e1,e7, tag1) values
('2023-8-23 12:13:16', 'eight', 'six', 4, 2.2, 'five2', true, 2, 'seven', 2),
('2023-8-23 12:13:17', 'eight', 'six', 4, 2.2, 'five1', true, 111, 'seven', 1),
('2023-8-23 12:13:18', 'eight', 'six', 4, 2.2, 'five2', true, 22, 'seven', 2),
('2023-8-23 12:13:19', 'eight', 'six', 4, 2.2, 'five1', true, 111, 'seven', 1);

insert into tsdb.nst1(ts,e8,e2,e5, tag1) values
('2023-8-23 12:13:20', 'eight', 2.2, 'five3', 3),
('2023-8-23 12:13:21', 'eight', 2.2, 'five4', 4),
('2023-8-23 12:13:22', 'eight', 2.2, 'five3', 3),
('2023-8-23 12:13:23', 'eight', 2.2, 'five4', 4),
('2023-8-23 12:13:24', 'eight', 2.2, 'five3', 3);

insert into tsdb.nst1(ts,e8,e2,e5,e3,e1,e7, tag1) values
('2023-8-23 12:13:25', 'eight', 2.2, 'five4', true, 4444, 'seven', 4),
('2023-8-23 12:13:26', 'eight', 2.2, 'five3', true, 3333, 'seven', 3),
('2023-8-23 12:13:27', 'eight', 2.2, 'five2', true, 2222, 'seven', 2),
('2023-8-23 12:13:28', 'eight', 2.2, 'five1', true, 1111, 'seven', 1);

insert into tsdb.nst1(ts,e8,e2,e5,e3,e1,e7, tag1) values
('2023-8-23 12:13:29', 'eight', 2.2, 'five5', true, 55555, 'seven', 5),
('2023-8-23 12:13:30', 'eight', 2.2, 'five5', true, 55555, 'seven', 5),
('2023-8-23 12:13:31', 'eight', 2.2, 'five5', true, 55555, 'seven', 5),
('2023-8-23 12:13:32', 'eight', 2.2, 'five5', true, 55555, 'seven', 5),
('2023-8-23 12:13:33', 'eight', 2.2, 'five5', true, 55555, 'seven', 5);

select * from tsdb.nst1 order by ts;
--------Clean test data
drop database if exists tsdb cascade;

--------Clean test data
drop database if exists ts_db cascade;

drop database if exists tsdb cascade;
create ts database tsdb;

--ts/e2/e5/e8 is not null.
create table tsdb.nst1(ts timestamp not null, e1 int, e2 float not null, e3 bool, e4 int8, e5 char(10) not null, e6 nchar(10),
    e7 char(10), e8 char(10) not null) attributes (tag1 int not null) primary tags(tag1);
insert into tsdb.nst1(e1, tag1) values(1, 1);
select * from tsdb.nst1 order by ts;

insert into tsdb.nst1(ts,e8,e6,e4,e2,e5,e3,e1,e7, tag1) values
('2023-8-23 12:13:16', 'eight', 'six', 1, 2.2, 'five1', true, 1, 'seven', 1);
select * from tsdb.nst1 order by ts;
------Clean test data
drop database if exists tsdb cascade;

--ZDP-30232
drop database if exists test cascade;
create ts database test;
create table test.tb(
k_timestamp timestamptz not null,a1 int2,a2 int4,a3 int8,a4 float4,a5 float8,a6 double,
a7 char,a8 char(10),a9 nchar,a10 nchar(10),a11 bool,a12 varbytes,a13 varbytes(10),a14 timestamp)
 tags(t1 int2 not null,t2 int4,t3 int8,t4 float4,t5 float8,t6 double,t7 char(1),t8 char(10),
 t9 nchar(1),t10 nchar(10),t11 bool,t12 varbytes(1),t13 varbytes(10)) primary tags(t1);

insert into test.tb values
('2018-10-10 10:00:00',1,2,3,4.4,5.5,6.6,'a','aaaaaaaaaa','a','aaaaaaaaaa',true,b'\xaa',b'\xaa','2011-11-11 11:11:11',
1,2,3,4.4,5.5,6.6,'a','aaaaaaaaaa','a','aaaaaaaaaa',true,b'\xaa',b'\xaa'),
('2018-10-10 10:00:01',2,4,6,8.8,10.10,12.12,'b','bbbbbbbbbb','b','bbbbbbbbbb',false,b'\xbb',b'\xbb','2022-02-02 22:22:22',
2,4,6,8.8,10.10,12.12,'b','bbbbbbbbbb','b','bbbbbbbbbb',false,b'\xbb',b'\xbb'),
('2018-10-10 10:00:02',3,5,7,9.9,11.11,13.13,'c','cccccccccc','c','cccccccccc',true,b'\xcc',b'\xcc','2033-03-03 23:23:32',
3,5,7,9.9,11.11,13.13,'c','cccccccccc','c','cccccccccc',true,b'\xcc',b'\xcc');

select a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a14, t1, t2,t3, t4, t5, t6, t7, t8, t9, t10, t11 from test.tb order by a1;
------Clean test data
drop database if exists test cascade;

--ZDP-30901
drop database if exists test_insert cascade;
create ts database test_insert;
create table test_insert.tb(k_timestamp timestamp not null ,e1 int2) tags(tag1 int not null) primary  tags(tag1);
insert into test_insert.tb values('2023-05-01 10:10',0,1);
insert into test_insert.tb values('2023-05-01 10:11',-1,0);
insert into test_insert.tb values('2023-05-01 10:12',true,2);
insert into test_insert.tb values('2023-05-01 10:13',false,3);
insert into test_insert.tb values('2023-05-01 10:14',-32768,4);
insert into test_insert.tb values('2023-05-01 10:15',32767,5);
insert into test_insert.tb values('2023-06-15',null);
select * from test_insert.tb order by k_timestamp;

set time zone 8;
select * from test_insert.tb order by k_timestamp;
set time zone 0;

------Clean test data
drop database if exists test_insert cascade;

--Tests the default length of character types
drop database if exists test_insert1 cascade;
create ts database test_insert1;
create table test_insert1.t1(ts timestamptz not null, ch1 char, ch2 char(5), bt1 varbytes, bt2 varbytes(5), nch1 nchar, nch2 nchar(5))
tags(tag1 int not null, tag2 char, tag3 char(5), tag4 varbytes, tag5 varbytes(5)) primary tags(tag1);
insert into test_insert1.t1 values('2000-10-10 10:10:11', '1','aaaaa','2','bbbbb', '3', 'ccccc', 111, '4', 'eeeee', '5', 'fffff');

select * from test_insert1.t1;
------Clean test data
drop database if exists test_insert1 cascade;


--ZDP-31516
drop database if exists test_insert2 cascade;
create ts database test_insert2;
create table test_insert2.t1(ts timestamptz not null, e1 char, e2 char(64))
tags(tag1 char not null, tag2 char(64) not null, tag3 int not null) primary tags(tag1,tag2,tag3);
insert into test_insert2.t1(ts,e2,tag1,tag2,tag3) values('2000-10-10 10:10:11', '中abc','1','多abc',2222);

select ts, e1, e2, tag1, tag2, tag3 from test_insert2.t1;
------Clean test data
drop database if exists test_insert2 cascade;


--ZDP-31528
drop database if exists ts_db cascade;
create ts database ts_db;
create table ts_db.t1(
k_timestamp timestamptz not null,
e1 int2  not null,
e2 int,
e3 int8 not null,
e4 float4,
e5 float8 not null,
e6 bool,
e7 timestamptz not null,
e8 char(1023),
e9 nchar(255) not null,
e10 nchar(200),
e11 char not null,
e12 nchar(200),
e13 nchar not null,
e14 nchar(200),
e15 nchar(200) not null,
e16 varbytes(200),
e17 nchar(200) not null,
e18 nchar(200),e19 varbytes not null,
e20 varbytes(1023),
e21 varbytes(200) not null,
e22 varbytes(200)
) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
insert into ts_db.t1 values('2024-01-01 10:00:00',10000,1000000,1000,1047200.0000,109810.0,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',100,200,300,false,100.0,200.0,'beijing','red',b'\x26','fuluolidazhou','160','big','2','社会性别女','1','cuteandlovely');
insert into ts_db.t1(k_timestamp,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,flag,color) values(2021688553000,32050,NULL,4000,9845.87,200.123456,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1\0','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',700,false,'red');
insert into ts_db.t1(k_timestamp,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,flag,color) values(2021688554000,32050,NULL,4000,9845.87,200.123456,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1\0','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',700,false,'red');
insert into ts_db.t1(k_timestamp,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,flag,color) values(2021688555000,32050,NULL,4000,9845.87,200.123456,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1\0','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',700,false,'red');
select e11 from ts_db.t1;

------Clean test data
drop database if exists ts_db cascade;

--ZDP-31549
drop database if exists test_insert2 cascade;
create ts database test_insert2;
create table test_insert2.t1(ts timestamptz not null,e1 timestamp,e2 int2,e3 int4,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(64),e10 nchar,e11 nchar(64),e16 varbytes, e17 varbytes(64))tags (tag1 bool not null,tag2 smallint, tag3 int,tag4 bigint, tag5 float4, tag6 double, tag7 varbytes, tag8 varbytes(64), tag11 char,tag12 char(64), tag13 nchar, tag14 nchar(64))primary tags(tag1);
insert into test_insert2.t1(ts, e7, tag1, tag2, tag3) values
('2022-02-02 03:11:11+00',true,false,1111,11110),
('2022-02-02 03:11:12+00',0,1,2222,22220),
('2022-02-02 03:11:13+00',FALSE,TRUE,3333,33330);
select ts, e7, tag1, tag2, tag3 from test_insert2.t1 order by ts;
drop table test_insert2.t1;

create table test_insert2.t1(ts timestamptz not null,e1 timestamp,e2 int2,e3 int4,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(64),e10 nchar,e11 nchar(64),e16 varbytes, e17 varbytes(64))tags (tag1 bool not null,tag2 smallint, tag3 int,tag4 bigint, tag5 float4, tag6 double, tag7 varbytes, tag8 varbytes(64), tag11 char,tag12 char(64), tag13 nchar, tag14 nchar(64))primary tags(tag1);
insert into test_insert2.t1(ts, e7, tag1, tag2, tag3) values
('2022-02-02 03:11:11+00',true,false,1111,11110),
('2022-02-02 03:11:12+00',0,1,2222,22220),
('2022-02-02 03:11:13+00',FALSE,TRUE,3333,33330);
select ts, e7, tag1, tag2, tag3 from test_insert2.t1 order by ts;
drop table test_insert2.t1;

create table test_insert2.t1(ts timestamptz not null,e1 timestamp,e2 int2,e3 int4,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(64),e10 nchar,e11 nchar(64),e16 varbytes, e17 varbytes(64))tags (tag1 bool not null,tag2 smallint, tag3 int,tag4 bigint, tag5 float4, tag6 double, tag7 varbytes, tag8 varbytes(64), tag11 char,tag12 char(64), tag13 nchar, tag14 nchar(64))primary tags(tag1);
insert into test_insert2.t1(ts, e7, tag1, tag2, tag3) values
('2022-02-02 03:11:11+00',true,false,1111,11110),
('2022-02-02 03:11:12+00',0,1,2222,22220),
('2022-02-02 03:11:13+00',FALSE,TRUE,3333,33330);
select ts, e7, tag1, tag2, tag3 from test_insert2.t1 order by ts;
drop table test_insert2.t1;

------Clean test data
drop database if exists test_insert2 cascade;

------Clean test data
drop database if exists ts_db cascade;

-- testcase0007 BOOL insert
create ts database test_insert1;
create table test_insert1.t1(ts timestamptz not null, e1 bool not null, e2 char(64) not null)  tags(tag1 bool not null, tag2 char(64) not null) primary tags(tag1);
insert into test_insert1.t1 values('2022-02-02 03:11:11+00','true','aaa','false','bbb'),('2022-02-02 03:11:12+00',0,'111',1,'222'),('2022-02-02 03:11:13+00','FALSE','33333','TRUE','444444');
insert into test_insert1.t1 values('2022-02-02 03:11:14+00','t','aaa','false','bbb'),('2022-02-02 03:11:15+00','1','111',1,'222'),('2022-02-02 03:11:16+00','f','33333','TRUE','444444');
insert into test_insert1.t1 values('2022-02-02 03:11:17+00','T','aaa','false','bbb'),('2022-02-02 03:11:18+00',1,'111','1','222'),('2022-02-02 03:11:19+00','F','33333','TRUE','444444');
insert into test_insert1.t1 values('2022-02-02 03:11:20+00','tasd','aaa','false','bbb');
insert into test_insert1.t1 values('2022-02-02 03:11:21+00','fabc','aaa','false','bbb');
insert into test_insert1.t1 values('2022-02-02 03:11:22+00','3','aaa','false','bbb');
insert into test_insert1.t1 values('2022-02-02 03:11:23+00',6,'aaa','false','bbb');
select * from test_insert1.t1 order by ts;
drop database test_insert1;

-- testcase0008 timestampTZ insert
--ZDP-36191
drop database if exists test_insert2 cascade;
create ts database test_insert2;
create table test_insert2.t1(ts timestamptz not null,e1 timestamp,e2 int2)tags (tag1 int not null)primary tags(tag1);
-- time zone 0
insert into test_insert2.t1 values('2022-02-02 11:11:11+00','2024-02-02 11:11:11+00', 1, 111);
insert into test_insert2.t1 values('2022-02-02 11:11:12+00','2024-02-02 11:11:11+00', 2, 111);
insert into test_insert2.t1 values('2022-02-02 11:11:13+00','2024-02-02 11:11:11+00', 3, 111);
-- expect 3 rows
select * from test_insert2.t1 order by ts;
-- time zone 8
set time zone 8;
delete from test_insert2.t1 where ts = '2022-02-02 11:11:11+00';
-- expect 2 rows
select * from test_insert2.t1 order by ts;
insert into test_insert2.t1 values('2022-02-02 11:11:14+00','2024-02-02 12:11:11+00', 4, 222);
insert into test_insert2.t1 values('2022-02-02 19:11:15','2024-02-02 12:11:11+00', 5, 222);
insert into test_insert2.t1 values('2022-02-02 19:11:16','2024-02-02 12:11:11+00', 6, 222);
-- expect 5 rows
select * from test_insert2.t1 order by ts;
-- time zone 8
set time zone 1;
-- expect 5 rows
select * from test_insert2.t1 order by ts;
delete from test_insert2.t1 where ts = '2022-02-02 11:11:14+00';
delete from test_insert2.t1 where ts = '2022-02-02 12:11:12';
delete from test_insert2.t1 where ts = '2022-02-02 12:11:15';
-- expect 2 rows
select * from test_insert2.t1 order by ts;
delete from test_insert2.t1 where ts < '2022-02-02 12:11:14';
delete from test_insert2.t1 where ts > '2022-02-02 11:11:15+00';
-- expect 0 rows
select * from test_insert2.t1 order by ts;
drop table test_insert2.t1;
drop database if exists test_insert2 cascade;

-- primary tag: tag1 + tag2 + tag3
-- input1: tag1:'aa', tag2:'bb', tag3:'cc'
-- input2: tag1:'a', tag2:'abb', tag3:'cc'
-- input3: tag1:'a', tag2:'a', tag3:'bbcc'
-- old primary group: input1,input2,input3 -> 'aabbcc'
-- fixed primary group:
    --input1-> '2:aa2:bb2:cc'
    --input2-> '1:a3:abb2:cc'
    --input3-> '1:a1:a4:bbcc'
drop database if exists test_insert2 cascade;
create ts database test_insert2;
CREATE TABLE test_insert2.t1(
 k_timestamp TIMESTAMPTZ NOT NULL,
 id INT NOT NULL)
ATTRIBUTES (
 code1 INT2 NOT NULL,
 code8 VARCHAR(128) NOT NULL,
 code14 CHAR(1023) NOT NULL,
 code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code1,code14,code8,code16);
insert into test_insert2.t1 (k_timestamp,id,code1,code8,code14,code16) values
 (0,1,1,'aa','bb','cc'),
 (1000,1,1,'a','abb','cc'),
 (2000,1,1,'a','a','bbcc');
select k_timestamp,id,code1,code8,code14,code16 from test_insert2.t1 order by k_timestamp;
drop database if exists test_insert2 cascade;
