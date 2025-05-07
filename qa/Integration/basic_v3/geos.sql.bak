-- DDL
-- test_case0001
create ts database test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
  e1 geometry) ATTRIBUTES (
code1 int2 not null,
code2 int4) PRIMARY TAGS(code1);
show create table test_geometry.t1;
set sql_safe_updates = false;
drop database if exists test_geometry cascade;

-- test_case0002
create ts database test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
  e1 geometry) ATTRIBUTES (
code1 int2 not null,
code2 int4) PRIMARY TAGS(code1);
show columns from test_geometry.t1;
set sql_safe_updates = false;
drop database if exists test_geometry cascade;

-- test_case0003
create ts database test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
  e1 int not null) ATTRIBUTES (
code1 int2 not null,
code2 geometry ) PRIMARY TAGS(code1);
show create table test_geometry.t1;
set sql_safe_updates = false;
drop database if exists test_geometry cascade;

-- test_case0004
create ts database test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
  e1 int not null) ATTRIBUTES (
code1 int2 not null,
code2 geometry ) PRIMARY TAGS(code1);
show columns from test_geometry.t1;
set sql_safe_updates = false;
drop database if exists test_geometry cascade;

-- test_case0005
create ts database test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
  e1 int not null) ATTRIBUTES (
code1 int2 not null,
code2 geometry ) PRIMARY TAGS(code1);
show tags from test_geometry.t1;
set sql_safe_updates = false;
drop database if exists test_geometry cascade;

-- test_case0006
create ts database test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
  e1 int not null) ATTRIBUTES (
code1 geometry not null,
code2 int ) PRIMARY TAGS(code1);
show create table test_geometry.t1;
set sql_safe_updates = false;
drop database if exists test_geometry cascade;

-- test_case0007
create ts database test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
  e1 int not null) ATTRIBUTES (
code1 geometry not null,
code2 int ) PRIMARY TAGS(code1);
show columns from test_geometry.t1;
set sql_safe_updates = false;
drop database if exists test_geometry cascade;

-- test_case0008
create ts database test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
  e1 int not null) ATTRIBUTES (
code1 geometry not null,
code2 int ) PRIMARY TAGS(code1);
show tags from test_geometry.t1;
set sql_safe_updates = false;
drop database if exists test_geometry cascade;

-- test_case0009
create ts database test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
  e1 geometry) ATTRIBUTES (
code1 int2 not null,
code2 int4) PRIMARY TAGS(code1);
create table test_geometry.t2 (k_timestamp timestamptz not null,
  e1 geometry null) ATTRIBUTES (
code1 int2 not null,
code2 int4) PRIMARY TAGS(code1);
show create table test_geometry.t1;
show create table test_geometry.t2;
set sql_safe_updates = false;
drop database if exists test_geometry cascade;

-- test_case0010
create ts database test_geometry;
use test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
  e1 geometry not null) ATTRIBUTES (
code1 int2 not null,
code2 int4) PRIMARY TAGS(code1);
show create table test_geometry.t1;
set sql_safe_updates = false;
drop database if exists test_geometry cascade;

-- test_case0011
create ts database test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
  e1 int) ATTRIBUTES (
code1 int2 not null,
code2 geometry) PRIMARY TAGS(code1);
create table test_geometry.t2 (k_timestamp timestamptz not null,
  e1 int) ATTRIBUTES (
code1 int2 not null,
code2 geometry null) PRIMARY TAGS(code1);
show create table test_geometry.t1;
show create table test_geometry.t2;
set sql_safe_updates = false;
drop database if exists test_geometry cascade;

-- test_case0012
create ts database test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
  e1 int) ATTRIBUTES (
code1 int2 not null,
code2 geometry not null) PRIMARY TAGS(code1);
show create table test_geometry.t1;
set sql_safe_updates = false;
drop database if exists test_geometry cascade;

-- test_case0013
create ts database test_geometry;
create table test_geometry.t1 (k_timestamp timestamptz not null,
 e1 int2 not null,
 e2 smallint not null,
 e3 int4,
 e4 int,
 e5 integer,
 e6 int8 not null,
 e7 float4,
 e8 real,
 e9 float8 not null,
 e10 float,
 e11 double,
 e12 double precision,
 e13 bool,
 e14 boolean,
 e15 timestamp not null,
 e16 timestamptz not null,
 e17 char not null,
 e18 char(1023),
 e19 character not null,
 e20 character(1023),
 e21 nchar not null,
 e22 nchar(255) not null,
 e23 varchar,
 e24 varchar(4096),
 e25 varchar(255),
 e26 nvarchar,
 e27 nvarchar(255) not null,
 e28 nvarchar(4096) not null,
 e29 varbytes not null,
 e30 varbytes(1023),
 e31 varbytes,
 e32 varbytes(4096) not null,
 e33 varbytes(254),
 e34 geometry
 ) ATTRIBUTES (
 code1 int2 not null,
 code2 int2) PRIMARY TAGS(code1);
SELECT * FROM test_geometry.t1;
drop database if exists test_geometry cascade;

-- test_case0014
CREATE TS DATABASE test_geometry;
CREATE TABLE test_geometry.tb1(k_timestamp timestamptz not null,
e1 int2,
e2 int,
e3 int8,
e4 float,
e5 float8,
e6 bool,
e7 timestamp,
e8 char(1023),
e9 nchar(255),
e10 varchar(4096),
e11 char,
e12 varchar(255),
e13 nchar,
e14 varchar,
e15 nvarchar(4096),
e16 varbytes,
e17 nvarchar(255),
e18 nvarchar,
e19 varbytes,
e20 varbytes(1023),
e21 varbytes(4096),
e22 varbytes(254),
e23 timestamptz)
TAGS (code1 bool not null, code2 smallint, code3 int, code4 bigint, code5 int2, code6 int4, code7 int8, code8 float, code9 real, code10 float4, code11 float8, code12 double, code13 char, code14 char(128), code15 nchar, code16 nchar(128), code17 varchar, code18 varchar(128), code19 varbytes, code20 varbytes(128), code21 varbytes, code22 varbytes(128), code23 timestamp, code24 timestamptz, code25 nvarchar, code26 nvarchar(128),code27 geometry) primary tags (code1);
SHOW CREATE TABLE test_geometry.tb1;
SELECT * FROM test_geometry.tb1;
DROP DATABASE test_geometry CASCADE;

-- Insert
-- test_case0015
create ts database test_geometry;
create table test_geometry.tb(k_timestamptz timestamptz not null ,e1 geometry , e2  geometry ) tags(tag1 int not null) primary  tags(tag1);
insert into test_geometry.tb values('2023-05-01 10:10', 'Point(0.0 0.0)','Point(0.0 0.0)',0);
insert into test_geometry.tb values('2023-05-01 10:11', 'Point(0 0)','Point(1 1)',1);
insert into test_geometry.tb values('2023-05-01 10:12', 'Point(-1 -1)','Point(0 -1)',2);
insert into test_geometry.tb values('2023-05-01 10:13', 'Point(-32768 -1)','Point(0 -1)',3);
insert into test_geometry.tb values('2023-05-01 10:14', 'Point(-1 -1)','Point(0 32767)',4);
insert into test_geometry.tb values('2023-05-01 10:15', 'Point(-2147483648 -2147483648)','Point(0 2147483647)',5);
insert into test_geometry.tb values('2023-05-01 10:16', 'Point(-9223372036854775807 -1)','Point(0 9223372036854775807)',6);
insert into test_geometry.tb values('2023-05-01 10:17', 'Point(1000.101156789 1000.101156789)','Point(0 -2000.2022)',7);
insert into test_geometry.tb values('2023-05-01 10:18', 'Point(-32767.111156 -1)','Point(0 -32767.111156)',8);
insert into test_geometry.tb values('2023-05-01 10:19', 'Point(1.2345678911111 -1)','Point(0 2147483646.6789)',9);
insert into test_geometry.tb values('2023-05-01 10:20', 'Point(-1.0000000 -1.0000)','Point(0 9223372036854775806.12345)',10);
insert into test_geometry.tb values('2023-05-01 10:21', 'Point(0.0000000 0.0000)','Point(600000.606066666 9223372036854775806.12345)',11);
select * from test_geometry.tb order by k_timestamptz;
select ST_Distance(e1,e2) from test_geometry.tb order by k_timestamptz;
drop table test_geometry.tb;
drop database test_geometry CASCADE;

-- test_case0016
create ts database test_geometry;
create table test_geometry.tb(k_timestamptz timestamptz not null ,e1 geometry , e2  geometry ) tags(tag1 int not null) primary  tags(tag1);
insert into test_geometry.tb values('2023-05-01 10:10', 'Linestring(0.0 0.0,0.0 0.0)','Linestring(1.0 1.0, 2.0 2.0)',0);
insert into test_geometry.tb values('2023-05-01 10:11', 'Linestring(0 0,1 1)','Linestring(1 1,60 90)',1);
insert into test_geometry.tb values('2023-05-01 10:12', 'Linestring(-1 -1,0.0 0.0)','Linestring(0 -1,1 1)',2);
insert into test_geometry.tb values('2023-05-01 10:13', 'Linestring(-32768 -1,1 32768)','Linestring(0 -1,1000 1000)',3);
insert into test_geometry.tb values('2023-05-01 10:14', 'Linestring(-1 -1,0 32767)','Linestring(0 32767,-1 -1)',4);
insert into test_geometry.tb values('2023-05-01 10:15', 'Linestring(-2147483648 -2147483648,0 2147483647)','Linestring(0 2147483647,-2147483648 -214748364)',5);
insert into test_geometry.tb values('2023-05-01 10:16', 'Linestring(-9223372036854775807 -1,0 9223372036854775807)','Linestring(0 9223372036854775807,-9223372036854775807 -1)',6);
insert into test_geometry.tb values('2023-05-01 10:17', 'Linestring(1000.101156789 1000.101156789,0 -2000.2022)','Linestring(0 -2000.2022,1000.101156789 1000.101156789)',7);
insert into test_geometry.tb values('2023-05-01 10:18', 'Linestring(-32767.111156 -1,0 -32767.111156)','Linestring(0 -32767.111156,-32767.111156 -1)',8);
insert into test_geometry.tb values('2023-05-01 10:19', 'Linestring(1.2345678911111 -1,0 2147483646.6789)','Linestring(0 2147483646.6789,1.2345678911111 -1)',9);
insert into test_geometry.tb values('2023-05-01 10:20', 'Linestring(-1.0000000 -1.0000,0 9223372036854775806.12345)','Linestring(0 9223372036854775806.12345,-1.0000000 -1.0000)',10);
insert into test_geometry.tb values('2023-05-01 10:21', 'Linestring(0.0000000 0.0000,1.435243 7.673421)','Linestring(600000.606066666 9223372036854775806.12345,0.0000000 0.0000)',11);
insert into test_geometry.tb values('2023-05-01 10:22', 'Linestring(0 0,1 1,2 3,3 3,4 4,5 5,6 6,9 9,111 445,999 999999)','Linestring(0.0 0.0,1.0 2.0,3.0 4.0,5.0 5.0,6.0 7.0,8.0 9.0,11.0 15.0,45.0 22.0,0.1111 0.0,9.0 0.0,0.0 0.0)',1);
select * from test_geometry.tb order by k_timestamptz;
select ST_Distance(e1,e2) from test_geometry.tb order by k_timestamptz;
drop table test_geometry.tb;
drop database test_geometry CASCADE;

-- test_case0017
create ts database test_geometry;
create table test_geometry.tb(k_timestamptz timestamptz not null ,e1 geometry , e2  geometry ) tags(tag1 int not null) primary  tags(tag1);
insert into test_geometry.tb values('2023-05-01 10:11', 'Polygon((0 0,1 1,60 90,0 0))','Polygon((1 1,60 90,0 0,1 1))',1);
insert into test_geometry.tb values('2023-05-01 10:12', 'Polygon((-1 -1,0.0 0.0,0 -1,-1 -1))','Polygon((0 -1,1 1,1000 1000,0 -1))',2);
insert into test_geometry.tb values('2023-05-01 10:13', 'Polygon((-32768 -1,1 32768,0.0 0.0,-32768 -1))','Polygon((0 -1,1000 1000,0 32767,0 -1))',3);
insert into test_geometry.tb values('2023-05-01 10:14', 'Polygon((-1 -1,0 32767,0 2147483647,-2147483648 -2147483648,-1 -1))','Polygon((0 32767,0 2147483647,-1 -1,0.0 0.0,0 32767))',4);
insert into test_geometry.tb values('2023-05-01 10:15', 'Polygon((-2147483648 -2147483648,0 2147483647,0.0 0.0,-2147483648 -2147483648))','Polygon((60 90,0 2147483647,-2147483648 -214748364,60 90))',5);
insert into test_geometry.tb values('2023-05-01 10:16', 'Polygon((-9223372036854775807 -1,0 9223372036854775807,0.0,-9223372036854775807 -1))','Polygon((0 -2000.2022,0 9223372036854775807,-9223372036854775807 -1,0 -2000.2022))',6);
insert into test_geometry.tb values('2023-05-01 10:17', 'Polygon((1000.101156789 0,0 -2000.2022,60 90,1000.101156789 0))','Polygon((0 -2000.2022,1000.101156789 1000.101156789,-32767.111156 -1,0 -2000.2022))',7);
insert into test_geometry.tb values('2023-05-01 10:18', 'Polygon((-32767.111156 -1,0 -32767.111156,-1.0000000 -1.0000,-32767.111156 -1))','Polygon((0 -32767.111156,-32767.111156 -1,1000 1000,0 -32767.111156))',8);
insert into test_geometry.tb values('2023-05-01 10:19', 'Polygon((1.2345678911111 -1,0 2147483646.6789,1.435243 7.673421,1.2345678911111 -1))','Polygon((0 2147483646.6789,1.2345678911111 -1,1.435243 7.673421,0 2147483646.6789))',9);
insert into test_geometry.tb values('2023-05-01 10:20', 'Polygon((-1.0000000 -1.0000,0 9223372036854775806.12345,600000.606066666 9223372036854775806.12345,-1.0000000 -1.0000))','Polygon((0 9223372036854775806.12345,-1.0000000 -1.0000,1.435243 7.673421,0 9223372036854775806.12345))',10);
insert into test_geometry.tb values('2023-05-01 10:22', 'Polygon((0 0,1 1,2 3,3 3,4 4,5 5,6 6,9 9,111 445,999 999999,0 0))','Polygon((0.0 0.0,1.0 2.0,3.0 4.0,5.0 5.0,6.0 7.0,8.0 9.0,11.0 15.0,45.0 22.0,0.1111 0.0,9.0 0.0,0.0 0.0))',1);

select * from test_geometry.tb order by k_timestamptz;
select ST_Distance(e1,e2) from test_geometry.tb order by k_timestamptz;
drop table test_geometry.tb;
drop database test_geometry CASCADE;

-- test_case0018
create ts database test_geometry;
create table test_geometry.tb(k_timestamptz timestamptz not null ,e1 geometry , e2  geometry ) tags(tag1 int not null) primary  tags(tag1);
insert into test_geometry.tb values('2023-05-01 10:10', 'Point(0.0 0.0)', 'Linestring(1.0 1.0, 2.0 2.0)',1);
insert into test_geometry.tb values('2023-05-01 10:11', 'Point(0.0 0.0)', 'Polygon((1.0 1.0, 2.0 2.0, 3.0 2.0, 1.0 1.0))',2);
insert into test_geometry.tb values('2023-05-01 10:12', 'Linestring(1.0 1.0, 2.0 2.0)','Polygon((1 1,60 90,0 0,1 1))',1);
select * from test_geometry.tb order by k_timestamptz;
select ST_Distance(e1,e2) from test_geometry.tb order by k_timestamptz;
drop table test_geometry.tb;
drop database test_geometry CASCADE;

-- test_case0019
create ts database test_geometry;
create table test_geometry.tb(k_timestamptz timestamptz not null ,e1 geometry , e2  geometry ) tags(tag1 int not null) primary  tags(tag1);
insert into test_geometry.tb(e1,e2,tag1)values('Point(0.0 0.0)','Linestring(1.0 1.0, 2.0 2.0)',1);
insert into test_geometry.tb(e1,tag1)values('Point(1.0 0.0)',2);
insert into test_geometry.tb(tag1)values(3);
select * from test_geometry.tb order by tag1;
show tag values from test_geometry.tb;
drop table test_geometry.tb;
drop database test_geometry CASCADE;

-- test_case0020
create ts database test_geometry;
create table test_geometry.tb(k_timestamptz timestamptz not null ,e1 geometry , e2  geometry ) tags(tag1 int not null) primary  tags(tag1);
insert into test_geometry.tb(k_timestamptz,e1,e2,tag1)values('2023-05-01 10:12','Point(0.0 0.0)','Linestring(1.0 1.0, 2.0 2.0)',1);
insert into test_geometry.tb(k_timestamptz,e1,e2,tag1)values('2023-05-01 10:13','Point(1.0 1.0)','Linestring(3.0 3.0, 4.0 4.0)',1);
insert into test_geometry.tb(e2,tag1)values('Linestring(4.0 4.0, 5.0 5.0)' ,1);
select * from test_geometry.tb order by tag1;
show tag values from test_geometry.tb;
drop table test_geometry.tb;
drop database test_geometry CASCADE;

-- test_case0021
create ts database test_geometry;
create table test_geometry.tb(k_timestamptz timestamptz not null ,e1 geometry , e2  geometry ) tags(tag1 int not null) primary  tags(tag1);
insert into test_geometry.tb(k_timestamptz,e1,e2,tag1,tag1)values('2023-05-01 10:12','Point(0.0 0.0)','Linestring(1.0 1.0, 2.0 2.0)',1,2);
insert into test_geometry.tb(k_timestamptz,e1,e1,tag1)values('2023-05-01 10:12','Point(0.0 0.0)','Linestring(1.0 1.0, 2.0 2.0)',2);
select * from test_geometry.tb order by tag1;
show tag values from test_geometry.tb;
drop table test_geometry.tb;
drop database test_geometry CASCADE;

-- test_case0022
create ts database test_geometry;
create table test_geometry.tb(k_timestamptz timestamptz not null ,e1 geometry , e2  geometry ) tags(tag1 int not null,tag2 int) primary  tags(tag1);
insert into test_geometry.tb(k_timestamptz,e1,e2,tag1)values('2023-05-01 10:12','Point(0.0 0.0)','Linestring(1.0 1.0, 2.0 2.0)',1.2);
insert into test_geometry.tb(k_timestamptz,e1,e2,tag1)values('2023-05-01 10:12','Point(0.0 0.0)','abc',1);
select * from test_geometry.tb order by tag1;
show tag values from test_geometry.tb;
drop table test_geometry.tb;
drop database test_geometry CASCADE;

-- query
-- test_case0023
USE defaultdb;
DROP DATABASE IF EXISTS test_SELECT_col_geo cascade;
CREATE ts DATABASE test_SELECT_col_geo;
USE defaultdb;DROP DATABASE IF EXISTS test_SELECT_col_geo cascade;
CREATE ts DATABASE test_SELECT_col_geo;
CREATE TABLE test_SELECT_col_geo.t1(
  k_timestamp timestamptz NOT NULL,
  a1 geometry ,
  b1 geometry ,
  c1 geometry ,
  a2 geometry ,
  b2 geometry ,
  c2 geometry
  ) ATTRIBUTES (code1 int2 NOT NULL) primary tags(code1);
INSERT INTO test_SELECT_col_geo.t1 VALUES(now(),'Point(0.0 0.0)', 'Linestring(1.0 1.0, 2.0 2.0)', 'Polygon((1.0 1.0, 2.0 2.0, 3.0 2.0, 1.0 1.0))','Point(0.0 0.0)', 'Linestring(1.0 1.0, 5.0 5.0)','Polygon((4.0 1.0, 2.0 2.0, 3.0 2.0, 4.0 1.0))',1);
INSERT INTO test_SELECT_col_geo.t1 VALUES(now(),'Point(0.0 2.0)', 'Linestring(1.0 1.0, 4.0 4.0)', 'Polygon((5.0 6.0, 2.0 2.0, 3.0 2.0, 5.0 6.0))','Point( 6.6 1.3)', 'Linestring(1.1 1.0, 4.0 4.0)','Polygon((4.0 1.8, 2.0 2.8, 3.0 2.8, 4.0 1.8))',2);
INSERT INTO test_SELECT_col_geo.t1 VALUES(now(),'Point(0.0 3.0)', 'Linestring(1.0 1.0, 2.0 3.0)', 'Polygon((7.0 7.0, 2.0 5.0, 3.0 1.0, 7.0 7.0))','Point(1.0 2.0)', 'Linestring(1.2 1.2, 8.0 8.0)','Polygon((5.6 1.0, 2.2 2.2, 3.3 2.0, 5.6 1.0))',3);
INSERT INTO test_SELECT_col_geo.t1 VALUES(now(),'Point(0.0 4.0)', 'Linestring(4.0 6.0, 5.0 1.0)', 'Polygon((1.0 4.0, 2.1 2.6, 3.1 2.2, 1.0 4.0))','Point(4.1 7.8)', 'Linestring(1.2 3.0, 5.0 5.0)','Polygon((4.0 1.0, 2.0 2.0, 3.0 2.0, 3.0 3.0, 4.0 1.0))',4);
INSERT INTO test_SELECT_col_geo.t1 VALUES(now(),'Point(0.0 5.0)', 'Linestring(7.0 3.0, 2.0 0.0)', 'Polygon((2.0 7.0, 2.4 6.0, 5.0 2.8, 2.0 7.0))','Point(5.6 6.5)', 'Linestring(1.4 4.0, 6.0 5.2)','Polygon((4.0 1.0, 2.0 8.0, 8.0 2.0, 5.0 0.0, 4.0 1.0))',5);
INSERT INTO test_SELECT_col_geo.t1 VALUES(now(),'Point(1.0 3.0)', 'Linestring(4.0 1.0, 7.0 7.0)', 'Polygon((3.0 8.0, 5.0 3.6, 3.0 5.0, 3.0 8.0))','Point(8.1 1.2)', 'Linestring(6.0 4.0, 5.7 5.4)','Polygon((4.0 1.0, 2.0 2.0, 3.0 2.0, 5.0 1.0, 4.0 1.0))',6);
INSERT INTO test_SELECT_col_geo.t1 VALUES(now(),'Point(1.0 8.0)', 'Linestring(4.0 2.0, 5.0 5.0)', 'Polygon((5.0 2.0, 7.8 2.0, 7.0 4.1, 5.0 2.0))','Point(3.3 1.9)', 'Linestring(7.0 1.0, 3.2 4.5)','Polygon((4.0 0.0, 2.0 8.0, 0.0 4.0, 4.0 0.0))',7);
INSERT INTO test_SELECT_col_geo.t1(k_timestamp,a1,b1,c1,a2,b2,c2,code1) VALUES(2021681353000,'Point(2.5 3.6)', 'Linestring(1.0 1.5, 2.6 3.8)', 'Polygon((4.0 5.0, 1.5 7.0, 3.9 2.0, 4.0 5.0))','Point(11.5 12.0)', 'Linestring(3.0 8.0, 5.4 8.0)','Polygon((3.0 1.0, 3.0 2.0, 2.0 2.0, 2.0 1.0, 2.5 0.0, 3.0 1.0))',8);
INSERT INTO test_SELECT_col_geo.t1(k_timestamp,a1,code1) VALUES(2021684953000,'Point(0.0 0.0)',9);
INSERT INTO test_SELECT_col_geo.t1(k_timestamp,a2,code1) VALUES(2021688553000,'Point(1.0 1.0)',10);
INSERT INTO test_SELECT_col_geo.t1(k_timestamp,b1,code1) VALUES(2021774953000,'Linestring(1.0 1.0, 2.0 2.0)',11);
INSERT INTO test_SELECT_col_geo.t1(c1,code1) VALUES('Polygon((5.6 1.0, 2.2 2.2, 3.3 2.0, 5.6 1.0))',12);
INSERT INTO test_SELECT_col_geo.t1(code1) VALUES(13);

SELECT a1,b1,c1,a2,b2,c2,code1 FROM test_SELECT_col_geo.t1 order by k_timestamp;
SELECT k_timestamp,a1,b1,c1,a2,b2,c2 FROM test_SELECT_col_geo.t1 WHERE k_timestamp > now() order by k_timestamp;
SELECT code1,a1,b1,c1,a2,b2,c2 FROM test_SELECT_col_geo.t1 order by k_timestamp;
SELECT a1 FROM test_SELECT_col_geo.t1 order by k_timestamp;
SELECT b1 FROM test_SELECT_col_geo.t1 order by k_timestamp;
SELECT c1 FROM test_SELECT_col_geo.t1 order by k_timestamp;
SELECT a2 FROM test_SELECT_col_geo.t1 order by k_timestamp;
SELECT b2 FROM test_SELECT_col_geo.t1 order by k_timestamp;
SELECT c2 FROM test_SELECT_col_geo.t1 order by k_timestamp;
SELECT a1,b1,c1,a2,b2,c2,code1 FROM test_SELECT_col_geo.t1 WHERE k_timestamp IS NULL order by k_timestamp;
SELECT a1,b1,c1,a2,b2,c2,code1 FROM test_SELECT_col_geo.t1 WHERE k_timestamp IS NOT NULL order by k_timestamp;
SELECT a1,b1,c1,a2,b2,c2,code1 FROM test_SELECT_col_geo.t1 WHERE a1 IS NOT NULL order by k_timestamp;
SELECT a1,b1,c1,a2,b2,c2,code1 FROM test_SELECT_col_geo.t1 WHERE a1 IS NOT NULL and b1 is NULL order by k_timestamp;
SELECT a1,b1,c1,a2,b2,c2,code1 FROM test_SELECT_col_geo.t1 WHERE a1 IS NOT NULL or b1 is NOT NULL order by k_timestamp;
SELECT a1,b1,c1,a2,b2,c2,code1 FROM test_SELECT_col_geo.t1 WHERE a1 IS NOT NULL OR b1 IS NULL order by k_timestamp;
SELECT a1,b1,c1,a2,b2,c2,code1 FROM test_SELECT_col_geo.t1 WHERE a1 IS NOT NULL AND b1 IS NULL AND a2 IS NOT NULL AND b2 IS NULL AND c1 IS NOT NULL AND c2 IS NULL order by k_timestamp;
SELECT concat(a1,b1) FROM test_SELECT_col_geo.t1 where concat(a1,b1) IS NULL order by k_timestamp;
select a1,b1,c1,a2,b2,c2,code1 from test_select_col_geo.t1 where st_equals(a1,a2) order by k_timestamp;
select a1,b1,c1,a2,b2,c2,code1 from test_select_col_geo.t1 where st_distance(a1,c1) < 10 order by k_timestamp;
SELECT a1+a2 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT b1+c2 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT a1+NULL FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT b1-b2 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT c1-NULL FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT a1*c2 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT b2*NULL FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT a1/c2 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT c1/0 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT a1/NULL FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT b1//b2 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT a1//8 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT b1//0 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT c1//NULL FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT c1%c2 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT b1%2 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT a1%NULL FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT a1^2 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT b1^0 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT c1^NULL FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT a1^9223372036854775807 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT c1&c2 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT a1&b2 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
SELECT b1||b2 FROM test_SELECT_col_geo.t1 ORDER BY k_timestamp;
drop database test_SELECT_col_geo cascade;

-- bug-34378
-- test_case0024
create ts database test_geometry;
create table test_geometry.tb(k_timestamptz timestamptz not null ,e1 geometry , e2  geometry ) tags(tag1 int not null) primary  tags(tag1);
prepare p1 as insert into test_geometry.tb values($1, $2, $3, $4);
execute p1 ('2023-05-01 10:10', 'Linestring(0.0 0.0,0.0 0.0)','Linestring(1.0 1.0, 2.0 2.0)',0);
execute p1 ('2023-05-01 10:11', 'Linestring(0 0,1 1)','Linestring(1 1,60 90)',1);
execute p1 ('2023-05-01 10:12', 'Linestring(-1 -1,0.0 0.0)','Linestring(0 -1,1 1)',2);
execute p1 ('2023-05-01 10:13', 'Linestring(-32768 -1,1 32768)','Linestring(0 -1,1000 1000)',3);
execute p1 ('2023-05-01 10:14', 'Linestring(-1 -1,0 32767)','Linestring(0 32767,-1 -1)',4);
execute p1 ('2023-05-01 10:15', 'Linestring(-2147483648 -2147483648,0 2147483647)','Linestring(0 2147483647,-2147483648 -214748364)',5);
execute p1 ('2023-05-01 10:16', 'Linestring(-9223372036854775807 -1,0 9223372036854775807)','Linestring(0 9223372036854775807,-9223372036854775807 -1)',6);
execute p1 ('2023-05-01 10:17', 'Linestring(1000.101156789 1000.101156789,0 -2000.2022)','Linestring(0 -2000.2022,1000.101156789 1000.101156789)',7);
execute p1 ('2023-05-01 10:18', 'Linestring(-32767.111156 -1,0 -32767.111156)','Linestring(0 -32767.111156,-32767.111156 -1)',8);
execute p1 ('2023-05-01 10:19', 'Linestring(1.2345678911111 -1,0 2147483646.6789)','Linestring(0 2147483646.6789,1.2345678911111 -1)',9);
execute p1 ('2023-05-01 10:20', 'Linestring(-1.0000000 -1.0000,0 9223372036854775806.12345)','Linestring(0 9223372036854775806.12345,-1.0000000 -1.0000)',10);
execute p1 ('2023-05-01 10:21', 'Linestring(0.0000000 0.0000,1.435243 7.673421)','Linestring(600000.606066666 9223372036854775806.12345,0.0000000 0.0000)',11);
execute p1 ('2023-05-01 10:22', 'Linestring(0 0,1 1,2 3,3 3,4 4,5 5,6 6,9 9,111 445,999 999999)','Linestring(0.0 0.0,1.0 2.0,3.0 4.0,5.0 5.0,6.0 7.0,8.0 9.0,11.0 15.0,45.0 22.0,0.1111 0.0,9.0 0.0,0.0 0.0)',1);
select * from test_geometry.tb order by k_timestamptz;
select ST_Distance(e1,e2) from test_geometry.tb order by k_timestamptz;
drop table test_geometry.tb;
drop database test_geometry CASCADE;