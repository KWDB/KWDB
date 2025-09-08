alter schedule scheduled_table_retention Recurring  '*/10 * * * * * *';
CREATE ts DATABASE d_lifetime RETENTIONS 100000000000000 YEAR;
CREATE ts DATABASE d_lifetime RETENTIONS 1001Y;
CREATE ts DATABASE d_lifetime RETENTIONS 13000MONTH;
CREATE ts DATABASE d_lifetime RETENTIONS 13000MON;
CREATE ts DATABASE d_lifetime RETENTIONS 100000000000000W;
CREATE ts DATABASE d_lifetime RETENTIONS 100000000000000WEEK;
CREATE ts DATABASE d_lifetime RETENTIONS 100000000000000DAY;
CREATE ts DATABASE d_lifetime RETENTIONS 100000000000000D;
CREATE ts DATABASE d_lifetime RETENTIONS 100000000000000H;
CREATE ts DATABASE d_lifetime RETENTIONS 100000000000000HOUR;
CREATE ts DATABASE d_lifetime RETENTIONS 100000000000000M;
CREATE ts DATABASE d_lifetime RETENTIONS 100000000000000MINUTE;
CREATE ts DATABASE d_lifetime RETENTIONS 100000000000000S;
CREATE ts DATABASE d_lifetime RETENTIONS 100000000000000SECOND;

create ts database tsdb Retentions 10d;
create table tsdb.t(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag); --t: 10d
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) Retentions 15d;  --t1: 15d
SELECT RETENTIONS from tsdb.information_schema.schemata where schema_name='public' order by RETENTIONS;
SHOW RETENTIONS ON TABLE tsdb.t;
SHOW RETENTIONS ON TABLE tsdb.t1;
SELECT LIFETIME from kwdb_internal.kwdb_retention where table_name = 't';
SELECT LIFETIME from kwdb_internal.kwdb_retention where table_name = 't1';
alter ts database tsdb set Retentions = 15d; -- tsdb, t1: 15d, t:10d
SELECT RETENTIONS from tsdb.information_schema.schemata where schema_name='public' order by RETENTIONS;
SHOW RETENTIONS ON TABLE tsdb.t;
SHOW RETENTIONS ON TABLE tsdb.t1;
SELECT LIFETIME from kwdb_internal.kwdb_retention where table_name = 't';
SELECT LIFETIME from kwdb_internal.kwdb_retention where table_name = 't1';
alter ts database tsdb set Retentions = 20d;  -- tsdb：20d，t:10 t1:15d
SELECT RETENTIONS from tsdb.information_schema.schemata where schema_name='public' order by RETENTIONS;
SHOW RETENTIONS ON TABLE tsdb.t;
SHOW RETENTIONS ON TABLE tsdb.t1;
SELECT LIFETIME from kwdb_internal.kwdb_retention where table_name = 't';
SELECT LIFETIME from kwdb_internal.kwdb_retention where table_name = 't1';
alter table tsdb.t1 set Retentions = 25d;  -- t1: 25d
create table tsdb.t2(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag); --t2: 20d
SHOW RETENTIONS ON TABLE tsdb.t1;
SHOW RETENTIONS ON TABLE tsdb.t2;
SELECT LIFETIME from kwdb_internal.kwdb_retention where table_name = 't1';
SELECT LIFETIME from kwdb_internal.kwdb_retention where table_name = 't2';
DROP database tsdb;

create ts database test1;
CREATE TABLE test1.t1(k_timestamp TIMESTAMP not null, a int, b int, c int, d int) TAGS (tag1 int not null) PRIMARY TAGS (tag1) retentions 1m;
insert into test1.t1 values('2024-01-24 10:33:36',101, 2, 3, 5, 1);
insert into test1.t1 values('2024-01-24 10:33:37',101, 2, 3, 5, 1);
insert into test1.t1 values('2024-01-24 10:33:38',101, 2, 3, 5, 1);
insert into test1.t1 values('2024-01-24 10:33:39',101, 2, 3, 5, 1);
insert into test1.t1 values('2024-01-24 10:33:40',101, 2, 3, 5, 1);
insert into test1.t1 values('1900-01-01 01:01:01',101, 2, 3, 5, 1);
insert into test1.t1 values('1930-01-01 01:01:01',101, 2, 3, 5, 1);
insert into test1.t1 values('1900-05-01 01:01:01',101, 2, 3, 5, 1);
-- select count(*) from test1.t1;

create ts database tsdb;
create table tsdb.t(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag); --t: 0
SELECT LIFETIME from kwdb_internal.kwdb_retention where table_name = 't';
show create table tsdb.t;
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) activetime 100d;
create table tsdb.t2(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) activetime 1000Y;
create table tsdb.t3(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) activetime 1001YEAR;
create table tsdb.t3(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) activetime 1000month;
show create table tsdb.t1;
show create table tsdb.t2;
show create table tsdb.t3;
alter table tsdb.t1 set activetime = 100000s;
alter table tsdb.t2 set activetime = 100000000000000WEEK;
alter table tsdb.t3 set activetime = 10hour;
show create table tsdb.t1;
show create table tsdb.t2;
show create table tsdb.t3;
drop database tsdb;

CREATE TS DATABASE ts_db8 retentions 10year;
use ts_db8;
create TABLE tb(k_timestamp timestamptz not null,e1 timestamp ,e2 smallint ,e3 int ,e4 bigint ,e5 float ,e6 float ,e7 char(5) ,e8 bool ,e9 varbytes(10)) ATTRIBUTES (code1 int not null) primary tags(code1);
INSERT into tb VALUES( '2024-01-28 00:01:45.137+00:00', 1111111111000,1000,1000000,100000000000000000,100000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
INSERT into tb VALUES( '2024-01-28 00:02:45.137+00:00', 2222222222000,2000,2000000,200000000000000000,200000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
INSERT into tb VALUES( '2024-01-28 00:03:45.137+00:00', 3333333333000,3000,3000000,300000000000000000,300000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
INSERT into tb VALUES( '2024-01-28 00:04:45.137+00:00', 4444444444000,4000,4000000,400000000000000000,400000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
INSERT into tb VALUES( '2024-01-28 00:05:45.137+00:00', 5555555555000,5000,5000000,500000000000000000,500000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
INSERT into tb VALUES( '2024-01-28 00:06:45.137+00:00', 6666666666000,1000,1000000,100000000000000000,100000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
INSERT into tb VALUES( '2024-01-28 00:07:45.137+00:00', 7777777777000,2000,2000000,200000000000000000,200000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
INSERT into tb VALUES( '2024-01-28 00:08:45.137+00:00', 3333333333000,3000,3000000,300000000000000000,300000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
INSERT into tb VALUES( '2024-01-28 00:09:45.137+00:00', 4444444444000,4000,4000000,400000000000000000,400000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
INSERT into tb VALUES( '2024-01-28 00:10:45.137+00:00', 5555555555000,5000,5000000,500000000000000000,500000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
INSERT into tb VALUES( '0000-01-01 00:00:00.000+00:00', 3333333333000,3000,3000000,300000000000000000,300000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
INSERT into tb VALUES( '0010-01-01 00:09:45.137+00:00', 4444444444000,4000,4000000,400000000000000000,400000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
INSERT into tb VALUES( '0020-01-28 00:10:45.137+00:00', 5555555555000,5000,5000000,500000000000000000,500000000000000000.101,100000000000000000.10101010101,'testb',true,'1010100011',1);
-- select count(*) from tb;

CREATE TS DATABASE test_lifetime;
create table test_lifetime.tb(
k_timestamp timestamptz not null,
e1 int2 not null,
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
) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color) RETENTIONS 10YEAR;
INSERT INTO test_lifetime.tb VALUES('2000-01-02 01:01:00+00:00',0,0,0,0,0,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',100,200,300,false,100.0,200.0,'beijing','red',b'\x26','fuluolidazhou','160','big','2','社会性别女','1','cuteandlovely');
INSERT INTO test_lifetime.tb VALUES('2000-01-02 01:02:00+00:00',20002,1000002,20000000002,1047200.0000,-109810.0,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',200,200,300,false,100.0,200.0,'beijing','red',b'\x26','fuluolidazhou','160','big','2','社会性别女','1','cuteandlovely');
INSERT INTO test_lifetime.tb values('2024-02-02 01:02:00+00:00',5,null,1000,null,100.0,null,'2020-1-7 12:00:00.000',null,'test时间精度通用查询测试,！！！@TEST1',null,'t',null,'中',null,'test时间精度通用查询测试,！！！@TEST1',null,'test时间精度通用查询测试,！！！@TEST1',null,b'\xaa',null,'test时间精度通用查询测试',null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO test_lifetime.tb VALUES('2024-02-02 01:01:00+00:00',-10001,-3000003,-40000000004,-39845.87,-200.123456,true,'2020-1-1 12:00:00.000','','','','','','','','','','','','','','','',400,400,100,false,400.0,300.0,'','','','','','','','','','');
INSERT INTO test_lifetime.tb values('1024-02-02 01:02:00+00:00',5,null,1000,null,100.0,null,'2020-1-7 12:00:00.000',null,'test时间精度通用查询测试,！！！@TEST1',null,'t',null,'中',null,'test时间精度通用查询测试,！！！@TEST1',null,'test时间精度通用查询测试,！！！@TEST1',null,b'\xaa',null,'test时间精度通用查询测试',null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO test_lifetime.tb VALUES('1124-02-02 01:01:00+00:00',-10001,-3000003,-40000000004,-39845.87,-200.123456,true,'2020-1-1 12:00:00.000','','','','','','','','','','','','','','','',400,400,100,false,400.0,300.0,'','','','','','','','','','');
-- SELECT k_timestamp FROM test_lifetime.tb order by k_timestamp;
ALTER TABLE test_lifetime.tb SET RETENTIONS=1week;
SELECT pg_sleep(60);

select count(*) from test1.t1;
drop database test1;

select count(*) from tb;
drop DATABASE ts_db8;

SELECT k_timestamp FROM test_lifetime.tb order by k_timestamp;
-- 0 rows
DROP TABLE test_lifetime.tb;

DROP DATABASE test_lifetime CASCADE;

alter schedule scheduled_table_retention Recurring  '@hourly';