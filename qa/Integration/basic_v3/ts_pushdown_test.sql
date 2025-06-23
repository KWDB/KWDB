DROP DATABASE IF EXISTS tsdb CASCADE;
CREATE ts DATABASE tsdb;
CREATE table tsdb.t1(k_timestamp timestamp not null,t1 int ) tags (t1_attribute int not null) primary tags(t1_attribute);
insert into tsdb.t1 values(now(),1234, 1);
select count(*) from (select distinct * from tsdb.t1);
DROP DATABASE IF EXISTS tsdb CASCADE;

-- push down optimize( count(*), date_trunc(), limit)
create ts database benchmark;
create table benchmark.cpu (ts timestamp not null,usage_user bigint,usage_system bigint,usage_idle bigint,usage_nice bigint,usage_iowait bigint,usage_irq bigint,usage_softirq bigint,usage_steal bigint,usage_guest bigint,usage_guest_nice bigint) tags (hostname varchar not null,region varchar,datacenter varchar,rack varchar,os varchar,arch varchar,team varchar,service varchar,service_version varchar,service_environment varchar)
primary tags(hostname);

use benchmark;
insert into benchmark.cpu (hostname, region, datacenter, rack, os, arch, team, service, service_version, service_environment) values ('host_0','eu-central-1','eu-central-1a','6','Ubuntu15.10','x86','SF','19','1','test');
insert into benchmark.cpu (hostname, region, datacenter, rack, os, arch, team, service, service_version, service_environment) values ('host_1','us-west-1','us-west-1a','41','Ubuntu15.10','x64','NYC','9','1','staging');
insert into benchmark.cpu (hostname, region, datacenter, rack, os, arch, team, service, service_version, service_environment) values ('host_2','sa-east-1','sa-east-1a','89','Ubuntu16.04LTS','x86','LON','13','0','staging');

-- push count(*)
explain select count(*) from cpu where hostname = 'host_0';
explain select count(*) from cpu;

-- push date_trunc
explain select  time_bucket(ts, '3600s') as date, count(distinct usage_system) as nums from cpu where hostname = 'host_0' and ts >= date_trunc('week', '2023-10-30 12:12:12'::timestamp) and ts < '2023-10-30 12:12:12' and usage_system = 1 group by date order by date;
explain select  time_bucket(ts, '3600s') as date, count(distinct usage_system) as nums from cpu where hostname = 'host_0' and ts >= date_trunc('week', '2023-10-30 12:12:12'::timestamp) and ts < '2023-10-30 12:12:12' and usage_system = 1 group by date order by date;

-- push order by, limit
explain select usage_user from cpu where hostname = 'host_0' and usage_system>2000 order by usage_user;
explain select usage_user from cpu where hostname = 'host_0' and usage_system>2000 limit 1;
explain select usage_user from cpu where hostname = 'host_0' and usage_system>2000 order by usage_user limit 2;
explain select usage_user from cpu where usage_system>2000 order by usage_idle;
explain select usage_user from cpu where usage_system>2000 limit 1;
explain select usage_user from cpu where usage_system>2000 order by usage_idle limit 2;

-- all push when group by tag
explain select max(usage_user) from cpu where ts>'2022-2-1 0:0:0' and ts<'2022-2-2 0:0:0' group by hostname;
explain select first(usage_user) as first,avg(usage_system) as pac from cpu  where ts>'2022-2-1 0:0:0' and ts<'2022-2-8 0:0:0' group by hostname;

-- open all_push_down
explain select count(*) from cpu where hostname = 'host_0';
explain select count(*) from cpu;
explain select count(*) from cpu group by hostname;

explain select  time_bucket(ts, '3600s') as date, count(distinct usage_system) as nums from cpu where hostname = 'host_0' and ts >= date_trunc('week', '2023-10-30 12:12:12'::timestamp) and ts < '2023-10-30 12:12:12' and usage_system = 1 group by date order by date;
explain select  time_bucket(ts, '3600s') as date, count(distinct usage_system) as nums from cpu where ts >= date_trunc('week', '2023-10-30 12:12:12'::timestamp) and ts < '2023-10-30 12:12:12' and usage_system = 1 group by date order by date;

explain select usage_user from cpu where hostname = 'host_0' and usage_system>2000 order by usage_user;
explain select usage_user from cpu where hostname = 'host_0' and usage_system>2000 limit 1;
explain select usage_user from cpu where hostname = 'host_0' and usage_system>2000 order by usage_user limit 2;
explain select usage_user from cpu where usage_system>2000 order by usage_user;
explain select usage_user from cpu where usage_system>2000 limit 1;
explain select usage_user from cpu where usage_system>2000 order by usage_user limit 2;

explain select max(usage_user) from cpu where ts>'2022-2-1 0:0:0' and ts<'2022-2-2 0:0:0' group by hostname;
explain select first(usage_user) as first,avg(usage_system) as pac from cpu  where ts>'2022-2-1 0:0:0' and ts<'2022-2-8 0:0:0' group by hostname;

drop database benchmark cascade;

-- ZDP-27525 【kaiwudb】【1.2-1117dev11】
CREATE TS DATABASE  ts_db;
CREATE TABLE ts_db.st(k_timestamp TIMESTAMP not null, e1 INT2, e2 INT, e3 INT8, e4 FLOAT4, e5 FLOAT8, e6 BOOL, e7 TIMESTAMP, e8 CHAR(100), e9 NCHAR(255), e10 VARCHAR(4096), e11 CHAR, e12 NCHAR, e13 VARCHAR, e14 NVARCHAR(4096),  e15 VARBYTES, e16 NVARCHAR, e17 varbytes, e18 varbytes(100), e19 VARBYTES(4096) ) tags (tabName varchar(10) not null, code1 INT2,code2 INT,code3 INT8,flag BOOL,val1 FLOAT4,val2 FLOAT8,location VARCHAR,color VARCHAR(65536),name varbytes,state varbytes(1023),tall VARBYTES,screen VARBYTES(65536),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(tabName);
INSERT INTO ts_db.st values(100000,100,1000000,1000,1000.0000,100.0,true,'2020-1-2 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','st_t1', NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO ts_db.st values(200000,100,1000000,1000,1000.0000,100.0,true,'2020-1-2 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','st_t1', NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO ts_db.st values(300000,100,1000000,1000,1000.0000,100.0,true,'2020-1-2 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','st_t1', NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO ts_db.st values(400000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'st_t1', NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO ts_db.st values(100000,100,1000000,1000,1000.0000,100.0,true,'2020-1-2 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','st_t2', 100,200,300,false,100.0,200.0,'beijing','red',b'\x26','fuluolidazhou','160','big','2','社会性别女','1','cuteANDlovely');
INSERT INTO ts_db.st values(200000,100,1000000,1000,1000.0000,100.0,true,'2020-1-2 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','st_t2', 100,200,300,false,100.0,200.0,'beijing','red',b'\x26','fuluolidazhou','160','big','2','社会性别女','1','cuteANDlovely');
INSERT INTO ts_db.st values(300000,100,1000000,1000,1000.0000,100.0,true,'2020-1-2 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','st_t2', 100,200,300,false,100.0,200.0,'beijing','red',b'\x26','fuluolidazhou','160','big','2','社会性别女','1','cuteANDlovely');
INSERT INTO ts_db.st values(400000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'st_t2', 100,200,300,false,100.0,200.0,'beijing','red',b'\x26','fuluolidazhou','160','big','2','社会性别女','1','cuteANDlovely');
INSERT INTO ts_db.st values(100000,100,1000000,1000,1000.0000,100.0,true,'2020-1-2 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','st_t3', 300,400,100,true,400.0,300.0,'tianjin','yellow',b'\x28','aisaiebiyazhou','183','small','3','社会性别男','7','matureANDgentle');
INSERT INTO ts_db.st values(200000,100,1000000,1000,1000.0000,100.0,true,'2020-1-2 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','st_t3', 300,400,100,true,400.0,300.0,'tianjin','yellow',b'\x28','aisaiebiyazhou','183','small','3','社会性别男','7','matureANDgentle');
INSERT INTO ts_db.st values(300000,100,1000000,1000,1000.0000,100.0,true,'2020-1-2 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','st_t3', 300,400,100,true,400.0,300.0,'tianjin','yellow',b'\x28','aisaiebiyazhou','183','small','3','社会性别男','7','matureANDgentle');
INSERT INTO ts_db.st values(400000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'st_t3', 300,400,100,true,400.0,300.0,'tianjin','yellow',b'\x28','aisaiebiyazhou','183','small','3','社会性别男','7','matureANDgentle');
SELECT e7 FROM ts_db.st WHERE now()-INTERVAL'60MS'>'1970-1-1 08:01:40' ORDER BY k_timestamp;
-- explain SELECT e7 FROM ts_db.st WHERE now()-e7>60s;
-- explain SELECT e7 FROM ts_db.st WHERE now()-'1970-1-1 08:01:40'>60s;
-- explain SELECT e7 FROM ts_db.st WHERE now()-INTERVAL'60MS'>e7;
-- explain SELECT e7 FROM ts_db.st WHERE now()+60S>e7;
use defaultdb;
DROP DATABASE ts_db cascade;

-- 【master分支】【coalesce函数】count函数结合coalesce函数查询错误
-- https://e.gitee.com/kaiwuDB/issues/table?issue=ICCJOV
create ts database test_timebucket_gapfill_ns;
create table test_timebucket_gapfill_ns.tb2(k_timestamp timestamptz not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096),e23 timestamp(6),e24 timestamp(9),e25 timestamptz(6),e26 timestamptz(9)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t2,t7,t9,t10);
insert into test_timebucket_gapfill_ns.tb2 values('2024-02-09 16:16:58.22522','2021-06-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','中文  中文', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,'2011-03-06 03:06:06.0654432','2023-05-05 05:05:05.0550005555','2022-09-21 20:20:17.14566666','2029-09-30 09:30:32.268688888',5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_timebucket_gapfill_ns.tb2 values('2024-02-10 04:18:19.2232','2021-06-10 10:00:00.1192',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,'2015-01-21 01:21:21.2232331','2050-10-15 10:25:25.257309656','2051-10-10 10:10:11.84598761','2228-06-06 11:01:12.2683567',6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
select t1,t2,coalesce(null,count(*)+count(t2),10) from test_timebucket_gapfill_ns.tb2 group by t1,t2 order by t1,t2;

use defaultdb;
DROP DATABASE test_timebucket_gapfill_ns cascade;