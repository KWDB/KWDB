SET CLUSTER SETTING ts.blocks_per_segment.max_limit=100;
SET CLUSTER SETTING ts.rows_per_block.max_limit=100;
-- create database and table
create ts database db1;
use db1;
create table t1(k_timestamp timestamp not null,e1 char(20) , e2 timestamp , e3 smallint , e4 int , e5 bigint , e6 float , e7 double) tags(tag1 int not null, tag2 int) primary tags(tag1);
-- insert values
insert into t1 values(1000,'a', 1667597776000, 1, 16, -1, 2.2, 10.0, 10, 11);
insert into t1 values(10000,'a', 1667597776000, 10, 16, -1, 2.2, 10.0, 10, 11);

select time_bucket_gapfill(k_timestamp,'1s') as ts from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,'1s') as ts, interpolate(max(e3), 333) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,'1s') as ts, interpolate(max(e3), PREV) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,'1s') as ts, interpolate(max(e3), NEXT) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,'1s') as ts, interpolate(max(e3), LINEAR) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,'1s') as ts, interpolate(max(e3), NULL) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,1) as ts from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), 333) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), PREV) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), NEXT) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), LINEAR) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), NULL) from t1 group by ts order by ts;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), NULL) from t1 group by ts order by ts limit 1;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), NULL) from t1 group by ts order by ts limit 2;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), NULL) from t1 group by ts order by ts limit 5;
select time_bucket_gapfill(k_timestamp,1) as ts, interpolate(max(e3), NULL) from t1 group by ts order by ts limit 10;
drop database db1;

create ts database t1;
create table t1.d1(k_timestamp timestamptz not null ,e1 bigint  not null, e2 char(20) not null , e3 timestamp  not null , e4 int not null, e5 smallint not null, e6 float not null, e7 bigint not null, e8 smallint not null, e9 float  not null, e10 float not null )  tags(t1_d1 int not null ) primary tags(t1_d1);
INSERT INTO t1.d1  VALUES (1667590000000, 444444444, 'a', 1667597776000, 98, 1, 499.999, 111111111, 10, 10.10, 0.0001,0);
INSERT INTO t1.d1  VALUES  (1667591000000,111111111, 'b', 1667597777111, 100, 1, 99.999, 111111111, 10, 10.10, 0.0001,1);
INSERT INTO t1.d1  VALUES   (1667592000000, 222222222, 'c', 1667597778112, 99, 1, 299.999, 111111111, 10, 10.10, 0.0001,2);
INSERT INTO t1.d1  VALUES (1667592010000, 333333333, 'd', 1667597779000, 98, 1, 55.999, 111111111, 10, 10.10, 0.0001,3);
INSERT INTO t1.d1  VALUES (1667592600000, 333333333, 'd', 1667597779000, 98, 1, 20.999, 111111111, 10, 10.10, 0.0001,4);
-- Normal Case
select time_bucket_gapfill(k_timestamp,'60s'),interpolate(avg(e6),'null') from t1.d1  group by time_bucket_gapfill(k_timestamp,'60s') order by time_bucket_gapfill(k_timestamp,'60s');
select time_bucket_gapfill(k_timestamp,'3600s'),round(interpolate(avg(e6),'null'),3) from t1.d1 group by time_bucket_gapfill(k_timestamp,'3600s') order by time_bucket_gapfill(k_timestamp,'3600s');
select time_bucket_gapfill(k_timestamp,'9999s'),interpolate(avg(e6),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,'9999s') order by time_bucket_gapfill(k_timestamp,'9999s');
select time_bucket_gapfill(k_timestamp,'300s'),interpolate(count(e2),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,'300s') order by time_bucket_gapfill(k_timestamp,'300s');
select time_bucket_gapfill(k_timestamp,'300s'),interpolate(count(e3),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,'300s') order by time_bucket_gapfill(k_timestamp,'300s');
select time_bucket_gapfill(k_timestamp,'300s'),interpolate(avg(e6),'null'),interpolate(max(e6),'null')from t1.d1 group by time_bucket_gapfill(k_timestamp,'300s') order by time_bucket_gapfill(k_timestamp,'300s');
select time_bucket_gapfill(k_timestamp,'300s'),interpolate(avg(e6),'null'),interpolate(max(e6),Prev)from t1.d1 group by time_bucket_gapfill(k_timestamp,'300s') order by time_bucket_gapfill(k_timestamp,'300s');
select time_bucket_gapfill(k_timestamp,'9999999s'),round(interpolate(avg(e6),null),3) from t1.d1 group by time_bucket_gapfill(k_timestamp,'9999999s') order by time_bucket_gapfill(k_timestamp,'9999999s');
select time_bucket_gapfill(k_timestamp,60),interpolate(avg(e6),'null') from t1.d1  group by time_bucket_gapfill(k_timestamp,60) order by time_bucket_gapfill(k_timestamp,60);
select time_bucket_gapfill(k_timestamp,3600),round(interpolate(avg(e6),'null'),3) from t1.d1 group by time_bucket_gapfill(k_timestamp,3600) order by time_bucket_gapfill(k_timestamp,3600);
select time_bucket_gapfill(k_timestamp,9999),interpolate(avg(e6),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,9999) order by time_bucket_gapfill(k_timestamp,9999);
select time_bucket_gapfill(k_timestamp,300),interpolate(count(e2),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
select time_bucket_gapfill(k_timestamp,300),interpolate(count(e3),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
select time_bucket_gapfill(k_timestamp,300),interpolate(avg(e6),'null'),interpolate(max(e6),'null')from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
select time_bucket_gapfill(k_timestamp,300),interpolate(avg(e6),'null'),interpolate(max(e6),Prev)from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
select time_bucket_gapfill(k_timestamp,9999999),round(interpolate(avg(e6),null),3) from t1.d1 group by time_bucket_gapfill(k_timestamp,9999999) order by time_bucket_gapfill(k_timestamp,9999999);

-- Expected error
select time_bucket_gapfill(k_timestamp,'300s') from t1.d1;
select time_bucket_gapfill(k_timestamp,'300s'),interpolate(max(e2),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,'300s') order by time_bucket_gapfill(k_timestamp,'300s');
select time_bucket_gapfill(k_timestamp,'300s'),interpolate(max(e3),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,'300s') order by time_bucket_gapfill(k_timestamp,'300s');
select time_bucket_gapfill(k_timestamp,'300s'),interpolate(STRING_AGG(e6),null) from t1.d1 group by time_bucket_gapfill(k_timestamp,'300s') order by time_bucket_gapfill(k_timestamp,'300s');
select interpolate(avg(e6),'null') from t1.d1 ;

select time_bucket_gapfill(k_timestamp,'300s'),interpolate(avg(e6),null) from t1.d1  order by time_bucket_gapfill(k_timestamp,'300s');
select time_bucket_gapfill(k_timestamp,300),interpolate(avg(e6),null) from t1.d1  order by time_bucket_gapfill(k_timestamp,300);


-- bug 33471
create table t1.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool ,t5 float4,t6 float8,t7 char,t8 char(100) ,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) ,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1);
insert into t1.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',null,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into t1.tb values ('2020-11-06 17:11:55.123','2019-12-06 18:11:23',null,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ','nchar','','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
select time_bucket_gapfill(k_timestamp,'10s') as a,interpolate(sum(e2),60) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,'10s') as a,interpolate(max(e10),60) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,'10s') as a,interpolate(max(e12),60) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,'10s') as a,interpolate(sum(e7),60) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,'10s') as a,interpolate(count(e15),60) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,'10s') as a,interpolate(min(e15),60) from t1.tb  group by a order by a;

--- bug 35520 35512
select time_bucket_gapfill(k_timestamp,'0s') as a,sum(e2) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,'0s') as a,max(e2) from t1.tb  group by a order by a;
select time_bucket_gapfill(k_timestamp,'-1s') as a,avg(e2) from t1.tb  group by a order by a;

drop database t1;

use defaultdb;drop database if exists test_select_timebucket_gapfill cascade;
create ts database test_select_timebucket_gapfill;
create table test_select_timebucket_gapfill.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);
create table test_select_timebucket_gapfill.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t2,t7,t9,t10);
create table test_select_timebucket_gapfill.tb3(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2,t2 int,t3 int8 not null,t4 bool,t5 float4,t6 float8,t7 char,t8 char(100),t9 nchar,t10 nchar(254),t11 varchar not null,t12 varchar(128),t13 varbytes not null,t14 varbytes(100) not null,t15 varbytes,t16 varbytes(255)) primary tags(t3,t11);
insert into test_select_timebucket_gapfill.tb values ('2011-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_timebucket_gapfill.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-922.123,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_timebucket_gapfill.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','查询查询 ','\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_timebucket_gapfill.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',32767,2147483647,9223372036854775807,true,922.123,500.578578,'','     ',' ','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_timebucket_gapfill.tb values ('2022-05-01 12:10:31.22','2020-05-01 22:30:11',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\', 'v2551cdf~#   ', '  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,300,300,false,60.666,600.678,'','\\test测试！！！@TEST1',' ','test测试！T  EST1xaa','查询查询 ','\\','','    ','','  ');
insert into test_select_timebucket_gapfill.tb values ('2023-05-10 09:08:19.22','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e','a',null,'测试测试 ','b','test测试10_1','vwwws中文_1',null);
insert into test_select_timebucket_gapfill.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'测试测试 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_timebucket_gapfill.tb values ('2023-05-10 09:08:18.223','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','5555 5','  bdbd','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_timebucket_gapfill.tb2 values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','中文  中文', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_timebucket_gapfill.tb2 values ('2023-05-10 09:20:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_timebucket_gapfill.tb3 values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',8,800,8000,false,-20.123,800.578578,'d','test测试！！！@TEST1  ','d','ddd','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_timebucket_gapfill.tb3 values ('2023-05-10 20:43:23.783','2024-06-10 16:16:15.183',800,8000,80000,800000.808888,8000000.808088,true,'d','test测试！！！@TEST1 ','d','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,false,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');

select time_bucket_gapfill(k_timestamp, '10second') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10SECONDS') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10secs') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10sec') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10minute') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10MINUTES') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10mins') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10min') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10m') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10Hour') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10hours') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10hrs') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10hr') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10h') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10day') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10days') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10d') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10week') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10weeks') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10w') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10month') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10months') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10mons') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10mon') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10year') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10years') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10yrs') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10yr') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10y') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '0s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '15s') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10h') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10day') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10w') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10month') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10yr') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;

select time_bucket_gapfill(k_timestamp, '15S') as tb from test_select_timebucket_gapfill.tb where k_timestamp between '2020-11-06 17:10:55.123+00:00' and '2023-05-10 09:08:18.223' group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10Sec') as tb, e2 from test_select_timebucket_gapfill.tb where e2 = 500 and e7 = false and e16 = 'abc255测试1()*&^%{}' group by tb,e2 order by tb,e2 limit 10;
select time_bucket_gapfill(k_timestamp, '10sEcs') as tb,e16 from test_select_timebucket_gapfill.tb where e16 like 'abc255测试1()*&^%{}' group by tb,e16 order by tb,e16 limit 10;
select time_bucket_gapfill(k_timestamp, '10SECOND') as tb, e3, e10 from test_select_timebucket_gapfill.tb where e3 < 6000 or e10 > 'a' group by tb,e3,e10 order by tb,e3,e10 limit 10;
select time_bucket_gapfill(k_timestamp, '10seconds') as tb,e8,t8,t12 from test_select_timebucket_gapfill.tb where k_timestamp+1m>k_timestamp group by tb,e8,t8,t12 order by tb,t8 limit 10;
select time_bucket_gapfill(k_timestamp, '10M') as tb, e8, e12, e13 from test_select_timebucket_gapfill.tb where e8 >= ' ' and e12 <= 'varchar  中文1' or e13 != ' ' group by tb,e8,e12,e13 order by tb,e8,e12,e13 limit 10;
select time_bucket_gapfill(k_timestamp, '5Min') as tb, e4, e5 from test_select_timebucket_gapfill.tb where e4 in (40000,50000) and e5 not in (-500000.5) group by tb,e4,e5 order by tb,e4,e5 limit 10;
select time_bucket_gapfill(k_timestamp, '10MINS') as tb, e6, e14 from test_select_timebucket_gapfill.tb where e6 between 4000000.4040404 and 7000000.1010101 and e14 like 'hof4096%' group by tb,e6,e14 order by tb,e6,e14 limit 10;
select time_bucket_gapfill(k_timestamp,'10mintute') as tb,e1,t2,t7,t9,t10,e10 from test_select_timebucket_gapfill.tb2 where k_timestamp-'2023-02-22 08:00:00'>1w group by tb,e1,t2,t7,t9,t10,e10 order by tb,e1 limit 10;
select time_bucket_gapfill(k_timestamp,'10mInutes') as tb,e19,e22,t3,e7,t11,t13,t14 from test_select_timebucket_gapfill.tb3 where '2023-02-22 08:00:00'-k_timestamp>1d group by tb,e19,e22,t3,e7,t11,t13,t14 order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10H') as tb, e18,e20 from test_select_timebucket_gapfill.tb where e18 is null and e20 is null group by tb,e18,e20 limit 10;
select time_bucket_gapfill(k_timestamp, '10Hr') as tb,t2 from test_select_timebucket_gapfill.tb where t2 between -2147483648 and 2147483647 and t7 = 'd' and t16 = '\x7777343039365f32' group by tb,t2 order by tb,t2 limit 10;
select time_bucket_gapfill(k_timestamp, '10HRS') as tb,t6 from test_select_timebucket_gapfill.tb where t6 in(null) or t6 in(100.111111) group by tb,t6 order by tb desc limit 10;
select time_bucket_gapfill(k_timestamp, '5hOUr') as tb,t3,t10 from test_select_timebucket_gapfill.tb where t3 < 6000 and t10 like '"_\\%' group by tb,t3,t10 order by tb,t3,t10 limit 10;
select time_bucket_gapfill(k_timestamp,'10hours') as tb,k_timestamp,t1,t3,e1,e2 from test_select_timebucket_gapfill.tb where k_timestamp between '2020-11-06 17:10:55.123+00:00' and '2023-05-10 09:08:18.223' group by tb,k_timestamp,t1,t3,e1,e2 order by tb,k_timestamp limit 10;
select time_bucket_gapfill(k_timestamp, '10d') as tb,t8,t12,t13 from test_select_timebucket_gapfill.tb where t8 not in('\\test测试！！！@TEST1') and t12 <= '测试测试' or e13 != ' ' group by tb,t8,t12,t13 order by tb,t8,t12,t13 limit 10;
select time_bucket_gapfill(k_timestamp, '5day') as tb,t4,t5 from test_select_timebucket_gapfill.tb where t4 in (true) and t5 not in (-10.123) group by tb,t4,t5 order by tb,t4,t5 limit 10;
select time_bucket_gapfill(k_timestamp, '10days') as tb,t11,t14 from test_select_timebucket_gapfill.tb where t11 is null and t14 is not null group by tb,t11,t14 order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10W') as tb,t15,t16 from test_select_timebucket_gapfill.tb where t15 is not null and t16 is not null group by tb,t15,t16 order by tb,t15 limit 10;
select time_bucket_gapfill(k_timestamp, '10week') as tb,t1,t4 from test_select_timebucket_gapfill.tb where t1 between -2147483648 and 2147483647 and t4 != false group by tb,t1,t4 order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10Weeks') as tb,t8,t12 from test_select_timebucket_gapfill.tb where t8 not in('\\test测试！！！@TEST1') and t12 <='测试测试' group by tb,t8,t12 order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '5Month') as tb,t2,t7,t9,t10 from test_select_timebucket_gapfill.tb2 where t2 <= 800 and t10 like '\\a' group by tb,t2,t7,t9,t10 order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10monTHs') as tb,t3,t11,t14 from test_select_timebucket_gapfill.tb3 where t11 is null or t14 is not null group by tb,t3,t11,t14 order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10mon') as tb,t13 from test_select_timebucket_gapfill.tb3 where t13 is not null and t13 is not null group by tb,t13 order by tb limit 10;
select time_bucket_gapfill(k_timestamp,'10mons') as tb,t2,t8,t12 from test_select_timebucket_gapfill.tb where k_timestamp+1m>k_timestamp group by tb,t2,t8,t12 order by tb,t2 limit 10;
select time_bucket_gapfill(k_timestamp,'10year') as tb,t8,t9,t10,t12 from test_select_timebucket_gapfill.tb2 where k_timestamp-'2023-02-22 08:00:00'>1w group by tb,t8,t9,t10,t12 order by t8 limit 10;
select time_bucket_gapfill(k_timestamp,'10YEARS') as tb,t14,t3,t7,t13 from test_select_timebucket_gapfill.tb3 where '2023-02-22 08:00:00'-k_timestamp>260d group by tb,t14,t3,t7,t13 order by tb limit 10;
select time_bucket_gapfill(k_timestamp,'10yr') as tb,e2,t8,t12 from test_select_timebucket_gapfill.tb where k_timestamp+1m>k_timestamp group by tb,e2,t8,t12 order by tb,t8 limit 10;
select time_bucket_gapfill(k_timestamp,'10Yrs') as tb,e8,t9,t10,e12 from test_select_timebucket_gapfill.tb2 where k_timestamp-'2023-02-22 08:00:00'>1w group by tb,e8,t9,t10,e12 order by tb,t9 limit 10;
select time_bucket_gapfill(k_timestamp,'10y') as tb,e14,t3,e7,t13 from test_select_timebucket_gapfill.tb3 where '2023-02-22 08:00:00'-k_timestamp>260d group by tb,e14,t3,e7,t13 order by tb limit 10;
select time_bucket_gapfill(k_timestamp,'10s') as tb,e2,t4,e5,t5,t8,t10 from test_select_timebucket_gapfill.tb where e2 = 500 and t10 like '"_\\%' group by tb,e2,t4,e5,t5,t8,t10 order by tb,t4 limit 10;
select time_bucket_gapfill(k_timestamp,'10m') as tb,e3,e6,t7,t9,t11,t15 from test_select_timebucket_gapfill.tb2 where e3 < 6000 and e6 between 4000000.4040404 and 7000000.1010101 group by tb,e3,e6,t7,t9,t11,t15 order by e3 limit 10;
select time_bucket_gapfill(k_timestamp,'10h') as tb,e18,e21,t3,t7,t13,t16 from test_select_timebucket_gapfill.tb3 where e18 is null and e20 is null group by tb,e18,e21,t3,t7,t13,t16 order by tb limit 10;

select time_bucket_gapfill(e2,'10s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(e5,'10s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(e7,'10s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill('e8','10s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(null,'10s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;

select time_bucket_gapfill(k_timestamp,'0s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp,'5.5s') as tb, e2 from test_select_timebucket_gapfill.tb group by tb,e2 order by tb,e2 limit 10;
select time_bucket_gapfill(k_timestamp,'-1000000s') as tb, e2 from test_select_timebucket_gapfill.tb group by tb,e2 order by tb,e2 limit 10;
select time_bucket_gapfill(k_timestamp,'-5s') as tb, e20 from test_select_timebucket_gapfill.tb where e20 is not null group by tb,e20 limit 10;

select time_bucket_gapfill(k_timestamp,0) as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp,'@ s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp,@#%) as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp,'null') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp,null) as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp,'') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp,' ') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;

select time_bucket_gapfill(k_timestamp,'s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp,'10') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;

select time_bucket_gapfill(k_timestamp,'5min1s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp,'2h3min') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp,'1d2w') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp,'2y5mons') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;

select time_bucket_gapfill(,'10s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill('10s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp,) as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp) as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;

set time zone 8;
select time_bucket_gapfill(k_timestamp, '10second') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10second') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10SECONDS') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10SECONDS') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10secs') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10secs') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10sec') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10sec') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10minute') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10minute') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10MINUTES') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10MINUTES') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10mins') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10mins') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10min') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10min') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10m') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10m') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10Hour') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10Hour') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10hours') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10hours') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10hrs') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10hrs') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10hr') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10hr') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10h') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10h') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10day') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10day') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10days') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10days') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10d') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10d') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10week') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10week') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10weeks') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10weeks') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10w') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10w') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10month') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10month') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10months') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10months') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10mons') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10mons') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10mon') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10mon') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10year') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10year') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10years') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10years') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10yrs') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10yrs') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10yr') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10yr') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10y') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10y') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '0s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '0s') as tb from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '15s') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '15s') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10h') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10h') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10day') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10day') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10w') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10w') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10month') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10month') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket_gapfill(k_timestamp, '10yr') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
select time_bucket(k_timestamp, '10yr') as tb,* from test_select_timebucket_gapfill.tb group by tb order by tb limit 10;
set time zone 0;
drop database test_select_timebucket_gapfill cascade;
--- bug ZDP-39810
create ts database test_timebucket_gapfill;
create table test_timebucket_gapfill.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);
set timezone=8;
insert into test_timebucket_gapfill.tb values('1970-11-06 17:10:55.123','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_timebucket_gapfill.tb values('1970-11-06 17:10:23','1999-02-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_timebucket_gapfill.tb values('1970-12-01 12:10:25','2000-03-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,400,4000,false,50.555,500.578578,'d','\test测试！！！@TEST1','e','test测试！T  EST1xaa','查询查询 ','\','e','es1023_2','s_ 4','ww4096_2');
insert into test_timebucket_gapfill.tb values('1970-12-01 12:10:23.456','2000-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'','     ',' ','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_timebucket_gapfill.tb values('1971-01-03 09:08:31.22','2000-05-01 22:30:11',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\', 'v2551cdf~#   ', '  测试%&!','ar-1', 'ar255()&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',3,300,300,false,60.666,600.678,'','\test测试！！！@TEST1',' ','test测试！T  EST1xaa','查询查询 ','\','','    ','','  ');
insert into test_timebucket_gapfill.tb values('1971-01-10 09:08:19','2008-07-15 22:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e','a',null,'测试测试 ','b','test测试10_1','vwwws中文_1',null);
insert into test_timebucket_gapfill.tb values('1972-05-10 23:37:15.783','2008-07-15 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'测试测试 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_timebucket_gapfill.tb values('1972-05-10 23:42:18.223','2008-07-15 07:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,false,-10.123,100.111111,'b','\TEST1 ','f','测试！TEST1xaa','5555 5','  bdbd','y','test@测试！10_1','vwwws_1','cddde');

select k_timestamp,e2 from test_timebucket_gapfill.tb order by k_timestamp;
select time_bucket_gapfill(k_timestamp,31104000) as a,interpolate(first(e2),'null') from test_timebucket_gapfill.tb group by a order by a;
select time_bucket_gapfill(k_timestamp,31104000) as a,interpolate(first_row(e2),'null') from test_timebucket_gapfill.tb group by a order by a;
select time_bucket_gapfill(k_timestamp,31104000) as a,interpolate(last(e2),'null') from test_timebucket_gapfill.tb group by a order by a;
select time_bucket_gapfill(k_timestamp,31104000) as a,interpolate(last_row(e2),'null') from test_timebucket_gapfill.tb group by a order by a;

drop database test_timebucket_gapfill cascade;

--- bug ZDP-40217
DROP DATABASE IF exists test_time_addsub cascade;
CREATE ts DATABASE test_time_addsub;
CREATE TABLE test_time_addsub.t1(
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
    ATTRIBUTES (code1 INT2 NOT NULL,code2 INT,code3 INT8,code4 FLOAT4 ,code5 FLOAT8,code6 BOOL,code7 VARCHAR,code8 VARCHAR(128) NOT NULL,code9 VARBYTES,code10 VARBYTES(60),code11 VARCHAR,code12 VARCHAR(60),code13 CHAR(2),code14 CHAR(1023) NOT NULL,code15 NCHAR,code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code1,code14,code8,code16);
INSERT INTO test_time_addsub.t1 VALUES(0,1,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');
INSERT INTO test_time_addsub.t1 VALUES(1,2,0,0,0,0,0,true,999999,'          ','          ','          ',' ','          ',' ',' ','          ','          ','          ',' ',' ','          ','          ','          ',0,0,0,0,0,TRUE,'          ',' ',' ','          ','          ','          ','  ','          ',' ','          ');
INSERT INTO test_time_addsub.t1 VALUES('1976-10-20 12:00:12.123',3,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
INSERT INTO test_time_addsub.t1 VALUES('1979-2-28 11:59:01.999',4,20002,20000002,200000000002,-20873209.0220322201,-22012110.113011921,false,123,'test数据库语法查询测试！！！@TEST4-8','test数据库语法查询测试！！！@TEST4-9','test数据库语法查询测试！！！@TEST4-10','t','test数据库语法查询测试！！！@TEST4-12','中','test数据库语法查询测试！！！@TEST4-14','test数据库语法查询测试！！！@TEST4-15','test数据库语法查询测试！TEST4-16xaa','test数据库语法查询测试！！！@TEST4-17','test数据库语法查询测试！！！@TEST4-18',b'\xca','test数据库语法查询测试！！！@TEST4-20','test数据库语法查询测试！！！@TEST4-21','test数据库语法查询测试！！！@TEST4-22',20002,-20000002,200000000002,-20873209.0220322201,22012110.113011921,true,'test数据库语法查询测试！！！@TEST4-7','test数据库语法查询测试！！！@TEST4-8',b'\xbb','test数据库语法查询测试！！！@TEST4-10','test数据库语法查询测试！！！@TEST4-11','test数据库语法查询测试！！！@TEST4-12','t3','test数据库语法查询测试！！！@TEST4-14','中','test数据库语法查询测试！！！@TEST4-16');
INSERT INTO test_time_addsub.t1 VALUES(318193261000,5,30003,30000003,300000000003,-33472098.11312001,-39009810.333011921,true,'2015-3-12 10:00:00.234','test数据库语法查询测试！！！@TEST5-8','test数据库语法查询测试！！！@TEST5-9','test数据库语法查询测试！！！@TEST5-10','t','test数据库语法查询测试！！！@TEST5-12','中','test数据库语法查询测试！！！@TEST5-14','test数据库语法查询测试！！！@TEST5-15','test数据库语法查询测试！TEST5-16xaa','test数据库语法查询测试！！！@TEST5-17','test数据库语法查询测试！！！@TEST5-18',b'\xca','test数据库语法查询测试！！！@TEST5-20','test数据库语法查询测试！！！@TEST5-21','test数据库语法查询测试！！！@TEST5-22',-30003,30000003,-300000000003,33472098.11312001,-39009810.333011921,false,'test数据库语法查询测试！！！@TEST5-7','test数据库语法查询测试！！！@TEST5-8',b'\xcc','test数据库语法查询测试！！！@TEST5-10','test数据库语法查询测试！！！@TEST5-11','test数据库语法查询测试！！！@TEST5-12','t3','test数据库语法查询测试！！！@TEST5-14','中','test数据库语法查询测试！！！@TEST5-16');
INSERT INTO test_time_addsub.t1 VALUES(318993291090,6,-10001,10000001,-100000000001,1047200.00312001,1109810.113011921,false,'2023-6-23 05:00:00.55','test数据库语法查询测试！！！@TEST6-8','test数据库语法查询测试！！！@TEST6-9','test数据库语法查询测试！！！@TEST6-10','t','test数据库语法查询测试！！！@TEST6-12','中','test数据库语法查询测试！！！@TEST6-14','test数据库语法查询测试！！！@TEST6-15','test数据库语法查询测试！TEST6-16xaa','test数据库语法查询测试！！！@TEST6-17','test数据库语法查询测试！！！@TEST6-18',b'\xca','test数据库语法查询测试！！！@TEST6-20','test数据库语法查询测试！！！@TEST6-21','test数据库语法查询测试！！！@TEST6-22',10001,-10000001,100000000001,424721.022311,4909810.11301191,true,'test数据库语法查询测试！！！@TEST6-7','test数据库语法查询测试！！！@TEST6-8',b'\xdd','test数据库语法查询测试！！！@TEST6-10','test数据库语法查询测试！！！@TEST6-11','test数据库语法查询测试！！！@TEST6-12','t3','test数据库语法查询测试！！！@TEST6-14','中','test数据库语法查询测试！！！@TEST6-16');
INSERT INTO test_time_addsub.t1 VALUES(318995291029,7,-20002,20000002,-200000000002,20873209.0220322201,22012110.113011921,true,'2016-7-17 20:12:00.12','test数据库语法查询测试！！！@TEST7-8','test数据库语法查询测试！！！@TEST7-9','test数据库语法查询测试！！！@TEST7-10','t','test数据库语法查询测试！！！@TEST7-12','中','test数据库语法查询测试！！！@TEST7-14','test数据库语法查询测试！！！@TEST7-15','test数据库语法查询测试！TEST7-16xaa','test数据库语法查询测试！！！@TEST7-17','test数据库语法查询测试！！！@TEST7-18',b'\xca','test数据库语法查询测试！！！@TEST7-20','test数据库语法查询测试！！！@TEST7-21','test数据库语法查询测试！！！@TEST7-22',-20002,20000002,-200000000002,555500.0055505,55505532.553015321,false,'test数据库语法查询测试！！！@TEST7-7','test数据库语法查询测试！！！@TEST7-8',b'\xee','test数据库语法查询测试！！！@TEST7-10','test数据库语法查询测试！！！@TEST7-11','test数据库语法查询测试！！！@TEST7-12','t3','test数据库语法查询测试！！！@TEST7-14','中','test数据库语法查询测试！！！@TEST7-16');
INSERT INTO test_time_addsub.t1 VALUES(318995302501,8,-30003,30000003,-300000000003,33472098.11312001,39009810.333011921,false,4565476,'test数据库语法查询测试！！！@TEST8-8','test数据库语法查询测试！！！@TEST8-9','test数据库语法查询测试！！！@TEST8-10','t','test数据库语法查询测试！！！@TEST8-12','中','test数据库语法查询测试！！！@TEST8-14','test数据库语法查询测试！！！@TEST8-15','test数据库语法查询测试！TEST8-16xaa','test数据库语法查询测试！！！@TEST8-17','test数据库语法查询测试！！！@TEST8-18',b'\xca','test数据库语法查询测试！！！@TEST8-20','test数据库语法查询测试！！！@TEST8-21','test数据库语法查询测试！！！@TEST8-22',30003,-30000003,300000000003,6900.0012345,6612.1215,true,'test数据库语法查询测试！！！@TEST8-7','test数据库语法查询测试！！！@TEST8-8',b'\xff','test数据库语法查询测试！！！@TEST8-10','test数据库语法查询测试！！！@TEST8-11','test数据库语法查询测试！！！@TEST8-12','t3','test数据库语法查询测试！！！@TEST8-14','中','test数据库语法查询测试！！！@TEST8-16');
INSERT INTO test_time_addsub.t1 VALUES('2001-12-9 09:48:12.30',9,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,30000,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull');
INSERT INTO test_time_addsub.t1 VALUES('2002-2-22 10:48:12.899',10,32767,-2147483648,9223372036854775807,-99999999991.9999999991,9999999999991.999999999991,true,'2020-10-1 12:00:01.0','test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t','test数据库语法查询测试！！！@TEST10-12','中','test数据库语法查询测试！！！@TEST10-14','test数据库语法查询测试！！！@TEST10-15','test数据库语法查询测试！TEST10-16xaa','test数据库语法查询测试！！！@TEST10-17','test数据库语法查询测试！！！@TEST10-18',b'\xca','test数据库语法查询测试！！！@TEST10-20','test数据库语法查询测试！！！@TEST10-21','test数据库语法查询测试！！！@TEST10-22',1,111,1111111,1472011.12345,1109810.113011921,false,'test数据库语法查询测试！！！@TEST10-7','test数据库语法查询测试！！！@TEST10-8',b'\xcc','test数据库语法查询测试！！！@TEST10-10','test数据库语法查询测试！！！@TEST10-11','test数据库语法查询测试！！！@TEST10-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16');
INSERT INTO test_time_addsub.t1 VALUES('2003-10-1 11:48:12.1',11,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',2,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST11-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16');
INSERT INTO test_time_addsub.t1 VALUES('2004-9-9 00:00:00.9',12,12000,12000000,120000000000,-12000021.003125,-122209810.1131921,true,'2129-3-1 12:00:00.011','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','t','aaaaaabbbbbbcccccc','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','c','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'aaaaaabbbbbbcccccc','b','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','ty','aaaaaabbbbbbcccccc','u','aaaaaabbbbbbcccccc');
INSERT INTO test_time_addsub.t1 VALUES('2004-12-31 12:10:10.911',13,23000,23000000,230000000000,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK',20002,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK');
INSERT INTO test_time_addsub.t1 VALUES('2008-2-29 2:10:10.111',14,32767,34000000,340000000000,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321');
INSERT INTO test_time_addsub.t1 VALUES('2012-02-29 1:10:10.000',15,-32767,-34000000,-340000000000,43000079.07812032,122209810.1131921,true,'2099-9-1 11:01:00.111','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','1','数据库语法查询测试','2','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','9','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试',10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'数据库语法查询测试','8','7','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','65','数据库语法查询测试','4','数据库语法查询测试');

SELECT time_bucket(k_timestamp+'1y'+'10y','15s') FROM test_time_addsub.t1 order by k_timestamp;
SELECT time_bucket(e7+'1y'+'10y','15s') FROM test_time_addsub.t1 order by k_timestamp;
DROP database test_time_addsub cascade;

create ts database test_timebucket_gapfill;
create table test_timebucket_gapfill.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);
prepare p1 as select time_bucket_gapfill(k_timestamp,9900) as tb from test_timebucket_gapfill.tb where e2 + t2 > 0 group by tb union select time_bucket_gapfill(k_timestamp,10000) as tb from test_timebucket_gapfill.tb where e2 + t2 < 0 group by tb order by tb;
prepare p2 as select time_bucket_gapfill(k_timestamp,9900) as tb from test_timebucket_gapfill.tb where e2 + $1 > 0 group by tb union select time_bucket_gapfill(k_timestamp,10000) as tb from test_timebucket_gapfill.tb where e2 + $2 < 0 group by tb order by tb;
execute p2(10,20);
DROP database test_timebucket_gapfill cascade;

--- ZDP-40955
create database test_timebucket_gapfill_rel;

create table test_timebucket_gapfill_rel.tb3(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varchar(100),e20 varchar,e21 varchar(254),e22 varchar(100),t1 int2,t2 int,t3 int8 not null,t4 bool,t5 float4,t6 float8,t7 char,t8 char(100),t9 nchar,t10 nchar(254),t11 varchar not null,t12 varchar(128),t13 varchar not null,t14 varchar(100) not null,t15 varchar,t16 varchar(255));

insert into test_timebucket_gapfill_rel.tb3 values('2025-06-06 08:00:00','2024-06-10 16:16:15.183',800,8000,80000,800000.808888,8000000.808088,true,'d','test测试！！！@TEST1 ','d','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,false,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_timebucket_gapfill_rel.tb3 values('2025-06-06 11:15:15.783','2024-06-10 17:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',8,800,8000,false,-20.123,800.578578,'d','test测试！！！@TEST1  ','d','ddd','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');

select time_bucket_gapfill(k_timestamp,3600) as a,interpolate(sum(t3),'prev') from test_timebucket_gapfill_rel.tb3 group by a order by a;
select time_bucket_gapfill(k_timestamp,21600) as a,interpolate(avg(e4),'next') from test_timebucket_gapfill_rel.tb3 group by a order by a;
select time_bucket_gapfill(k_timestamp,604800) as a,interpolate(count(t6),'linear') from test_timebucket_gapfill_rel.tb3 group by a order by a;
DROP database test_timebucket_gapfill_rel cascade;