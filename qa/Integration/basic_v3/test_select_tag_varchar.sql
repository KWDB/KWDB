-- tag/primary tag select
-- testcase0001 no data select
use defaultdb;drop database if exists test_select_tag cascade;
create ts database test_select_tag;
create table test_select_tag.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
create table test_select_tag.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t2,t7,t9,t10);
-- tag col
select t1,t2,t3 from test_select_tag.tb order by k_timestamp;
select t4,t5 from test_select_tag.tb order by k_timestamp;
select t6 from test_select_tag.tb order by k_timestamp;
select t7,t8 from test_select_tag.tb order by k_timestamp;
select t9,t10 from test_select_tag.tb order by k_timestamp;
select t11,t12 from test_select_tag.tb order by k_timestamp;
select t1,t5,t6,t7,t10,t11 from test_select_tag.tb order by k_timestamp;
select e2,t1,t8 from test_select_tag.tb order by k_timestamp;
-- tag col data col
select k_timestamp,e1,t1,t2,t3 from test_select_tag.tb order by k_timestamp;
select e2,e3,e4,t4,t5 from test_select_tag.tb order by k_timestamp;
select e5,e6,t6 from test_select_tag.tb order by k_timestamp;
select e7,e8,t7,t8 from test_select_tag.tb order by k_timestamp;
select e9,e10,t9,t10 from test_select_tag.tb order by k_timestamp;
select e11,e12,t11,t12 from test_select_tag.tb order by k_timestamp;
select e15,e16,t5,t6 from test_select_tag.tb order by k_timestamp;
select e17,t1,t5,t6,t7,t10,t11 from test_select_tag.tb order by k_timestamp;
select e12,e2,t1,t8 from test_select_tag.tb order by k_timestamp;
-- tag col pt col-nodata
select t1,t2,t4,t6,t8,t12 from test_select_tag.tb order by k_timestamp;
select t2,t7,t9,t10,t8 from test_select_tag.tb2 order by k_timestamp;
-- pt col
select t1,t4,t8,t12 from test_select_tag.tb order by k_timestamp;
select t2,t7,t9,t10 from test_select_tag.tb2 order by k_timestamp;
-- pt
select k_timestamp,e1,t1,t4,t8,t12 from test_select_tag.tb order by k_timestamp;
select e2,e3,e4,t2,t7,t9,t11 from test_select_tag.tb2 order by k_timestamp;
-- data col\tag col nodata
select k_timestamp,e1,e3,t4,t6,t8,t12 from test_select_tag.tb order by k_timestamp;
select e4,e6,t2,t7,t8,t11 from test_select_tag.tb2 order by k_timestamp;
drop table test_select_tag.tb cascade;
drop table test_select_tag.tb2 cascade;
use defaultdb;drop database test_select_tag cascade;

-- testcase0002 basic select
use defaultdb;drop database test_select_tag cascade;
create ts database test_select_tag1;
create table test_select_tag1.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
create table test_select_tag1.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t2,t7,t9,t10);
insert into test_select_tag1.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_tag1.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
-32678,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','f','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_tag1.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ',e'\\','255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag1.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',
4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','\0test查询！！！@TEST1\0 ',e'\\\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_tag1.tb values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag1.tb values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_tag1.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',
7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag1.tb2 values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag1.tb2 values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');

-- tag col
select k_timestamp from test_select_tag1.tb order by k_timestamp;
select * from test_select_tag1.tb order by k_timestamp;
select t1,t4,t6,t7,t8,t10,t12 from test_select_tag1.tb order by k_timestamp;
select t2,t3,t5,t9,t11 from test_select_tag1.tb order by k_timestamp;
select t2,t2 from test_select_tag1.tb order by k_timestamp;
-- pt
select t1,t4,t8,t10 from test_select_tag1.tb order by k_timestamp;
select t2,t7,t9,t11 from test_select_tag1.tb2 order by k_timestamp;
-- tag col pt col
select t1,t4,t5,t8,t10 from test_select_tag1.tb order by k_timestamp;
select t2,t7,t9,t11,t5 from test_select_tag1.tb2 order by k_timestamp;
-- col、tag col pt col
select k_timestamp,e1,t1,t4,t5,t8,t10 from test_select_tag1.tb order by k_timestamp;
select e2,e3,e4,t2,t7,t9,t11 from test_select_tag1.tb2 order by k_timestamp;
drop table test_select_tag1.tb cascade;
drop table test_select_tag1.tb2 cascade;
use defaultdb;drop database test_select_tag1 cascade;

-- testcase0003 where
use defaultdb;drop database test_select_tag2 cascade;
create ts database test_select_tag2;
create table test_select_tag2.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
create table test_select_tag2.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t2,t7,t9,t10);
insert into test_select_tag2.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_tag2.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
-32678,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','f','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_tag2.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ',e'\\','255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag2.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',
4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','\0test查询！！！@TEST1\0 ',e'\\\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_tag2.tb values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag2.tb values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_tag2.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',
7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag2.tb2 values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag2.tb2 values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
-- tag col
select t1,t2,t3,t4 from test_select_tag2.tb where t2 is null and t4 = false order by k_timestamp;
select t1,t2,t3,t4 from test_select_tag2.tb where t2 is null or t4 = false order by k_timestamp;
select t1,t2 from test_select_tag2.tb where t1 = -32768 and t2 is null order by k_timestamp;
select t3,t4 from test_select_tag2.tb where t3 <= 7000 and t4 = true order by k_timestamp;
select t5,t6,t7 from test_select_tag2.tb where t5 <= 1 or t6 > 100.111111 or t7 in('a','b') order by k_timestamp;
select t6,t8,t9 from test_select_tag2.tb where t8 in('test测试！！！@TEST1  ') and t9 not in('f',null) or t6 between 100.111111 and 100.111111 order by k_timestamp desc;
select t11,t12 from test_select_tag2.tb where t11 in('\0test查询  @TEST1\0',null) or t12 >'chch4_1' order by k_timestamp;--bug
select t11 from test_select_tag2.tb where t11 in(null,'\0test查询  @TEST1\0') order by k_timestamp;--bug
select t11 from test_select_tag2.tb where t11 in('\0test查询  @TEST1\0',null) order by k_timestamp;--bug
select t12 from test_select_tag2.tb where t12 like '%\\' order by k_timestamp;--bug
select t12 from test_select_tag2.tb where t12 like '%\\\\' order by k_timestamp;--bug
select t10,t11,t12 from test_select_tag2.tb where t10 like '"%' or t11 is null or t11 in('\0test查询！！！@TEST1\0','\0test查询！！！@TEST1\0') order by k_timestamp;
select t8,t10 from test_select_tag2.tb where t8 > 'test测试！！！@TEST1'  order by k_timestamp;
select t8,t10 from test_select_tag2.tb where t8 >= 'test测试！！！@TEST1 ' and t10 like '"_\'  order by k_timestamp;
-- pt col
select t1,t4 from test_select_tag2.tb where t1 is not null and t4 = false order by k_timestamp;
select t8,t12 from test_select_tag2.tb where t8 in('') or t12 in('vvvaa64_1') order by k_timestamp;
select t2,t7,t9,t10 from test_select_tag2.tb2 where t2 is null and t9 not in('e','f') or t7 in('a','b','c') and t10 is not null order by k_timestamp;
-- tag col pt col
select t1,t4,t5 from test_select_tag2.tb where t1 is not null and t4 = false and t5 >= 50.555 order by k_timestamp;
select t6,t8,t12 from test_select_tag2.tb where t8 in('test测试！！！@TEST1  ','test测试！！！@TEST1') or t12 in(e'\\\\') order by k_timestamp;--bug
select t2,t7,t9,t11,t10 from test_select_tag2.tb2 where t2 is null and t9 not in('nchar254_5','nchar254_6') or t7 in('a','b','c') and t11 is not null order by k_timestamp;
-- col tag col
select e8,t6,t8,t10 from test_select_tag2.tb where e8 < 't' and t8 in('char1023_1','char1023_4') or t10 in('0') order by k_timestamp;--bug
select e19,t2,t7,t9,t11 from test_select_tag2.tb2 where t2 is null and t9 not in('nchar254_5','nchar254_6') or t7 in('a','b','c') and t11 is not null order by k_timestamp;--bug
drop table test_select_tag2.tb cascade;
drop table test_select_tag2.tb2 cascade;
use defaultdb;drop database test_select_tag2 cascade;

-- testcase0004 groupby
use defaultdb;drop database test_select_tag3 cascade;
create ts database test_select_tag3;
create table test_select_tag3.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
create table test_select_tag3.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t2,t7,t9,t10);
insert into test_select_tag3.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_tag3.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
-32678,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','f','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_tag3.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ',e'\\','255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag3.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',
4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','\0test查询！！！@TEST1\0 ',e'\\\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_tag3.tb values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag3.tb values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_tag3.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',
7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag3.tb2 values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag3.tb2 values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');

-- tag
select t1,t2,t3 from test_select_tag3.tb where t2 < 500 group by t1,t2,t3 order by k_timestamp;
select t4,t5,t6,t11 from test_select_tag3.tb where t4 = false group by t4,t5,t6,t11 order by k_timestamp,t4,t5,t6;
-- pt
select t4 from test_select_tag3.tb where t4 in(true,false) group by t4 order by k_timestamp;
select t1,t8,t10 from test_select_tag3.tb where t1 between 0 and 10 group by t1,t8,t12 order by k_timestamp;--bug
select t2,t7,t9,t11 from test_select_tag3.tb2 where t2 >= 200 and t2 <= 600 and t9 not in('f') group by t2,t7,t9,t11 order by k_timestamp;
-- tag pt 
select t4 from test_select_tag3.tb where t4 in(true,false) group by t4 order by k_timestamp;
select t1,t5,t12 from test_select_tag3.tb where t1 between 3 and 6 group by t1,t5,t12 order by k_timestamp;
select t2,t7,t6,t9 from test_select_tag3.tb2 where t2 >= 200 or t2 <= 600 or t6 in(100.111111,700.567568) and t7 not in('c','d') group by t2,t7,t9,t11 order by k_timestamp;
-- col、tag col pt col
select e20,t4 from test_select_tag3.tb where t4 in(true,false) group by e20,t4 order by k_timestamp;
select e15,t1,t5,t12 from test_select_tag3.tb where t1 between 3 and 6 group by t1,t5,t12,e15 order by k_timestamp;--bug
select e2,t2,t7,t6,t9 from test_select_tag3.tb2 where t2 >= 200 or t2 <= 600 or t6 in(100.111111,700.567568) and t7 not in('c','d') group by e2,t2,t7,t6,t9 order by k_timestamp;--bug
drop table test_select_tag3.tb cascade;
drop table test_select_tag3.tb2 cascade;
use defaultdb;drop database test_select_tag3 cascade;

-- testcase0005 order by
use defaultdb;drop database test_select_tag4 cascade;
create ts database test_select_tag4;
create table test_select_tag4.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
create table test_select_tag4.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t2,t7,t9,t10);
insert into test_select_tag4.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_tag4.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
-32678,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','f','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_tag4.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ',e'\\','255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag4.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',
4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','\0test查询！！！@TEST1\0 ',e'\\\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_tag4.tb values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag4.tb values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_tag4.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',
7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag4.tb2 values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag4.tb2 values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');

-- tag
select t1,t2,t3,t4,t5,t6 from test_select_tag4.tb where t2 is not null order by k_timestamp,t1,t2,t3,t4,t5,t6;
select t7,t8,t9,t10 from test_select_tag4.tb order by k_timestamp,t7,t8 asc, t9 desc;
-- pt
select t4 from test_select_tag4.tb where t4 < true order by k_timestamp, t4 desc;
select t2,t7,t9,t11 from test_select_tag4.tb2 where t2 is not null order by k_timestamp,t1,t2,t3;
-- tag \ pt
select t4,t5,t6 from test_select_tag4.tb where t4 < true order by k_timestamp;
select t2,t3,t9,t11 from test_select_tag4.tb2 where t2 is not null order by k_timestamp,t1,t2,t9;
-- col\tag col\pt
select k_timestamp,t4,t5,t6 from test_select_tag4.tb where t4 < false order by k_timestamp desc;
select e2,e3,t2,t3,t9,t11 from test_select_tag4.tb2 where t2 is not null order by k_timestamp, t1,t2,t9;
drop table test_select_tag4.tb cascade;
drop table test_select_tag4.tb2 cascade;
use defaultdb;drop database test_select_tag4 cascade;

-- testcase0006 limit
use defaultdb;drop database test_select_tag5 cascade;
create ts database test_select_tag5;
create table test_select_tag5.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
create table test_select_tag5.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t2,t7,t9,t10);
insert into test_select_tag5.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_tag5.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
-32678,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','f','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_tag5.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ',e'\\','255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag5.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',
4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','\0test查询！！！@TEST1\0 ',e'\\\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_tag5.tb values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag5.tb values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_tag5.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',
7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag5.tb2 values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag5.tb2 values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
-- tag
select t11,t12 from test_select_tag5.tb order by k_timestamp,t11,t12 limit 3;
select t8,t11 from test_select_tag5.tb order by k_timestamp,t8,t11 limit 1 offset 1;
select t7 from test_select_tag5.tb order by k_timestamp, t7 desc limit -1;
-- pt
select t8,t10 from test_select_tag5.tb order by k_timestamp,t8,t10 limit 3;
select t2,t11 from test_select_tag5.tb2 where t2 > 100 order by k_timestamp,t2 asc, t11 desc limit 2 offset 3;
select t7 from test_select_tag5.tb2 order by k_timestamp,t7 desc limit -1;
-- tag \ pt
select t3,t8,t12 from test_select_tag5.tb order by k_timestamp,t3,t8,t12 limit 3;
select t5,t13,t14 from test_select_tag5.tb2 where t13 = 'b' or t14 in('bytes1023_1','bytes1023_2') order by k_timestamp,t13,t14 limit 1;
-- col \ tag col \pt
select e4,e6,t3,t8,t10 from test_select_tag5.tb order by k_timestamp, e4,t8,t12 limit 3;
drop table test_select_tag5.tb cascade;
drop table test_select_tag5.tb2 cascade;
use defaultdb;drop database test_select_tag5 cascade;

-- not support
-- testcase0007 groupby+having
use defaultdb;drop database test_select_tag6 cascade;
create ts database test_select_tag6;
create table test_select_tag6.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
create table test_select_tag6.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t2,t7,t9,t10);
insert into test_select_tag6.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_tag6.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
-32678,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','f','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_tag6.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ',e'\\','255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag6.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',
4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','\0test查询！！！@TEST1\0 ',e'\\\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_tag6.tb values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag6.tb values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_tag6.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',
7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag6.tb2 values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag6.tb2 values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');

-- tag
-- select t2,t7,t8,t9,t10,t12 from test_select_tag6.tb group by t7,t8,t9,t10,t12 having t2 between 200 and 500 order by t7,t8,t9,t10,t12;
-- select t13,t14,t15,t16 from test_select_tag6.tb group by t13,t14,t15,t16 having t13 not in('','e','s') and t15 like '__f' order by t13,t14,t15,t16;
-- pt
-- select t1,t4,t8,t12 from test_select_tag6.tb having t8 <= 'char1023_2' and t12 like '%ar%';
-- select t2,t7,t11 from test_select_tag6.tb2 having t2 between 200 and 500 or t7 not in('c','d') and t11 is not null;
-- select t3,t13 from test_select_tag6.tb3 group by t3 having t3 != 3000 and t13 not in('','e','s');
-- tag
-- select t2,t7,t8,t12 from test_select_tag6.tb group by t2,t7,t8,t12 having t8 <= 'char1023_2' and t12 like '%ar%';
-- select t6,t7,t11 from test_select_tag6.tb2 group by t6,t7,t11 having t6 between 222.222279 and 600.345346 or t7 not in('c','d') and t11 is not null;
-- select t3,t4,t13 from test_select_tag6.tb3 group by t3,t4,t13 having t3 != 3000 and t4 = true and t13 not in('','e','s');
-- 
-- select e2,t2,t7,t8,t12 from test_select_tag6.tb group by e2,t2,t7,t8,t12 having t8 <= 'char1023_2' and t12 like '%ar%';
-- select e6,t6,t7,t11 from test_select_tag6.tb2 group by e6,t6,t7,t11 having t6 between 222.222279 and 600.345346 or t7 not in('c','d') and t11 is not null;
-- select e8,t3,t4,t13 from test_select_tag6.tb3 group by e8,t3,t4,t13 having t3 != 3000 and t4 = true and t13 not in('','e','s');
drop table test_select_tag6.tb cascade;
drop table test_select_tag6.tb2 cascade;
use defaultdb;drop database test_select_tag6 cascade;

-- not support
-- testcase0008 case when
use defaultdb;drop database test_select_tag7 cascade;
create ts database test_select_tag7;
create table test_select_tag7.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
create table test_select_tag7.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t2,t7,t9,t10);
insert into test_select_tag7.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_tag7.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
-32678,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','f','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_tag7.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ',e'\\','255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag7.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',
4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','\0test查询！！！@TEST1\0 ',e'\\\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_tag7.tb values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag7.tb values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_tag7.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',
7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag7.tb2 values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag7.tb2 values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');

-- tag
select t2, case t2 when 100 then '100' when 200 then '200' when 300 then 300 end as result from test_select_tag7.tb where t2 between 100 and 600 order by k_timestamp;
select t6, case when t6 <= 300.333333 then ' <= 300.333333' when t6 > 300.333333 and t6 <= 500.578578 then ' > 300.333333 && <= 500.578578' else ' > 500.578578' end as result from test_select_tag7.tb order by k_timestamp;
select t10, case when t10 = '\a' then '\a' when t10 = 'a\0\0' then 'a\0\0' end as result from test_select_tag7.tb where t10 not in('" \','nchar254_ 4') order by k_timestamp;
-- pt
select t2, case t2 when 100 then '100' when 200 then '200' when 300 then 300 end as result from test_select_tag7.tb where t2 between 100 and 600 order by k_timestamp;
-- tag col\pt col
select t2,t5, case t2 when 100 then '100' when 200 then '200' when 300 then 300 end as result from test_select_tag7.tb2 where t2 between 100 and 600 order by k_timestamp;
select t6,t8,t12, case when t6 <= 300.333333 then ' <= 300.333333' when t6 > 300.333333 and t6 <= 500.578578 then ' > 300.333333 && <= 500.578578' else ' > 500.578578' end as result from test_select_tag7.tb order by k_timestamp;
-- col\tag col\pt
select t2,t5, case t2 when 100 then '100' when 200 then '200' when 300 then 300 end as result from test_select_tag7.tb2 where t2 between 100 and 600 order by t1;
select t6,t8,t12, case when t6 <= 300.333333 then ' <= 300.333333' when t6 > 300.333333 and t6 <= 500.578578 then ' > 300.333333 && <= 500.578578' else ' > 500.578578' end as result from test_select_tag7.tb order by k_timestamp;
drop table test_select_tag7.tb cascade;
drop table test_select_tag7.tb2 cascade;
use defaultdb;drop database test_select_tag7 cascade;

-- not support
-- testcase0009 time add\sub
use defaultdb;drop database test_select_tag8 cascade;
create ts database test_select_tag8;
create table test_select_tag8.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
create table test_select_tag8.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t2,t7,t9,t10);
insert into test_select_tag8.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_tag8.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
-32678,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','f','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_tag8.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ',e'\\','255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag8.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',
4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','\0test查询！！！@TEST1\0 ',e'\\\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_tag8.tb values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag8.tb values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_tag8.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',
7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag8.tb2 values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag8.tb2 values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');

-- tag
-- select t2,t4,t8 from test_select_tag8.tb where k_timestamp+1m>k_timestamp;
-- select t10,t12 from test_select_tag8.tb where k_timestamp-now()>1w;
-- select t15,t17,t21 from test_select_tag8.tb where now()-k_timestamp>1y;
-- pt
-- select t1,t4,t8,t12 from test_select_tag8.tb where k_timestamp+1m>k_timestamp;
-- select t2,t7,t9,t11 from test_select_tag8.tb2 where k_timestamp-now()>1w;
-- select t3,t10,t13,t14 from test_select_tag8.tb3 where now()-k_timestamp>1y;
-- tag
-- select t2,t4,t8 from test_select_tag8.tb where k_timestamp+1m>k_timestamp;
-- select t10,t12 from test_select_tag8.tb where k_timestamp-now()>1w;
-- select t14,t15,t17,t21 from test_select_tag8.tb3 where now()-k_timestamp>1y;
-- 
-- select k_timestamp+1m,t2,t4,t8 from test_select_tag8.tb where k_timestamp+1m>k_timestamp;
-- select e1,e3,t10,t12 from test_select_tag8.tb where k_timestamp-now()>1w;
-- select e9,e10,t15,t17,t21 from test_select_tag8.tb where now()-k_timestamp>1y;
drop table test_select_tag8.tb cascade;
drop table test_select_tag8.tb2 cascade;
use defaultdb;drop database test_select_tag8 cascade;

-- testcase0010 col cal
use defaultdb;drop database test_select_tag9 cascade;
create ts database test_select_tag9;
create table test_select_tag9.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
create table test_select_tag9.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t2,t7,t9,t10);
insert into test_select_tag9.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_tag9.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
-32678,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','f','test测试！TEST1xaa','\0test查询  @TEST1\0',e'\\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_tag9.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ',e'\\','255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag9.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',
4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','\0test查询！！！@TEST1\0 ',e'\\\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_tag9.tb values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag9.tb values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_tag9.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',
7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','" \','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_tag9.tb2 values ('2023-05-10 09:08:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_tag9.tb2 values ('2023-05-10 09:08:19.22','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');

-- tag col select
select t1+t1 from test_select_tag9.tb  order by k_timestamp;
select t1+t2 from test_select_tag9.tb order by k_timestamp;
select t1-t3 from test_select_tag9.tb order by k_timestamp;
select t1*t4 from test_select_tag9.tb order by k_timestamp;
select t1/t5 from test_select_tag9.tb order by k_timestamp;
select t1%t6 from test_select_tag9.tb order by k_timestamp;
select t1-t7 from test_select_tag9.tb order by k_timestamp;
select t2-t2 from test_select_tag9.tb order by k_timestamp;
select t2%t3 from test_select_tag9.tb order by k_timestamp;
select t2-t4 from test_select_tag9.tb order by k_timestamp;
select t2*t5 from test_select_tag9.tb order by k_timestamp;
select t2/t6 from test_select_tag9.tb order by k_timestamp;
select t2+t8 from test_select_tag9.tb order by k_timestamp;
select t3%t3 from test_select_tag9.tb order by k_timestamp;
select t3+t4 from test_select_tag9.tb order by k_timestamp;
select t3-t5 from test_select_tag9.tb order by k_timestamp;
select t3*t6 from test_select_tag9.tb order by k_timestamp;
select t3/t9 from test_select_tag9.tb order by k_timestamp;
select t5/t5 from test_select_tag9.tb order by k_timestamp;
select t5-t6 from test_select_tag9.tb order by k_timestamp;
select t5*t10 from test_select_tag9.tb order by k_timestamp;
select t6+t6 from test_select_tag9.tb order by k_timestamp;
select t6-t11 from test_select_tag9.tb order by k_timestamp;
select t12*t13 from test_select_tag9.tb order by k_timestamp;
-- tag
select t1+e1 from test_select_tag9.tb order by k_timestamp;
select t2+e2 from test_select_tag9.tb order by k_timestamp;
select t3-e3 from test_select_tag9.tb order by k_timestamp;
select t4*e4 from test_select_tag9.tb order by k_timestamp;
select t5/e5 from test_select_tag9.tb order by k_timestamp;
select t6%e6 from test_select_tag9.tb order by k_timestamp;
select t7-e7 from test_select_tag9.tb order by k_timestamp;
select t2-e8 from test_select_tag9.tb order by k_timestamp;
select t3%e10 from test_select_tag9.tb order by k_timestamp;
select t4-e12 from test_select_tag9.tb order by k_timestamp;
select t5*e15 from test_select_tag9.tb order by k_timestamp;
select t3%e2 from test_select_tag9.tb order by k_timestamp;
select t4+e3 from test_select_tag9.tb order by k_timestamp;
select t5-e4 from test_select_tag9.tb order by k_timestamp;
select t6*e5 from test_select_tag9.tb order by k_timestamp;
select t9/e6 from test_select_tag9.tb order by k_timestamp;
-- tag \ pt
select e2+t1 from test_select_tag9.tb order by k_timestamp;
select e3-t4 from test_select_tag9.tb order by k_timestamp;
select e4*t8 from test_select_tag9.tb order by k_timestamp;
select e5/t4 from test_select_tag9.tb order by k_timestamp;
select e6%t12 from test_select_tag9.tb order by k_timestamp;
select e2-t2 from test_select_tag9.tb2 order by k_timestamp;
select e3-t7 from test_select_tag9.tb2 order by k_timestamp;
select e4*t2 from test_select_tag9.tb2 order by k_timestamp;
select e5/t11 from test_select_tag9.tb2 order by k_timestamp;
-- pt 
select t1+t1 from test_select_tag9.tb order by k_timestamp;
select t1-t4 from test_select_tag9.tb order by k_timestamp;
select t1*t8 from test_select_tag9.tb order by k_timestamp;
select t4/t4 from test_select_tag9.tb order by k_timestamp;
select t10%t10 from test_select_tag9.tb order by k_timestamp;
select t2-t2 from test_select_tag9.tb2 order by k_timestamp;
select t2-t7 from test_select_tag9.tb2 order by k_timestamp;
select t9*t2 from test_select_tag9.tb2 order by k_timestamp;
select t11/t11 from test_select_tag9.tb2 order by k_timestamp;
-- pt 
select t1+e1 from test_select_tag9.tb order by k_timestamp;
select t1-e2 from test_select_tag9.tb order by k_timestamp;
select t1*e3 from test_select_tag9.tb order by k_timestamp;
select t4/e4 from test_select_tag9.tb order by k_timestamp;
select t10%e5 from test_select_tag9.tb order by k_timestamp;
select t2-e6 from test_select_tag9.tb2 order by k_timestamp;
select t2-e7 from test_select_tag9.tb2 order by k_timestamp;
select t9*e9 from test_select_tag9.tb2 order by k_timestamp;
select t11/e11 from test_select_tag9.tb2 order by k_timestamp;
drop table test_select_tag9.tb cascade;
drop table test_select_tag9.tb2 cascade;
use defaultdb;drop database test_select_tag9 cascade;
