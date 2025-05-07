drop database  if EXISTS  ts_db cascade;
create ts database ts_db;
create table ts_db.t1(
k_timestamp timestamp not null,
e1 int8 not null,
e2 float,
e3 bool,
e4 timestamp not null,
e5 int2
) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8) primary tags(code1);

select max(e3) from ts_db.t1;
select min(e3) from ts_db.t1;

insert into ts_db.t1 values('2018-10-10 10:00:00',1000,1000.0000,true,'2020-1-1 12:00:00.000',1000,100,200,300);
insert into ts_db.t1 values('2018-10-10 10:00:01',2000,2000.0000,true,'2020-1-1 12:00:00.000',2000,100,400,100);
insert into ts_db.t1 values('2018-10-10 10:00:02',3000,3000.0000,true,'2020-1-1 12:00:00.000',3000,100,400,100);
insert into ts_db.t1(k_timestamp,e1,e2,e3,e4,e5,code1) values('2018-10-10 10:00:03',4000,4000.0000,true,'2020-1-1 12:00:00.000',4000,100);
insert into ts_db.t1(k_timestamp,e1,e2,e3,e4,e5,code1) values('2018-10-10 10:00:04',4000,NULL,NULL,'2020-1-1 12:00:00.000',NULL,100);
insert into ts_db.t1 values('2018-10-10 10:00:05',9223372036854775807,1000.0000,true,'2020-1-1 12:00:00.000',1000,100,200,300);

SELECT code1 FROM ts_db.t1 WHERE e2=10000;
SELECT code1 FROM ts_db.t1 WHERE e2=1000;
SELECT e1>10000 FROM ts_db.t1 ORDER BY k_timestamp;

select e1,e3 from ts_db.t1 where code2 is null and code1=100;
use defaultdb;
drop DATABASE ts_db cascade;

create ts database ts_db;
use ts_db;
create table ts_db.t1(ts timestamp not null,a int, b int) tags(tag1 int not null, tag2 int) primary tags(tag1);
select avg(a),max(b) from ts_db.t1;
select avg(a), max(b),sum(tag1) from ts_db.t1;
select a, avg(a),max(b) from ts_db.t1 group by a;
select count(a) from ts_db.t1;
select count(*) from ts_db.t1;
select a, count(b) from ts_db.t1 group by a;
select a, count(*) from ts_db.t1 group by a;
insert into ts_db.t1 values('2018-10-10 10:00:00',11,11,33,44);
insert into ts_db.t1 values('2018-10-10 10:00:01',22,22,33,44);
insert into ts_db.t1 values('2018-10-10 10:00:02',11,33,33,44);
insert into ts_db.t1 values('2018-10-10 10:00:03',22,44,33,44);
insert into ts_db.t1 values('2018-10-10 10:00:04',33,55,44,44);
insert into ts_db.t1 values('2018-10-10 10:00:05',22,44,44,44);
insert into ts_db.t1 values('2018-10-10 10:00:06',33,44,55,44);
insert into ts_db.t1 values('2018-10-10 10:00:07',null,null,66,66);
insert into ts_db.t1 values('2018-10-10 10:00:08',null,null,66,77);
select a, sum(b) from ts_db.t1 group by a order by a;
select a, count(b) from ts_db.t1 group by a order by a;
select a, max(b) from ts_db.t1 group by a order by a;
select a, min(b) from ts_db.t1 group by a order by a;
select a, sum(b) from ts_db.t1 group by a having sum(b) < 100 order by a;
select a, avg(b) from ts_db.t1 where b = 22 group by a order by a;
select a+b, sum(b) from ts_db.t1 group by a+b order by a+b;
select a, sum(b) from ts_db.t1 group by a order by sum(b);
select a, sum(b) from ts_db.t1 group by a order by a limit 2 offset 1;
select sum(a) from ts_db.t1;
select count(*) from ts_db.t1;
use defaultdb;
drop database ts_db cascade;

create ts database test_select_function;
create table test_select_function.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 char(200),e13 char(255),e14 char(100),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 char(100),t12 char(200) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
insert into test_select_function.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_function.tb values ('2021-04-01 15:00:00','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
-32678,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','f','test测试！TEST1xaa','\0test查询  @TEST1\0','\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_function.tb values ('2022-05-01 17:00:00','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ','\','255测试1cdf~# ','@TEST1  ','abc255测试1()&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
32767,2147483647,9223372036854775807,false,9223372036854775806.12345,500.578578,'c','test测试！！！@TEST1  ','g','" ','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_select_function.tb values ('2023-05-01 08:00:00','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',
4,400,4000,false,50.555,500.578578,'d','\test测试！！！@TEST1','e','test测试！T  EST1xaa','\0test查询！！！@TEST1\0 ','\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_function.tb values ('2023-05-10 09:04:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
5,null,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_function.tb values ('2023-06-01 08:00:00','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
6,100,1000,false,-10.123,100.111111,'b','\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_function.tb values ('2023-07-10 08:04:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',
7,200,2000,false,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','" ','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');

select last_row(e2),last_row(e2),count(e3),count(e3) from test_select_function.tb group by e2,e3 order by e2, e3;
select max(e12) from test_select_function.tb group by e12 order by e12;
select max(e10),max(e11),max(e12) from test_select_function.tb;
select min(e10),min(e11),min(e12) from test_select_function.tb;
select count(*) from test_select_function.tb where e3 = 6000;
select max(e20),max(e22) from test_select_function.tb;
select max(e16),max(e17),max(e18),max(e19),max(e20),max(e21),max(e22) from test_select_function.tb;
select min(e16),min(e17),min(e18),min(e19),min(e20),min(e21),min(e22) from test_select_function.tb;
select min(e20),min(e21),min(e22) from test_select_function.tb;
drop database test_select_function cascade;

create ts database test_nullable_1;
create table test_nullable_1.t1(
k_timestamp timestamptz not null,
e1 timestamp,
e2 int2 ,
e3 int4 ,
e4 int64 ,
e5 float4 ,
e6 float8 ,
e7 bool)
tags (
code1 int2 NOT NULL,
code2 int4 NOT NULL,
code3 int64 NOT NULL,
code4 float4,
code5 float8,
code6 double NULL,
code7 bool NOT NULL
)
primary tags(code1);

insert into test_nullable_1.t1 values ('2018-10-10 10:00:00',111111,null,null,null,null,null,null,1,1,1,1,1,1,1);
insert into test_nullable_1.t1 values ('2018-10-10 10:00:01',null,2,null,null,null,null,null,2,2,2,null,null,null,0);
insert into test_nullable_1.t1 values ('2018-10-10 10:00:02',null,null,3,3,3,3,true,3,3,3,3,3,3,1);
insert into test_nullable_1.t1 values ('2018-10-10 10:00:03',444444,null,4,4,null,4,0,4,4,4,4,4,4,false);
insert into test_nullable_1.t1 values ('2018-10-10 10:00:04',555555,5,null,5,5,null,false,5,5,5,5,5,5,1);
insert into test_nullable_1.t1 values ('2018-10-10 10:00:05',666666,6,6,null,6,6,null,6,6,6,6,6,6,true);

select max(e2) + min(e2) from test_nullable_1.t1;
select max(e2), min(e2) from test_nullable_1.t1;

drop database test_nullable_1 cascade;

create ts database test_ts;
create table test_ts.c1(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar) tags(attr1 smallint not null, attr2 int, attr3 bigint, attr4 float, attr5 bool, attr6 varchar) primary tags(attr1);
insert into test_ts.c1 values('2023-05-31 10:00:00', 3,1000000,100000000000000000,100000000000000000.101,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts'), ('2023-05-31 11:00:00', 3,2000000,200000000000000000,200000000000000000.202,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts');

select last(e3)+3,last(e3)-3,last(e3)*3,last(e3)/3,last(e3)%3,last(e3)=3,last(e3)<3,last(e3)<=3,last(e3)>3,last(e3)>=3,last(e3)!=3,last(e1)<<3,last(e1)>>3 from test_ts.c1;

use test_ts;
create table test_filter(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 char(200),e13 char(255),e14 char(100),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 char(100),t12 char(200) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
insert into test_filter values ('2020-11-06 07:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
                                1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_filter values ('2020-11-06 08:00:00','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                -32678,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','f','test测试！TEST1xaa','\0test查询  @TEST1\0','\','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_filter values ('2020-11-06 09:00:00','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ','\','255测试1cdf~# ','@TEST1  ','abc255测试1()&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                32767,2147483647,9223372036854775807,false,9223372036854775806.12345,500.578578,'c','test测试！！！@TEST1  ','g','" ','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');
insert into test_filter values ('2020-11-06 17:00:00','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',
                                4,400,4000,false,50.555,500.578578,'d','\test测试！！！@TEST1','e','test测试！T  EST1xaa','\0test查询！！！@TEST1\0 ','\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_filter values ('2020-11-06 18:04:18.223','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,
                                5,null,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_filter values ('2020-11-06 19:00:00','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,
                                6,100,1000,false,-10.123,100.111111,'b','\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_filter values ('2020-11-06 20:04:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',
                                            7,200,2000,false,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','" ','\0test查询！！！@TEST1\0','64_3','t','es1023_2','\\f','tes4096_2');

select count(*) from test_filter where k_timestamp > '2020-11-06 08:10:23';
explain select count(e1) from test_filter where k_timestamp > '2020-11-06 08:10:23';
select count(*) from test_filter where k_timestamp > '2020-11-06 08:10:23+08';
explain select count(e1) from test_filter where k_timestamp > '2020-11-06 08:10:23+08';

drop database test_ts cascade;