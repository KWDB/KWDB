SET CLUSTER SETTING ts.blocks_per_segment.max_limit=100;
SET CLUSTER SETTING ts.rows_per_block.max_limit=100;
-- time_bucket_gapfill(timestamp_column:timestamptz,interval:string)
-- time_bucket_gapfill

use defaultdb;drop database if exists test_timebucket_gapfill cascade;
create ts database test_timebucket_gapfill;
create table test_timebucket_gapfill.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);
create table test_timebucket_gapfill.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t2,t7,t9,t10);
create table test_timebucket_gapfill.tb3(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2,t2 int,t3 int8 not null,t4 bool,t5 float4,t6 float8,t7 char,t8 char(100),t9 nchar,t10 nchar(254),t11 varchar not null,t12 varchar(128),t13 varbytes not null,t14 varbytes(100) not null,t15 varbytes,t16 varbytes(255)) primary tags(t3,t11);
set timezone=8;
insert into test_timebucket_gapfill.tb values('0001-11-06 17:10:55.123','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_timebucket_gapfill.tb values('0001-11-06 17:10:23','1999-02-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_timebucket_gapfill.tb values('0001-12-01 12:10:25','2000-03-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','查询查询 ','\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_timebucket_gapfill.tb values('0001-12-01 12:10:23.456','2000-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'','     ',' ','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_timebucket_gapfill.tb values('0002-01-03 09:08:31.22','2000-05-01 22:30:11',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\', 'v2551cdf~#   ', '  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',3,300,300,false,60.666,600.678,'','\\test测试！！！@TEST1',' ','test测试！T  EST1xaa','查询查询 ','\\','','    ','','  ');
insert into test_timebucket_gapfill.tb values('0002-01-10 09:08:19','2008-07-15 22:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e','a',null,'测试测试 ','b','test测试10_1','vwwws中文_1',null);
insert into test_timebucket_gapfill.tb values('0003-05-10 23:37:15.783','2008-07-15 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'测试测试 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_timebucket_gapfill.tb values('0003-05-10 23:42:18.223','2008-07-15 07:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','5555 5','  bdbd','y','test@测试！10_1','vwwws_1','cddde');
insert into test_timebucket_gapfill.tb2 values('2024-02-09 16:16:58.223','2021-06-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','中文  中文', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_timebucket_gapfill.tb2 values('2024-02-10 04:18:19.22','2021-06-10 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_timebucket_gapfill.tb3 values('2025-06-06 08:00:00','2024-06-10 16:16:15.183',800,8000,80000,800000.808888,8000000.808088,true,'d','test测试！！！@TEST1 ','d','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,false,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_timebucket_gapfill.tb3 values('2025-06-06 11:15:15.783','2024-06-10 17:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',8,800,8000,false,-20.123,800.578578,'d','test测试！！！@TEST1  ','d','ddd','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');

select k_timestamp from test_timebucket_gapfill.tb order by k_timestamp;
select k_timestamp from test_timebucket_gapfill.tb2 order by k_timestamp;
select k_timestamp from test_timebucket_gapfill.tb3 order by k_timestamp;

--- limit
select time_bucket_gapfill(k_timestamp,'1mons') as tb from test_timebucket_gapfill.tb group by tb order by tb limit 10;

--- limit..offset..
select time_bucket_gapfill(k_timestamp,'1mons') as tb from test_timebucket_gapfill.tb group by tb order by tb limit 10 offset 2;

-- from subquery
select * from (select time_bucket_gapfill(k_timestamp,'1Y') as tb,max(e1),avg(e6)::int from test_timebucket_gapfill.tb group by tb order by tb) order by tb;
select time_bucket_gapfill(tb1,'4000h') as tb2 from (select time_bucket_gapfill(tb,'3000M') as tb1 from (select time_bucket_gapfill(k_timestamp,'8000000s') as tb,last_row(e11),first(e12) from test_timebucket_gapfill.tb group by tb order by tb) group by tb1 order by tb1) group by tb2 order by tb2;

-- unrelated subquery
select k_timestamp > (select time_bucket_gapfill(k_timestamp,'2000000sec') as tb from test_timebucket_gapfill.tb group by tb order by tb limit 1) from test_timebucket_gapfill.tb order by k_timestamp;

-- correlated subquery
select time_bucket_gapfill(k_timestamp,'100days') as tb from test_timebucket_gapfill.tb as tab1 where e1 <= (select e1 from test_timebucket_gapfill.tb2 as tab2 where tab1.k_timestamp=tab2.k_timestamp) group by tb order by tb;

-- case when
select time_bucket_gapfill(k_timestamp,'9223372036s') as tb,case when max(t3) <= 1000 then 't3的值不大于1000' when max(t3) > 1000 and max(t3) <= 4000 then 't3的值在1000和4000之间' when max(t3) >= 7000 then 't3的值不小于7000' end as result from test_timebucket_gapfill.tb group by tb order by tb;

-- union
select time_bucket_gapfill(k_timestamp,'120min') as tb from test_timebucket_gapfill.tb2 group by tb union select time_bucket_gapfill(k_timestamp,'99min') as tb from test_timebucket_gapfill.tb3 group by tb order by tb;

-- unionall
select time_bucket_gapfill(k_timestamp,'120min') as tb from test_timebucket_gapfill.tb2 group by tb union all select time_bucket_gapfill(k_timestamp,'99min') as tb from test_timebucket_gapfill.tb3 group by tb order by tb;

-- join
select time_bucket_gapfill(tab1.k_timestamp,'60week') as tb from test_timebucket_gapfill.tb as tab1 join test_timebucket_gapfill.tb as tab2 on tab1.k_timestamp = tab2.k_timestamp group by tb order by tb;

-- inner join
select time_bucket_gapfill(tab1.k_timestamp,'12000000s') as tb from test_timebucket_gapfill.tb as tab1 inner join test_timebucket_gapfill.tb as tab2 on tab1.k_timestamp = tab2.k_timestamp group by tb order by tb;

-- full join
select time_bucket_gapfill(tab1.k_timestamp,'50day') as tb from test_timebucket_gapfill.tb as tab1 full join test_timebucket_gapfill.tb as tab2 on tab1.k_timestamp = tab2.k_timestamp group by tb order by tb;

-- left join
select time_bucket_gapfill(tab1.k_timestamp,'45000mins') as tb from test_timebucket_gapfill.tb as tab1 left join test_timebucket_gapfill.tb as tab2 on tab1.k_timestamp = tab2.k_timestamp group by tb order by tb;


-- right join
select time_bucket_gapfill(tab1.k_timestamp,'100weeks') as tb from test_timebucket_gapfill.tb as tab1 right join test_timebucket_gapfill.tb as tab2 on tab1.k_timestamp = tab2.k_timestamp group by tb order by tb;

drop database if exists test_timebucket_gapfill cascade;

-- group multi column
drop database if exists test cascade;
create ts database test;use test;
create table t1(time timestamp not null, a int) tags(b int not null) primary tags(b);
insert into t1 values('2024-08-01 12:00:00', 1,1);
insert into t1 values('2024-09-01 12:00:00', 1,1);
insert into t1 values('2024-07-31 12:00:00', 2,2);
insert into t1 values('2024-08-01 12:00:00', 2,2);
insert into t1 values('2024-09-01 12:00:00', 2,2);
select time_bucket_gapfill(t1.time, '10 day') as tb, b from t1 group by tb, b;
select time_bucket_gapfill(t1.time, '10 day') as tb, b from t1 group by tb, b order by b desc, tb;
select time_bucket_gapfill(t1.time, '10 day') as tb, b from t1 group by tb, b order by b desc, tb desc;
drop database if exists test cascade;

SET CLUSTER SETTING ts.blocks_per_segment.max_limit=100;
SET CLUSTER SETTING ts.rows_per_block.max_limit=100;
create ts database test_timebucket_gapfill1;
create table test_timebucket_gapfill1.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);
create table test_timebucket_gapfill1.tb2(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t2,t7,t9,t10);
create table test_timebucket_gapfill1.tb3(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2,t2 int,t3 int8 not null,t4 bool,t5 float4,t6 float8,t7 char,t8 char(100),t9 nchar,t10 nchar(254),t11 varchar not null,t12 varchar(128),t13 varbytes not null,t14 varbytes(100) not null,t15 varbytes,t16 varbytes(255)) primary tags(t3,t11);
insert into test_timebucket_gapfill1.tb values('0001-11-06 17:10:55.123','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_timebucket_gapfill1.tb values('0001-11-06 17:10:23','1999-02-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_timebucket_gapfill1.tb values('0001-12-01 12:10:25','2000-03-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','查询查询 ','\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_timebucket_gapfill1.tb values('0001-12-01 12:10:23.456','2000-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'','     ',' ','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_timebucket_gapfill1.tb values('0002-01-03 09:08:31.22','2000-05-01 22:30:11',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\', 'v2551cdf~#   ', '  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',3,300,300,false,60.666,600.678,'','\\test测试！！！@TEST1',' ','test测试！T  EST1xaa','查询查询 ','\\','','    ','','  ');
insert into test_timebucket_gapfill1.tb values('0002-01-10 09:08:19','2008-07-15 22:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e','a',null,'测试测试 ','b','test测试10_1','vwwws中文_1',null);
insert into test_timebucket_gapfill1.tb values('0003-05-10 23:37:15.783','2008-07-15 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'测试测试 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_timebucket_gapfill1.tb values('0003-05-10 23:42:18.223','2008-07-15 07:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','5555 5','  bdbd','y','test@测试！10_1','vwwws_1','cddde');
insert into test_timebucket_gapfill1.tb2 values('2024-02-09 16:16:58.223','2021-06-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','中文  中文', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_timebucket_gapfill1.tb2 values('2024-02-10 04:18:19.22','2021-06-10 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_timebucket_gapfill1.tb3 values('2025-06-06 08:00:00','2024-06-10 16:16:15.183',800,8000,80000,800000.808888,8000000.808088,true,'d','test测试！！！@TEST1 ','d','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,false,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_timebucket_gapfill1.tb3 values('2025-06-06 11:15:15.783','2024-06-10 17:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',8,800,8000,false,-20.123,800.578578,'d','test测试！！！@TEST1  ','d','ddd','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');

select time_bucket_gapfill(tb,'30000000s') as tb1 from (select time_bucket_gapfill(k_timestamp,'10000000s') as tb,interpolate(count(e2),null),interpolate(count(distinct(t1)),'null') from test_timebucket_gapfill1.tb where e2 + t1 < 0 group by tb,e2,t1 order by tb,e2,t1) group by tb1 order by tb1;
select time_bucket_gapfill(tb,'30000000s') as tb1, ip1  from (select time_bucket_gapfill(k_timestamp,'10000000s') as tb,interpolate(count(e2),null) as ip1,interpolate(count(distinct(t1)),'null') as ip2 from test_timebucket_gapfill1.tb where e2 + t1 < 0 group by tb,e2,t1 order by tb,e2,t1) group by tb1, ip1 order by tb1;
select time_bucket_gapfill(k_timestamp,'600000mins') as tb,interpolate(sum(e3),'prev') from test_timebucket_gapfill1.tb group by tb union select time_bucket_gapfill(k_timestamp,'5months') as tb,interpolate(avg(t2),prev) from test_timebucket_gapfill1.tb2 group by tb order by tb;
drop database if exists test_timebucket_gapfill1 cascade;

-- time_bucket_gapFill support microsecond
use defaultdb;drop database if exists db1 cascade;
create ts database db1;
create table db1.t1(ts timestamp not null, a int) tags(b int not null) primary tags(b);
insert into db1.t1 values(1, 100, 100);
insert into db1.t1 values(10, 100, 100);
insert into db1.t1 values(100, 100, 100);
select time_bucket_gapfill(ts, '1ms') as tb from db1.t1 group by tb order by tb limit 11;
drop database if exists db1 cascade;

-- fix(ZDP-43653)
use defaultdb;drop database if exists test_select_timebucket_ms cascade;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit=50;
SET CLUSTER SETTING ts.rows_per_block.max_limit=50;
create ts database test_select_timebucket_ms;
create table test_select_timebucket_ms.tb(k_timestamp timestamptz not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);
create table test_select_timebucket_ms.tb2(k_timestamp timestamptz not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2,t2 int not null,t3 int8,t4 bool,t5 float4,t6 float8,t7 char not null,t8 char(100),t9 nchar not null,t10 nchar(254) not null,t11 varchar,t12 varchar(128),t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t2,t7,t9,t10);
create table test_select_timebucket_ms.tb3(k_timestamp timestamptz not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (t1 int2,t2 int,t3 int8 not null,t4 bool,t5 float4,t6 float8,t7 char,t8 char(100),t9 nchar,t10 nchar(254),t11 varchar not null,t12 varchar(128),t13 varbytes not null,t14 varbytes(100) not null,t15 varbytes,t16 varbytes(255)) primary tags(t3,t11);
insert into test_select_timebucket_ms.tb values('0000-02-16 17:10:20.123','0000-11-01 08:30:00.867',600,6000,60000,760000.767,600000.6060606,true,'w',null,null,null,'test---->测试',null,'测试中文testing-----',null,null,null,null,null,null,null,'中文测试abcwww+++',1,null,6000,true,66.66666,500.56756,'t','test测试！！！@TEST1','e','a',null,'test----->>>64_1','c','test----<>1023_1','test中文测试_1','uuuuwww');
insert into test_select_timebucket_ms.tb values('0000-02-16 17:10:20.123','0000-11-01 08:30:00.867',600,6000,60000,760000.767,600000.6060606,true,'w',null,null,null,'test---->测试',null,'测试中文testing-----',null,null,null,null,null,null,null,'中文测试abcwww+++',2,null,6000,true,66.66666,500.56756,'t','test测试！@TEST1','e','a',null,'test----->>>64_1','c','test<>1023_1','test中文测试_1','uuuuwww');
insert into test_select_timebucket_ms.tb values('0001-11-06 17:10:20.523','1970-01-01 08:00:00.347',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_timebucket_ms.tb values('0001-11-06 17:10:23.500','1970-01-28 08:10:23.126',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_timebucket_ms.tb values('0001-11-06 17:20:35.655','1970-03-01 20:30:00.238',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,400,4000,false,50.555,500.578578,'d','\\test测试！！！@TEST1','e','test测试！T  EST1xaa','查询查询 ','\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_timebucket_ms.tb values('0001-12-20 20:10:43.456','2000-05-01 17:55:55.019',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'','     ',' ','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_timebucket_ms.tb values('0001-12-20 20:10:31.222','2000-05-01 18:30:11.976',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()*&^%{}','\\', 'v2551cdf~#   ', '  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',3,300,300,false,60.666,600.678,'','\\test测试！！！@TEST1',' ','test测试！T  EST1xaa','查询查询 ','\\','','    ','','  ');
insert into test_select_timebucket_ms.tb values('0001-12-20 21:08:19.789','2000-05-01 22:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e','a',null,'测试测试 ','b','test测试10_1','vwwws中文_1',null);
insert into test_select_timebucket_ms.tb values('0002-05-10 03:37:15.783','2008-07-15 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'测试测试 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_timebucket_ms.tb values('0002-05-10 03:42:18.223','2008-07-15 07:00:00.778',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,false,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','5555 5','  bdbd','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_timebucket_ms.tb2 values('0003-02-09 06:16:58.223','2021-06-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','中文  中文', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,600,6000,false,60.6066,600.123455,'a','test测试！！！@TEST1','e','\a',null,'chch4_1','b','test测试10_1','vwwws中文_1',null);
insert into test_select_timebucket_ms.tb2 values('0003-02-10 06:18:19.055','2021-06-10 10:00:00.335',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,true,-10.123,100.111111,'b','\\TEST1 ','f','测试！TEST1xaa','test查询  @TEST1\0','bd64_1','y','test@测试！10_1','vwwws_1','cddde');
insert into test_select_timebucket_ms.tb3 values('0002-06-06 08:00:00','2024-06-10 16:16:15.183',800,8000,80000,800000.808888,8000000.808088,true,'d','test测试！！！@TEST1 ','d','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,false,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_timebucket_ms.tb3 values('0002-06-06 11:15:15.783','2024-06-10 17:04:05.683',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'255测试1cdf~# ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',8,800,8000,false,-20.123,800.578578,'d','test测试！！！@TEST1  ','d','ddd','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');

select time_bucket_gapfill(k_timestamp,'1800000MILLiseconds') as a,interpolate(avg(e4),'next'),interpolate(count(distinct e22),'next') from test_select_timebucket_ms.tb3 group by a order by a;

use defaultdb;drop database test_select_timebucket_ms cascade;

create ts database tsdb;
use tsdb;
create table t2 (ts timestamp not null, status int) tags(a int not null) primary tags(a);
insert into t2 values('2025-01-10 00:00:00+00:00', 1, 1);
insert into t2 values('2025-01-11 00:00:00+00:00', 1, 1);
insert into t2 values('2025-01-12 00:00:00+00:00', 1, 1);
insert into t2 values('2025-01-14 00:00:00+00:00', 1, 1);
insert into t2 values('2025-01-16 00:00:00+00:00', 1, 1);
insert into t2 values('2025-01-20 00:00:00+00:00', 1, 1);
select time_bucket_gapfill(ts, '1day') as tb,COALESCE(interpolate(sum(status),null),25) from t2 group by tb;
use defaultdb;drop database tsdb cascade;