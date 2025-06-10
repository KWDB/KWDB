
-----Part 1: override mode scenario exercise
SET CLUSTER SETTING ts.dedup.rule = 'override';

create ts database test_dedup_function;
create table test_dedup_function.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 char(200),e13 char(255),e14 char(100),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null) primary tags(t1);
insert into test_dedup_function.tb values ('2021-04-01 15:00:00','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                           2);
insert into test_dedup_function.tb values ('2022-05-01 17:00:00','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ','\','255测试1cdf~# ','@TEST1  ','abc255测试1()&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                           3);
-----Write duplicate data
insert into test_dedup_function.tb values ('2021-04-01 15:00:00','2021-05-01 17:00:00',1000,10000,120000,1000000.505555,10000000.505055,true,'z','测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ','\','255测试1cdf~# ','@TEST1  ','abc255测试1()&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xbb\xcc\xcc',b'\xaa\xcc\xcc\xbb',
                                           2);
insert into test_dedup_function.tb values ('2022-05-01 17:00:00','2020-12-06 18:10:23',200,6000,80000,1200000.60612,8000000.4040404,true,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                           3);

select count(*) from test_dedup_function.tb;
select * from test_dedup_function.tb order by k_timestamp;

drop database test_dedup_function cascade;

-----Part 3: Keep Mode Scenario Practice
SET CLUSTER SETTING ts.dedup.rule = 'keep';

create ts database test_dedup_function;
create table test_dedup_function.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 char(200),e13 char(255),e14 char(100),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null) primary tags(t1);
insert into test_dedup_function.tb values ('2021-04-01 15:00:00','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                           2);
insert into test_dedup_function.tb values ('2022-05-01 17:00:00','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ','\','255测试1cdf~# ','@TEST1  ','abc255测试1()&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',
                                           3);
-----Write duplicate data
insert into test_dedup_function.tb values ('2021-04-01 15:00:00','2021-05-01 17:00:00',1000,10000,120000,1000000.505555,10000000.505055,true,'z','测试！！！@TEST1 ','n','类型测试1()*  ','test！@TEST1  ','\','255测试1cdf~# ','@TEST1  ','abc255测试1()&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xbb\xcc\xcc',b'\xaa\xcc\xcc\xbb',
                                           2);
insert into test_dedup_function.tb values ('2022-05-01 17:00:00','2020-12-06 18:10:23',200,6000,80000,1200000.60612,8000000.4040404,true,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,
                                           3);

select count(*) from test_dedup_function.tb;
select * from test_dedup_function.tb order by k_timestamp;

drop database test_dedup_function cascade;

-----Part 6: Repeated data in payload Scenario walkthrough
SET CLUSTER SETTING ts.dedup.rule = 'override';

create ts database test_dedup_function;
create table test_dedup_function.tb(ts timestamp not null, e1 int, e2 float not null, e3 bool, e4 int8, e5 char(10) not null, e6 nchar(10),
                       e7 char(10), e8 char(10) not null) attributes (tag1 int not null) primary tags(tag1);
CREATE TABLE test_dedup_function.var_tb(k_timestamp TIMESTAMP NOT NULL,e1 VARCHAR,e2 VARCHAR(60),e3 VARCHAR(60)) attributes (tag1 int not null) primary tags(tag1);
insert into test_dedup_function.tb values('2023-8-23 12:13:14', 1, 2.2, true, 4, 'a','five', 'six', 'seven', 1),  ('2023-8-23 12:13:14', 2, 4.4, false, 8, 'a','six', 'seven', 'eight', 1),  ('2023-8-23 12:13:14', 4, 8.8, true, 16, 'a','seven', 'eight', 'nine', 1);
INSERT INTO test_dedup_function.var_tb VALUES('2023-8-23 12:13:14','中国','重Abx1827*()!-《》！aa','重Abx1827*()!-《》！aa20', 6),  ('2023-8-23 12:13:14','山东','重Bcx1879*()!-《》！aa','重Bcx1879*()!-《》！bb20', 6), ('2023-8-23 12:13:14','济南','轻Bcx1879*()!-《》！aa','中Bcx1879*()!-《》！bb20', 6);
select count(*) from test_dedup_function.tb;
select * from test_dedup_function.tb order by ts;
select count(*) from test_dedup_function.var_tb;
select * from test_dedup_function.var_tb order by k_timestamp;

drop database test_dedup_function cascade;

-- Fix bug ZDP-34380
set cluster setting ts.dedup.rule='override';
create ts database test_dedup;
create table test_dedup.ds_tb(k_timestamp timestamptz not null, e1 int2 not null, e2 int, e3 int8 not null, e4 float4, e5 float8 not null, e6 bool, e7 timestamptz not null, e8 char(1023), e9 nchar(255) not null, e10 nchar(200), e11 char not null, e12 nchar(200), e13 nchar not null, e14 nchar(200), e15 nchar(200) not null, e16 varbytes, e17 nchar(200) not null, e18 nchar(200),e19 varbytes not null, e20 varbytes, e21 varbytes not null, e22 varbytes, e23 varchar not null, e24 nvarchar
) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes,tall varbytes,screen varbytes,age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
INSERT INTO test_dedup.ds_tb values
('2023-12-12 12:00:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria'),
('2023-12-12 12:00:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria'),
('1970-01-01 00:00:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria'),
('1970-01-01 00:00:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria'),
('2023-12-12 12:10:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria'),
('2023-12-12 12:10:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria'),
('1970-01-01 00:00:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria'),
('1970-01-01 00:00:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
select * from test_dedup.ds_tb order by k_timestamp;
drop table test_dedup.ds_tb;
drop database test_dedup cascade;
