create ts database test_select_function;
create table test_select_function.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 char(100),e13 nchar(100),e14 char(100),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 char(100),t12 nchar(100) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);

SELECT MAX(k_timestamp) AS max_k_timestamp,MIN(k_timestamp) AS min_k_timestamp,COUNT(k_timestamp) AS count_k_timestamp,MAX(e5) AS max_e1,MIN(e5) AS min_e1,SUM(e5) AS sum_e1,AVG(e5) AS avg_e1,COUNT(e5) AS count_e1,MAX(e2) AS max_e2,MIN(e2) AS min_e2,SUM(e2) AS sum_e2,AVG(e2) AS avg_e2,COUNT(e2) AS count_e2,MAX(e3) AS max_e3,MIN(e3) AS min_e3,SUM(e3) AS sum_e3,AVG(e3) AS avg_e3,COUNT(e3) AS count_e3,
       MAX(e4) AS max_e4,MIN(e4) AS min_e4,SUM(e4) AS sum_e4,AVG(e4) AS avg_e4,COUNT(e4) AS count_e4,MAX(e5) AS max_e5,MIN(e5) AS min_e5,SUM(e5) AS sum_e5,AVG(e5) AS avg_e5,COUNT(e5) AS count_e5,MAX(e6) AS max_e6,MIN(e6) AS min_e6,COUNT(e6) AS count_e6,MAX(e7) AS max_e7,MIN(e7) AS min_e7,COUNT(e7) AS count_e7,MAX(e8) AS max_e8,MIN(e8) AS min_e8,COUNT(e8) AS count_e8,MAX(e9) AS max_e9,MIN(e9) AS min_e9,COUNT(e9) AS count_e9,
       MAX(e10) AS max_e10,MIN(e10) AS min_e10,COUNT(e10) AS count_e10,MAX(e11) AS max_e11,MIN(e11) AS min_e11,COUNT(e11) AS count_e11,MAX(e12) AS max_e12,MIN(e12) AS min_e12,COUNT(e12) AS count_e12,MAX(e13) AS max_e13,MIN(e13) AS min_e13,COUNT(e13) AS count_e13,MAX(e14) AS max_e14,MIN(e14) AS min_e14,COUNT(e14) AS count_e14,MAX(e15) AS max_e15,MIN(e15) AS min_e15,COUNT(e15) AS count_e15,
       MAX(e16) AS max_e16,MIN(e16) AS min_e16,COUNT(e16) AS count_e16,MAX(e17) AS max_e17,MIN(e17) AS min_e17,COUNT(e17) AS count_e17,MAX(e18) AS max_e18,MIN(e18) AS min_e18,COUNT(e18) AS count_e18,COUNT(e19) AS count_e19,MAX(e20) AS max_e20,MIN(e20) AS min_e20,COUNT(e20) AS count_e20,MAX(e21) AS max_e21,MIN(e21) AS min_e21,COUNT(e21) AS count_e21,MAX(e22) AS max_e22,MIN(e22) AS min_e22,COUNT(e22) AS count_e22,
       MAX(t1) AS max_code1,MIN(t1) AS min_code1,SUM(t1) AS sum_code1,AVG(t1) AS avg_code1,COUNT(t1) AS count_code1,MAX(t2) AS max_code2,MIN(t2) AS min_code2,SUM(t2) AS sum_code2,AVG(t2) AS avg_code2,COUNT(t2) AS count_code2,MAX(t3) AS max_code3,MIN(t3) AS min_code3,SUM(t3) AS sum_code3,AVG(t3) AS avg_code3,COUNT(t3) AS count_code3,MAX(t4) AS max_flag,MIN(t4) AS min_flag,COUNT(t4) AS count_flag,MAX(t5) AS max_val1,MIN(t5) AS min_val1,SUM(t5) AS sum_val1,AVG(t5) AS avg_val1,COUNT(t5) AS count_val1,MAX(t6) AS max_val2,MIN(t6) AS min_val2,SUM(t6) AS sum_val2,AVG(t6) AS avg_val2,COUNT(t6) AS count_val2,MAX(t7) AS max_location,MIN(t7) AS min_location,COUNT(t7) AS count_location,MAX(t8) AS max_color,MIN(t8) AS min_color,COUNT(t8) AS count_color,COUNT(t9) AS count_name,COUNT(t9) AS count_state,MAX(t9) AS max_tall,MIN(t9) AS min_tall,COUNT(t9) AS count_tall,MAX(t9) AS max_screen,MIN(t9) AS min_screen,COUNT(t9) AS count_screen,MAX(t10) AS max_age,MIN(t10) AS min_age,COUNT(t10) AS count_age,MAX(t11) AS max_sex,MIN(t11) AS min_sex,COUNT(t11) AS count_sex,
       MAX(t12) AS max_year,MIN(t12) AS min_year,COUNT(t12) AS count_year,MAX(t13) AS max_type,MIN(t13) AS min_type,COUNT(t13) AS count_type FROM test_select_function.tb;

drop database test_select_function cascade;

-- ZDP-32383
set cluster setting sql.add_white_list='';
set cluster setting sql.delete_white_list='';
reset cluster setting sql.add_white_list;
reset cluster setting sql.delete_white_list;

-- ZDP-32380
create ts database test;
create table test.tb(k_timestamp timestamptz not null, e1 int2) tags (code1 int2 not null) primary tags(code1);
insert into test.tb values('2024-02-18 06:25:17.353+00:00', 1, 1);
select * from test.tb limit 10;
drop database test cascade;

create ts database loc;
create table loc.tb(k_timestamp timestamptz not null, e1 int2) tags (code1 int2 not null) primary tags(code1);
insert into loc.tb values('2024-02-18 06:25:17.353+00:00', 1, 1);
select k_timestamp from loc.tb;
set time zone 'Asia/Shanghai';
select k_timestamp from loc.tb;
set time zone 'America/New_York';
select k_timestamp from loc.tb;
set time zone 0;
select k_timestamp from loc.tb;
set time zone 8;
select k_timestamp from loc.tb;
drop database loc cascade;

-- ZDP-34369
create ts database test;
create table test.tvar(k_timestamp timestamptz not null, e1 int)tags(code1 int not null, tag1 varchar(32),tag2 varchar(32)) primary tags(code1);
insert into test.tvar values('2024-02-18 06:25:17.351+00:00', 1, 1,'tag1','tag2');
insert into test.tvar(k_timestamp, e1,code1) values('2024-02-18 06:25:17.352+00:00', 1, 2);
insert into test.tvar(k_timestamp, e1,code1) values('2024-02-18 06:25:17.353+00:00', 1, 3);
select * from test.tvar order by k_timestamp;
delete from test.tvar where code1 = 3;
select * from test.tvar order by k_timestamp;
drop database test cascade;

create ts database test;
create table test.tvar(k_timestamp timestamptz not null, e1 int)tags(code1 int not null, tag1 varchar(32),tag2 varchar(32)) primary tags(code1);
insert into test.tvar(k_timestamp, e1,code1) values('2024-02-18 06:25:17.351+00:00', 1, 1);
insert into test.tvar(k_timestamp, e1,code1) values('2024-02-18 06:25:17.352+00:00', 1, 2);
insert into test.tvar(k_timestamp, e1,code1) values('2024-02-18 06:25:17.353+00:00', 1, 3);
select * from test.tvar order by k_timestamp;
drop database test cascade;

--ZDP-32998
create ts database test_select_timebucket;
create table test_select_timebucket.tb(k_timestamp timestamptz not null,e1 timestamptz,e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nchar(100),e16 nchar(255),e17 nchar(255),e18 varbytes,e19 varbytes(100),e20 varbytes(200),e21 varbytes(254),e22 varbytes(200)) tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,t13 varbytes,t14 varbytes(100),t15 varbytes(100),t16 varbytes(200)) primary tags(t1,t4,t8,t12);
insert into test_select_timebucket.tb values ('2020-11-06 17:10:23','1970-01-01 08:00:00',700,7000,70000,700000.707,7000000.1010101,true,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,7000,false,70.7077,700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
insert into test_select_timebucket.tb values ('2020-11-06 17:10:55.123','2019-12-06 18:10:23',100,3000,40000,600000.60612,4000000.4040404,false,' ',' ',' ',' ',' ',' ',' ',null,'','','','','','',null,-32768,-2147483648,-9223372036854775808,false,-9223372036854775807.12345,100.111111,'b','test测试！！！@TEST1 ','','test测试！TEST1xaa','\0test查询  @TEST1\0','e','y','test@@测试！1023_1','vwwws测试_1','cccddde');
insert into test_select_timebucket.tb values ('2022-05-01 12:10:25','2020-05-01 20:30:00',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\\', 'v255测试1cdf~#   ', 'lengthis4096  测试%&!','ar-1', 'ar255()&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,400,4000,false,50.555,500.578578,'d','\test测试！！！@TEST1','e','test测试！T  EST1xaa','查询查询 ',e'\\','e','es1023_2','s_ 4','ww4096_2');
insert into test_select_timebucket.tb values ('2022-05-01 12:10:23.456','2020-05-01 17:00:00',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ','testTest  ','e','40964096 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xbb\xcc',b'\xaa\xaa\xbb\xbb',32767,2147483647,9223372036854775807,true,9223372036854775806.12345,500.578578,'','     ',' ','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_timebucket.tb values ('2022-05-01 12:10:31.22','2020-05-01 22:30:11',500,5000,50000,-500000.505,-5000000.505055,true,'h', 'ar2  ', 'c', 'r255测试2()&^%{}','\', 'v2551cdf~#   ', '  测试%&!','ar-1', 'ar255()*&^%{}  ','6_1测试1&^%{} ','y', 's1023_1','ytes_2', null,b'\xcc\xcc\xdd\xdd',4,300,300,false,60.666,600.678,'','\test测试！！！@TEST1',' ','test测试！T  EST1xaa','查询查询 ',e'\\','','    ','','  ');
insert into test_select_timebucket.tb values ('2023-05-10 09:08:19.22','2021-05-10 09:04:18.223',600,6000,60000,600000.666,666660.101011,true,'r', 'a r3', 'a', 'r255测试1(){}','varchar  中文1', null, 'hof4096查询test%%&!   ',null, 'ar255{}', 'ar4096测试1%{}','e','es1023_0', null, b'\xbb\xee\xff', null,5,null,6000,true,60.6066,600.123455,'a','test测试！！！@TEST1','e',e'\a',null,'测试测试 ','b','test测试10_1','vwwws中文_1',null);
insert into test_select_timebucket.tb values ('2023-05-10 09:15:15.783','2021-06-10 06:04:15.183',500,5000,60000,500000.505555,5000000.505055,false,'c','test测试！！！@TEST1 ','n','类型测试1()*  ',null,null,'测试测试 ','@TEST1  ','abc255测试1()*&^%{}','deg4096测试1(','b','查询1023_2','tes_测试1',b'\xaa\xaa\xaa',b'\xbb\xcc\xbb\xbb',7,200,2000,true,-10.123,500.578578,'c','test测试！！！@TEST1  ','g','abc','\0test查询！！！@TEST1\0','64_3','t','es1023_2','f','tes4096_2');
insert into test_select_timebucket.tb values ('2023-05-10 09:08:18.223','2021-06-01 10:00:00',100,3000,40000,600000.60612,4000000.4040404,false,'r', '\a r3', 'a', 'r255测试1{}','varchar  中文1', null, 'hof4096查询test%&!   ',null, 'ar255{}', 'ar96测试1%{}','e','es1023_0', null, b'\xcc\xee\xdd', null,6,100,1000,false,-10.123,100.111111,'b','\TEST1 ','f','测试！TEST1xaa','5555 5','  bdbd','y','test@测试！10_1','vwwws_1','cddde');
--SELECT
select k_timestamp from test_select_timebucket.tb order by k_timestamp;
select time_bucket(k_timestamp, '10s') as tb from test_select_timebucket.tb group by tb order by tb;
drop database test_select_timebucket cascade;

--ZDP-33076
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
                                    e20 varbytes(60),
                                    e21 VARCHAR,
                                    e22 NVARCHAR)
    ATTRIBUTES (code1 INT2 NOT NULL,code2 INT,code3 INT8,code4 FLOAT4 ,code5 FLOAT8,code6 BOOL,code7 VARCHAR,code8 VARCHAR(128) NOT NULL,code9 varbytes,code10 varbytes(60),code11 VARCHAR,code12 VARCHAR(60),code13 CHAR(2),code14 CHAR(1023) NOT NULL,code15 NCHAR,code16 NCHAR(254) NOT NULL)
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
INSERT INTO test_time_addsub.t1 VALUES(1344618710110,16,11111,-11111111,111111111111,-11111.11111,11111111.11111111,false,'2017-12-11 09:10:00.200',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',0,0,0,0,0,false,e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'');
INSERT INTO test_time_addsub.t1 VALUES(1374618710110,17,-11111,11111111,-111111111111,11111.11111,-11111111.11111111,true,'2036-2-3 10:10:00.089',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',0,0,0,0,0,false,e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ');
INSERT INTO test_time_addsub.t1 VALUES(1574618710110,18,22222,-22222222,222222222222,-22222.22222,22222222.22222222,false,'2012-1-1 12:12:00.049' ,e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',0,0,0,0,0,false,e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\');
INSERT INTO test_time_addsub.t1 VALUES(1874618710110,19,-22222,22222222,-222222222222,22222.22222,-22222222.22222222,true,'1980-6-27 19:17:00.123','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,0,0,0,0,0,false,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\ ' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\');
INSERT INTO test_time_addsub.t1 VALUES(9223372036000,20,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');
SELECT k_timestamp FROM test_time_addsub.t1 GROUP BY k_timestamp HAVING k_timestamp+1month >'2020-1-1 12:00:00' ORDER BY k_timestamp;

--ZDP-35825
SELECT * FROM test_time_addsub.t1 WHERE id > 1 AND k_timestamp >= '2004-9-9 00:00:00.9' AND k_timestamp < '2012-02-29 1:10:10.000' order by id;
SELECT * FROM test_time_addsub.t1 WHERE id > 1 AND k_timestamp >= '2004-9-9 00:00:00.9' AND k_timestamp < '2012-02-29 1:10:10.000' order by id;
SELECT * FROM test_time_addsub.t1 WHERE id > 1 AND k_timestamp >= '2004-9-9 00:00:00.9' AND k_timestamp < '2012-02-29 1:10:10.000' order by id;

DROP DATABASE test_time_addsub cascade;

--ZDP-36085
create ts database ts_db1;
create table ts_db1.ts_table1(k_timestamp timestamp NOT NULL, value int)
attributes(tag1 varchar(4) not null , tag2 varchar(4))
primary tags(tag1);
insert into ts_db1.ts_table1 values('2001-12-9 09:48:12.30', 1, 'AAAA', 'BBBB');
insert into ts_db1.ts_table1 values('2001-12-9 10:48:12.30', 2, 'AAAA', 'BBBB');

select
(select last(t.k_timestamp)
from ts_db1.ts_table1 t
where t.tag1 = 'AAAA')::int -
(select t.k_timestamp
from ts_db1.ts_table1 t
where t.tag1 = 'AAAA'
order by t.k_timestamp desc limit 1 offset 1)::int
as diff_ms;
DROP DATABASE ts_db1 cascade;
