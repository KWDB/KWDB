-- 测试数据准备
set cluster setting ts.ordered_table.enabled=true;
-- 测试数据准备
create ts database test_disorder;
create table test_disorder.t1 (
k_timestamp timestamptz not null,
id int not null,
e1 int2,
e2 int,
e3 int8,
e4 float4,
e5 float8,
e6 bool,
e7 timestamptz,
e8 char(1023),
e9 nchar(255),
e10 varchar(4096),
e11 char,
e12 char(255),
e13 nchar,
e14 nvarchar(4096),
e15 varchar(1023),
e16 nvarchar(200),
e17 nchar(255),
e18 char(200),
e19 varbytes,
e20 varbytes(60),
e21 varchar,
e22 nvarchar) 
tags (code1 int2 not null,
code2 int,
code3 int8,
code4 float4,
code5 float8,
code6 bool,
code7 varchar,
code8 varchar(128) not null,
code9 varbytes,
code10 varbytes(60),
code11 varchar,
code12 varchar(60),
code13 char(2),
code14 char(1023) not null,
code15 nchar,
code16 nchar(254) not null) 
primary tags (code1);

-- 插入乱序数据 十八条数据 三条乱序 数据在两个设备
INSERT INTO test_disorder.t1 VALUES('2022-10-01 12:00:12',1,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',1,0,0,0,0,false,'','','','','','','','','','');
INSERT INTO test_disorder.t1 VALUES('2022-10-06 12:10:12',2,0,0,0,0,0,true,999999,'          ','          ','          ',' ','          ',' ',' ','          ','          ','          ',' ',' ','          ','          ','          ',1,0,0,0,0,TRUE,'          ',' ',' ','          ','          ','          ','  ','          ',' ','          ');
INSERT INTO test_disorder.t1 VALUES('2022-10-10 12:00:12',3,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',1,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
INSERT INTO test_disorder.t1 VALUES('2022-10-14 11:59:01',4,20002,20000002,200000000002,-20873209.0220322201,-22012110.113011921,false,123,'test数据库语法查询测试！！！@TEST4-8','test数据库语法查询测试！！！@TEST4-9','test数据库语法查询测试！！！@TEST4-10','t','test数据库语法查询测试！！！@TEST4-12','中','test数据库语法查询测试！！！@TEST4-14','test数据库语法查询测试！！！@TEST4-15','test数据库语法查询测试！TEST4-16xaa','test数据库语法查询测试！！！@TEST4-17','test数据库语法查询测试！！！@TEST4-18',b'\xca','test数据库语法查询测试！！！@TEST4-20','test数据库语法查询测试！！！@TEST4-21','test数据库语法查询测试！！！@TEST4-22',1,-20000002,200000000002,-20873209.0220322201,22012110.113011921,true,'test数据库语法查询测试！！！@TEST4-7','test数据库语法查询测试！！！@TEST4-8',b'\xbb','test数据库语法查询测试！！！@TEST4-10','test数据库语法查询测试！！！@TEST4-11','test数据库语法查询测试！！！@TEST4-12','t3','test数据库语法查询测试！！！@TEST4-14','中','test数据库语法查询测试！！！@TEST4-16');
INSERT INTO test_disorder.t1 VALUES('2022-10-19 12:00:00',5,30003,30000003,300000000003,-33472098.11312001,-39009810.333011921,true,'2015-3-12 10:00:00.234','test数据库语法查询测试！！！@TEST5-8','test数据库语法查询测试！！！@TEST5-9','test数据库语法查询测试！！！@TEST5-10','t','test数据库语法查询测试！！！@TEST5-12','中','test数据库语法查询测试！！！@TEST5-14','test数据库语法查询测试！！！@TEST5-15','test数据库语法查询测试！TEST5-16xaa','test数据库语法查询测试！！！@TEST5-17','test数据库语法查询测试！！！@TEST5-18',b'\xca','test数据库语法查询测试！！！@TEST5-20','test数据库语法查询测试！！！@TEST5-21','test数据库语法查询测试！！！@TEST5-22',1,30000003,-300000000003,33472098.11312001,-39009810.333011921,false,'test数据库语法查询测试！！！@TEST5-7','test数据库语法查询测试！！！@TEST5-8',b'\xcc','test数据库语法查询测试！！！@TEST5-10','test数据库语法查询测试！！！@TEST5-11','test数据库语法查询测试！！！@TEST5-12','t3','test数据库语法查询测试！！！@TEST5-14','中','test数据库语法查询测试！！！@TEST5-16');
INSERT INTO test_disorder.t1 VALUES('2022-09-15 12:00:00',6,-10001,10000001,-100000000001,1047200.00312001,1109810.113011921,false,'2023-6-23 05:00:00.55','test数据库语法查询测试！！！@TEST6-8','test数据库语法查询测试！！！@TEST6-9','test数据库语法查询测试！！！@TEST6-10','t','test数据库语法查询测试！！！@TEST6-12','中','test数据库语法查询测试！！！@TEST6-14','test数据库语法查询测试！！！@TEST6-15','test数据库语法查询测试！TEST6-16xaa','test数据库语法查询测试！！！@TEST6-17','test数据库语法查询测试！！！@TEST6-18',b'\xca','test数据库语法查询测试！！！@TEST6-20','test数据库语法查询测试！！！@TEST6-21','test数据库语法查询测试！！！@TEST6-22',1,-10000001,100000000001,424721.022311,4909810.11301191,true,'test数据库语法查询测试！！！@TEST6-7','test数据库语法查询测试！！！@TEST6-8',b'\xdd','test数据库语法查询测试！！！@TEST6-10','test数据库语法查询测试！！！@TEST6-11','test数据库语法查询测试！！！@TEST6-12','t3','test数据库语法查询测试！！！@TEST6-14','中','test数据库语法查询测试！！！@TEST6-16');
INSERT INTO test_disorder.t1 VALUES('2022-10-23 12:00:00',7,-20002,20000002,-200000000002,20873209.0220322201,22012110.113011921,true,'2016-7-17 20:12:00.12','test数据库语法查询测试！！！@TEST7-8','test数据库语法查询测试！！！@TEST7-9','test数据库语法查询测试！！！@TEST7-10','t','test数据库语法查询测试！！！@TEST7-12','中','test数据库语法查询测试！！！@TEST7-14','test数据库语法查询测试！！！@TEST7-15','test数据库语法查询测试！TEST7-16xaa','test数据库语法查询测试！！！@TEST7-17','test数据库语法查询测试！！！@TEST7-18',b'\xca','test数据库语法查询测试！！！@TEST7-20','test数据库语法查询测试！！！@TEST7-21','test数据库语法查询测试！！！@TEST7-22',1,20000002,-200000000002,555500.0055505,55505532.553015321,false,'test数据库语法查询测试！！！@TEST7-7','test数据库语法查询测试！！！@TEST7-8',b'\xee','test数据库语法查询测试！！！@TEST7-10','test数据库语法查询测试！！！@TEST7-11','test数据库语法查询测试！！！@TEST7-12','t3','test数据库语法查询测试！！！@TEST7-14','中','test数据库语法查询测试！！！@TEST7-16');
INSERT INTO test_disorder.t1 VALUES('2022-10-27 12:00:00',8,-30003,30000003,-300000000003,33472098.11312001,39009810.333011921,false,4565476,'test数据库语法查询测试！！！@TEST8-8','test数据库语法查询测试！！！@TEST8-9','test数据库语法查询测试！！！@TEST8-10','t','test数据库语法查询测试！！！@TEST8-12','中','test数据库语法查询测试！！！@TEST8-14','test数据库语法查询测试！！！@TEST8-15','test数据库语法查询测试！TEST8-16xaa','test数据库语法查询测试！！！@TEST8-17','test数据库语法查询测试！！！@TEST8-18',b'\xca','test数据库语法查询测试！！！@TEST8-20','test数据库语法查询测试！！！@TEST8-21','test数据库语法查询测试！！！@TEST8-22',1,-30000003,300000000003,6900.0012345,6612.1215,true,'test数据库语法查询测试！！！@TEST8-7','test数据库语法查询测试！！！@TEST8-8',b'\xff','test数据库语法查询测试！！！@TEST8-10','test数据库语法查询测试！！！@TEST8-11','test数据库语法查询测试！！！@TEST8-12','t3','test数据库语法查询测试！！！@TEST8-14','中','test数据库语法查询测试！！！@TEST8-16');
INSERT INTO test_disorder.t1 VALUES('2022-11-02 13:00:00',9,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull');
INSERT INTO test_disorder.t1 VALUES('2022-11-09 10:00:00',10,32767,-2147483648,9223372036854775807,-99999999991.9999999991,9999999999991.999999999991,true,'2020-10-1 12:00:01.0','test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t','test数据库语法查询测试！！！@TEST10-12','中','test数据库语法查询测试！！！@TEST10-14','test数据库语法查询测试！！！@TEST10-15','test数据库语法查询测试！TEST10-16xaa','test数据库语法查询测试！！！@TEST10-17','test数据库语法查询测试！！！@TEST10-18',b'\xca','test数据库语法查询测试！！！@TEST10-20','test数据库语法查询测试！！！@TEST10-21','test数据库语法查询测试！！！@TEST10-22',2,111,1111111,1472011.12345,1109810.113011921,false,'test数据库语法查询测试！！！@TEST10-7','test数据库语法查询测试！！！@TEST10-8',b'\xcc','test数据库语法查询测试！！！@TEST10-10','test数据库语法查询测试！！！@TEST10-11','test数据库语法查询测试！！！@TEST10-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16');
INSERT INTO test_disorder.t1 VALUES('2022-11-13 11:00:00',11,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',2,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST11-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16');
INSERT INTO test_disorder.t1 VALUES('2022-11-20 12:00:00',12,12000,12000000,120000000000,-12000021.003125,-122209810.1131921,true,'2129-3-1 12:00:00.011','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','t','aaaaaabbbbbbcccccc','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','c','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc',2,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'aaaaaabbbbbbcccccc','b','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','ty','aaaaaabbbbbbcccccc','u','aaaaaabbbbbbcccccc');
INSERT INTO test_disorder.t1 VALUES('2022-10-11 03:00:00',13,23000,23000000,230000000000,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK',2,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK');
INSERT INTO test_disorder.t1 VALUES('2022-12-03 10:10:10',14,32767,34000000,340000000000,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321',2,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321');
INSERT INTO test_disorder.t1 VALUES('2022-12-09 12:10:10',15,-32767,-34000000,-340000000000,43000079.07812032,122209810.1131921,true,'2099-9-1 11:01:00.111','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','1','数据库语法查询测试','2','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','9','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试',2,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'数据库语法查询测试','8','7','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','65','数据库语法查询测试','4','数据库语法查询测试');
INSERT INTO test_disorder.t1 VALUES('2022-12-13 13:30:00',16,11111,-11111111,111111111111,-11111.11111,11111111.11111111,false,'2017-12-11 09:10:00.200',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',2,0,0,0,0,false,e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'');
INSERT INTO test_disorder.t1 VALUES('2022-11-01 03:00:00',17,-11111,11111111,-111111111111,11111.11111,-11111111.11111111,true,'2036-2-3 10:10:00.089',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',2,0,0,0,0,false,e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ');
INSERT INTO test_disorder.t1 VALUES('2023-01-01 01:00:00',18,22222,-22222222,222222222222,-22222.22222,22222222.22222222,false,'2012-1-1 12:12:00.049' ,e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',2,0,0,0,0,false,e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\');

-- 打开集群参数
set cluster setting ts.ordered_table.enabled=true;

-- 列查询
select * from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,code1 from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1 from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,code1,code2,code3,code4,code5,code6,code7,code8,code9,code10,code11,code12,code13,code14,code15,code16 from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,code4,code5,code6,code7,code8,code9,code10,code11,code12,code13,code14,code15,code16 from test_disorder.t1 WHERE code1=1 ;

-- 常量查询
select 1 from test_disorder.t1 WHERE code1=1 ;
select '常量' from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,code1,'常量' from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,1,2.2 from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,code1,code2,code3,'常量',code4,code5,code6,code7,code8,code9,code10,code11,code12,code13,code14,code15,code16 from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,'changliang','常量',123000000000,id,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,'常量22',code3,code4,code5,code6,code7,code8,code9,code10,code11,code12,code13,code14,code15,code16 from test_disorder.t1 WHERE code1=1 ;

-- 数值函数
select k_timestamp, id, code1, ceiling(e1) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, ceiling(e4) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, round(e4, 1) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, cos(e4),cos(e5) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, cos(e4)+cos(e5) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, pow(e1,2), pow(round(e5,1), 1), pow(e4, 0) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, abs(e1), abs(e2), abs(e3), abs(e4),abs(e5),abs(code1) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, sqrt(abs(e4)), sqrt(abs(e5)) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, e8, fnv64(e8) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, e5, exp(e5) from test_disorder.t1 WHERE code1=1 ;

-- 空值函数
select k_timestamp, id, code1, e1, ifnull(e1,99111), ifnull(e2,11999), ifnull(e3, 22999), ifnull(e4, 33999), ifnull(e5,44999) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, e1, ifnull(e1+null,1) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, code2, ifnull(code2,1) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, e8, ifnull(e8,'aaaaaabbbbbbcccccc') from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, e14, nullif(e14,'aaaaaabbbbbbcccccc') from test_disorder.t1 WHERE code1=1 ;
select k_timestamp, id, code1, e4, nullif(e4, 0) from test_disorder.t1 WHERE code1=1 ;

-- 字符函数
select k_timestamp,id,code1,e8,substr(e8,4) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,code1,e9,substr(e9,5) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,code1,e10,substr(e10,6) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,code1,e11,substr(e11,7) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,code1,e8,lower(substring(e8,4)) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,code1,e9,upper(substring(e9,5)) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,code1,e10,length(substring(e10,6)) from test_disorder.t1 WHERE code1=1 ;
select k_timestamp,id,code1,e8,e11,concat(substring(e8,4),substring(e11,7)) from test_disorder.t1 WHERE code1=1 ;

-- 时间函数
select time_bucket(k_timestamp, '3600second') as tb from test_disorder.t1 WHERE code1=1 group by tb;
select time_bucket(k_timestamp, '10SECONDS') as tb from test_disorder.t1 WHERE code1=1 group by tb;
select time_bucket(k_timestamp, '10secs') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10sec') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '30s') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10minute') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10MINUTES') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10mins') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10min') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10m') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10Hour') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10hours') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10hrs') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10hr') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10h') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10day') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10days') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10d') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10week') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10weeks') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10w') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '12month') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '12months') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '24mons') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '24mon') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10year') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10years') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10yrs') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10yr') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10y') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '0s') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10h') as tb,* from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10day') as tb,* from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10w') as tb,* from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10month') as tb,* from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket(k_timestamp, '10yr') as tb,* from test_disorder.t1 WHERE code1=1 group by tb ;

--基础验证，最后输出内容较多
select time_bucket_gapfill(k_timestamp, '3600second') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '1000SECONDS') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '10000secs') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '2500sec') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '3000s') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '30minute') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '40MINUTES') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '60mins') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '90min') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '100m') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '100Hour') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '20hours') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '24000hrs') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '5000hr') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '550h') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '106751day') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '106750days') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '106752d') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '200week') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '35weeks') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '4w') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '12month') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '1months') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '24mons') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '2mon') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '5year') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '3years') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '1yrs') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '100yr') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '200y') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '0s') as tb from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '15s') as tb,* from test_disorder.t1 WHERE code1=1 group by tb;
select time_bucket_gapfill(k_timestamp, '10h') as tb,* from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '10day') as tb,* from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '10w') as tb,* from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '10month') as tb,* from test_disorder.t1 WHERE code1=1 group by tb ;
select time_bucket_gapfill(k_timestamp, '10yr') as tb,* from test_disorder.t1 WHERE code1=1 group by tb ;

SELECT k_timestamp,id,e1,e5,e6,e8,e16,code1 FROM test_disorder.t1 WHERE k_timestamp!='2022-12-13 13:30:00' AND code1=1;
SELECT k_timestamp,id,e1,e5,e6,e8,e16,code1 FROM test_disorder.t1 WHERE k_timestamp>'2022-01-13 13:30:00' AND code1=1;
SELECT k_timestamp,id,e1,e5,e6,e8,e16,code1 FROM test_disorder.t1 WHERE k_timestamp<'2023-01-10 10:00:00' AND code1=1;

set cluster setting ts.ordered_table.enabled=default;
drop database test_disorder cascade;
