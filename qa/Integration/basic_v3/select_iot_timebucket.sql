DROP DATABASE IF EXISTS test_select_timebacket_value cascade;
CREATE ts DATABASE test_select_timebacket_value;
CREATE TABLE test_select_timebacket_value.t1(
k_timestamp TIMESTAMP NOT NULL, 
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
e21 VARCHAR, e22 NVARCHAR) ATTRIBUTES (
id INT NOT NULL, 
code1 INT2 NOT NULL,
code2 INT,
code3 INT8, 
code4 FLOAT4,
code5 FLOAT8, 
code6 BOOL, 
code7 VARCHAR,
code8 VARCHAR(128) NOT NULL,
code9 VARBYTES,
code10 varbytes(60), 
code11 VARCHAR,
code12 VARCHAR(60), 
code13 CHAR(2),
code14 CHAR(1023) NOT NULL, 
code15 NCHAR,code16 NCHAR(254) NOT NULL) PRIMARY TAGS(code1,code14,code8,id);

INSERT INTO test_select_timebacket_value.t1 VALUES(0,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',1,0,0,0,0,0,false,'','','','','','','','','','');
INSERT INTO test_select_timebacket_value.t1 VALUES(1,0,0,0,0,0,true,999999,'          ','          ','          ',' ','          ',' ',' ','          ','          ','          ',' ',' ','          ','          ','          ',2,0,0,0,0,0,TRUE,'          ',' ',' ','          ','          ','          ','  ','          ',' ','          ');
INSERT INTO test_select_timebacket_value.t1 VALUES('1976-10-20 12:00:12.123',10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',3,-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
INSERT INTO test_select_timebacket_value.t1 VALUES('1979-2-28 11:59:01.999',20002,20000002,200000000002,-20873209.0220322201,-22012110.113011921,false,123,'test数据库语法查询测试！！！@TEST4-8','test数据库语法查询测试！！！@TEST4-9','test数据库语法查询测试！！！@TEST4-10','t','test数据库语法查询测试！！！@TEST4-12','中','test数据库语法查询测试！！！@TEST4-14','test数据库语法查询测试！！！@TEST4-15','test数据库语法查询测试！TEST4-16xaa','test数据库语法查询测试！！！@TEST4-17','test数据库语法查询测试！！！@TEST4-18',b'\xca','test数据库语法查询测试！！！@TEST4-20','test数据库语法查询测试！！！@TEST4-21','test数据库语法查询测试！！！@TEST4-22',4,20002,-20000002,200000000002,-20873209.0220322201,22012110.113011921,true,'test数据库语法查询测试！！！@TEST4-7','test数据库语法查询测试！！！@TEST4-8',b'\xbb','test数据库语法查询测试！！！@TEST4-10','test数据库语法查询测试！！！@TEST4-11','test数据库语法查询测试！！！@TEST4-12','t3','test数据库语法查询测试！！！@TEST4-14','中','test数据库语法查询测试！！！@TEST4-16');
INSERT INTO test_select_timebacket_value.t1 VALUES(318193261000,30003,30000003,300000000003,-33472098.11312001,-39009810.333011921,true,'2015-3-12 10:00:00.234','test数据库语法查询测试！！！@TEST5-8','test数据库语法查询测试！！！@TEST5-9','test数据库语法查询测试！！！@TEST5-10','t','test数据库语法查询测试！！！@TEST5-12','中','test数据库语法查询测试！！！@TEST5-14','test数据库语法查询测试！！！@TEST5-15','test数据库语法查询测试！TEST5-16xaa','test数据库语法查询测试！！！@TEST5-17','test数据库语法查询测试！！！@TEST5-18',b'\xca','test数据库语法查询测试！！！@TEST5-20','test数据库语法查询测试！！！@TEST5-21','test数据库语法查询测试！！！@TEST5-22',5,-30003,30000003,-300000000003,33472098.11312001,-39009810.333011921,false,'test数据库语法查询测试！！！@TEST5-7','test数据库语法查询测试！！！@TEST5-8',b'\xcc','test数据库语法查询测试！！！@TEST5-10','test数据库语法查询测试！！！@TEST5-11','test数据库语法查询测试！！！@TEST5-12','t3','test数据库语法查询测试！！！@TEST5-14','中','test数据库语法查询测试！！！@TEST5-16');
INSERT INTO test_select_timebacket_value.t1 VALUES(318993291090,-10001,10000001,-100000000001,1047200.00312001,1109810.113011921,false,'2023-6-23 05:00:00.55','test数据库语法查询测试！！！@TEST6-8','test数据库语法查询测试！！！@TEST6-9','test数据库语法查询测试！！！@TEST6-10','t','test数据库语法查询测试！！！@TEST6-12','中','test数据库语法查询测试！！！@TEST6-14','test数据库语法查询测试！！！@TEST6-15','test数据库语法查询测试！TEST6-16xaa','test数据库语法查询测试！！！@TEST6-17','test数据库语法查询测试！！！@TEST6-18',b'\xca','test数据库语法查询测试！！！@TEST6-20','test数据库语法查询测试！！！@TEST6-21','test数据库语法查询测试！！！@TEST6-22',6,10001,-10000001,100000000001,424721.022311,4909810.11301191,true,'test数据库语法查询测试！！！@TEST6-7','test数据库语法查询测试！！！@TEST6-8',b'\xdd','test数据库语法查询测试！！！@TEST6-10','test数据库语法查询测试！！！@TEST6-11','test数据库语法查询测试！！！@TEST6-12','t3','test数据库语法查询测试！！！@TEST6-14','中','test数据库语法查询测试！！！@TEST6-16');
INSERT INTO test_select_timebacket_value.t1 VALUES(318995291029,-20002,20000002,-200000000002,20873209.0220322201,22012110.113011921,true,'2016-7-17 20:12:00.12','test数据库语法查询测试！！！@TEST7-8','test数据库语法查询测试！！！@TEST7-9','test数据库语法查询测试！！！@TEST7-10','t','test数据库语法查询测试！！！@TEST7-12','中','test数据库语法查询测试！！！@TEST7-14','test数据库语法查询测试！！！@TEST7-15','test数据库语法查询测试！TEST7-16xaa','test数据库语法查询测试！！！@TEST7-17','test数据库语法查询测试！！！@TEST7-18',b'\xca','test数据库语法查询测试！！！@TEST7-20','test数据库语法查询测试！！！@TEST7-21','test数据库语法查询测试！！！@TEST7-22',7,-20002,20000002,-200000000002,555500.0055505,55505532.553015321,false,'test数据库语法查询测试！！！@TEST7-7','test数据库语法查询测试！！！@TEST7-8',b'\xee','test数据库语法查询测试！！！@TEST7-10','test数据库语法查询测试！！！@TEST7-11','test数据库语法查询测试！！！@TEST7-12','t3','test数据库语法查询测试！！！@TEST7-14','中','test数据库语法查询测试！！！@TEST7-16');
INSERT INTO test_select_timebacket_value.t1 VALUES(318995302501,-30003,30000003,-300000000003,33472098.11312001,39009810.333011921,false,4565476,'test数据库语法查询测试！！！@TEST8-8','test数据库语法查询测试！！！@TEST8-9','test数据库语法查询测试！！！@TEST8-10','t','test数据库语法查询测试！！！@TEST8-12','中','test数据库语法查询测试！！！@TEST8-14','test数据库语法查询测试！！！@TEST8-15','test数据库语法查询测试！TEST8-16xaa','test数据库语法查询测试！！！@TEST8-17','test数据库语法查询测试！！！@TEST8-18',b'\xca','test数据库语法查询测试！！！@TEST8-20','test数据库语法查询测试！！！@TEST8-21','test数据库语法查询测试！！！@TEST8-22',8,30003,-30000003,300000000003,6900.0012345,6612.1215,true,'test数据库语法查询测试！！！@TEST8-7','test数据库语法查询测试！！！@TEST8-8',b'\xff','test数据库语法查询测试！！！@TEST8-10','test数据库语法查询测试！！！@TEST8-11','test数据库语法查询测试！！！@TEST8-12','t3','test数据库语法查询测试！！！@TEST8-14','中','test数据库语法查询测试！！！@TEST8-16');
INSERT INTO test_select_timebacket_value.t1 VALUES('2001-12-9 09:48:12.30',null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,9,30000,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull');
INSERT INTO test_select_timebacket_value.t1 VALUES('2002-2-22 10:48:12.899',32767,-2147483648,9223372036854775807,-99999999991.9999999991,9999999999991.999999999991,true,'2020-10-1 12:00:01.0','test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t','test数据库语法查询测试！！！@TEST10-12','中','test数据库语法查询测试！！！@TEST10-14','test数据库语法查询测试！！！@TEST10-15','test数据库语法查询测试！TEST10-16xaa','test数据库语法查询测试！！！@TEST10-17','test数据库语法查询测试！！！@TEST10-18',b'\xca','test数据库语法查询测试！！！@TEST10-20','test数据库语法查询测试！！！@TEST10-21','test数据库语法查询测试！！！@TEST10-22',10,1,111,1111111,1472011.12345,1109810.113011921,false,'test数据库语法查询测试！！！@TEST10-7','test数据库语法查询测试！！！@TEST10-8',b'\xcc','test数据库语法查询测试！！！@TEST10-10','test数据库语法查询测试！！！@TEST10-11','test数据库语法查询测试！！！@TEST10-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16');
INSERT INTO test_select_timebacket_value.t1 VALUES('2003-10-1 11:48:12.1',-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',11,2,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST11-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16');
INSERT INTO test_select_timebacket_value.t1 VALUES('2004-9-9 00:00:00.9',12000,12000000,120000000000,-12000021.003125,-122209810.1131921,true,'2129-3-1 12:00:00.011','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','t','aaaaaabbbbbbcccccc','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','c','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc',12,-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'aaaaaabbbbbbcccccc','b','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','ty','aaaaaabbbbbbcccccc','u','aaaaaabbbbbbcccccc');
INSERT INTO test_select_timebacket_value.t1 VALUES('2004-12-31 12:10:10.911',23000,23000000,230000000000,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK',13,20002,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK');
INSERT INTO test_select_timebacket_value.t1 VALUES('2008-2-29 2:10:10.111',32767,34000000,340000000000,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321',14,-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321');
INSERT INTO test_select_timebacket_value.t1 VALUES('2012-02-29 1:10:10.000',-32767,-34000000,-340000000000,43000079.07812032,122209810.1131921,true,'2099-9-1 11:01:00.111','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','1','数据库语法查询测试','2','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','9','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试',15,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'数据库语法查询测试','8','7','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','65','数据库语法查询测试','4','数据库语法查询测试');
INSERT INTO test_select_timebacket_value.t1 VALUES(1344618710110,11111,-11111111,111111111111,-11111.11111,11111111.11111111,false,'2017-12-11 09:10:00.200',e' ',e' ',e' ',e' ',e' ',e' ',e' ',e' ',e' ',e' ',e' ',e' ',e' ',e' ',e' ',16,0,0,0,0,0,false,e' ',e' ',e' ',e' ',e' ',e' ',e' ',e' ',e' ',e' ');
INSERT INTO test_select_timebacket_value.t1 VALUES(1374618710110,-11111,11111111,-111111111111,11111.11111,-11111111.11111111,true,'2036-2-3 10:10:00.089',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',17,0,0,0,0,0,false,e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ');
INSERT INTO test_select_timebacket_value.t1 VALUES(1574618710110,22222,-22222222,222222222222,-22222.22222,22222222.22222222,false,'2012-1-1 12:12:00.049' ,e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',18,0,0,0,0,0,false,e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\');
INSERT INTO test_select_timebacket_value.t1 VALUES(1874618710110,-22222,22222222,-222222222222,22222.22222,-22222222.22222222,true,'1980-6-27 19:17:00.123','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\','\\\\\\\\' ,'\','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,' ','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,19,0,0,0,0,0,false,'\\\\\\\\' ,'\\\\\\\\' ,' ','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\ ' ,'\\\\\\\\' ,'\','\\\\\\\\');
INSERT INTO test_select_timebacket_value.t1 VALUES(9223372036000,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\','\0\0中文te@@~eng TE./。\0\\0\0' ,'\','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,20,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\','\0\0中文te@@~eng TE./。\0\\0\0');

select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(e1 + e4) as value from  test_select_timebacket_value.t1 group by k_timestamp order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(e1-e4) as value from  test_select_timebacket_value.t1 group by k_timestamp order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(e1 * e6::int4) as value from  test_select_timebacket_value.t1  group by k_timestamp order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(e1 / e6::int4) as value from  test_select_timebacket_value.t1  group by k_timestamp order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(date_trunc('month', k_timestamp)::int4) AS value FROM test_select_timebacket_value.t1  group by k_timestamp order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(e3#e2) AS value FROM test_select_timebacket_value.t1  group by k_timestamp order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(e3%2) AS value FROM test_select_timebacket_value.t1  group by k_timestamp order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(crc32c(e8)) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(crc32ieee(e8)) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(fnv32(e8)) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(fnv32a(e8)) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(fnv64(e8)) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(fnv64a(e8)) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(age(k_timestamp,k_timestamp)) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(((case e1 when e1 then 0 else -1 end))) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(e1>>1) AS value FROM test_select_timebacket_value.t1  group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(e1<<1) AS value FROM test_select_timebacket_value.t1  group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(e1&1) AS value FROM test_select_timebacket_value.t1  group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(e1|1) AS value FROM test_select_timebacket_value.t1  group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum((e1=1)::int4) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum((e1!=1)::int4) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum((e1>1)::int4) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(length(e8)) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(bit_length(e8)) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(octet_length(e8)) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(get_bit(e20,2)) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,max(initcap(e8)) as value from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(experimental_strftime(timestamp(3)'1970-01-01 00:00:00.123', '%d')::int4) AS value FROM test_select_timebacket_value.t1  group by id,time_bucket(k_timestamp, '600s') order by id asc;
select time_bucket(k_timestamp, '600s') as ts,last(id) as id,sum(width_bucket(0, e2, e3, 3)) from test_select_timebacket_value.t1 group by id,time_bucket(k_timestamp, '600s') order by id asc;
DROP DATABASE IF EXISTS test_select_timebacket_value cascade;