set cluster setting ts.sql.query_opt_mode = 1100;
DROP DATABASE IF exists test_select_subquery cascade;
CREATE ts DATABASE test_select_subquery;
CREATE TABLE test_select_subquery.t1(
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

DROP DATABASE IF exists test_select_subquery2 cascade;
CREATE DATABASE test_select_subquery2;
CREATE TABLE test_select_subquery2.t1(
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
                e20 VARBYTES,
                e21 VARCHAR,
                e22 NVARCHAR,
                code1 INT2 NOT NULL,
       code2 INT,code3 INT8,
                code4 FLOAT4 ,code5 FLOAT8,
                code6 BOOL,
                code7 VARCHAR,code8 VARCHAR(128) NOT NULL,
                code9 VARBYTES,code10 VARBYTES,
                code11 VARCHAR,code12 VARCHAR(60),
                code13 CHAR(2),code14 CHAR(1023) NOT NULL,
                code15 NCHAR,code16 NCHAR(254) NOT NULL );
INSERT INTO test_select_subquery2.t1 VALUES('1970-1-1',1,0,0,0,0,0,true,'1970-1-1','','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');
INSERT INTO test_select_subquery2.t1 VALUES('1970-1-1 8:00:00.001',2,0,0,0,0,0,true,'1970-1-1 8:00:00.001','          ','          ','          ',' ','          ',' ',' ','          ','          ','          ',' ',' ','          ','          ','          ',0,0,0,0,0,TRUE,'          ',' ',' ','          ','          ','          ','  ','          ',' ','          ');
INSERT INTO test_select_subquery2.t1 VALUES('1976-10-20 12:00:12.123',3,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
INSERT INTO test_select_subquery2.t1 VALUES('1979-2-28 11:59:01.999',4,20002,20000002,200000000002,-20873209.0220322201,-22012110.113011921,false,'1979-2-28 11:59:01.999','test数据库语法查询测试！！！@TEST4-8','test数据库语法查询测试！！！@TEST4-9','test数据库语法查询测试！！！@TEST4-10','t','test数据库语法查询测试！！！@TEST4-12','中','test数据库语法查询测试！！！@TEST4-14','test数据库语法查询测试！！！@TEST4-15','test数据库语法查询测试！TEST4-16xaa','test数据库语法查询测试！！！@TEST4-17','test数据库语法查询测试！！！@TEST4-18',b'\xca','test数据库语法查询测试！！！@TEST4-20','test数据库语法查询测试！！！@TEST4-21','test数据库语法查询测试！！！@TEST4-22',20002,-20000002,200000000002,-20873209.0220322201,22012110.113011921,true,'test数据库语法查询测试！！！@TEST4-7','test数据库语法查询测试！！！@TEST4-8',b'\xbb','test数据库语法查询测试！！！@TEST4-10','test数据库语法查询测试！！！@TEST4-11','test数据库语法查询测试！！！@TEST4-12','t3','test数据库语法查询测试！！！@TEST4-14','中','test数据库语法查询测试！！！@TEST4-16');
INSERT INTO test_select_subquery2.t1 VALUES('1980-2-28 11:59:01.999',5,30003,30000003,300000000003,-33472098.11312001,-39009810.333011921,true,'2015-3-12 10:00:00.234','test数据库语法查询测试！！！@TEST5-8','test数据库语法查询测试！！！@TEST5-9','test数据库语法查询测试！！！@TEST5-10','t','test数据库语法查询测试！！！@TEST5-12','中','test数据库语法查询测试！！！@TEST5-14','test数据库语法查询测试！！！@TEST5-15','test数据库语法查询测试！TEST5-16xaa','test数据库语法查询测试！！！@TEST5-17','test数据库语法查询测试！！！@TEST5-18',b'\xca','test数据库语法查询测试！！！@TEST5-20','test数据库语法查询测试！！！@TEST5-21','test数据库语法查询测试！！！@TEST5-22',-30003,30000003,-300000000003,33472098.11312001,-39009810.333011921,false,'test数据库语法查询测试！！！@TEST5-7','test数据库语法查询测试！！！@TEST5-8',b'\xcc','test数据库语法查询测试！！！@TEST5-10','test数据库语法查询测试！！！@TEST5-11','test数据库语法查询测试！！！@TEST5-12','t3','test数据库语法查询测试！！！@TEST5-14','中','test数据库语法查询测试！！！@TEST5-16');
INSERT INTO test_select_subquery2.t1 VALUES('2000-2-28 11:59:01.999',6,-10001,10000001,-100000000001,1047200.00312001,1109810.113011921,false,'2023-6-23 05:00:00.55','test数据库语法查询测试！！！@TEST6-8','test数据库语法查询测试！！！@TEST6-9','test数据库语法查询测试！！！@TEST6-10','t','test数据库语法查询测试！！！@TEST6-12','中','test数据库语法查询测试！！！@TEST6-14','test数据库语法查询测试！！！@TEST6-15','test数据库语法查询测试！TEST6-16xaa','test数据库语法查询测试！！！@TEST6-17','test数据库语法查询测试！！！@TEST6-18',b'\xca','test数据库语法查询测试！！！@TEST6-20','test数据库语法查询测试！！！@TEST6-21','test数据库语法查询测试！！！@TEST6-22',10001,-10000001,100000000001,424721.022311,4909810.11301191,true,'test数据库语法查询测试！！！@TEST6-7','test数据库语法查询测试！！！@TEST6-8',b'\xdd','test数据库语法查询测试！！！@TEST6-10','test数据库语法查询测试！！！@TEST6-11','test数据库语法查询测试！！！@TEST6-12','t3','test数据库语法查询测试！！！@TEST6-14','中','test数据库语法查询测试！！！@TEST6-16');
INSERT INTO test_select_subquery2.t1 VALUES('2003-1-28 11:59:01.999',7,-20002,20000002,-200000000002,20873209.0220322201,22012110.113011921,true,'2016-7-17 20:12:00.12','test数据库语法查询测试！！！@TEST7-8','test数据库语法查询测试！！！@TEST7-9','test数据库语法查询测试！！！@TEST7-10','t','test数据库语法查询测试！！！@TEST7-12','中','test数据库语法查询测试！！！@TEST7-14','test数据库语法查询测试！！！@TEST7-15','test数据库语法查询测试！TEST7-16xaa','test数据库语法查询测试！！！@TEST7-17','test数据库语法查询测试！！！@TEST7-18',b'\xca','test数据库语法查询测试！！！@TEST7-20','test数据库语法查询测试！！！@TEST7-21','test数据库语法查询测试！！！@TEST7-22',-20002,20000002,-200000000002,555500.0055505,55505532.553015321,false,'test数据库语法查询测试！！！@TEST7-7','test数据库语法查询测试！！！@TEST7-8',b'\xee','test数据库语法查询测试！！！@TEST7-10','test数据库语法查询测试！！！@TEST7-11','test数据库语法查询测试！！！@TEST7-12','t3','test数据库语法查询测试！！！@TEST7-14','中','test数据库语法查询测试！！！@TEST7-16');
INSERT INTO test_select_subquery2.t1 VALUES('2014-10-28 11:59:01.999',8,-30003,30000003,-300000000003,33472098.11312001,39009810.333011921,false,'1979-2-28 11:59:01.999','test数据库语法查询测试！！！@TEST8-8','test数据库语法查询测试！！！@TEST8-9','test数据库语法查询测试！！！@TEST8-10','t','test数据库语法查询测试！！！@TEST8-12','中','test数据库语法查询测试！！！@TEST8-14','test数据库语法查询测试！！！@TEST8-15','test数据库语法查询测试！TEST8-16xaa','test数据库语法查询测试！！！@TEST8-17','test数据库语法查询测试！！！@TEST8-18',b'\xca','test数据库语法查询测试！！！@TEST8-20','test数据库语法查询测试！！！@TEST8-21','test数据库语法查询测试！！！@TEST8-22',30003,-30000003,300000000003,6900.0012345,6612.1215,true,'test数据库语法查询测试！！！@TEST8-7','test数据库语法查询测试！！！@TEST8-8',b'\xff','test数据库语法查询测试！！！@TEST8-10','test数据库语法查询测试！！！@TEST8-11','test数据库语法查询测试！！！@TEST8-12','t3','test数据库语法查询测试！！！@TEST8-14','中','test数据库语法查询测试！！！@TEST8-16');
INSERT INTO test_select_subquery2.t1 VALUES('2001-12-9 09:48:12.30',9,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,30000,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull');
INSERT INTO test_select_subquery2.t1 VALUES('2002-2-22 10:48:12.899',10,32767,-2147483648,9223372036854775807,-99999999991.9999999991,9999999999991.999999999991,true,'2020-10-1 12:00:01.0','test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t','test数据库语法查询测试！！！@TEST10-12','中','test数据库语法查询测试！！！@TEST10-14','test数据库语法查询测试！！！@TEST10-15','test数据库语法查询测试！TEST10-16xaa','test数据库语法查询测试！！！@TEST10-17','test数据库语法查询测试！！！@TEST10-18',b'\xca','test数据库语法查询测试！！！@TEST10-20','test数据库语法查询测试！！！@TEST10-21','test数据库语法查询测试！！！@TEST10-22',1,111,1111111,1472011.12345,1109810.113011921,false,'test数据库语法查询测试！！！@TEST10-7','test数据库语法查询测试！！！@TEST10-8',b'\xcc','test数据库语法查询测试！！！@TEST10-10','test数据库语法查询测试！！！@TEST10-11','test数据库语法查询测试！！！@TEST10-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16');
INSERT INTO test_select_subquery2.t1 VALUES('2003-10-1 11:48:12.1',11,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,'2003-10-1 11:48:12.1','test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',2,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST11-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16');
INSERT INTO test_select_subquery2.t1 VALUES('2004-9-9 00:00:00.9',12,12000,12000000,120000000000,-12000021.003125,-122209810.1131921,true,'2129-3-1 12:00:00.011','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','t','aaaaaabbbbbbcccccc','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','c','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'aaaaaabbbbbbcccccc','b','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','ty','aaaaaabbbbbbcccccc','u','aaaaaabbbbbbcccccc');
INSERT INTO test_select_subquery2.t1 VALUES('2004-12-31 12:10:10.911',13,23000,23000000,230000000000,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK',20002,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK');
INSERT INTO test_select_subquery2.t1 VALUES('2008-2-29 2:10:10.111',14,32767,34000000,340000000000,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321');
INSERT INTO test_select_subquery2.t1 VALUES('2012-02-29 1:10:10.000',15,-32767,-34000000,-340000000000,43000079.07812032,122209810.1131921,true,'2099-9-1 11:01:00.111','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','1','数据库语法查询测试','2','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','9','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试',10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'数据库语法查询测试','8','7','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','65','数据库语法查询测试','4','数据库语法查询测试');
INSERT INTO test_select_subquery2.t1 VALUES('2100-10-1 11:48:12.1',16,11111,-11111111,111111111111,-11111.11111,11111111.11111111,false,'2017-12-11 09:10:00.200',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',0,0,0,0,0,false,e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'');
INSERT INTO test_select_subquery2.t1 VALUES('2113-10-1 11:48:12.1',17,-11111,11111111,-111111111111,11111.11111,-11111111.11111111,true,'2036-2-3 10:10:00.089',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',0,0,0,0,0,false,e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ');
INSERT INTO test_select_subquery2.t1 VALUES('2115-10-1 11:48:12.1',18,22222,-22222222,222222222222,-22222.22222,22222222.22222222,false,'2012-1-1 12:12:00.049' ,e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',0,0,0,0,0,false,e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\');
INSERT INTO test_select_subquery2.t1 VALUES('2117-10-1 11:48:12.1',19,-22222,22222222,-222222222222,22222.22222,-22222222.22222222,true,'1980-6-27 19:17:00.123','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,0,0,0,0,0,false,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\ ' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\');
INSERT INTO test_select_subquery2.t1 VALUES('2213-10-1 11:48:12.1',20,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');


set enable_multimodel=true;
SELECT id,e7,e7,e8    FROM test_select_subquery.t1 t1 GROUP BY id,e7 ,e8  HAVING count(e7 ) > (SELECT count(e7)  FROM test_select_subquery2.t1 t2 WHERE t1.e8 <=t2.e8  ) ORDER BY id;
explain SELECT id,e7,e7,e8    FROM test_select_subquery.t1 t1 GROUP BY id,e7 ,e8  HAVING count(e7 ) > (SELECT count(e7)  FROM test_select_subquery2.t1 t2 WHERE t1.e8 <=t2.e8  ) ORDER BY id;

set enable_multimodel=false;
set cluster setting ts.sql.query_opt_mode = 1110;
DROP DATABASE IF exists test_select_subquery cascade;
DROP DATABASE IF exists test_select_subquery2 cascade;


set enable_multimodel=true;
CREATE DATABASE IF NOT EXISTS runba_tra;

CREATE TABLE IF NOT EXISTS runba_tra.cd_device_point (
  id INT4 NOT NULL,
  point_name VARCHAR(500) NULL,
  adr VARCHAR(255) NULL,
  device_name VARCHAR(500) NULL,
  device_id INT4 NOT NULL,
  index_id INT4 NULL,
  index_upper_value DECIMAL(15, 6) NULL,
  index_lower_value DECIMAL(15, 6) NULL,
  company_id INT4 NULL,
  create_time TIMESTAMP NULL,
  update_time TIMESTAMP NULL,
  CONSTRAINT "primary" PRIMARY KEY (
    id ASC
  ),
  FAMILY "primary" (
    id,
    point_name,
    adr,
    device_name,
    device_id,
    index_id,
    index_upper_value,
    index_lower_value,
    company_id,
    create_time,
    update_time
  )
);

CREATE DATABASE pipec_r;
CREATE TABLE pipec_r.pipeline_info (
  pipeline_sn varchar(16) PRIMARY KEY,
  pipeline_name varchar(60),
  pipe_start varchar(80),
  pipe_end varchar(80),
  pipe_properties varchar(30));
CREATE INDEX pipeline_sn_index ON pipec_r.pipeline_info (pipeline_sn);
CREATE INDEX pipeline_name_index ON pipec_r.pipeline_info (pipeline_name);

CREATE TS DATABASE db_pipec;
CREATE TABLE db_pipec.t_point (
  k_timestamp timestamp NOT NULL,
  measure_value double
 ) ATTRIBUTES (
    point_sn varchar(64) NOT NULL,
    sub_com_sn varchar(32),
    work_area_sn varchar(16),
    station_sn varchar(16),
    pipeline_sn varchar(16),
    measure_type smallint,
    measure_location varchar(64))
  PRIMARY TAGS(point_sn)
  ACTIVETIME 3h;

explain SELECT li.pipeline_name,
       dp.adr,
       SUM(t.measure_value) AS total_value
FROM pipec_r.pipeline_info li
JOIN db_pipec.t_point t ON li.pipeline_sn = t.pipeline_sn
JOIN runba_tra.cd_device_point dp ON cast(dp.index_id as int2) = t.measure_type
WHERE t.k_timestamp BETWEEN '2024-01-01' AND '2024-01-05'
  AND t.measure_value BETWEEN 50 AND 51
  AND dp.index_upper_value > 1.5
GROUP BY li.pipeline_name, dp.adr
ORDER BY li.pipeline_name, total_value;
set enable_multimodel=false;

drop database runba_tra cascade;
drop database pipec_r cascade;
drop database db_pipec cascade;