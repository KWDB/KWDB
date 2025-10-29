USE defaultdb;
DROP DATABASE IF EXISTS test_ddl_dml_mix1 cascade;
CREATE ts DATABASE test_ddl_dml_mix1;
CREATE TABLE test_ddl_dml_mix1.test1(
    ts       TIMESTAMPTZ     NOT NULL,
    id       INT                 ,
    e1       INT2                ,
    e2       INT                 ,
    e3       INT8                ,
    e4       FLOAT4              ,
    e5       FLOAT8              ,
    e6       BOOL                ,
    e7       TIMESTAMP           ,
    e8       CHAR(1023)          ,
    e9       NCHAR(255)          ,
    e10      VARCHAR(4096)       ,
    e11      CHAR                ,
    e12      CHAR(255)           ,
    e13      NCHAR               ,
    e14      NVARCHAR(4096)      ,
    e15      VARCHAR(1023)       , 
    e16      NVARCHAR(200)       ,
    e17      NCHAR(255)          ,
    e18      CHAR(200)           ,           
    e19      VARBYTES            ,
    e20      VARBYTES(60)        ,
    e21      VARCHAR             ,
    e22      NVARCHAR            ) 
TAGS(       
    code1    INT2             NOT NULL ,
    code2    INT                       ,
    code3    INT8                      ,
    code4    FLOAT4                    ,
    code5    FLOAT8                    ,
    code6    BOOL                      ,
    code7    VARCHAR                   ,
    code8    VARCHAR(128)      NOT NULL,
    code9    VARBYTES                  ,
    code10   VARBYTES(60)              ,
    code11   VARCHAR                   ,
    code12   VARCHAR(60)               ,
    code13   CHAR(2)                   ,
    code14   CHAR(1023)       NOT NULL ,
    code15   NCHAR                     ,
    code16   NCHAR(254)       NOT NULL ) 
PRIMARY TAGS(code1,code14,code8,code16);
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1970-01-01 00:00:00+00:00'    ,1 ,0     ,0          ,0                   ,0                   ,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1970-1-1 00:00:00.001'        ,2 ,0     ,0          ,0                   ,0                   ,0,true,999999,'          ','          ','          ',' ','          ',' ',' ','          ','          ','          ',' ',' ','          ','          ','          ',0,0,0,0,0,TRUE,'          ',' ',' ','          ','          ','          ','  ','          ',' ','          ');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1976-10-20 12:00:12.123+00:00',3 ,10001 ,10000001   ,100000000001        ,-1047200.00312001   ,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1979-2-28 11:59:01.999'       ,4 ,20002 ,20000002   ,200000000002        ,-20873209.0220322201,-22012110.113011921,false,123,'test数据库语法查询测试！！！@TEST4-8','test数据库语法查询测试！！！@TEST4-9','test数据库语法查询测试！！！@TEST4-10','t','test数据库语法查询测试！！！@TEST4-12','中','test数据库语法查询测试！！！@TEST4-14','test数据库语法查询测试！！！@TEST4-15','test数据库语法查询测试！TEST4-16xaa','test数据库语法查询测试！！！@TEST4-17','test数据库语法查询测试！！！@TEST4-18',b'\xca','test数据库语法查询测试！！！@TEST4-20','test数据库语法查询测试！！！@TEST4-21','test数据库语法查询测试！！！@TEST4-22',20002,-20000002,200000000002,-20873209.0220322201,22012110.113011921,true,'test数据库语法查询测试！！！@TEST4-7','test数据库语法查询测试！！！@TEST4-8',b'\xbb','test数据库语法查询测试！！！@TEST4-10','test数据库语法查询测试！！！@TEST4-11','test数据库语法查询测试！！！@TEST4-12','t3','test数据库语法查询测试！！！@TEST4-14','中','test数据库语法查询测试！！！@TEST4-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1980-01-31 19:01:01+00:00'    ,5 ,30003 ,30000003   ,300000000003        ,-33472098.11312001  ,-39009810.333011921,true,'2015-3-12 10:00:00.234','test数据库语法查询测试！！！@TEST5-8','test数据库语法查询测试！！！@TEST5-9','test数据库语法查询测试！！！@TEST5-10','t','test数据库语法查询测试！！！@TEST5-12','中','test数据库语法查询测试！！！@TEST5-14','test数据库语法查询测试！！！@TEST5-15','test数据库语法查询测试！TEST5-16xaa','test数据库语法查询测试！！！@TEST5-17','test数据库语法查询测试！！！@TEST5-18',b'\xca','test数据库语法查询测试！！！@TEST5-20','test数据库语法查询测试！！！@TEST5-21','test数据库语法查询测试！！！@TEST5-22',-30003,30000003,-300000000003,33472098.11312001,-39009810.333011921,false,'test数据库语法查询测试！！！@TEST5-7','test数据库语法查询测试！！！@TEST5-8',b'\xcc','test数据库语法查询测试！！！@TEST5-10','test数据库语法查询测试！！！@TEST5-11','test数据库语法查询测试！！！@TEST5-12','t3','test数据库语法查询测试！！！@TEST5-14','中','test数据库语法查询测试！！！@TEST5-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1980-02-10 01:14:51.09+00:00' ,6 ,-10001,10000001   ,-100000000001       ,1047200.00312001    ,1109810.113011921,false,'2023-6-23 05:00:00.55','test数据库语法查询测试！！！@TEST6-8','test数据库语法查询测试！！！@TEST6-9','test数据库语法查询测试！！！@TEST6-10','t','test数据库语法查询测试！！！@TEST6-12','中','test数据库语法查询测试！！！@TEST6-14','test数据库语法查询测试！！！@TEST6-15','test数据库语法查询测试！TEST6-16xaa','test数据库语法查询测试！！！@TEST6-17','test数据库语法查询测试！！！@TEST6-18',b'\xca','test数据库语法查询测试！！！@TEST6-20','test数据库语法查询测试！！！@TEST6-21','test数据库语法查询测试！！！@TEST6-22',10001,-10000001,100000000001,424721.022311,4909810.11301191,true,'test数据库语法查询测试！！！@TEST6-7','test数据库语法查询测试！！！@TEST6-8',b'\xdd','test数据库语法查询测试！！！@TEST6-10','test数据库语法查询测试！！！@TEST6-11','test数据库语法查询测试！！！@TEST6-12','t3','test数据库语法查询测试！！！@TEST6-14','中','test数据库语法查询测试！！！@TEST6-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1980-02-10 01:48:11.029+00:00',7 ,-20002,20000002   ,-200000000002       ,20873209.0220322201 ,22012110.113011921,true,'2016-7-17 20:12:00.12','test数据库语法查询测试！！！@TEST7-8','test数据库语法查询测试！！！@TEST7-9','test数据库语法查询测试！！！@TEST7-10','t','test数据库语法查询测试！！！@TEST7-12','中','test数据库语法查询测试！！！@TEST7-14','test数据库语法查询测试！！！@TEST7-15','test数据库语法查询测试！TEST7-16xaa','test数据库语法查询测试！！！@TEST7-17','test数据库语法查询测试！！！@TEST7-18',b'\xca','test数据库语法查询测试！！！@TEST7-20','test数据库语法查询测试！！！@TEST7-21','test数据库语法查询测试！！！@TEST7-22',-20002,20000002,-200000000002,555500.0055505,55505532.553015321,false,'test数据库语法查询测试！！！@TEST7-7','test数据库语法查询测试！！！@TEST7-8',b'\xee','test数据库语法查询测试！！！@TEST7-10','test数据库语法查询测试！！！@TEST7-11','test数据库语法查询测试！！！@TEST7-12','t3','test数据库语法查询测试！！！@TEST7-14','中','test数据库语法查询测试！！！@TEST7-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1980-02-10 01:48:22.501+00:00',8 ,-30003,30000003   ,-300000000003       ,33472098.11312001   ,39009810.333011921,false,4565476,'test数据库语法查询测试！！！@TEST8-8','test数据库语法查询测试！！！@TEST8-9','test数据库语法查询测试！！！@TEST8-10','t','test数据库语法查询测试！！！@TEST8-12','中','test数据库语法查询测试！！！@TEST8-14','test数据库语法查询测试！！！@TEST8-15','test数据库语法查询测试！TEST8-16xaa','test数据库语法查询测试！！！@TEST8-17','test数据库语法查询测试！！！@TEST8-18',b'\xca','test数据库语法查询测试！！！@TEST8-20','test数据库语法查询测试！！！@TEST8-21','test数据库语法查询测试！！！@TEST8-22',30003,-30000003,300000000003,6900.0012345,6612.1215,true,'test数据库语法查询测试！！！@TEST8-7','test数据库语法查询测试！！！@TEST8-8',b'\xff','test数据库语法查询测试！！！@TEST8-10','test数据库语法查询测试！！！@TEST8-11','test数据库语法查询测试！！！@TEST8-12','t3','test数据库语法查询测试！！！@TEST8-14','中','test数据库语法查询测试！！！@TEST8-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2001-12-09 09:48:12.3+00:00'  ,9 ,null  ,null       ,null                ,null                ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,30000,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2002-2-22 10:48:12.899'       ,10,32767 ,-2147483648,9223372036854775807 ,-99999999991.9999999991,9999999999991.999999999991,true,'2020-10-1 12:00:01.0','test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t','test数据库语法查询测试！！！@TEST10-12','中','test数据库语法查询测试！！！@TEST10-14','test数据库语法查询测试！！！@TEST10-15','test数据库语法查询测试！TEST10-16xaa','test数据库语法查询测试！！！@TEST10-17','test数据库语法查询测试！！！@TEST10-18',b'\xca','test数据库语法查询测试！！！@TEST10-20','test数据库语法查询测试！！！@TEST10-21','test数据库语法查询测试！！！@TEST10-22',1,111,1111111,1472011.12345,1109810.113011921,false,'test数据库语法查询测试！！！@TEST10-7','test数据库语法查询测试！！！@TEST10-8',b'\xcc','test数据库语法查询测试！！！@TEST10-10','test数据库语法查询测试！！！@TEST10-11','test数据库语法查询测试！！！@TEST10-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2003-10-1 11:48:12.1'         ,11,-32768,2147483647 ,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',2,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST11-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2004-09-09 00:00:00.9+00:00'  ,12,12000 ,12000000   ,120000000000        ,-12000021.003125,-122209810.1131921,true,'2129-3-1 12:00:00.011','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','t','aaaaaabbbbbbcccccc','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','c','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'aaaaaabbbbbbcccccc','b','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','ty','aaaaaabbbbbbcccccc','u','aaaaaabbbbbbcccccc');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2004-12-31 12:10:10.911+00:00',13,23000 ,23000000   ,230000000000        ,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK',20002,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2008-2-29 2:10:10.111'        ,14,32767 ,34000000   ,340000000000        ,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2012-02-29 1:10:10.000'       ,15,-32767,-34000000  ,-340000000000       ,43000079.07812032,122209810.1131921,true,'2099-9-1 11:01:00.111','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','1','数据库语法查询测试','2','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','9','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试',10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'数据库语法查询测试','8','7','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','65','数据库语法查询测试','4','数据库语法查询测试');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2012-08-10 17:11:50.11+00:00' ,16,11111 ,-11111111  ,111111111111        ,-11111.11111,11111111.11111111,false,'2017-12-11 09:10:00.200',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',0,0,0,0,0,false,e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2013-07-23 22:31:50.11+00:00' ,17,-11111,11111111   ,-111111111111       ,11111.11111,-11111111.11111111,true,'2036-2-3 10:10:00.089',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',0,0,0,0,0,false,e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2019-11-24 18:05:10.11+00:00' ,18,22222 ,-22222222  ,222222222222        ,-22222.22222,22222222.22222222,false,'2012-1-1 12:12:00.049' ,e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',0,0,0,0,0,false,e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2029-05-27 23:25:10.11+00:00' ,19,-22222,22222222   ,-222222222222       ,22222.22222,-22222222.22222222,true,'1980-6-27 19:17:00.123','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,0,0,0,0,0,false,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\ ' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2262-04-11 23:47:16+00:00'    ,20,-1    ,1          ,-1,1.125            ,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1980-01-01 00:00:00+00:00'    ,1 ,0     ,0          ,0                   ,0                   ,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1980-1-1 00:00:00.001'        ,2 ,0     ,0          ,0                   ,0                   ,0,true,999999,'          ','          ','          ',' ','          ',' ',' ','          ','          ','          ',' ',' ','          ','          ','          ',0,0,0,0,0,TRUE,'          ',' ',' ','          ','          ','          ','  ','          ',' ','          ');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1986-10-20 12:00:12.123+00:00',3 ,10001 ,10000001   ,100000000001        ,-1047200.00312001   ,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1989-2-28 11:59:01.999'       ,4 ,20002 ,20000002   ,200000000002        ,-20873209.0220322201,-22012110.113011921,false,123,'test数据库语法查询测试！！！@TEST4-8','test数据库语法查询测试！！！@TEST4-9','test数据库语法查询测试！！！@TEST4-10','t','test数据库语法查询测试！！！@TEST4-12','中','test数据库语法查询测试！！！@TEST4-14','test数据库语法查询测试！！！@TEST4-15','test数据库语法查询测试！TEST4-16xaa','test数据库语法查询测试！！！@TEST4-17','test数据库语法查询测试！！！@TEST4-18',b'\xca','test数据库语法查询测试！！！@TEST4-20','test数据库语法查询测试！！！@TEST4-21','test数据库语法查询测试！！！@TEST4-22',20002,-20000002,200000000002,-20873209.0220322201,22012110.113011921,true,'test数据库语法查询测试！！！@TEST4-7','test数据库语法查询测试！！！@TEST4-8',b'\xbb','test数据库语法查询测试！！！@TEST4-10','test数据库语法查询测试！！！@TEST4-11','test数据库语法查询测试！！！@TEST4-12','t3','test数据库语法查询测试！！！@TEST4-14','中','test数据库语法查询测试！！！@TEST4-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1990-01-31 19:01:01+00:00'    ,5 ,30003 ,30000003   ,300000000003        ,-33472098.11312001  ,-39009810.333011921,true,'2015-3-12 10:00:00.234','test数据库语法查询测试！！！@TEST5-8','test数据库语法查询测试！！！@TEST5-9','test数据库语法查询测试！！！@TEST5-10','t','test数据库语法查询测试！！！@TEST5-12','中','test数据库语法查询测试！！！@TEST5-14','test数据库语法查询测试！！！@TEST5-15','test数据库语法查询测试！TEST5-16xaa','test数据库语法查询测试！！！@TEST5-17','test数据库语法查询测试！！！@TEST5-18',b'\xca','test数据库语法查询测试！！！@TEST5-20','test数据库语法查询测试！！！@TEST5-21','test数据库语法查询测试！！！@TEST5-22',-30003,30000003,-300000000003,33472098.11312001,-39009810.333011921,false,'test数据库语法查询测试！！！@TEST5-7','test数据库语法查询测试！！！@TEST5-8',b'\xcc','test数据库语法查询测试！！！@TEST5-10','test数据库语法查询测试！！！@TEST5-11','test数据库语法查询测试！！！@TEST5-12','t3','test数据库语法查询测试！！！@TEST5-14','中','test数据库语法查询测试！！！@TEST5-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1990-02-10 01:14:51.09+00:00' ,6 ,-10001,10000001   ,-100000000001       ,1047200.00312001    ,1109810.113011921,false,'2023-6-23 05:00:00.55','test数据库语法查询测试！！！@TEST6-8','test数据库语法查询测试！！！@TEST6-9','test数据库语法查询测试！！！@TEST6-10','t','test数据库语法查询测试！！！@TEST6-12','中','test数据库语法查询测试！！！@TEST6-14','test数据库语法查询测试！！！@TEST6-15','test数据库语法查询测试！TEST6-16xaa','test数据库语法查询测试！！！@TEST6-17','test数据库语法查询测试！！！@TEST6-18',b'\xca','test数据库语法查询测试！！！@TEST6-20','test数据库语法查询测试！！！@TEST6-21','test数据库语法查询测试！！！@TEST6-22',10001,-10000001,100000000001,424721.022311,4909810.11301191,true,'test数据库语法查询测试！！！@TEST6-7','test数据库语法查询测试！！！@TEST6-8',b'\xdd','test数据库语法查询测试！！！@TEST6-10','test数据库语法查询测试！！！@TEST6-11','test数据库语法查询测试！！！@TEST6-12','t3','test数据库语法查询测试！！！@TEST6-14','中','test数据库语法查询测试！！！@TEST6-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1990-02-10 01:48:11.029+00:00',7 ,-20002,20000002   ,-200000000002       ,20873209.0220322201 ,22012110.113011921,true,'2016-7-17 20:12:00.12','test数据库语法查询测试！！！@TEST7-8','test数据库语法查询测试！！！@TEST7-9','test数据库语法查询测试！！！@TEST7-10','t','test数据库语法查询测试！！！@TEST7-12','中','test数据库语法查询测试！！！@TEST7-14','test数据库语法查询测试！！！@TEST7-15','test数据库语法查询测试！TEST7-16xaa','test数据库语法查询测试！！！@TEST7-17','test数据库语法查询测试！！！@TEST7-18',b'\xca','test数据库语法查询测试！！！@TEST7-20','test数据库语法查询测试！！！@TEST7-21','test数据库语法查询测试！！！@TEST7-22',-20002,20000002,-200000000002,555500.0055505,55505532.553015321,false,'test数据库语法查询测试！！！@TEST7-7','test数据库语法查询测试！！！@TEST7-8',b'\xee','test数据库语法查询测试！！！@TEST7-10','test数据库语法查询测试！！！@TEST7-11','test数据库语法查询测试！！！@TEST7-12','t3','test数据库语法查询测试！！！@TEST7-14','中','test数据库语法查询测试！！！@TEST7-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('1990-02-10 01:48:22.501+00:00',8 ,-30003,30000003   ,-300000000003       ,33472098.11312001   ,39009810.333011921,false,4565476,'test数据库语法查询测试！！！@TEST8-8','test数据库语法查询测试！！！@TEST8-9','test数据库语法查询测试！！！@TEST8-10','t','test数据库语法查询测试！！！@TEST8-12','中','test数据库语法查询测试！！！@TEST8-14','test数据库语法查询测试！！！@TEST8-15','test数据库语法查询测试！TEST8-16xaa','test数据库语法查询测试！！！@TEST8-17','test数据库语法查询测试！！！@TEST8-18',b'\xca','test数据库语法查询测试！！！@TEST8-20','test数据库语法查询测试！！！@TEST8-21','test数据库语法查询测试！！！@TEST8-22',30003,-30000003,300000000003,6900.0012345,6612.1215,true,'test数据库语法查询测试！！！@TEST8-7','test数据库语法查询测试！！！@TEST8-8',b'\xff','test数据库语法查询测试！！！@TEST8-10','test数据库语法查询测试！！！@TEST8-11','test数据库语法查询测试！！！@TEST8-12','t3','test数据库语法查询测试！！！@TEST8-14','中','test数据库语法查询测试！！！@TEST8-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2011-12-09 09:48:12.3+00:00'  ,9 ,null  ,null       ,null                ,null                ,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,30000,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2012-2-22 10:48:12.899'       ,10,32767 ,-2147483648,9223372036854775807 ,-99999999991.9999999991,9999999999991.999999999991,true,'2020-10-1 12:00:01.0','test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t','test数据库语法查询测试！！！@TEST10-12','中','test数据库语法查询测试！！！@TEST10-14','test数据库语法查询测试！！！@TEST10-15','test数据库语法查询测试！TEST10-16xaa','test数据库语法查询测试！！！@TEST10-17','test数据库语法查询测试！！！@TEST10-18',b'\xca','test数据库语法查询测试！！！@TEST10-20','test数据库语法查询测试！！！@TEST10-21','test数据库语法查询测试！！！@TEST10-22',1,111,1111111,1472011.12345,1109810.113011921,false,'test数据库语法查询测试！！！@TEST10-7','test数据库语法查询测试！！！@TEST10-8',b'\xcc','test数据库语法查询测试！！！@TEST10-10','test数据库语法查询测试！！！@TEST10-11','test数据库语法查询测试！！！@TEST10-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2013-10-1 11:48:12.1'         ,11,-32768,2147483647 ,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',2,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST11-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2014-09-09 00:00:00.9+00:00'  ,12,12000 ,12000000   ,120000000000        ,-12000021.003125,-122209810.1131921,true,'2129-3-1 12:00:00.011','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','t','aaaaaabbbbbbcccccc','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','c','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'aaaaaabbbbbbcccccc','b','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','ty','aaaaaabbbbbbcccccc','u','aaaaaabbbbbbcccccc');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2014-12-31 12:10:10.911+00:00',13,23000 ,23000000   ,230000000000        ,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK',20002,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2018-3-24 2:10:10.111'        ,14,32767 ,34000000   ,340000000000        ,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2022-07-29 1:10:10.000'       ,15,-32767,-34000000  ,-340000000000       ,43000079.07812032,122209810.1131921,true,'2099-9-1 11:01:00.111','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','1','数据库语法查询测试','2','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','9','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试',10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'数据库语法查询测试','8','7','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','65','数据库语法查询测试','4','数据库语法查询测试');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2022-08-10 17:11:50.11+00:00' ,16,11111 ,-11111111  ,111111111111        ,-11111.11111,11111111.11111111,false,'2017-12-11 09:10:00.200',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',0,0,0,0,0,false,e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2023-07-23 22:31:50.11+00:00' ,17,-11111,11111111   ,-111111111111       ,11111.11111,-11111111.11111111,true,'2036-2-3 10:10:00.089',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',0,0,0,0,0,false,e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2029-11-24 18:05:10.11+00:00' ,18,22222 ,-22222222  ,222222222222        ,-22222.22222,22222222.22222222,false,'2012-1-1 12:12:00.049' ,e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',0,0,0,0,0,false,e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2039-05-27 23:25:10.11+00:00' ,19,-22222,22222222   ,-222222222222       ,22222.22222,-22222222.22222222,true,'1980-6-27 19:17:00.123','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\'  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,0,0,0,0,0,false,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\ ' ,'\\\\\\\\' ,'\'  ,'\\\\\\\\');
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2252-04-11 23:47:16+00:00'    ,20,-1    ,1          ,-1,1.125            ,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');


DELETE FROM test_ddl_dml_mix1.test1 WHERE code1 = 1 AND code14 = 'test数据库语法查询测试！！！@TEST10-14' AND code8 = 'test数据库语法查询测试！！！@TEST10-8' AND code16 = 'test数据库语法查询测试！！！@TEST10-16';
DELETE FROM test_ddl_dml_mix1.test1 WHERE ts = '1990-01-31 19:01:01+00:00';
DELETE FROM test_ddl_dml_mix1.test1 WHERE ts = '1980-01-31 19:01:01+00:00';



ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e1     SET DATA TYPE VARCHAR(10);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e2     SET DATA TYPE VARCHAR(40);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e3     SET DATA TYPE VARCHAR(200);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e4     SET DATA TYPE VARCHAR(100);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e5     SET DATA TYPE VARCHAR(40);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e6     SET DATA TYPE BOOL;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e7     SET DATA TYPE TIMESTAMPTZ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e8     SET DATA TYPE VARCHAR(2000);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e9     SET DATA TYPE VARCHAR(1456);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e10    SET DATA TYPE NVARCHAR(4096);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e11    SET DATA TYPE NVARCHAR(4096);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e12    SET DATA TYPE VARCHAR(600);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e13    SET DATA TYPE VARCHAR(600);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e14    SET DATA TYPE VARCHAR(4096);--无法转换，原类型已经是最大
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e15    SET DATA TYPE NVARCHAR(1023);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e16    SET DATA TYPE VARCHAR(801);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e17    SET DATA TYPE NVARCHAR(600);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e18    SET DATA TYPE VARCHAR(600);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e19    SET DATA TYPE VARBYTES(300);--无法转换，VARBYTES不支持
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e20    SET DATA TYPE VARBYTES(100);--无法转换，VARBYTES不支持
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e21    SET DATA TYPE VARCHAR(600);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e22    SET DATA TYPE VARCHAR(600);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code1  SET DATA TYPE INT8;         --无法转换，code1为ptag
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code2  SET DATA TYPE INT8;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code3  SET DATA TYPE INT8;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code4  SET DATA TYPE VARCHAR;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code5  SET DATA TYPE VARCHAR;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code6  SET DATA TYPE BOOL;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code7  SET DATA TYPE VARCHAR;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code8  SET DATA TYPE VARCHAR;     --无法转换，code1为ptag
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code9  SET DATA TYPE VARCHAR;     --无法转换，VARBYTES不支持
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code10 SET DATA TYPE VARCHAR;     --无法转换，VARBYTES不支持
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code11 SET DATA TYPE VARCHAR;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code12 SET DATA TYPE VARCHAR;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code13 SET DATA TYPE VARCHAR;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code14 SET DATA TYPE VARCHAR;    --无法转换，code1为ptag
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code15 SET DATA TYPE VARCHAR;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code16 SET DATA TYPE VARCHAR;    --无法转换，code1为ptag

ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e1     SET DATA TYPE INT2         ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e2     SET DATA TYPE INT          ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e3     SET DATA TYPE INT8         ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e4     SET DATA TYPE FLOAT4       ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e5     SET DATA TYPE FLOAT8       ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e6     SET DATA TYPE BOOL         ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e7     SET DATA TYPE TIMESTAMP    ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e8     SET DATA TYPE CHAR(1023)   ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e9     SET DATA TYPE NCHAR(255)   ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e10    SET DATA TYPE VARCHAR(4096);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e11    SET DATA TYPE CHAR         ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e12    SET DATA TYPE CHAR(255)    ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e13    SET DATA TYPE NCHAR        ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e14    SET DATA TYPE NVARCHAR(4096);  
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e15    SET DATA TYPE VARCHAR(1023);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e16    SET DATA TYPE NVARCHAR(200);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e17    SET DATA TYPE NCHAR(255)   ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e18    SET DATA TYPE CHAR(200)    ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e19    SET DATA TYPE VARBYTES     ;  --无法转换，VARBYTES不支持
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e20    SET DATA TYPE varbytes(60) ;  --无法转换，VARBYTES不支持
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e21    SET DATA TYPE VARCHAR      ;  
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e22    SET DATA TYPE NVARCHAR     ;  
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code1  SET DATA TYPE INT2         ;  --无法转换，code1为ptag
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code2  SET DATA TYPE INT          ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code3  SET DATA TYPE INT8         ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code4  SET DATA TYPE FLOAT4       ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code5  SET DATA TYPE FLOAT8       ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code6  SET DATA TYPE BOOL         ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code7  SET DATA TYPE VARCHAR      ;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code8  SET DATA TYPE VARCHAR(128) ;  --无法转换，code1为ptag
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code9  SET DATA TYPE VARBYTES     ;  --无法转换，VARBYTES不支持
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code10 SET DATA TYPE VARBYTES(60) ;  --无法转换，VARBYTES不支持
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code11 SET DATA TYPE VARCHAR      ;  
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code12 SET DATA TYPE VARCHAR(60)  ;  
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code13 SET DATA TYPE CHAR(2)      ;  
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code14 SET DATA TYPE CHAR(1023)   ;  --无法转换，code1为ptag
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code15 SET DATA TYPE NCHAR        ;  
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code16 SET DATA TYPE NCHAR(254)   ;  --无法转换，code1为ptag


SET sql_safe_updates = FALSE;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e1 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 1;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e2 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 2;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e3 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 3;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e4 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 4;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e5 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 5;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e6 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 6;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e7 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 7;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e8 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 8;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e9 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 9;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e10 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 10;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e11 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 11;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e12 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 12;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e19 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 13;
ALTER TABLE test_ddl_dml_mix1.test1 DROP COLUMN e20 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 14;
ALTER TABLE test_ddl_dml_mix1.test1 DROP TAG code2 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 2;
ALTER TABLE test_ddl_dml_mix1.test1 DROP TAG code3 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 3;
ALTER TABLE test_ddl_dml_mix1.test1 DROP TAG code4 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 4;
ALTER TABLE test_ddl_dml_mix1.test1 DROP TAG code5 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 5;
ALTER TABLE test_ddl_dml_mix1.test1 DROP TAG code6 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 6;
ALTER TABLE test_ddl_dml_mix1.test1 DROP TAG code7 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 7;
ALTER TABLE test_ddl_dml_mix1.test1 DROP TAG code9 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 9;
ALTER TABLE test_ddl_dml_mix1.test1 DROP TAG code10 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 10;
ALTER TABLE test_ddl_dml_mix1.test1 DROP TAG code11 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 11;
ALTER TABLE test_ddl_dml_mix1.test1 DROP TAG code12 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 12;
ALTER TABLE test_ddl_dml_mix1.test1 DROP TAG code15 ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 13;
SET sql_safe_updates = TRUE;

SET sql_safe_updates = FALSE;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e1 INT4;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 1;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e2 INT8;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 2;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e3 INT2;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 3;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e4 VARCHAR;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 4;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e5 NCHAR;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 5;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e6 NVARCHAR(4096);
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 6;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e7 NCHAR(155);
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 7;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e8 CHAR(200);
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 8;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e9 VARBYTES;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 9;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e10 FLOAT4;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 10;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e11 BOOL;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 11;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e12 FLOAT8;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 12;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e19 VARCHAR(20);
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 13;
ALTER TABLE test_ddl_dml_mix1.test1 ADD COLUMN e20 TIMESTAMP ;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts LIMIT 40 OFFSET 14;
ALTER TABLE test_ddl_dml_mix1.test1 ADD TAG code2 BOOL;
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 2;
ALTER TABLE test_ddl_dml_mix1.test1 ADD TAG code3 CHAR(20);
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 3;
ALTER TABLE test_ddl_dml_mix1.test1 ADD TAG code4 NCHAR(10);
SELECT id,* FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 4;
SET sql_safe_updates = TRUE;
SELECT * FROM test_ddl_dml_mix1.test1 ORDER BY ts DESC LIMIT 40 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' ORDER BY ts DESC LIMIT 40 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 ORDER BY ts DESC LIMIT 40 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 IN (1,2,-10001,0) ORDER BY ts DESC LIMIT 40 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' ORDER BY ts DESC LIMIT 40 OFFSET 2;
SHOW CREATE TABLE test_ddl_dml_mix1.test1;
INSERT INTO test_ddl_dml_mix1.test1 VALUES('2002-2-22 2:22:22.222',22222,'test数据库语法查询测试！！！@TEST222','test数据库语法查询测试！！！@TEST222','test数据库语法查询测试！！！@TEST222','test数据库语法查询测试！！！@TEST222','test数据库语法查询测试！！！@TEST222','test数据库语法查询测试！！！@TEST222','test数据库语法查询测试！！！@TEST222','test数据库语法查询测试！！！@TEST222',22222222,222222222,222,'222222222','2','test数据库语法查询测试！！！@TEST222','test数据库语法查询测试！！！@TEST222','test数据库语法查询测试！！！@TEST222','test数据库语法查询测试！！！@TEST222',222222.222222,false,222222222.2222222222,'222222','2022-2-2',222,'test数据库语法查询测试！！！@TEST222','test数据库语法查询测试！！！@TEST222','2','test数据库语法查询测试！！！@TEST222',false,'222','222');
SELECT * FROM test_ddl_dml_mix1.test1 ORDER BY ts DESC LIMIT 40 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 ORDER BY ts DESC LIMIT 10 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 ORDER BY ts DESC LIMIT  1 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 ORDER BY ts DESC LIMIT  1 OFFSET 0;
SELECT * FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 = 0 ORDER BY ts DESC LIMIT 40 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code4 = '222' ORDER BY ts DESC LIMIT 40 OFFSET 0;
SELECT * FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' ORDER BY ts DESC LIMIT 40 OFFSET 2;
SELECT id FROM test_ddl_dml_mix1.test1 WHERE ts = '2023-07-23 22:31:50.11+00:00';
DELETE FROM test_ddl_dml_mix1.test1 WHERE ts = '2023-07-23 22:31:50.11+00:00';
SELECT * FROM test_ddl_dml_mix1.test1 ORDER BY ts DESC LIMIT 40 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 ORDER BY ts DESC LIMIT 10 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 ORDER BY ts DESC LIMIT  1 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 ORDER BY ts DESC LIMIT  1 OFFSET 0;
SELECT * FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code1 = 0 ORDER BY ts DESC LIMIT 40 OFFSET 2;
SELECT * FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' AND code4 = '222' ORDER BY ts DESC LIMIT 40 OFFSET 0;
SELECT * FROM test_ddl_dml_mix1.test1 WHERE ts <= '2262-04-11 23:47:16+00:00' AND ts >= '1970-01-01 00:00:00+00:00' ORDER BY ts DESC LIMIT 40 OFFSET 2;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e2     SET DATA TYPE VARCHAR(40);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e3     SET DATA TYPE VARCHAR(200);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e5     SET DATA TYPE VARCHAR(40);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e8     SET DATA TYPE VARCHAR(2000);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e12    SET DATA TYPE VARCHAR(600);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e13    SET DATA TYPE VARCHAR(600);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e15    SET DATA TYPE NVARCHAR(1023);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e16    SET DATA TYPE VARCHAR(801);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e17    SET DATA TYPE NVARCHAR(600);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER COLUMN e18    SET DATA TYPE VARCHAR(600);
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code4  SET DATA TYPE VARCHAR;
ALTER TABLE test_ddl_dml_mix1.test1 ALTER TAG    code13 SET DATA TYPE VARCHAR;
SELECT LAST(*     ),LAST_ROW(*     ),LAST(*,'2012-02-29 1:10:10.000'            ) FROM test_ddl_dml_mix1.test1;
SELECT LAST(e1    ),LAST_ROW(e1    ),LAST(e1    ,'1970-01-01 00:00:00+00:00'    ) FROM test_ddl_dml_mix1.test1 ;
SELECT LAST(e2    ),LAST_ROW(e2    ),LAST(e2    ,'1970-1-1 00:00:00.001'        ) FROM test_ddl_dml_mix1.test1 ;
SELECT LAST(e3    ),LAST_ROW(e3    ),LAST(e3    ,'1976-10-20 12:00:12.123+00:00') FROM test_ddl_dml_mix1.test1 ;
SELECT LAST(e4    ),LAST_ROW(e4    ),LAST(e4    ,'1979-2-28 11:59:01.999'       ) FROM test_ddl_dml_mix1.test1 ;

SELECT ts,id FROM test_ddl_dml_mix1.test1 ORDER BY ts LIMIT 3 OFFSET 1 ;

USE defaultdb;
DROP DATABASE IF EXISTS test_ddl_dml_mix1 cascade;