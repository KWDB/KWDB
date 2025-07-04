drop database test_SELECT_col cascade;
create ts database test_SELECT_col;
create table test_SELECT_col.t1(
                k_timestamp timestamptz not null,
                e1 int2  not null,
				e2 int,
				e3 int8 not null,
				e4 float4,
				e5 float8 not null,
				e6 bool,
				e7 timestamptz not null,
				e8 char(1023),
				e9 nchar(255) not null,
				e10 varchar(4096),
				e11 char not null,
				e12 varchar(255),
				e13 nchar not null,
				e14 varchar,
				e15 varchar(4096) not null, 
                e16 varbytes(200),
                e17 varchar(255) not null,
                e18 varchar,           
                e19 varbytes not null,
                e20 varbytes(1023),
                e21 varbytes(200) not null,
                e22 varbytes(200)
				) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location VARCHAR(128),color VARCHAR(128) not null,name varbytes,state varbytes(1023),tall VARBYTES,screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:33.123',0,0,0,0,0,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',100,200,300,false,100.0,200.0,'beijing','red',b'\x26','fuluolidazhou','160','big','2','社会性别女','1','cuteandlovely');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:34.223',20002,1000002,20000000002,1047200.0000,-109810.0,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',200,200,300,false,100.0,200.0,'beijing','red',b'\x26','fuluolidazhou','160','big','2','社会性别女','1','cuteandlovely');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:35.323',20002,2000003,-30000000003,22845.87,200.123456,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',300,400,100,false,400.0,300.0,'tianjin','yellow',b'\x28','aisaiebiyazhou','183','small','3','社会性别男','7','matureandgentle');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:36.423',30003,3000000,-40000000004,39845.87,-200.123456,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',400,400,100,false,400.0,300.0,'hebei','blue',b'\x28','aisaiebiyazhou','183','small','3','社会性别男','7','matureandgentle');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:37.523',-10001,-3000003,-40000000004,-39845.87,-200.123456,true,'2020-1-1 12:00:00.000','','','','','','','','','','','','','','','',400,400,100,false,400.0,300.0,'','','','','','','','','','');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:38.623',-20002,-2000002,-30000000003,-29845.87,-200.123456,true,'2020-1-1 12:00:00.000','\0test时间精度通用查询！！！@TEST1\0\0\0','\0test时间精度通用查询！！！@TEST1\0\0\0','\0test时间精度通用查询！！！@TEST1\0\0\0','0','\0test时间精度通用查询！！！@TEST1\0\0\0','\','\0test时间精度通用查询！！！@TEST1\0\0\0','\0test时间精度通用查询！！！@TEST1','0test时间精度通用查询！！！@TEST1','\0test时间精度通用查询！！！@TEST1\0\0\0','\0test时间精度通用查询！！！@TEST1\0\0\0','0','0test时间精度通用查询！！！@TEST1000','0test时间精度通用查询！！！@TEST1','0test时间精度通用查询！！！@TEST1',400,400,100,false,400.0,300.0,'hebei','blue',b'\x28','aisaiebiyazhou','183','small','3','社会性别男','7','matureandgentle');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:39.723',-30003,-1000001,-10000000001,-19845.87,-200.123456,true,'2020-1-1 12:00:00.000','     ','     ','     ',' ','     ',' ','     ','     ','     ','     ','     ',' ','     ','     ','     ',0,0,0,false,0,0,' ',' ',' ',' ',' ',' ',' ',' ',' ',' ');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:40.823',-30004,-1000001,-10000000001,-19845.87,-200.123456,true,'2020-1-1 12:00:00.000',e'\\',e'\\',e'\\',e'\\',e'\\',e'\\',e'\\',e'\\',e'\\\\',e'\\',e'\\','0',e'\\\\',e'\\\\',e'\\\\',0,0,0,false,0,0,' ',e'\\',' ',' ',' ',' ',' ',' ',' ',' ');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:41.923',-30005,-1000001,-10000000001,-19845.87,-200.123456,true,'2020-1-1 12:00:00.000','\','\','\','\','\','\','\','\','\\','\','\','0','\\','\\','\\',0,0,0,false,0,0,' ','\',' ',' ',' ',' ',' ',' ',' ',' ');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:42.221',-30006,-1000001,-10000000001,-19845.87,-200.123456,true,'2020-1-1 12:00:00.000',e'\\\\',e'\\\\',e'\\\\',e'\\',e'\\\\',e'\\',e'\\\\',e'\\\\',e'\\\\',e'\\\\',e'\\\\','0',e'\\\\',e'\\\\',e'\\\\',0,0,0,false,0,0,' ',e'\\\\',' ',' ',' ',' ',' ',' ',' ',' ');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:43.222',-30007,-1000001,-10000000001,-19845.87,-200.123456,true,'2020-1-1 12:00:00.000','\\','\\','\\','\','\\','\','\\','\\','\\','\\','\\','0','\\','\\','\\',0,0,0,false,0,0,' ','\\',' ',' ',' ',' ',' ',' ',' ',' ');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:44.224',-30008,-1000001,-10000000001,-19845.87,-200.123456,true,'2020-1-1 12:00:00.000',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\','0',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',0,0,0,false,0,0,' ',e'\\\\\\\\',' ',' ',' ',' ',' ',' ',' ',' ');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:45.225',-30009,-1000001,-10000000001,-19845.87,-200.123456,true,'2020-1-1 12:00:00.000','\\\\','\\\\','\\\\','\','\\\\','\','\\\\','\\\\','\\\\','\\\\','\\\\','0','\\\\','\\\\','\\\\',0,0,0,false,0,0,' ','\\\\',' ',' ',' ',' ',' ',' ',' ',' ');
INSERT INTO test_SELECT_col.t1 VALUES('2024-2-21 16:32:46.226',-30010,-1000001,-10000000001,-19845.87,-200.123456,true,'2020-1-1 12:00:00.000',e'\ ',e'\ ',e'\ ','\',e'\ ','\',e'\ ',e'\ ',e'\ \ ',e'\ ',e'\ ','0',e'\ \ ',e'\ \ ',e'\ \ ',0,0,0,false,0,0,' ',e'\ ',' ',' ',' ',' ',' ',' ',' ',' ');
INSERT INTO test_SELECT_col.t1(k_timestamp,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,flag,color) VALUES(2021681353000,32767,2147483647,9223372036854775807,9845.87,200.123456,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',500,false,'red');
INSERT INTO test_SELECT_col.t1(k_timestamp,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,flag,color) VALUES(2021684953000,-32768,-2147483648,-9223372036854775808,9842323145.87,200.123456,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',600,false,'red');
INSERT INTO test_SELECT_col.t1(k_timestamp,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,flag,color) VALUES(2021688553000,32050,NULL,4000,9845.87,200.123456,true,'2020-1-1 12:00:00.000','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1\0','t','test时间精度通用查询测试！！！@TEST1','中','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！TEST1xaa','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试','test时间精度通用查询测试','test时间精度通用查询测试',700,false,'red');
INSERT INTO test_SELECT_col.t1(k_timestamp,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,flag,color) VALUES(2021774953000,32767,NULL,9223372036854775807,NULL,20435435343430.123434356,NULL,'2020-1-1 12:00:00.000',NULL,'test时间精度通用查询测试！！！@TEST1',NULL,'t',NULL,'中',NULL,'test时间精度通用查询测试！！！@TEST1',NULL,'test时间精度通用查询测试！！！@TEST1',NULL,'b',NULL,'test时间精度通用查询测试！！！@TEST1',NULL,800,false,'red');
INSERT INTO test_SELECT_col.t1(code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type) VALUES(900,200,300,false,100.0,200.0,'beijing','red',b'\x26','fuluolidazhou','160','big','2','社会性别女','1','cuteandlovely');
INSERT INTO test_SELECT_col.t1(code1,flag,color,tall) VALUES(1000,false,'red','183');
---test_case0000alldata
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT code1,flag,color,e1,e2,e3 FROM test_SELECT_col.t1 order by k_timestamp;
---test_case0000singlecol
SELECT e1 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e2 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e3 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e4 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e5 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e6 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e7 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e8 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e9 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e10 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e11 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e12 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e13 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e14 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e15 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e16 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e17 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e18 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e19 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e20 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e21 FROM test_SELECT_col.t1 order by k_timestamp;
SELECT e22 FROM test_SELECT_col.t1 order by k_timestamp;
---test_case0000filtersinglecol

SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE k_timestamp IS NULL order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE k_timestamp IS NOT NULL order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1 IS NULL order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1 IS NOT NULL order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e2 IS NULL order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e2 IS NOT NULL order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1 IS NULL AND e1 IS NOT NULL order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1 IS NULL OR e1 IS NOT NULL order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1 IS NOT  NULL AND e2 IS NULL order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1 IS NOT  NULL OR e2 IS NULL order by k_timestamp;---BUG 
---test_case0000filteralltypesofNULLqueries
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1 IS NOT  NULL AND e2 IS NULL AND e3 IS NOT NULL AND e4 IS NULL AND e5 IS NOT NULL AND e6 IS NULL AND e8 IS NULL order by k_timestamp;---BUG
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e2 IS NULL AND e3 IS NULL AND e4 IS NULL AND e5 IS NULL AND e6 IS NULL AND e7 IS NULL AND e8 IS NULL AND e9 IS NULL AND e10 IS NULL AND e11 IS NULL AND e12 IS NULL AND e13 IS NULL AND e14 IS NULL AND e15 IS NULL AND e16 IS NULL AND e17 IS NULL AND e18 IS NULL AND e19 IS NULL AND e20 IS NULL AND e21 IS NULL AND e22 IS NULL order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e2 IS NOT NULL AND e3 IS NOT NULL AND e4 IS NOT NULL AND e5 IS NOT NULL AND e6 IS NOT NULL AND e7 IS NOT NULL AND e8 IS NOT NULL AND e9 IS NOT NULL AND e10 IS NOT NULL AND e11 IS NOT NULL AND e12 IS NOT NULL AND e13 IS NOT NULL AND e14 IS NOT NULL AND e15 IS NOT NULL AND e16 IS NOT NULL AND e17 IS NOT NULL AND e18 IS NOT NULL AND e19 IS NOT NULL AND e20 IS NOT NULL AND e21 IS NOT NULL AND e22 IS NOT NULL order by k_timestamp;---BUG
---test_case0000nullselect
SELECT e1*e2 FROM test_SELECT_col.t1 where e1*e2 IS NULL order by k_timestamp;---BUG
SELECT e1,e2 FROM test_SELECT_col.t1 where e1*e2=0 order by k_timestamp;
SELECT e1,e2 FROM test_SELECT_col.t1 where e1+e2=e1 order by k_timestamp;
SELECT e1,e2 FROM test_SELECT_col.t1 where e1+e2=10000 order by k_timestamp;
SELECT e1*e2 FROM test_SELECT_col.t1 where e1*e2 IS NOT NULL order by k_timestamp;
SELECT e1+e2 FROM test_SELECT_col.t1 where e1+e2 IS NULL order by k_timestamp;---BUG
SELECT e1+e2 FROM test_SELECT_col.t1 where e1+e2 IS NOT NULL order by k_timestamp;---BUG
SELECT e1-e2 FROM test_SELECT_col.t1 where e1-e2 IS NULL order by k_timestamp;---BUG
SELECT e1-e2 FROM test_SELECT_col.t1 where e1-e2 IS NOT NULL order by k_timestamp;---BUG
SELECT e1/e2 FROM test_SELECT_col.t1 where e1/e2 IS NULL order by k_timestamp;---BUG
SELECT e1/e2 FROM test_SELECT_col.t1 where e1/e2 IS NOT NULL order by k_timestamp;---BUG
SELECT e1//e2 FROM test_SELECT_col.t1 where e1//e2 IS NULL order by k_timestamp;---BUG
SELECT e1//e2 FROM test_SELECT_col.t1 where e1//e2 IS NOT NULL order by k_timestamp;
SELECT e1%e2 FROM test_SELECT_col.t1 where e1%e2 IS NULL order by k_timestamp;
SELECT e1%e2 FROM test_SELECT_col.t1 where e1%e2 IS NOT NULL order by k_timestamp;
---test_case0000 ISNULL
SELECT e1 IS NULL FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e2 IS NULL FROM test_SELECT_col.t1 ORDER BY k_timestamp;---BUG
SELECT e7 IS NOT NULL AND e2 IS NULL FROM test_SELECT_col.t1 ORDER BY k_timestamp;---BUG

---test_case0000typeconvert
select e1::float from test_SELECT_col.t1 order by k_timestamp;
select e1::int4 from test_SELECT_col.t1 order by k_timestamp;
select e2::int2 from test_SELECT_col.t1 order by k_timestamp;
select e2::int8 from test_SELECT_col.t1 order by k_timestamp;
select e3::int2 from test_SELECT_col.t1 order by k_timestamp;
select e3::int4 from test_SELECT_col.t1 order by k_timestamp;
select e4::float8 from test_SELECT_col.t1 order by k_timestamp;
select e5::float4 from test_SELECT_col.t1 order by k_timestamp;
select e6::int from test_SELECT_col.t1 order by k_timestamp;
select e6::varchar from test_SELECT_col.t1 order by k_timestamp;
select e6::char(4) from test_SELECT_col.t1 order by k_timestamp;
select e7::int from test_SELECT_col.t1 order by k_timestamp;
select e7::varchar from test_SELECT_col.t1 order by k_timestamp;
select e8::nchar(255) from test_SELECT_col.t1 order by k_timestamp;
select e8::varchar(255) from test_SELECT_col.t1 order by k_timestamp;
select e9::char(255) from test_SELECT_col.t1 order by k_timestamp;
select e9::varchar(255) from test_SELECT_col.t1 order by k_timestamp;
select e10:char(255) from test_SELECT_col.t1 order by k_timestamp;
select e10:nchar(255) from test_SELECT_col.t1 order by k_timestamp;
select e11:nchar(255) from test_SELECT_col.t1 order by k_timestamp;
select e15:nchar(255) from test_SELECT_col.t1 order by k_timestamp;
select e15:varchar(255) from test_SELECT_col.t1 order by k_timestamp;
select e15:char(255) from test_SELECT_col.t1 order by k_timestamp;
select e19:varchar(255) from test_SELECT_col.t1 order by k_timestamp;
select e22:bytes(1023) from test_SELECT_col.t1 order by k_timestamp;
---test_case0000filtertstimestamp
select e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type from test_SELECT_col.t1 where k_timestamp='2020-1-1 12:00:00.000' order by k_timestamp;
select e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type from test_SELECT_col.t1 where k_timestamp>='2020-1-1 12:00:00.000' order by k_timestamp;
---test_case0000filtertimestamp
select e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type from test_SELECT_col.t1 where e7='2020-1-1 12:00:00.000' order by k_timestamp;
select e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type from test_SELECT_col.t1 where e7!='2020-1-1 12:00:00.000' order by k_timestamp;
select e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type from test_SELECT_col.t1 where e7>'2020-1-1 12:00:00.000' order by k_timestamp;
select e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type from test_SELECT_col.t1 where e7<'2020-1-1 12:12:12' order by k_timestamp;
select e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type from test_SELECT_col.t1 where e7 IN ('2020-1-1 12:00:00.000') order by k_timestamp;
select e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type from test_SELECT_col.t1 where e7 NOT IN ('2020-1-1 12:00:00.000') order by k_timestamp;
select e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type from test_SELECT_col.t1 where e7='2020-1-1 12:12:12' and e7<'2020-1-1 12:12:12' or e7>'2020-1-1 12:12:12' order by k_timestamp;
select e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type from test_SELECT_col.t1 where e7=1577851932000::timestamp order by k_timestamp;
select e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type from test_SELECT_col.t1 where e7>'2020-1-1 12:12:12' and k_timestamp>'2020-1-1 12:12:12' order by k_timestamp;

---test_case0000filterwhere
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1=-30003 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1=32767 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e2=2147483647 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e3=9223372036854775807 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1=-32768 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e2=-2147483648 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e3=-9223372036854775808 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1>10000 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1<10000 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e2=2000000 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e2>=2000000 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e3=4000 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e3<=4000 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e4=9845.87 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e4>4000.00 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e5=200.123456 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e5<400.0000 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e6=false order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e6='true' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e6=1::bool order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e7='2020-1-1 12:00:00.000' order by k_timestamp;
SELECT e8 FROM test_SELECT_col.t1 WHERE e8='test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e8 FROM test_SELECT_col.t1 WHERE e8='\' order by k_timestamp;
SELECT e8 FROM test_SELECT_col.t1 WHERE e8=e'\\' order by k_timestamp;
SELECT e8 FROM test_SELECT_col.t1 WHERE e8<'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e8 FROM test_SELECT_col.t1 WHERE e8>='\0test时间精度通用查询！！！@TEST1\0\0\0' ORDER BY k_timestamp;---BUG
SELECT e8 FROM test_SELECT_col.t1 WHERE e8>=NULL ORDER BY k_timestamp;
SELECT e8 FROM test_SELECT_col.t1 WHERE e8='\' order by k_timestamp;---BUG 
SELECT e8 FROM test_SELECT_col.t1 WHERE e8=e'\\' order by k_timestamp;---BUG 
SELECT e8 FROM test_SELECT_col.t1 WHERE e8='\\' order by k_timestamp;---BUG 
SELECT e8 FROM test_SELECT_col.t1 WHERE e8=e'\\\\' order by k_timestamp;---BUG 
SELECT e8 FROM test_SELECT_col.t1 WHERE e8='\\\\' order by k_timestamp;---BUG 
SELECT e8 FROM test_SELECT_col.t1 WHERE e8=e'\\\\\\\\' order by k_timestamp;---BUG 
SELECT e8 FROM test_SELECT_col.t1 WHERE e8=e'\ ' order by k_timestamp;---BUG 
SELECT e8 FROM test_SELECT_col.t1 WHERE e8=' ' order by k_timestamp;---BUG 
SELECT e8 FROM test_SELECT_col.t1 WHERE e8>'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e9 FROM test_SELECT_col.t1 WHERE e9='test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e9 FROM test_SELECT_col.t1 WHERE e9>='\0test时间精度通用查询！！！@TEST1\0\0\0' ORDER BY k_timestamp;---BUG
SELECT e9 FROM test_SELECT_col.t1 WHERE e9<'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e9 FROM test_SELECT_col.t1 WHERE e9=e'\\' order by k_timestamp;---BUG 
SELECT e10 FROM test_SELECT_col.t1 WHERE e10='test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e10 FROM test_SELECT_col.t1 WHERE e10>'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e10 FROM test_SELECT_col.t1 WHERE e10>='\0test时间精度通用查询！！！@TEST1\0\0\0' ORDER BY k_timestamp;---BUG
SELECT e10 FROM test_SELECT_col.t1 WHERE e10=e'\\' order by k_timestamp;---BUG 
SELECT e11 FROM test_SELECT_col.t1 WHERE e11='t' order by k_timestamp;
SELECT e11 FROM test_SELECT_col.t1 WHERE e11='tt' order by k_timestamp;---BUG

SELECT e11 FROM test_SELECT_col.t1 WHERE e11>'t' order by k_timestamp;
SELECT e12 FROM test_SELECT_col.t1 WHERE e12='test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e12<'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e13='中' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e13>'中' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e14='test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e14<'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e15='test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e15>'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e16='test时间精度通用查询测试！TEST1xaa' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e16<'test时间精度通用查询测试！TEST1xaa' order by k_timestamp;
SELECT e16 FROM test_SELECT_col.t1 WHERE e16>='\x3074657374e697b6e997b4e7b2bee5baa6e9809ae794a8e69fa5e8afa2efbc81efbc81efbc81405445535431000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000' ORDER BY k_timestamp;
SELECT e16 FROM test_SELECT_col.t1 WHERE e16>='\0test时间精度通用查询！！！@TEST1\0\0\0' order by k_timestamp;
SELECT e16 FROM test_SELECT_col.t1 WHERE e16=e'\\\\' order by k_timestamp;---BUG 

SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e17='test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e17 FROM test_SELECT_col.t1 WHERE e17>'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e18='test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e18 FROM test_SELECT_col.t1 WHERE e18<'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e19 FROM test_SELECT_col.t1 WHERE e19=b'\xaa' order by k_timestamp;
SELECT e19 FROM test_SELECT_col.t1 WHERE e19<b'\xaa' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e20='test时间精度通用查询测试' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e20<'test时间精度通用查询测试' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e21='test时间精度通用查询测试' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e21>'test时间精度通用查询测试' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e22='test时间精度通用查询测试' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e22<'test时间精度通用查询测试' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1!=10000 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1>e2 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e2>10000 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e4<10000 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e4!=e5 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e2>=10000 AND e2<=20000 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e8=e9 order by k_timestamp;
SELECT e10,e11 FROM test_SELECT_col.t1 WHERE e10=e11 order by k_timestamp;---BUG 
SELECT e8,e11 FROM test_SELECT_col.t1 WHERE e8=e11 order by k_timestamp;---BUG 
SELECT e16,e21 FROM test_SELECT_col.t1 WHERE e16>e21 order by k_timestamp;---BUG
SELECT e1,e4,e6,e7,e11,e12,e13,e19 FROM test_SELECT_col.t1 WHERE e1>10000 AND e4=4000.00 AND e7='2020-1-1 12:00:00.000' OR e6=false OR e11='t' AND e12='test时间精度通用查询测试！！！@TEST1' AND  e13='中' OR e19=b'\xaa' AND e8=e9 order by k_timestamp;
---test_case0000whereANDORfilter
SELECT e6,code1 FROM test_SELECT_col.t1 WHERE e6=true AND code1=100 order by k_timestamp;---BUG
SELECT e7,k_timestamp FROM test_SELECT_col.t1 WHERE e7='2020-1-1 12:00:00.000' AND k_timestamp>=now() order by k_timestamp;
SELECT e10,e11,e12 FROM test_SELECT_col.t1 WHERE e10='test时间精度通用查询测试！！！@TEST1' AND e11='t' AND e12='test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e13,e14,e15,e16 FROM test_SELECT_col.t1 WHERE e13='中' AND e14='test时间精度通用查询测试！！！@TEST1' AND e15='test时间精度通用查询测试！！！@TEST1' AND e16='test时间精度通用查询测试！TEST1xaa' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e17='test时间精度通用查询测试！！！@TEST1' AND e18='test时间精度通用查询测试！！！@TEST1' AND e19=b'\xaa' AND e20='test时间精度通用查询测试' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e21='test时间精度通用查询测试！！！@TEST1' AND e22='test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
---test_case0000NOT IN/IN
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e1 IN (10000) order by k_timestamp;
SELECT e1 FROM test_SELECT_col.t1 WHERE e1 NOT IN (10000) order by k_timestamp;
SELECT e1 FROM test_SELECT_col.t1 WHERE e1 IN (20000,NULL,0) order by k_timestamp;
SELECT e1 FROM test_SELECT_col.t1 WHERE e1 IN (32767,-32768) order by k_timestamp;---BUG 
SELECT e2 FROM test_SELECT_col.t1 WHERE e2 IN (-2147483648) order by k_timestamp;---BUG 
SELECT e3 FROM test_SELECT_col.t1 WHERE e3 IN (-9223372036854775808) order by k_timestamp;--BUG 
SELECT e4 FROM test_SELECT_col.t1 WHERE e4 IN (4000.0000,0) order by k_timestamp;
SELECT e4 FROM test_SELECT_col.t1 WHERE e4 NOT IN (4000.0000,0) order by k_timestamp;---BUG
SELECT e4 FROM test_SELECT_col.t1 WHERE e4 NOT IN (NULL<4000.0000,0) order by k_timestamp;---BUG
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e6 IN (TRUE) order by k_timestamp;
SELECT e6 FROM test_SELECT_col.t1 WHERE e6 NOT IN (TRUE) order by k_timestamp;---BUG 
SELECT e1 FROM test_SELECT_col.t1 WHERE e1 IN (10000) AND e3 IN (4000.0000,0) order by k_timestamp;
SELECT e4 FROM test_SELECT_col.t1 WHERE e4 NOT IN (4000.0000,0) order by k_timestamp;---BUG
SELECT e7 FROM test_SELECT_col.t1 WHERE e7 NOT IN (TIMESTAMPTZ'2020-1-1 12:12:12.000') order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e7 IN (TIMESTAMPTZ'2020-1-1 12:12:12.000',50000000000::TIMESTAMPTZ) order by k_timestamp;
SELECT e8 FROM test_SELECT_col.t1 WHERE e8 IN (NULL,NULL) order by k_timestamp;
SELECT e8 FROM test_SELECT_col.t1 WHERE e8 NOT IN (NULL,'0','') order by k_timestamp;---BUG

SELECT e8 FROM test_SELECT_col.t1 WHERE e8 IN ('\') order by k_timestamp;---BUG 
SELECT e8 FROM test_SELECT_col.t1 WHERE e8 IN ('\\') order by k_timestamp;---BUG 
SELECT e8 FROM test_SELECT_col.t1 WHERE e8 IN (' ') order by k_timestamp;---BUG 
SELECT e8 FROM test_SELECT_col.t1 WHERE e8 IN (e'\\') order by k_timestamp;---BUG
SELECT e8 FROM test_SELECT_col.t1 WHERE e8 IN (e'\\\\') order by k_timestamp;---BUG


SELECT e8 FROM test_SELECT_col.t1 WHERE e8 IN ('test时间精度通用查询测试！！！@TEST1              ') order by k_timestamp;
SELECT e8 FROM test_SELECT_col.t1 WHERE e8  IN ('test时间精度通用查询测试！！！@TEST1\0') order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e9 IN ('test时间精度通用查询测试！！！@TEST1      ') order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e9 IN ('test时间精度通用查询测试！！！@TEST1\0') order by k_timestamp; 

SELECT e9 FROM test_SELECT_col.t1 WHERE e9 NOT IN ('','test时间精度通用查询测试！！！@TEST1') order by k_timestamp;
SELECT e10 FROM test_SELECT_col.t1 WHERE e10 IN ('test时间精度通用查询测试！！！@TEST1\0') order by k_timestamp;
SELECT e10 FROM test_SELECT_col.t1 WHERE e10 IN ('test时间精度通用查询测试！！！@TEST1%%') order by k_timestamp;
SELECT e10 FROM test_SELECT_col.t1 WHERE e10 IN ('test时间精度通用查询测试！！！@TEST1_') order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e10 NOT IN ('','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1      ','test时间精度通用查询测试！！！@TEST1\0') order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e13 IN ('') order by k_timestamp;
SELECT e13 FROM test_SELECT_col.t1 WHERE e13 IN ('中') order by k_timestamp;
SELECT e13 FROM test_SELECT_col.t1 WHERE e13 IN ('中      ') order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e13 IN ('中\0') order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e13 NOT IN ('','中','中      ','中\0') order by k_timestamp;
SELECT e13 FROM test_SELECT_col.t1 WHERE e13 IN('\\',0) ORDER BY k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e14 IN ('','test时间精度通用查询测试！！！@TEST1      ','test时间精度通用查询测试！！！@TEST1\0') order by k_timestamp;
SELECT e19 FROM test_SELECT_col.t1 WHERE e19 IN ('',b'\xaa',b'\xaa',' ') order by k_timestamp;---BUG
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e19 IN ('       ') order by k_timestamp;---BUG
SELECT e19 FROM test_SELECT_col.t1 WHERE e19 NOT IN ('',b'\xaa') order by k_timestamp;---BUG
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e20 IN ('0test时间精度通用查询！！！@TEST1000') order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e20 IN ('','\0test时间精度通用查询！！！@TEST1\0\0\0') order by k_timestamp;
---test_case0000BETWEEN AND
SELECT e1 FROM test_SELECT_col.t1 WHERE e1 BETWEEN 10000 AND 30000 order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e5 BETWEEN 200.00 AND 500.00 order by k_timestamp;
SELECT e8 FROM test_SELECT_col.t1 WHERE e8 BETWEEN '\0test时间精度通用查询！！！@TEST1\0\0\0\0' AND 'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;---BUG
SELECT e9 FROM test_SELECT_col.t1 WHERE e9 BETWEEN '\0test时间精度通用查询！！！@TEST1\0\0\0' AND '\0test时间精度通用查询！！！@TEST1\0\0\0' order by k_timestamp;---BUG
SELECT e13 FROM test_SELECT_col.t1 WHERE e13 BETWEEN '\ ' AND '中\0' order by k_timestamp;---BUG 
SELECT e13 FROM test_SELECT_col.t1 WHERE e13 BETWEEN '' AND '' order by k_timestamp;
SELECT e13 FROM test_SELECT_col.t1 WHERE e13<='' order by k_timestamp;
SELECT e19 FROM test_SELECT_col.t1 WHERE e19 BETWEEN b'\x00' AND b'\x62' order by k_timestamp;
SELECT e20 FROM test_SELECT_col.t1 WHERE e20 BETWEEN b'\x74657374e697b6e997b4e7b2bee5baa6e9809ae794a8e69fa5e8afa2e6b58be8af95' AND b'\x000000000000000' order by k_timestamp;

---test_case0000LIKE
select e10 from test_SELECT_col.t1 where e10 like ('') order by k_timestamp;---BUG
select e10 from test_SELECT_col.t1 where e10 like ('    %') order by k_timestamp;
select e10 from test_SELECT_col.t1 where e10 like ('%    ') order by k_timestamp;---BUG
select e10 from test_SELECT_col.t1 where e10 like ('  %  ') order by k_timestamp;---BUG
select e10 from test_SELECT_col.t1 where e10 like ('*') order by k_timestamp;
select e10 from test_SELECT_col.t1 where e10 like '_    ' order by k_timestamp;
select e10 from test_SELECT_col.t1 where e10 like '  _  ' order by k_timestamp;
select e10 from test_SELECT_col.t1 where e10 like '    _' order by k_timestamp;
select e10 from test_SELECT_col.t1 where e10 like '  '||'   '||'' order by k_timestamp;
select e10 from test_SELECT_col.t1 where e10 like '''' order by k_timestamp;---BUG 
select e10 from test_SELECT_col.t1 where e10 like '\0test时间精度通用查询！！！@TEST1\0\0\0' order by k_timestamp;---BUG 
select e12 from test_SELECT_col.t1 where e12 like '\0test时间精度通用查询！！！@TEST1\0\0\0' order by k_timestamp;---BUG
select e13 from test_SELECT_col.t1 where e13 like '\' and color like 'blue' ORDER BY k_timestamp;	
select color,e13 from test_SELECT_col.t1 where color like 'blue' ORDER BY k_timestamp;
SELECT e9 FROM test_SELECT_col.t1 WHERE e9 LIKE 'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;---BUG
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e9 LIKE '%___%' order by k_timestamp;---BUG
SELECT e10 FROM test_SELECT_col.t1 WHERE e10 NOT LIKE 'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;---BUG
SELECT e10 FROM test_SELECT_col.t1 WHERE e10  LIKE 'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;---BUG
SELECT e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e22 LIKE 'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
---test_case0000SIMILARfilternosupport
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e11 SIMILAR TO 'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e12 NOT SIMILAR TO 'test时间精度通用查询测试！！！@TEST1' order by k_timestamp;
---test_case0000ILIKEnosupport
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e13 ILIKE 'test时间精度通用查询测试！！！@test1' order by k_timestamp;
SELECT e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20,e21,e22,code1,code2,code3,flag,val1,val2,location,color,name,state,tall,screen,age,sex,year,type FROM test_SELECT_col.t1 WHERE e14 NOT ILIKE 'test时间精度通用查询测试！！！@test1' order by k_timestamp;

---test_case0000compare
SELECT e1>10000 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e2>10000 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e4<3000.0000 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e6!=true FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e7='2020-12-12 12:12:00.000' FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e8>='test时间精度通用查询测试！！！@TEST1' FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e10<='test时间精度通用查询测试！！！@TEST1' FROM test_SELECT_col.t1 ORDER BY k_timestamp;
---test_case0000selectcompare
SELECT e11>=e13 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1<=e3 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e4!=e5 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e8<=e9 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e10<=e11 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e16<=e20 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
---test_case0000Columnopquery
SELECT e1+e2 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e3+e3 FROM test_SELECT_col.t1 ORDER BY k_timestamp;---BUG
SELECT e1+NULL FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1-e2 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1-NULL FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1*e2 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1*NULL FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1/e2 FROM test_SELECT_col.t1 ORDER BY k_timestamp;---BUG
SELECT e1/0 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1/NULL FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1//e2 FROM test_SELECT_col.t1 ORDER BY k_timestamp;---BUG
SELECT e1//8 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1//0 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1//NULL FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1%e2 FROM test_SELECT_col.t1 ORDER BY k_timestamp;---BUG
SELECT e1%2 FROM test_SELECT_col.t1 ORDER BY k_timestamp;---BUG 
SELECT e1%NULL FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1^2 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1^0 FROM test_SELECT_col.t1 ORDER BY k_timestamp;---BUG
SELECT e1^NULL FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1^9223372036854775807 FROM test_SELECT_col.t1 ORDER BY k_timestamp;---BUG
SELECT e6&e6 FROM test_SELECT_col.t1 ORDER BY k_timestamp;
SELECT e1&e2 FROM test_SELECT_col.t1 ORDER BY k_timestamp;---BUG
SELECT e8||e9 FROM test_SELECT_col.t1 ORDER BY k_timestamp;---BUG
---test_case0000AGGSelect
SELECT sum(e1) FROM test_SELECT_col.t1;--e1为大数据量，求和看是否溢出
SELECT sum(e1+e2) FROM test_SELECT_col.t1 WHERE code1=100 GROUP BY code1,code2  ORDER BY k_timestamp;
SELECT avg(e1) FROM test_SELECT_col.t1 WHERE e1=10000 GROUP BY code1,code2 ORDER BY k_timestamp;
SELECT max(e1) FROM test_SELECT_col.t1 WHERE e1=10000 GROUP BY code1,code2 ORDER BY k_timestamp;
SELECT min(e1) FROM test_SELECT_col.t1 WHERE e1=10000 GROUP BY e2 ORDER BY k_timestamp;
SELECT max(e1)+min(e2) FROM test_SELECT_col.t1;
SELECT last(*) FROM test_SELECT_col.t1;
SELECT last_row(*) FROM test_SELECT_col.t1;
--SELECT first(e1) FROM test_SELECT_col.t1;
SELECT last_row(e1) FROM test_SELECT_col.t1;
SELECT count(e1) FROM test_SELECT_col.t1;
SELECT count(*) FROM test_SELECT_col.t1;
SELECT variance(e1) FROM test_SELECT_col.t1;
SELECT stddev(e1) FROM test_SELECT_col.t1;
SELECT count(e1) FROM test_SELECT_col.t1;
SELECT count(distinct e1) FROM test_SELECT_col.t1;
---test_case0000downsample
select time_bucket(k_timestamp,'1s') as tb,e1 from test_SELECT_col.t1 where e1=100 group by tb,e1 ORDER BY k_timestamp;
select time_bucket(k_timestamp,'1s') as tb,last(e1) from test_SELECT_col.t1 where e1=100 group by tb ORDER BY k_timestamp;
select time_bucket(k_timestamp,'1s') as tb,last(*) from test_SELECT_col.t1 where e1=100 group by tb ORDER BY k_timestamp;
--select time_bucket(k_timestamp,'1s') as tb,first(e1) from test_SELECT_col.t1 where e1=100 group by tb ORDER BY k_timestamp;
select time_bucket(k_timestamp,'1s') as tb,max(e1) from test_SELECT_col.t1 where e1=100 group by tb ORDER BY k_timestamp;
select time_bucket(k_timestamp,'1s') as tb,min(e6) from test_SELECT_col.t1 where e1=100 group by tb ORDER BY k_timestamp;
select time_bucket(k_timestamp,'1s') as tb,count(*) from test_SELECT_col.t1 where e1=100 group by tb ORDER BY k_timestamp;
select time_bucket(k_timestamp,'1s') as tb,count(*) from test_SELECT_col.t1 where e1=100 group by tb ORDER BY k_timestamp;
select time_bucket(k_timestamp,'1s') as tb,count(distinct e1) from test_SELECT_col.t1 where e1=100 group by tb ORDER BY k_timestamp;
---test_case0000LIKENOTLIKESELECT
select e1 from test_SELECT_col.t1 where color Like 'red' ORDER BY k_timestamp;
select e1 from test_SELECT_col.t1 where color NOT Like 'red' ORDER BY k_timestamp;
select e1 from test_SELECT_col.t1 where flag=true ORDER BY k_timestamp;
select e1 from test_SELECT_col.t1 where flag=false ORDER BY k_timestamp;
---test_case0000group by
select code2 from test_SELECT_col.t1 group by code2 ORDER BY k_timestamp;
select e2 from test_SELECT_col.t1 group by e2 ORDER BY k_timestamp;
select code1 from test_SELECT_col.t1 group by code1,code2,code1+code2 ORDER BY k_timestamp;
SELECT max(e2) FROM test_SELECT_col.t1 GROUP BY e2 ORDER BY k_timestamp ORDER BY k_timestamp;---BUG
SELECT e1+e2 FROM test_SELECT_col.t1 GROUP BY e1+e2 ORDER BY k_timestamp;
SELECT count(e2) FROM test_SELECT_col.t1 GROUP BY e2 ORDER BY k_timestamp;
select code1 from test_SELECT_col.t1 GROUP BY code1,e1 ORDER BY k_timestamp;

---test_case0000order by
select e1 from test_SELECT_col.t1 order by k_timestamp;
select e1 from test_SELECT_col.t1 order by k_timestamp;
--select e1 from test_SELECT_col.t1 order by e1;
---test_case0000LIMIT
--select e1 from test_SELECT_col.t1 limit 10;
--select e1 from test_SELECT_col.t1 limit -1;
--select e1 from test_SELECT_col.t1 limit 0;
--select e1 from test_SELECT_col.t1 limit 2 offset 2;
--select e1 from test_SELECT_col.t1 limit 2 offset 0;
show create table test_SELECT_col.t1;
---test_case0000castfuncint
select cast(e1 as float4) from test_SELECT_col.t1  order by k_timestamp;
select cast(e1 as float8) from test_SELECT_col.t1 order by k_timestamp;
select cast(e1 as int4) from test_SELECT_col.t1  order by k_timestamp;
select cast(e1 as int8) from test_SELECT_col.t1 order by k_timestamp;
select cast(e3 as int4) from test_SELECT_col.t1 order by k_timestamp;
select cast(e3 as int2) from test_SELECT_col.t1 order by k_timestamp;
select cast(e1 as bool) from test_SELECT_col.t1 order by k_timestamp;
select cast(e1 as varchar) from test_SELECT_col.t1 order by k_timestamp;
select cast(e1 as varbytes) from test_SELECT_col.t1 order by k_timestamp;
select cast(e1 as char) from test_SELECT_col.t1 order by k_timestamp;
select cast(e1 as char(1023)) from test_SELECT_col.t1 order by k_timestamp;
select cast(e1 as nchar(1023)) from test_SELECT_col.t1 order by k_timestamp;
---castfloat
select cast(val1 as int2) from test_SELECT_col.t1 order by k_timestamp;
select cast(val1 as bool) from test_SELECT_col.t1 order by k_timestamp;
select cast(val1 as varchar) from test_SELECT_col.t1 order by k_timestamp;
select cast(val1 as char) from test_SELECT_col.t1 order by k_timestamp;
select cast(val1 as char(1023)) from test_SELECT_col.t1 order by k_timestamp;
select cast(val1 as nchar(1023)) from test_SELECT_col.t1 order by k_timestamp;
---castchar
--select cast(color as int) from test_SELECT_col.t1 order by color;
--select cast(color as bool) from test_SELECT_col.t1 order by color;
--select cast(color as float) from test_SELECT_col.t1 order by color;
---test_case0000attrcompare
select code1 from test_SELECT_col.t1 where code1<code2 order by k_timestamp;
select code1 from test_SELECT_col.t1 where code1<color order by k_timestamp;
select code1 from test_SELECT_col.t1 where location<color order by k_timestamp;
select code1 from test_SELECT_col.t1 where name<state order by k_timestamp;
---where expression
select code1 from test_SELECT_col.t1 where code1+code2=300 order by k_timestamp;
select e1 from test_SELECT_col.t1 where e1+e2=2000 order by k_timestamp;
---where functest
select e1 from test_SELECT_col.t1 where length(color)=3 order by k_timestamp;
select e1 from test_SELECT_col.t1 where length(e4)=3 order by k_timestamp;
select e1 from test_SELECT_col.t1 where concat(color,location)='redbeijing' order by k_timestamp;

---char func
select length(color) from test_SELECT_col.t1 where color='red';
select substring(color,1) from test_SELECT_col.t1  order by k_timestamp;
select substring(location,1,3) from test_SELECT_col.t1 where location='beijing' order by k_timestamp;
select concat(color,location) from test_SELECT_col.t1  order by k_timestamp;
select lower(color) from test_SELECT_col.t1  order by k_timestamp;

---inttest
select ceil(code1) from test_SELECT_col.t1  order by k_timestamp;
select floor(code1) from test_SELECT_col.t1 order by k_timestamp;
select sin(code1) from test_SELECT_col.t1 order by k_timestamp;
select cos(code1) from test_SELECT_col.t1 order by k_timestamp;
select pi(code1) from test_SELECT_col.t1 order by k_timestamp;
select power(code1) from test_SELECT_col.t1 order by k_timestamp;
select pow(code1) from test_SELECT_col.t1 order by k_timestamp;
select round(code1) from test_SELECT_col.t1 order by k_timestamp;
select sqrt(code1) from test_SELECT_col.t1 order by k_timestamp;

---time test
--SELECT now() from test_SELECT_col.t1 order by k_timestamp;
select extract(year from k_timestamp) from test_SELECT_col.t1 order by k_timestamp;
---test_case0028attraggselect
SELECT last(e2),code1 FROM test_SELECT_col.t1 WHERE e2 NOT IN(100000) GROUP BY code1  order by k_timestamp;
--SELECT first(e2),code1 FROM test_SELECT_col.t1 WHERE e2 NOT IN(100000) GROUP BY code1;
SELECT min(e2), code1 FROM test_SELECT_col.t1 WHERE e2 NOT IN(100000) GROUP BY code1 order by k_timestamp; 
SELECT max(e2), code1 FROM test_SELECT_col.t1 WHERE e2 NOT IN(100000) GROUP BY code1 order by k_timestamp;
SELECT count(e2), code1 FROM test_SELECT_col.t1 WHERE e2 NOT IN(100000) GROUP BY code1 order by k_timestamp;
SELECT count(distinct e2), code1 FROM test_SELECT_col.t1 WHERE e2 NOT IN(100000) GROUP BY code1 order by k_timestamp;
SELECT sum(e2), code1 FROM test_SELECT_col.t1 WHERE e2 NOT IN(100000) GROUP BY code1 order by k_timestamp;
SELECT avg(e2), code1 FROM test_SELECT_col.t1 WHERE e2 NOT IN(100000) GROUP BY code1 order by k_timestamp;
SELECT stddev(e2), code1 FROM test_SELECT_col.t1 WHERE e2 NOT IN(100000) GROUP BY code1 order by k_timestamp;
SELECT variance(e2), code1 FROM test_SELECT_col.t1 WHERE e2 NOT IN(100000) GROUP BY code1 order by k_timestamp;
drop database test_SELECT_col cascade;
