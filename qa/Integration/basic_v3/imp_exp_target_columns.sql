-- prepare data
CREATE TS DATABASE test_impexp;
use test_impexp;
CREATE TABLE test_impexp.ds_tb(
k_timestamp timestamptz not null,
e1 int2 not null,
e2 int,
e3 int8 not null,
e4 float4,
e5 float8 not null,
e6 bool,
e7 timestamptz not null,
e8 char(1023),
e9 nchar(255) not null,
e10 nchar(200),
e11 char not null,
e12 nchar(200),
e13 nchar not null,
e14 nchar(200),
e15 nchar(200) not null,
e16 varbytes,
e17 nchar(200) not null,
e18 nchar(200),e19 varbytes not null,
e20 varbytes,
e21 varbytes not null,
e22 varbytes,
e23 varchar not null,
e24 nvarchar
) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes,tall varbytes,screen varbytes,age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
INSERT INTO test_impexp.ds_tb values('2023-12-12 12:00:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000',E'\"" 转义符测试',E'\\ 转义符测试2',E'\""包围符测试前1 包围符测试后 \""','t',E'\'包围符测试前2，包围符测试后\' ','中',E'\""包围符测试前',E'包围符测试后\""',E'\'包围符测试前',E'包围符测试后\'','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试',',包围符后通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,'a', E'转义符\""', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO test_impexp.ds_tb values('2023-12-12 12:10:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000',E'\" 转义符测试',E'\\ 转义符测试2',E'\"包围符测试前 包围符测试后 \"','t',E'\'包围符测试前，包围符测试后\'','中',E'\"包围符测试前',E'包围符测试后\"',E'\'包围符测试前',E'包围符测试后\'','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试',',包围符后通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,'a', E'转义符\\', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO test_impexp.ds_tb values('2023-12-12 12:11:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000',E'\" 转义符测试',E'\\ 转义符测试2',E'\"包围符测试前 包围符测试后 \"','t',E'\'包围符测试前，包围符测试后\'','中',E'\"包围符测试前',E'包围符测试后\"',E'\'包围符测试前',E'包围符测试后\'','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试',',包围符后通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,E'\"包围符前，包围符后\"', E'转义符\\ 11', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO test_impexp.ds_tb values('2023-12-12 12:12:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000',E'\" 转义符测试',E'\\ 转义符测试2',E'\"包围符测试前 包围符测试后 \"','t',E'\'包围符测试前，包围符测试后\'','中',E'\"包围符测试前',E'包围符测试后\"',E'\'包围符测试前',E'包围符测试后\'','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试',',包围符后通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,E'\'包围符前', E'包围符后\'', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO test_impexp.ds_tb values('2023-12-12 12:13:00.000+00:00',1,1000000,1000,6000.0000,100.0,true,'2020-1-7 12:00:00.000',E'\" 转义符测试',E'\\ 转义符测试2',E'\"包围符测试前 包围符测试后 \"','t',E'\'包围符测试前，包围符测试后\'','中',E'\"包围符测试前',E'包围符测试后\"',E'\'包围符测试前',E'包围符测试后\'','test时间精度通用查询测试！！！@TEST1',b'\xaa','test时间精度通用查询测试',',包围符后通用查询测试','test时间精度通用查询测试', '测试test11111', '测试变长123', 1, 2, 3, false, 1.1, 1.2,E'\"包围符前', E'包围符后\"', 'T','China', 'a', 'b', '1', '女', '1', 'pria');

-- 01 指定全部列顺序导入导出
CREATE TABLE test_impexp.tb1(
k_timestamp timestamptz not null,
e1 int2 not null,
e2 int,
e3 int8 not null,
e4 float4,
e5 float8 not null,
e6 bool,
e7 timestamptz not null,
e8 char(1023),
e9 nchar(255) not null,
e10 nchar(200),
e11 char not null,
e12 nchar(200),
e13 nchar not null,
e14 nchar(200),
e15 nchar(200) not null,
e16 varbytes,
e17 nchar(200) not null,
e18 nchar(200),e19 varbytes not null,
e20 varbytes,
e21 varbytes not null,
e22 varbytes,
e23 varchar not null,
e24 nvarchar
) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes,tall varbytes,screen varbytes,age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
EXPORT INTO CSV "nodelocal://1/export_column/test_1/" FROM SELECT * from test_impexp.ds_tb;
IMPORT INTO test_impexp.tb1 CSV DATA ("nodelocal://1/export_column/test_1/");
SELECT * FROM test_impexp.tb1 ORDER BY k_timestamp;
SELECT k_timestamp FROM test_impexp.tb1 ORDER BY k_timestamp;
DROP TABLE test_impexp.tb1;

-- 02 指定INT类型列导入导出
CREATE TABLE test_impexp.tb1 (k_timestamp timestamptz not null, e1 int2 not null, e2 int2) tags (code1 int2 not null) primary tags (code1);
EXPORT INTO CSV "nodelocal://1/export_column/test_2/" FROM SELECT k_timestamp, e1, code1 FROM test_impexp.ds_tb;
IMPORT INTO test_impexp.tb1(k_timestamp, e1, code1) CSV DATA ("nodelocal://1/export_column/test_2/");
SELECT * FROM test_impexp.tb1 ORDER BY k_timestamp;
SELECT k_timestamp FROM test_impexp.tb1 ORDER BY k_timestamp;
DROP TABLE test_impexp.tb1;

-- 03 指定CHAR类型列导入导出
CREATE TABLE test_impexp.tb1 (k_timestamp timestamptz not null, e1 char(1023), e2 varchar) tags (code1 int2 not null, code2 bool) PRIMARY tags (code1);
EXPORT INTO CSV "nodelocal://1/export_column/test_3/" FROM SELECT k_timestamp, e12, e1 FROM test_impexp.ds_tb;
IMPORT INTO test_impexp.tb1(k_timestamp, e1, code1) CSV DATA ("nodelocal://1/export_column/test_3/");
SELECT * FROM test_impexp.tb1 ORDER BY k_timestamp;
SELECT k_timestamp FROM test_impexp.tb1 ORDER BY k_timestamp;
DROP TABLE test_impexp.tb1 ;

-- 04 指定TAGS导入导出
CREATE TABLE test_impexp.tb1 (k_timestamp timestamptz not null, e1 int2, e2 float, e3 bool) tags (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes,tall varbytes,screen varbytes,age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
EXPORT INTO CSV "nodelocal://1/export_column/test_4/" FROM SELECT k_timestamp, code1, code2, code3, flag, val1, val2, location, color, name, state, tall, screen, age, sex, year, type FROM test_impexp.ds_tb;
IMPORT INTO test_impexp.tb1(k_timestamp, code1, code2, code3, flag, val1, val2, location, color, name, state, tall, screen, age, sex, year, type) CSV DATA ("nodelocal://1/export_column/test_4");
SELECT * FROM test_impexp.tb1 order by k_timestamp;
select k_timestamp from test_impexp.tb1 order by k_timestamp;
drop table test_impexp.tb1;

-- 05 指定不存在列导入导出
create table test_impexp.tb1 (k_timestamp timestamptz not null, e1 int2 , e2 float, e3 varchar) tags (code1 int2 not null, code2 bool) primary tags (code1);
export into csv "nodelocal://1/export_column/test_5/" from select a,b,c from test_impexp.ds_tb;
export into csv "nodelocal://1/export_column/test_5" from select k_timestamp, e1, e2, e11, code1 from test_impexp.ds_tb;
import into test_impexp.tb1(k_timestamp, a, b, c) csv data ("nodelocal://1/export_column/test_5/");
import into test_impexp.tb1(k_timestamp,e2,e1,e3,code1) csv data ("nodelocal://1/export_column/test_5");
select * from test_impexp.tb1 order by k_timestamp;
select k_timestamp from test_impexp.tb1 order by k_timestamp;
drop table test_impexp.tb1;

-- 06 指定重复列导入
create table test_impexp.tb1 (k_timestamp timestamptz not null, e1 float4, e2 float, e3 int2) tags (code1 int2 not null , code2 bool) primary tags(code1);
export into csv "nodelocal://1/export_column/test_6/" from select k_timestamp, e1, e4, e5, e1, e6 from test_impexp.ds_tb;
import into test_impexp.tb1 (k_timestamp,e3,e3,e3,code1,code2) csv data ("nodelocal://1/export_column/test_6/");
import into test_impexp.tb1(k_timestamp,e3,e2,e1,code1,code2) csv data("nodelocal://1/export_column/test_6/");
select * from test_impexp.tb1 order by k_timestamp;
select k_timestamp from test_impexp.tb1 order by k_timestamp;
drop table test_impexp.tb1;

-- 07 err. order by or group by
EXPORT INTO CSV "nodelocal://1/export_column/test_7/" FROM SELECT e1 FROM test_impexp.ds_tb order by e1;
EXPORT INTO CSV "nodelocal://1/export_column/test_7/" FROM SELECT e1 FROM test_impexp.ds_tb group by e1;
EXPORT INTO CSV "nodelocal://1/export_column/test_7/" FROM SELECT max(e1) FROM test_impexp.ds_tb;


use defaultdb;
drop database test_impexp cascade;