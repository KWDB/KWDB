CREATE TS DATABASE test_impexp;
create table test_impexp.ds_tb( k_timestamp timestamptz not null,e8 char(1023), e9 nchar(255) not null, e10 nchar(200), e11 char not null, e12 nchar(200), e13 nchar not null, e14 nchar(200), e15 nchar(200) not null, e16 varbytes(200), e17 nchar(200) not null,  e18 nchar(200),e19 varbytes not null, e20 varbytes(1023), e21 varbytes(200) not null, e22 varbytes(200) )  ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null, name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
use test_impexp;
-- basic_string test
INSERT INTO ds_tb values('2023-12-12 12:00:00.000+00:00', null, 'basic_string基础测试', null, 't', null, '中', null, 'basic_string基础测试', null, 'basic_string基础测试', null, b'\xaa', null, 'basic_string基础测试', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');

-- "Front, middle and back"
INSERT INTO ds_tb values('2023-12-12 12:00:01.000+00:00', null, '"begin_enclose前', null, 't', null, '中', null, '"begin_enclose前', null, '"begin_enclose前', null, b'\xaa', null, '"begin_enclose前', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO ds_tb values('2023-12-12 12:00:02.000+00:00', null, 'middle_enclose"中', null, 't', null, '中', null, 'middle_enclose"中', null, 'middle_enclose"中', null, b'\xaa', null, 'middle_enclose"中', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO ds_tb values('2023-12-12 12:00:03.000+00:00', null, 'end_enclose后"', null, 't', null, '中', null, 'end_enclose后"', null, 'end_enclose后"', null, b'\xaa', null, 'end_enclose后"', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');

-- 'Front, middle and back'
INSERT INTO ds_tb values('2023-12-12 12:00:04.000+00:00', null, E'\'begin_enclose前', null, 't', null, '中', null, E'\'begin_enclose前', null, E'\'begin_enclose前', null, b'\xaa', null, E'\'begin_enclose前', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO ds_tb values('2023-12-12 12:00:05.000+00:00', null, E'middle_enclose\'中', null, 't', null, '中', null, E'middle_enclose\'中', null, E'middle_enclose\'中', null, b'\xaa', null, E'middle_enclose\'中', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO ds_tb values('2023-12-12 12:00:06.000+00:00', null, E'end_enclose后\'', null, 't', null, '中', null, E'end_enclose后\'', null, E'end_enclose后\'', null, b'\xaa', null, E'end_enclose后\'', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');

-- Line break
INSERT INTO ds_tb values('2023-12-12 12:00:07.000+00:00', null, E'end_enclose\n跨行', null, 't', null, '中', null, E'end_enclose\n跨行', null, E'end_enclose\n跨行', null, b'\xaa', null, E'end_enclose\n跨行', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');

-- separator
INSERT INTO ds_tb values('2023-12-12 12:00:08.000+00:00', null, 'seprator,分隔符basic测试', null, 't', null, '中', null, 'seprator,分隔符basic测试', null, 'seprator,分隔符basic测试', null, b'\xaa', null, 'seprator,分隔符basic测试', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');

INSERT INTO ds_tb values('2023-12-12 12:00:09.000+00:00', null, ',seprator分隔符前', null, 't', null, '中', null, ',seprator分隔符前', null, ',seprator分隔符前', null, b'\xaa', null, ',seprator分隔符前', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO ds_tb values('2023-12-12 12:00:10.000+00:00', null, 'seprator,分隔符中', null, 't', null, '中', null, 'seprator,分隔符中', null, 'seprator,分隔符中', null, b'\xaa', null, 'seprator,分隔符中', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO ds_tb values('2023-12-12 12:00:11.000+00:00', null, 'seprator分隔符后,', null, 't', null, '中', null, 'seprator分隔符后,', null, 'seprator分隔符后,', null, b'\xaa', null, 'seprator分隔符后,', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');

-- Escape character
INSERT INTO ds_tb values('2023-12-12 12:00:12.000+00:00', null, E'\\escape基础测试', null, 't', null, '中', null, E'\\escape基础测试', null, E'\\escape基础测试', null, b'\xaa', null, E'\\escape基础测试', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO ds_tb values('2023-12-12 12:00:13.000+00:00', null, E'escape基础\\测试中', null, 't', null, '中', null, E'escape基础\\测试中', null, E'escape基础\\测试中', null, b'\xaa', null, E'escape基础\\测试中', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');
INSERT INTO ds_tb values('2023-12-12 12:00:14.000+00:00', null, E'escape测试后\\', null, 't', null, '中', null, E'escape测试后\\', null, E'escape测试后\\', null, b'\xaa', null, E'escape测试后\\', null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');


select * from ds_tb;

export into csv "nodelocal://1/exp_enclose_single_escape_double" from table ds_tb with enclosed=E'\'', escaped=E'"';
-- "
export into csv "nodelocal://1/exp_enclose_single_escape_slash" from table ds_tb with enclosed=E'\'', escaped=E'\\';
-- '
export into csv "nodelocal://1/exp_enclose_double_escape_slash" from table ds_tb with enclosed=E'"', escaped=E'\\';
-- "
export into csv "nodelocal://1/exp_enclose_double_escape_double" from table ds_tb with enclosed=E'"', escaped=E'"';

CREATE TABLE in_enclose_single_escape_double ( k_timestamp timestamptz not null,e8 char(1023), e9 nchar(255) not null, e10 nchar(200), e11 char not null, e12 nchar(200), e13 nchar not null, e14 nchar(200), e15 nchar(200) not null, e16 varbytes(200), e17 nchar(200) not null,  e18 nchar(200),e19 varbytes not null, e20 varbytes(1023), e21 varbytes(200) not null, e22 varbytes(200) )  ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null, name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
import into in_enclose_single_escape_double csv data ("nodelocal://1/exp_enclose_single_escape_double/") with enclosed=E'\'', escaped=E'"';
-- "
select * from in_enclose_single_escape_double;

CREATE TABLE in_enclose_single_escape_slash ( k_timestamp timestamptz not null,e8 char(1023), e9 nchar(255) not null, e10 nchar(200), e11 char not null, e12 nchar(200), e13 nchar not null, e14 nchar(200), e15 nchar(200) not null, e16 varbytes(200), e17 nchar(200) not null,  e18 nchar(200),e19 varbytes not null, e20 varbytes(1023), e21 varbytes(200) not null, e22 varbytes(200) )  ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null, name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
import into in_enclose_single_escape_slash csv data ("nodelocal://1/exp_enclose_single_escape_slash/") with enclosed=E'\'', escaped=E'\\';
-- '
select * from in_enclose_single_escape_slash;

CREATE TABLE in_enclose_double_escape_slash ( k_timestamp timestamptz not null,e8 char(1023), e9 nchar(255) not null, e10 nchar(200), e11 char not null, e12 nchar(200), e13 nchar not null, e14 nchar(200), e15 nchar(200) not null, e16 varbytes(200), e17 nchar(200) not null,  e18 nchar(200),e19 varbytes not null, e20 varbytes(1023), e21 varbytes(200) not null, e22 varbytes(200) )  ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null, name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
import into in_enclose_double_escape_slash csv data ("nodelocal://1/exp_enclose_double_escape_slash/") with enclosed=E'"', escaped=E'\\';
select * from in_enclose_double_escape_slash;

CREATE TABLE in_enclose_double_escape_double ( k_timestamp timestamptz not null,e8 char(1023), e9 nchar(255) not null, e10 nchar(200), e11 char not null, e12 nchar(200), e13 nchar not null, e14 nchar(200), e15 nchar(200) not null, e16 varbytes(200), e17 nchar(200) not null,  e18 nchar(200),e19 varbytes not null, e20 varbytes(1023), e21 varbytes(200) not null, e22 varbytes(200) )  ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null, name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
import into in_enclose_double_escape_double csv data ("nodelocal://1/exp_enclose_double_escape_double/") with enclosed=E'"', escaped=E'"';
select * from in_enclose_double_escape_double;

drop table ds_tb;
drop table in_enclose_single_escape_double;
drop table in_enclose_single_escape_slash;
drop table in_enclose_double_escape_slash;
drop table in_enclose_double_escape_double;
use defaultdb;
drop database test_impexp;