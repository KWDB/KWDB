
-- TEST NULLAS NULLIF
CREATE TS DATABASE test_impexp;
create table test_impexp.ds_tb( k_timestamp timestamptz not null, e1 int2  not null, e2 int, e3 int8 not null, e4 float4, e5 float8 not null, e6 bool, e7 timestamptz not null, e8 char(1023), e9 nchar(255) not null, e10 nchar(200), e11 char not null, e12 nchar(200), e13 nchar not null, e14 nchar(200), e15 nchar(200) not null, e16 varbytes(200), e17 nchar(200) not null, e18 nchar(200),e19 varbytes not null, e20 varbytes(1023), e21 varbytes(200) not null, e22 varbytes(200) ) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
use test_impexp;
INSERT INTO ds_tb values('2023-12-12 12:00:00.000+00:00',1,null,1000,null,100.0,null,'2020-1-7 12:00:00.000',null,'test时间精度通用查询测试,！！！@TEST1',null,'t',null,'中',null,'test时间精度通用查询测试,！！！@TEST1',null,'test时间精度通用查询测试,！！！@TEST1',null,b'\xaa',null,'test时间精度通用查询测试',null, 1, 2, 3, false, 1.1, 1.2,'a', 'red', 'T','China', 'a', 'b', '1', '女', '1', 'pria');

select * from test_impexp.ds_tb;
EXPORT INTO CSV "nodelocal://01/nullasbasic" FROM TABLE test_impexp.ds_tb;
EXPORT INTO CSV "nodelocal://01/nullasNULL" FROM TABLE test_impexp.ds_tb with nullas='NULL';
EXPORT INTO CSV "nodelocal://01/nullasNull" FROM TABLE test_impexp.ds_tb with nullas='Null';
EXPORT INTO CSV "nodelocal://01/nullasnull" FROM TABLE test_impexp.ds_tb with nullas='null';
EXPORT INTO CSV "nodelocal://01/nullasSlashN" FROM TABLE test_impexp.ds_tb with nullas='\N';
EXPORT INTO CSV "nodelocal://01/mutilOption" FROM TABLE test_impexp.ds_tb with nullas='NULL',nullas='\N';
create table test_impexp.ds_tb1 ( k_timestamp timestamptz not null, e1 int2  not null, e2 int, e3 int8 not null, e4 float4, e5 float8 not null, e6 bool, e7 timestamptz not null, e8 char(1023), e9 nchar(255) not null, e10 nchar(200), e11 char not null, e12 nchar(200), e13 nchar not null, e14 nchar(200), e15 nchar(200) not null, e16 varbytes(200), e17 nchar(200) not null, e18 nchar(200),e19 varbytes not null, e20 varbytes(1023), e21 varbytes(200) not null, e22 varbytes(200) ) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
create table test_impexp.ds_tb2 ( k_timestamp timestamptz not null, e1 int2  not null, e2 int, e3 int8 not null, e4 float4, e5 float8 not null, e6 bool, e7 timestamptz not null, e8 char(1023), e9 nchar(255) not null, e10 nchar(200), e11 char not null, e12 nchar(200), e13 nchar not null, e14 nchar(200), e15 nchar(200) not null, e16 varbytes(200), e17 nchar(200) not null, e18 nchar(200),e19 varbytes not null, e20 varbytes(1023), e21 varbytes(200) not null, e22 varbytes(200) ) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
create table test_impexp.ds_tb3 ( k_timestamp timestamptz not null, e1 int2  not null, e2 int, e3 int8 not null, e4 float4, e5 float8 not null, e6 bool, e7 timestamptz not null, e8 char(1023), e9 nchar(255) not null, e10 nchar(200), e11 char not null, e12 nchar(200), e13 nchar not null, e14 nchar(200), e15 nchar(200) not null, e16 varbytes(200), e17 nchar(200) not null, e18 nchar(200),e19 varbytes not null, e20 varbytes(1023), e21 varbytes(200) not null, e22 varbytes(200) ) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
create table test_impexp.ds_tb4 ( k_timestamp timestamptz not null, e1 int2  not null, e2 int, e3 int8 not null, e4 float4, e5 float8 not null, e6 bool, e7 timestamptz not null, e8 char(1023), e9 nchar(255) not null, e10 nchar(200), e11 char not null, e12 nchar(200), e13 nchar not null, e14 nchar(200), e15 nchar(200) not null, e16 varbytes(200), e17 nchar(200) not null, e18 nchar(200),e19 varbytes not null, e20 varbytes(1023), e21 varbytes(200) not null, e22 varbytes(200) ) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
create table test_impexp.ds_tb5 ( k_timestamp timestamptz not null, e1 int2  not null, e2 int, e3 int8 not null, e4 float4, e5 float8 not null, e6 bool, e7 timestamptz not null, e8 char(1023), e9 nchar(255) not null, e10 nchar(200), e11 char not null, e12 nchar(200), e13 nchar not null, e14 nchar(200), e15 nchar(200) not null, e16 varbytes(200), e17 nchar(200) not null, e18 nchar(200),e19 varbytes not null, e20 varbytes(1023), e21 varbytes(200) not null, e22 varbytes(200) ) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);
import into test_impexp.ds_tb1 csv data ("nodelocal://01/nullasNULL/") with nullif="NULL";
import INTO test_impexp.ds_tb2 CSV data ("nodelocal://01/nullasNull/") with nullif='Null';
import INTO test_impexp.ds_tb3 CSV data ("nodelocal://01/nullasnull/") with nullif='null';
import INTO test_impexp.ds_tb4 CSV data ("nodelocal://01/nullasSlashN/") with nullif='\N';
import INTO test_impexp.ds_tb5 CSV data ("nodelocal://01/nullasbasic/");
import INTO test_impexp.ds_tb5 CSV data ("nodelocal://01/nullasbasic/") with nullif='NULL',nullif='\N';;

select * from test_impexp.ds_tb1;
select * from test_impexp.ds_tb2;
select * from test_impexp.ds_tb3;
select * from test_impexp.ds_tb4;
select * from test_impexp.ds_tb5;
EXPORT INTO CSV "nodelocal://01/nullasNULLresult" FROM TABLE test_impexp.ds_tb1;
EXPORT INTO CSV "nodelocal://01/nullasNullresult" FROM TABLE test_impexp.ds_tb2;
EXPORT INTO CSV "nodelocal://01/nullasnullresult" FROM TABLE test_impexp.ds_tb3;
EXPORT INTO CSV "nodelocal://01/nullasSlashNresult" FROM TABLE test_impexp.ds_tb4;
EXPORT INTO CSV "nodelocal://01/nullasbasicresult" FROM TABLE test_impexp.ds_tb5;
drop table test_impexp.ds_tb1;
drop table test_impexp.ds_tb2;
drop table test_impexp.ds_tb3;
drop table test_impexp.ds_tb4;
drop table test_impexp.ds_tb5;
use defaultdb;
drop database test_impexp;