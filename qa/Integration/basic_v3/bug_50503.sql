drop database if exists test_vacuum cascade;
create ts database test_vacuum;
create table test_vacuum.t1 (k_timestamp timestamptz not null, id int not null, e1 int2, e2 int, e3 int8, e4 float4, e5 float8, e6 bool, e7 timestamptz, e8 char(1023), e9 nchar(255), e10 varchar(4096), e11 char, e12 char(255), e13 nchar, e14 nvarchar(4096), e15 varchar(1023), e16 nvarchar(200), e17 nchar(255), e18 char(200), e19 varbytes, e20 varbytes(60), e21 varchar, e22 nvarchar ) tags (code1 int2 not null, code2 int, code3 int8, code4 float4, code5 float8, code6 bool, code7 varchar, code8 varchar(128) not null, code9 varbytes, code10 varbytes(60), code11 varchar, code12 varchar(60), code13 char(2), code14 char(1023) not null, code15 nchar, code16 nchar(254) not null ) primary tags (code1);

INSERT INTO test_vacuum.t1 VALUES('2032-02-29 1:10:10.000',15,-32767,-34000000,-340000000000,43000079.07812032,122209810.1131921,true,'2099-9-1 11:01:00.111','数据库语法查询测试','>数据库语法查询测试','数据库语法查询测试','1','数据库语法查询测试','2','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','9','数据库语法查询测>试','数据库语法查询测试','数据库语法查询测试',1,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'数据库语法查询测试','8','7','数据库语法查询测试','数据库语法查询测试','数据库语法查>询测试','65','数据库语法查询测试','4','数据库语法查询测试');
select last_row(e8),last_row(e12) from test_vacuum.t1;

delete from test_vacuum.t1 where true;
select last_row(e8),last_row(e12) from test_vacuum.t1;

INSERT INTO test_vacuum.t1 VALUES('2011-12-9 09:48:12.30',9,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,1,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull');
INSERT INTO test_vacuum.t1 VALUES('2002-2-22 10:48:12.899',10,32767,-2147483648,9223372036854775807,-99999999991.9999999991,9999999999991.999999999991,true,'2020-10-1 12:00:01.0','test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t','test数据库语法查询测试！！！@TEST10-12','中','test数据库语法查询测试！！>！@TEST10-14','test数据库语法查询测试！！！@TEST10-15','test数据库语法查询测试！TEST10-16xaa','test数据库语法查询测试！！！@TEST10-17','test数据库语法查询测试！！！@TEST10-18',b'\\xca','test数据库>语法查询测试！！！@TEST10-20','test数据库语法查询测试！！！@TEST10-21','test数据库语法查询测试！！！@TEST10-22',1,111,1111111,1472011.12345,1109810.113011921,false,'test数据库语法查询测试！！！@TEST10-7','test数据库语法查询测试！！！@TEST10-8',b'\\xcc','test数据库语法查询测试！！！@TEST10-10','test数据库语法查询测试！！！@TEST10-11','test数据库语法查询测试！！！@TEST10-12','t3','test数据库语
法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16');
INSERT INTO test_vacuum.t1 VALUES('2003-10-1 11:48:12.1',11,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语
法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\\xca','test数据库语法查询测试>！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',1,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST11-8',b'\\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！
！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16');

select k_timestamp,id,e8,e12 from test_vacuum.t1;
select last_row(e8),last_row(e12) from test_vacuum.t1;

drop database test_vacuum;