drop database if exists test_function_2 cascade;
create ts database test_function_2;

create table test_function_2.t1(k_timestamp timestamp not null,e1 timestamp,e2 int2,e3 int4,e4 int8,e5 float4,e6 float8) ATTRIBUTES (code1 INT2 NOT NULL,code2 INT4,code3 INT8,code4 FLOAT4 ,code5 FLOAT8,code6 BOOL,code7 VARCHAR,code8 VARCHAR(128) NOT NULL,code9 VARBYTES,code10 VARBYTES(60),code11 VARCHAR,code12 VARCHAR(60),code13 CHAR(2),code14 CHAR(1023) NOT NULL,code15 NCHAR,code16 NCHAR(254) NOT NULL) PRIMARY TAGS(code1,code14,code8,code16);

insert into test_function_2.t1 values ('2021-04-01 15:00:00',111111110000,1000,1000000,100000000,100000.101,1000000.10101111,-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');

select ceiling(e2), ceiling(e3), ceiling(e4), ceiling(e5), ceiling(e6) from test_function_2.t1 where e2 < 5000 group by e2,e3,e4,e5,e6 order by e2,e3,e4,e5,e6;

select round(e5,1), ceiling(e5) from test_function_2.t1 where e3 > 3000000 group by e5 having e5 < 500000.505 order by e5 desc;

select abs(e2) from test_function_2.t1;

select abs(e2) from test_function_2.t1 group by e2;

select abs(e2) from test_function_2.t1 group by e2 order by e2;

select abs(max(e2)) from test_function_2.t1;

select abs(max(e2)) from test_function_2.t1 group by e2;

select abs(max(e2)) from test_function_2.t1 group by e2 order by e2;

select coalesce(e2, 1) from test_function_2.t1;

explain select coalesce(e2, 1) from test_function_2.t1;

select coalesce(e2, 1, 2) from test_function_2.t1;

explain select coalesce(e2, 1, 2) from test_function_2.t1;

drop database test_function_2 cascade;
