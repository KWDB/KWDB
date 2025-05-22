CREATE TS DATABASE test_function_3;
CREATE TABLE test_function_3.t1(
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
e18 CHAR(200),e19 VARBYTES,
e20 VARBYTES(60),
e21 VARCHAR,
e22 NVARCHAR)
ATTRIBUTES (
code1 INT2 NOT NULL,code2 INT,code3 INT8,
code4 FLOAT4 ,code5 FLOAT8,
code6 BOOL,
code7 VARCHAR,code8 VARCHAR(128) NOT NULL,
code9 VARBYTES,code10 VARBYTES(60),
code11 VARCHAR,code12 VARCHAR(60),
code13 CHAR(2),code14 CHAR(1023) NOT NULL,
code15 NCHAR,code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code1,code14,code8,code16);
INSERT INTO test_function_3.t1 VALUES('2001-12-9 09:48:12.30',9,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,30000,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull');

SELECT ifnull(e1,10) FROM test_function_3.t1 WHERE e5 is unknown ORDER BY k_timestamp;
SELECT ifnull(e1,NULL) FROM test_function_3.t1 WHERE e5 is unknown ORDER BY k_timestamp;
SELECT ifnull(id,10) FROM test_function_3.t1 WHERE e5 is unknown ORDER BY k_timestamp;
select pg_catalog.date_trunc(cast(case when e1 is NULL then cast(null as text) else cast(null as text) end
as text),cast(cast(null as "timestamp") as "timestamp")) from test_function_3.t1;
select pg_catalog.date_trunc('ms', k_timestamp) from test_function_3.t1;
drop database test_function_3 cascade;