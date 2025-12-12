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
ATTRIBUTES (code1 INT2 NOT NULL,code2 INT,code3 INT8,code4 FLOAT4 ,code5 FLOAT8,code6 BOOL,code7 VARCHAR,code8 VARCHAR(128) NOT NULL,code9 VARBYTES,code10 VARBYTES(60),code11 VARCHAR,code12 VARCHAR(60),code13 CHAR(2),code14 CHAR(1023) NOT NULL,code15 NCHAR,code16 NCHAR(254) NOT NULL, code17 NCHAR(254) NOT NULL) 
PRIMARY TAGS(code1,code14,code8,code16);
INSERT INTO test_select_subquery.t1 VALUES(
    '2023-05-10 10:00:00.000', -- k_timestamp (NOT NULL)
    1, -- id (NOT NULL)
    123, -- e1 INT2
    456, -- e2 INT
    789012345678, -- e3 INT8
    123.45, -- e4 FLOAT4
    678.90123456, -- e5 FLOAT8
    true, -- e6 BOOL
    '2023-05-10 10:00:00.000', -- e7 TIMESTAMPTZ
    'char_test_1023', -- e8 CHAR(1023)
    '中文字符测试', -- e9 NCHAR(255)
    'varchar_4096_test', -- e10 VARCHAR(4096)
    'a', -- e11 CHAR
    'char_255_test', -- e12 CHAR(255)
    '中', -- e13 NCHAR
    'nvarchar_4096_test', -- e14 NVARCHAR(4096)
    'varchar_1023_test', -- e15 VARCHAR(1023)
    'nvarchar_200_test', -- e16 NVARCHAR(200)
    'nchar_255_test', -- e17 NCHAR(255)
    'char_200_test', -- e18 CHAR(200)
    b'\x01\x02\x03', -- e19 VARBYTES
    b'\x04\x05\x06', -- e20 VARBYTES(60)
    'varchar_test', -- e21 VARCHAR
    'nvarchar_test', -- e22 NVARCHAR
    1, -- code1 INT2 NOT NULL
    2, -- code2 INT
    3, -- code3 INT8
    4.5, -- code4 FLOAT4
    6.789, -- code5 FLOAT8
    false, -- code6 BOOL
    'code7_varchar', -- code7 VARCHAR
    'code8_varchar_128', -- code8 VARCHAR(128) NOT NULL
    b'\x07\x08\x09', -- code9 VARBYTES
    b'\x0a\x0b\x0c', -- code10 VARBYTES(60)
    'code11_varchar', -- code11 VARCHAR
    'code12_varchar_60', -- code12 VARCHAR(60)
    'ab', -- code13 CHAR(2)
    'code14_char_1023', -- code14 CHAR(1023) NOT NULL
    '中', -- code15 NCHAR
    'code16_nchar_254', -- code16 NCHAR(254) NOT NULL
    '1234567890987654321' -- code17 NCHAR NOT NULL
);
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
                code15 NCHAR,code16 NCHAR(254) NOT NULL, code17 TEXT NOT NULL);
INSERT INTO test_select_subquery2.t1 VALUES('2008-2-29 2:10:10.111',14,32767,34000000,340000000000,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321','1234567890987654321');

set enable_multimodel=true;
-- Test JOIN on code17 between two tables
SELECT t1.id, t2.id, t1.e8, t2.e8 
FROM test_select_subquery.t1 t1 
JOIN test_select_subquery2.t1 t2 
ON t1.code17 = t2.code17 
ORDER BY t1.id;

-- Explain the JOIN execution plan
EXPLAIN SELECT t1.id, t2.id, t1.e8, t2.e8 
FROM test_select_subquery.t1 t1 
JOIN test_select_subquery2.t1 t2 
ON t1.code17 = t2.code17 
ORDER BY t1.id;

CREATE TABLE test_select_subquery.t2 (
	k_timestamp TIMESTAMPTZ(3) NOT NULL,
	e6 BOOL NULL,
	e7 TIMESTAMPTZ(3) NULL,
	e12 CHAR(255) NULL
) TAGS (
	code1 INT2 NOT NULL,
	code6 BOOL
 ) PRIMARY TAGS(code1)
	retentions 0s
	activetime 1d
	;

INSERT INTO test_select_subquery.t2 VALUES ('2023-05-10 10:00:00.000', 1, '2023-05-10 10:00:00.000', 'char_test_1023', 1, 1);

select
  subq_1.c0 as c0 
from
  test_select_subquery.t2 as ref_0
        inner join (select
              (select e12 from test_select_subquery.t2 limit 1) as c0,
              (select e6 from test_select_subquery.t2 limit 1) as c11
            from
              test_select_subquery.t2 as ref_1,
              lateral (select
                    test_select_subquery.t2.e7
                  from
                    test_select_subquery.t2
                  limit 1) as subq_0
            ) as subq_1
        on (ref_0.code6 = subq_1.c11 )
limit 1;

explain select
  subq_1.c0 as c0 
from
  test_select_subquery.t2 as ref_0
        inner join (select
              (select e12 from test_select_subquery.t2 limit 1) as c0,
              (select e6 from test_select_subquery.t2 limit 1) as c11
            from
              test_select_subquery.t2 as ref_1,
              lateral (select
                    test_select_subquery.t2.e7
                  from
                    test_select_subquery.t2
                  limit 1) as subq_0
            ) as subq_1
        on (ref_0.code6 = subq_1.c11 )
limit 1;

set enable_multimodel=false;
set cluster setting ts.sql.query_opt_mode = 1110;
DROP DATABASE IF exists test_select_subquery cascade;
DROP DATABASE IF exists test_select_subquery2 cascade;