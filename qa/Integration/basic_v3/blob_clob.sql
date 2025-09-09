--basic test
drop database if exists d1 cascade;
drop database if exists d2 cascade;
create database d1;
create ts database d2;

--- error
use d2;
create table t1 (ts timestamptz not null, e1 blob, e2 clob) tags (tag1 int not null) primary tags(tag1);
create table t2 (ts timestamptz not null, e1 clob, e2 blob) tags (tag1 int not null) primary tags(tag1);

use d1;
create table t1 (e1 blob, e2 clob);
show create t1;
insert into t1 values ('abc', 'def');
select * from t1;

-- error
create table t2 (e1 blob(3), e2 clob);
create table t3 (e1 blob, e2 clob(3));

create table t4 (e1 string);
insert into t4 values ('abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz');
select length(e1) from t4;
alter table t4 alter e1 type varchar(65);

drop database d1 cascade;
drop database d2 cascade;






drop database if exists test_blob_clob_rel cascade;
create database test_blob_clob_rel;
----第一部分：DDL
------不指定BLOB和CLOB类型长度，结合关系引擎其他类型共同建表、视图
DROP TABLE IF EXISTS test_blob_clob_rel.type CASCADE;
CREATE TABLE test_blob_clob_rel.type(
                                        id  INT,
                                        k_timestamp TIMESTAMPTZ,
                                        e1  INT,
                                        e2  SMALLINT,
                                        e3  BIGINT,
                                        e4  INTEGER,
                                        e5  INT2,
                                        e6  INT4,
                                        e7  INT8,
                                        e8  INT64,
                                        e9  SERIAL,
                                        e10 SMALLSERIAL,
                                        e11 BIGSERIAL,
                                        e12 SERIAL2,
                                        e13 SERIAL4,
                                        e14 SERIAL8,
                                        e15 FLOAT,
                                        e16 REAL,
                                        e17 DOUBLE PRECISION,
                                        e18 FLOAT4,
                                        e19 FLOAT8,
                                        e20 DECIMAL,
                                        e21 DEC,
                                        e22 NUMERIC,
                                        e23 BOOL,
                                        e24 BOOLEAN,
                                        e25 BIT,
                                        e26 BIT(3),
                                        e27 VARBIT,
                                        e28 VARBIT(3),
                                        e29 BYTES,
                                        e30 BLOB,
                                        e31 BYTEA,
                                        e32 STRING,
                                        e33 CHARACTER,
                                        e34 CHAR,
                                        e35 VARCHAR,
                                        e36 TEXT,
                                        e37 STRING(5),
                                        e38 CHARACTER(95),
                                        e39 CHARACTER VARYING(200),
                                        e40 CHAR(15),
                                        e41 CHAR VARYING(2500),
                                        e42 VARCHAR(5),
                                        e43 STRING COLLATE en,
                                        e44 TEXT COLLATE en,
                                        e45 DATE,
                                        e46 TIME,
                                        e47 TIMESTAMP,
                                        e48 DATE[],
                                        e49 TIMESTAMP WITHOUT TIME ZONE,
                                        e50 TIMESTAMP WITH TIME ZONE,
                                        e51 INTERVAL,
                                        e52 INET,
                                        e53 UUID,
                                        e54 JSONB,
                                        e55 JSON,
                                        e56 INT ARRAY,
                                        e57 FLOAT[],
                                        e58 STRING ARRAY,
                                        e59 OID,
                                        e60 VARBYTES,
                                        e61 NVARCHAR,
                                        e62 INET[],
                                        e63 DATE[],
                                        e64 UUID[],
                                        e65 TIMESTAMP ARRAY,
                                        e66 OID[],
                                        e67 DECIMAL[],
                                        e68 NVARCHAR(3000),
                                        e69 VARBYTES,
                                        e70 CLOB);

INSERT INTO test_blob_clob_rel.type VALUES (
                                               1,
                                               '1976-12-13 12:00:12.111222',
                                               2147483647,
                                               32767,
                                               9223372036854775807,
                                               2147483647,
                                               32767,
                                               2147483647,
                                               9223372036854775807,
                                               9223372036854775807,
                                               1,
                                               1,
                                               1,
                                               1,
                                               1,
                                               1,
                                               3.40282e+18,
                                               1.17549e+18,
                                               2.22507e-8,
                                               1.17549e+18,
                                               2.22507e+18,
                                               10.123,
                                               10.123,
                                               10.123,
                                               TRUE,
                                               TRUE,
                                               B'1',
                                               B'101',
                                               B'101',
                                               B'101',
                                               '1',
                                               'sample blob',
                                               'sample bytea',
                                               'Hello, world!',
                                               'A',
                                               'A',
                                               'Hello, world!',
                                               'Text content',
                                               'Hello',
                                               'A',
                                               'Variable length character string',
                                               'FixedLength',
                                               'Very long character varying string that exceeds normal lengths',
                                               'Short',
                                               null,
                                               null,
                                               '2024-04-28',
                                               '15:12:07',
                                               '2024-04-28 15:12:07',
                                               ARRAY['2024-04-28', '2024-02-29'],
                                               '2024-04-28 15:12:07',
                                               '2024-04-28 15:12:07+08',
                                               '1year 5months 2minutes 60 seconds',
                                               '192.168.1.1',
                                               'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
                                               '{"key": "value"}',
                                               '{"key": "value"}',
                                               ARRAY[1,2,3],
                                               ARRAY[1.0, 2.0, 3.0],
                                               ARRAY['text1', 'text2', 'text3'],
                                               2048,
                                               'var bytes',
                                               'nvarchar text',
                                               ARRAY['192.168.1.1', '10.0.0.1'],
                                               ARRAY['2024-04-28', '2024-05-01'],
                                               ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'],
                                               ARRAY['2024-04-28 15:12:07', '2024-05-01 12:00:00'],
                                               ARRAY[2048],
                                               ARRAY[10.123, 20.456],
                                               'Long NVARCHAR text',
                                               'var bytes',
                                               'test数据库语法查询测试！TEST1-111111xaa');

---建立视图
CREATE VIEW test_blob_clob_rel.typev1 AS SELECT * FROM test_blob_clob_rel.type;
INSERT INTO test_blob_clob_rel.type VALUES (
                                               2,
                                               '2020-4-3 12:00:12.1293',
                                               -2147483648,
                                               -32768,
                                               -9223372036854775808,
                                               -2147483648,
                                               -32768,
                                               -2147483648,
                                               -9223372036854775808,
                                               -9223372036854775808,
                                               -1,
                                               -1,
                                               -1,
                                               -1,
                                               -1,
                                               -1,
                                               -3.40282e+18,
                                               -1.17549e+18,
                                               -2.22507e-8,
                                               -1.17549e+18,
                                               -2.22507e+18,
                                               -10.123,
                                               -10.123,
                                               -10.123,
                                               TRUE,
                                               TRUE,
                                               B'1',
                                               B'101',
                                               B'101',
                                               B'101',
                                               '1',
                                               '中文#@<Ab《18',
                                               '中文#@<Ab《18',
                                               '中文#@<Ab《18',
                                               '中',
                                               '中',
                                               '中文#@<Ab《18',
                                               '中文#@<Ab《18',
                                               '中《1a',
                                               '中',
                                               '中文#@<Ab《18',
                                               '中文#@<Ab《18',
                                               '中文#@<Ab《18',
                                               '中《1a',
                                               null,
                                               null,
                                               '2024-04-28',
                                               '15:12:07',
                                               '2024-04-28 15:12:07',
                                               ARRAY['2024-04-28', '2024-02-29'],
                                               '2024-04-28 15:12:07',
                                               '2024-04-28 15:12:07+08',
                                               '1year 5months 2minutes 60 seconds',
                                               '192.168.1.1',
                                               'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
                                               '{"key": "value"}',
                                               '{"key": "value"}',
                                               ARRAY[-1,2,-3,0],
                                               ARRAY[-1.0, 2.0, -3.0],
                                               ARRAY['SJmnjd中', '0000', '中Ab1<《'],
                                               2048,
                                               '中文#@<Ab《18',
                                               '中文#@<Ab《18',
                                               ARRAY['192.168.1.1', '10.0.0.1'],
                                               ARRAY['2024-04-28', '2024-05-01'],
                                               ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'],
                                               ARRAY['2024-04-28 15:12:07', '2024-05-01 12:00:00'],
                                               ARRAY[2048],
                                               ARRAY[10.123, 20.456],
                                               'Long NVARCHAR text',
                                               'var bytes',
                                               'test数据库语法查询测试！TEST2-222222zbb');
---校验：
SELECT * FROM test_blob_clob_rel.type ORDER BY id;
SELECT * FROM test_blob_clob_rel.typev1 ORDER BY id;
SHOW CREATE TABLE test_blob_clob_rel.type;
SHOW CREATE TABLE test_blob_clob_rel.typev1;
---清理测试数据
DROP TABLE test_blob_clob_rel.type CASCADE;

----第二部分：插入

DROP TABLE IF EXISTS test_blob_clob_rel.data1 CASCADE;
CREATE TABLE test_blob_clob_rel.data1(
                                         tp  TIMESTAMPTZ NOT NULL,
                                         id  INT NOT NULL        ,
                                         e1  INT2                ,
                                         e2  INT,
                                         e3  INT8,
                                         e4  FLOAT4,
                                         e5  FLOAT8,
                                         e6  BOOL,
                                         e7  TIMESTAMP,
                                         e8  CHAR(1023),
                                         e9  NCHAR(255),
                                         e10 VARCHAR(4096),
                                         e11 CHAR,
                                         e12 NVARCHAR(4096),
                                         e13 NCHAR,
                                         e14 VARBYTES,
                                         e15 VARCHAR,
                                         e16 NVARCHAR,
                                         e17 BLOB,
                                         e18 CLOB);


INSERT INTO test_blob_clob_rel.data1 VALUES('1970-01-01 00:00:00+00:00'    ,1 ,0      ,0          ,0                   ,0                      ,0                          ,true ,'1970-01-01 00:00:00+00:00'    ,''                                     ,''                                     ,''                                      ,''   ,''                                      ,''   ,''                                       ,''                                       ,''                                    ,''                                    ,''                                    );
INSERT INTO test_blob_clob_rel.data1 VALUES('1970-1-1 00:00:00.001'        ,2 ,0      ,0          ,0                   ,0                      ,0                          ,true ,'1970-01-01 00:16:39.999+00:00','          '                           ,'          '                           ,'          '                            ,' '  ,'          '                            ,' '  ,' '                                      ,'          '                             ,'          '                          ,'          '                          ,'          '                          );
INSERT INTO test_blob_clob_rel.data1 VALUES('1976-10-20 12:00:12.123+00:00',3 ,10001  ,10000001   ,100000000001        ,-1047200.00312001      ,-1109810.113011921         ,true ,'2021-03-01 12:00:00.909+00:00','test数据库语法查询测试！！！@TEST3-8' ,'test数据库语法查询测试！！！@TEST3-9' ,'test数据库语法查询测试！！！@TEST3-10' ,'t'  ,'test数据库语法查询测试！！！@TEST3-12' ,'中' ,'test数据库语法查询测试！！！@TEST3-14'  ,'test数据库语法查询测试！！！@TEST3-15'  ,'test数据库语法查询测试！TEST3-16xaa' ,'test数据库语法查询测试！TEST3-16xaa' ,'test数据库语法查询测试！TEST3-16xaa' );
INSERT INTO test_blob_clob_rel.data1 VALUES('1979-2-28 11:59:01.999'       ,4 ,20002  ,20000002   ,200000000002        ,-20873209.0220322201   ,-22012110.113011921        ,false,'1970-01-01 00:00:00.123+00:00','test数据库语法查询测试！！！@TEST4-8' ,'test数据库语法查询测试！！！@TEST4-9' ,'test数据库语法查询测试！！！@TEST4-10' ,'t'  ,'test数据库语法查询测试！！！@TEST4-12' ,'中' ,'test数据库语法查询测试！！！@TEST4-14'  ,'test数据库语法查询测试！！！@TEST4-15'  ,'test数据库语法查询测试！TEST4-16xaa' ,'test数据库语法查询测试！TEST4-16xaa' ,'test数据库语法查询测试！TEST4-16xaa' );
INSERT INTO test_blob_clob_rel.data1 VALUES('1980-01-31 19:01:01+00:00'    ,5 ,30003  ,30000003   ,300000000003        ,-33472098.11312001     ,-39009810.333011921        ,true ,'2015-3-12 10:00:00.234'       ,'test数据库语法查询测试！！！@TEST5-8' ,'test数据库语法查询测试！！！@TEST5-9' ,'test数据库语法查询测试！！！@TEST5-10' ,'t'  ,'test数据库语法查询测试！！！@TEST5-12' ,'中' ,'test数据库语法查询测试！！！@TEST5-14'  ,'test数据库语法查询测试！！！@TEST5-15'  ,'test数据库语法查询测试！TEST5-16xaa' ,'test数据库语法查询测试！TEST5-16xaa' ,'test数据库语法查询测试！TEST5-16xaa' );
INSERT INTO test_blob_clob_rel.data1 VALUES('1980-02-10 01:14:51.09+00:00' ,6 ,-10001 ,10000001   ,-100000000001       ,1047200.00312001       ,1109810.113011921          ,false,'2023-6-23 05:00:00.55'        ,'test数据库语法查询测试！！！@TEST6-8' ,'test数据库语法查询测试！！！@TEST6-9' ,'test数据库语法查询测试！！！@TEST6-10' ,'t'  ,'test数据库语法查询测试！！！@TEST6-12' ,'中' ,'test数据库语法查询测试！！！@TEST6-14'  ,'test数据库语法查询测试！！！@TEST6-15'  ,'test数据库语法查询测试！TEST6-16xaa' ,'xaaxbbxcc'                           ,'xaaxbbxcc'                           );
INSERT INTO test_blob_clob_rel.data1 VALUES('1980-02-10 01:48:11.029+00:00',7 ,-20002 ,20000002   ,-200000000002       ,20873209.0220322201    ,22012110.113011921         ,true ,'2016-07-17 20:12:00.12+00:00' ,'test数据库语法查询测试！！！@TEST7-8' ,'test数据库语法查询测试！！！@TEST7-9' ,'test数据库语法查询测试！！！@TEST7-10' ,'t'  ,'test数据库语法查询测试！！！@TEST7-12' ,'中' ,'test数据库语法查询测试！！！@TEST7-14'  ,'test数据库语法查询测试！！！@TEST7-15'  ,'test数据库语法查询测试！TEST7-16xaa' ,'010101010101011010101010101010101010','010101010101011010101010101010101010');
INSERT INTO test_blob_clob_rel.data1 VALUES('1980-02-10 01:48:22.501+00:00',8 ,-30003 ,30000003   ,-300000000003       ,33472098.11312001      ,39009810.333011921         ,false,'1970-01-01 01:16:05.476+00:00','test数据库语法查询测试！！！@TEST8-8' ,'test数据库语法查询测试！！！@TEST8-9' ,'test数据库语法查询测试！！！@TEST8-10' ,'t'  ,'test数据库语法查询测试！！！@TEST8-12' ,'中' ,'test数据库语法查询测试！！！@TEST8-14'  ,'test数据库语法查询测试！！！@TEST8-15'  ,'test数据库语法查询测试！TEST8-16xaa' ,'test数据库语法查询测试！TEST8-16xaa' ,'test数据库语法查询测试！TEST8-16xaa' );
INSERT INTO test_blob_clob_rel.data1 VALUES('2001-12-09 09:48:12.3+00:00'  ,9 ,null   ,null       ,null                ,null                   ,null                       ,null ,null                           ,null                                   ,null                                   ,null                                    ,null ,null                                    ,null ,null                                     ,null                                     ,null                                  ,null                                  ,null                                  );
INSERT INTO test_blob_clob_rel.data1 VALUES('2002-2-22 10:48:12.899'       ,10,32767  ,-2147483648,9223372036854775807 ,-99999999991.9999999991,9999999999991.999999999991 ,true ,'2020-10-01 12:00:01+00:00'    ,'test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t'  ,'test数据库语法查询测试！！！@TEST10-12','中' ,'test数据库语法查询测试！！！@TEST10-14' ,'test数据库语法查询测试！！！@TEST10-15' ,'test数据库语法查询测试！TEST10-16xaa','110101010101011010101010101010101010','110101010101011010101010101010101010');
INSERT INTO test_blob_clob_rel.data1 VALUES('2003-10-1 11:48:12.1'         ,11,-32768 ,2147483647 ,-9223372036854775808,99999999991.9999999991 ,-9999999999991.999999999991,false,'1970-11-25 09:23:07.421+00:00','test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t'  ,'test数据库语法查询测试！！！@TEST11-12','中' ,'test数据库语法查询测试！！！@TEST11-14' ,'test数据库语法查询测试！！！@TEST11-15' ,'test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！TEST11-16xaa');
INSERT INTO test_blob_clob_rel.data1 VALUES('2004-09-09 00:00:00.9+00:00'  ,12,12000  ,12000000   ,120000000000        ,-12000021.003125       ,-122209810.1131921         ,true ,'2129-3-1 12:00:00.011'        ,'aaaaaabbbbbbcccccc'                   ,'aaaaaabbbbbbcccccc'                   ,'aaaaaabbbbbbcccccc'                    ,'t'  ,'aaaaaabbbbbbcccccc'                    ,'z'  ,'aaaaaabbbbbbcccccc'                     ,'aaaaaabbbbbbcccccc'                     ,'aaaaaabbbbbbcccccc'                  ,'aaaaaabbbbbbcccccc'                  ,'aaaaaabbbbbbcccccc'                  );
INSERT INTO test_blob_clob_rel.data1 VALUES('2004-12-31 12:10:10.911+00:00',13,23000  ,23000000   ,230000000000        ,-23000088.665120604    ,-122209810.1131921         ,true ,'2020-12-31 23:59:59.999'      ,'SSSSSSDDDDDDKKKKKK'                   ,'SSSSSSDDDDDDKKKKKK'                   ,'SSSSSSDDDDDDKKKKKK'                    ,'T'  ,'SSSSSSDDDDDDKKKKKK'                    ,'B'  ,'SSSSSSDDDDDDKKKKKK'                     ,'SSSSSSDDDDDDKKKKKK'                     ,'SSSSSSDDDDDDKKKKKK'                  ,'SSSSSSDDDDDDKKKKKK'                  ,'SSSSSSDDDDDDKKKKKK'                  );
INSERT INTO test_blob_clob_rel.data1 VALUES('2008-2-29 2:10:10.111'        ,14,32767  ,34000000   ,340000000000        ,-43000079.07812032     ,-122209810.1131921         ,true ,'1975-3-11 00:00:00.0'         ,'1234567890987654321'                  ,'1234567890987654321'                  ,'1234567890987654321'                   ,'1'  ,'1234567890987654321'                   ,'2'  ,'1234567890987654321'                    ,'1234567890987654321'                    ,'1234567890987654321'                 ,'1234567890987654321'                 ,'1234567890987654321'                 );
INSERT INTO test_blob_clob_rel.data1 VALUES('2012-02-29 1:10:10.000'       ,15,-32767 ,-34000000  ,-340000000000       ,43000079.07812032      ,122209810.1131921          ,true ,'2099-9-1 11:01:00.111'        ,'数据库语法查询测试'                   ,'数据库语法查询测试'                   ,'数据库语法查询测试'                    ,'1'  ,'数据库语法查询测试'                    ,'2'  ,'数据库语法查询测试'                     ,'数据库语法查询测试'                     ,'数据库语法查询测试'                  ,'数据库语法查询测试'                  ,'数据库语法查询测试'                  );

---'*/

---查询校验：
SELECT * FROM test_blob_clob_rel.data1 ORDER BY id;
SELECT * FROM test_blob_clob_rel.data1 ORDER BY e17;
SELECT * FROM test_blob_clob_rel.data1 ORDER BY e18;

---清理测试数据
DROP TABLE test_blob_clob_rel.data1 CASCADE;
DROP TABLE IF EXISTS test_blob_clob_rel.data1 CASCADE;
CREATE TABLE test_blob_clob_rel.data1(
                                         tp  TIMESTAMPTZ NOT NULL,
                                         id  INT NOT NULL        ,
                                         e1  INT2                ,
                                         e2  INT,
                                         e3  INT8,
                                         e4  FLOAT4,
                                         e5  FLOAT8,
                                         e6  BOOL,
                                         e7  TIMESTAMP,
                                         e8  CHAR(1023),
                                         e9  NCHAR(255),
                                         e10 VARCHAR(4096),
                                         e11 CHAR,
                                         e12 NVARCHAR(4096),
                                         e13 NCHAR,
                                         e14 VARBYTES,
                                         e15 VARCHAR,
                                         e16 NVARCHAR,
                                         e17 BLOB,
                                         e18 CLOB);


INSERT INTO test_blob_clob_rel.data1 VALUES('1970-01-01 00:00:00+00:00'    ,1 ,0      ,0          ,0                   ,0                      ,0                          ,true ,'1970-01-01 00:00:00+00:00'    ,''                                     ,''                                     ,''                                      ,''   ,''                                      ,''   ,''                                       ,''                                       ,''                                    ,''                                    ,''                                    );
INSERT INTO test_blob_clob_rel.data1 VALUES('1970-1-1 00:00:00.001'        ,2 ,0      ,0          ,0                   ,0                      ,0                          ,true ,'1970-01-01 00:16:39.999+00:00','          '                           ,'          '                           ,'          '                            ,' '  ,'          '                            ,' '  ,' '                                      ,'          '                             ,'          '                          ,'          '                          ,'          '                          );
INSERT INTO test_blob_clob_rel.data1 VALUES('1976-10-20 12:00:12.123+00:00',3 ,10001  ,10000001   ,100000000001        ,-1047200.00312001      ,-1109810.113011921         ,true ,'2021-03-01 12:00:00.909+00:00','test数据库语法查询测试！！！@TEST3-8' ,'test数据库语法查询测试！！！@TEST3-9' ,'test数据库语法查询测试！！！@TEST3-10' ,'t'  ,'test数据库语法查询测试！！！@TEST3-12' ,'中' ,'test数据库语法查询测试！！！@TEST3-14'  ,'test数据库语法查询测试！！！@TEST3-15'  ,'test数据库语法查询测试！TEST3-16xaa' ,'test数据库语法查询测试！TEST3-16xaa' ,'test数据库语法查询测试！TEST3-16xaa' );
INSERT INTO test_blob_clob_rel.data1 VALUES('1979-2-28 11:59:01.999'       ,4 ,20002  ,20000002   ,200000000002        ,-20873209.0220322201   ,-22012110.113011921        ,false,'1970-01-01 00:00:00.123+00:00','test数据库语法查询测试！！！@TEST4-8' ,'test数据库语法查询测试！！！@TEST4-9' ,'test数据库语法查询测试！！！@TEST4-10' ,'t'  ,'test数据库语法查询测试！！！@TEST4-12' ,'中' ,'test数据库语法查询测试！！！@TEST4-14'  ,'test数据库语法查询测试！！！@TEST4-15'  ,'test数据库语法查询测试！TEST4-16xaa' ,'test数据库语法查询测试！TEST4-16xaa' ,'test数据库语法查询测试！TEST4-16xaa' );
INSERT INTO test_blob_clob_rel.data1 VALUES('1980-01-31 19:01:01+00:00'    ,5 ,30003  ,30000003   ,300000000003        ,-33472098.11312001     ,-39009810.333011921        ,true ,'2015-3-12 10:00:00.234'       ,'test数据库语法查询测试！！！@TEST5-8' ,'test数据库语法查询测试！！！@TEST5-9' ,'test数据库语法查询测试！！！@TEST5-10' ,'t'  ,'test数据库语法查询测试！！！@TEST5-12' ,'中' ,'test数据库语法查询测试！！！@TEST5-14'  ,'test数据库语法查询测试！！！@TEST5-15'  ,'test数据库语法查询测试！TEST5-16xaa' ,'test数据库语法查询测试！TEST5-16xaa' ,'test数据库语法查询测试！TEST5-16xaa' );
INSERT INTO test_blob_clob_rel.data1 VALUES('1980-02-10 01:14:51.09+00:00' ,6 ,-10001 ,10000001   ,-100000000001       ,1047200.00312001       ,1109810.113011921          ,false,'2023-6-23 05:00:00.55'        ,'test数据库语法查询测试！！！@TEST6-8' ,'test数据库语法查询测试！！！@TEST6-9' ,'test数据库语法查询测试！！！@TEST6-10' ,'t'  ,'test数据库语法查询测试！！！@TEST6-12' ,'中' ,'test数据库语法查询测试！！！@TEST6-14'  ,'test数据库语法查询测试！！！@TEST6-15'  ,'test数据库语法查询测试！TEST6-16xaa' ,'xaaxbbxcc'                           ,'xaaxbbxcc'                           );
INSERT INTO test_blob_clob_rel.data1 VALUES('1980-02-10 01:48:11.029+00:00',7 ,-20002 ,20000002   ,-200000000002       ,20873209.0220322201    ,22012110.113011921         ,true ,'2016-07-17 20:12:00.12+00:00' ,'test数据库语法查询测试！！！@TEST7-8' ,'test数据库语法查询测试！！！@TEST7-9' ,'test数据库语法查询测试！！！@TEST7-10' ,'t'  ,'test数据库语法查询测试！！！@TEST7-12' ,'中' ,'test数据库语法查询测试！！！@TEST7-14'  ,'test数据库语法查询测试！！！@TEST7-15'  ,'test数据库语法查询测试！TEST7-16xaa' ,'010101010101011010101010101010101010','010101010101011010101010101010101010');
INSERT INTO test_blob_clob_rel.data1 VALUES('1980-02-10 01:48:22.501+00:00',8 ,-30003 ,30000003   ,-300000000003       ,33472098.11312001      ,39009810.333011921         ,false,'1970-01-01 01:16:05.476+00:00','test数据库语法查询测试！！！@TEST8-8' ,'test数据库语法查询测试！！！@TEST8-9' ,'test数据库语法查询测试！！！@TEST8-10' ,'t'  ,'test数据库语法查询测试！！！@TEST8-12' ,'中' ,'test数据库语法查询测试！！！@TEST8-14'  ,'test数据库语法查询测试！！！@TEST8-15'  ,'test数据库语法查询测试！TEST8-16xaa' ,'test数据库语法查询测试！TEST8-16xaa' ,'test数据库语法查询测试！TEST8-16xaa' );
INSERT INTO test_blob_clob_rel.data1 VALUES('2001-12-09 09:48:12.3+00:00'  ,9 ,null   ,null       ,null                ,null                   ,null                       ,null ,null                           ,null                                   ,null                                   ,null                                    ,null ,null                                    ,null ,null                                     ,null                                     ,null                                  ,null                                  ,null                                  );
INSERT INTO test_blob_clob_rel.data1 VALUES('2002-2-22 10:48:12.899'       ,10,32767  ,-2147483648,9223372036854775807 ,-99999999991.9999999991,9999999999991.999999999991 ,true ,'2020-10-01 12:00:01+00:00'    ,'test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t'  ,'test数据库语法查询测试！！！@TEST10-12','中' ,'test数据库语法查询测试！！！@TEST10-14' ,'test数据库语法查询测试！！！@TEST10-15' ,'test数据库语法查询测试！TEST10-16xaa','110101010101011010101010101010101010','110101010101011010101010101010101010');
INSERT INTO test_blob_clob_rel.data1 VALUES('2003-10-1 11:48:12.1'         ,11,-32768 ,2147483647 ,-9223372036854775808,99999999991.9999999991 ,-9999999999991.999999999991,false,'1970-11-25 09:23:07.421+00:00','test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t'  ,'test数据库语法查询测试！！！@TEST11-12','中' ,'test数据库语法查询测试！！！@TEST11-14' ,'test数据库语法查询测试！！！@TEST11-15' ,'test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！TEST11-16xaa');
INSERT INTO test_blob_clob_rel.data1 VALUES('2004-09-09 00:00:00.9+00:00'  ,12,12000  ,12000000   ,120000000000        ,-12000021.003125       ,-122209810.1131921         ,true ,'2129-3-1 12:00:00.011'        ,'aaaaaabbbbbbcccccc'                   ,'aaaaaabbbbbbcccccc'                   ,'aaaaaabbbbbbcccccc'                    ,'t'  ,'aaaaaabbbbbbcccccc'                    ,'z'  ,'aaaaaabbbbbbcccccc'                     ,'aaaaaabbbbbbcccccc'                     ,'aaaaaabbbbbbcccccc'                  ,'aaaaaabbbbbbcccccc'                  ,'aaaaaabbbbbbcccccc'                  );
INSERT INTO test_blob_clob_rel.data1 VALUES('2004-12-31 12:10:10.911+00:00',13,23000  ,23000000   ,230000000000        ,-23000088.665120604    ,-122209810.1131921         ,true ,'2020-12-31 23:59:59.999'      ,'SSSSSSDDDDDDKKKKKK'                   ,'SSSSSSDDDDDDKKKKKK'                   ,'SSSSSSDDDDDDKKKKKK'                    ,'T'  ,'SSSSSSDDDDDDKKKKKK'                    ,'B'  ,'SSSSSSDDDDDDKKKKKK'                     ,'SSSSSSDDDDDDKKKKKK'                     ,'SSSSSSDDDDDDKKKKKK'                  ,'SSSSSSDDDDDDKKKKKK'                  ,'SSSSSSDDDDDDKKKKKK'                  );
INSERT INTO test_blob_clob_rel.data1 VALUES('2008-2-29 2:10:10.111'        ,14,32767  ,34000000   ,340000000000        ,-43000079.07812032     ,-122209810.1131921         ,true ,'1975-3-11 00:00:00.0'         ,'1234567890987654321'                  ,'1234567890987654321'                  ,'1234567890987654321'                   ,'1'  ,'1234567890987654321'                   ,'2'  ,'1234567890987654321'                    ,'1234567890987654321'                    ,'1234567890987654321'                 ,'1234567890987654321'                 ,'1234567890987654321'                 );
INSERT INTO test_blob_clob_rel.data1 VALUES('2012-02-29 1:10:10.000'       ,15,-32767 ,-34000000  ,-340000000000       ,43000079.07812032      ,122209810.1131921          ,true ,'2099-9-1 11:01:00.111'        ,'数据库语法查询测试'                   ,'数据库语法查询测试'                   ,'数据库语法查询测试'                    ,'1'  ,'数据库语法查询测试'                    ,'2'  ,'数据库语法查询测试'                     ,'数据库语法查询测试'                     ,'数据库语法查询测试'                  ,'数据库语法查询测试'                  ,'数据库语法查询测试'                  );

---'*/

---查询校验：
SELECT * FROM test_blob_clob_rel.data1 ORDER BY id;
SELECT * FROM test_blob_clob_rel.data1 ORDER BY e18;

---清理测试数据
DROP TABLE test_blob_clob_rel.data1 CASCADE;

DROP database test_blob_clob_rel CASCADE;