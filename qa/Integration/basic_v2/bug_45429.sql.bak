SET CLUSTER SETTING ts.blocks_per_segment.max_limit=100;
SET CLUSTER SETTING ts.rows_per_block.max_limit=100;
USE defaultdb;
DROP DATABASE IF EXISTS test_SELECT_join cascade;
DROP DATABASE IF EXISTS test_SELECT_join_rel cascade;
CREATE ts DATABASE test_SELECT_join;
CREATE DATABASE test_SELECT_join_rel;
CREATE TABLE test_SELECT_join.t1(
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
                                    e20 varbytes(60),
                                    e21 VARCHAR,
                                    e22 NVARCHAR)
    ATTRIBUTES (
            code1 INT2 NOT NULL,code2 INT,code3 INT8,
            code4 FLOAT4 ,code5 FLOAT8,
            code6 BOOL,
            code7 VARCHAR,code8 VARCHAR(128) NOT NULL,
            code9 VARBYTES,code10 varbytes(60),
            code11 VARCHAR,code12 VARCHAR(60),
            code13 CHAR(2),code14 CHAR(1023) NOT NULL,
            code15 NCHAR,code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code1,code14,code8,code16);
CREATE TABLE test_SELECT_join_rel.t1(
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
                                        code15 NCHAR,code16 NCHAR(254) NOT NULL );
INSERT INTO test_select_join.t1 VALUES(0,1,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');
INSERT INTO test_select_join.t1 VALUES(318995291029,7,-20002,20000002,-200000000002,20873209.0220322201,22012110.113011921,true,'2016-7-17 20:12:00.12','test数据库语法查询测试！！！@TEST7-8','test数据库语法查询测试！！！@TEST7-9','test数据库语法查询测试！！！@TEST7-10','t','test数据库语法查询测试！！！@TEST7-12','中','test数据库语法查询测试！！！@TEST7-14','test数据库语法查询测试！！！@TEST7-15','test数据库语法查询测试！TEST7-16xaa','test数据库语法查询测试！！！@TEST7-17','test数据库语法查询测试！！！@TEST7-18',b'\xca','test数据库语法查询测试！！！@TEST7-20','test数据库语法查询测试！！！@TEST7-21','test数据库语法查询测试！！！@TEST7-22',-20002,20000002,-200000000002,555500.0055505,55505532.553015321,false,'test数据库语法查询测试！！！@TEST7-7','test数据库语法查询测试！！！@TEST7-8',b'\xee','test数据库语法查询测试！！！@TEST7-10','test数据库语法查询测试！！！@TEST7-11','test数据库语法查询测试！！！@TEST7-12','t3','test数据库语法查询测试！！！@TEST7-14','中','test数据库语法查询测试！！！@TEST7-16');
INSERT INTO test_select_join_rel.t1 VALUES('1970-01-01 00:00:00+00:00',1,0,0,0,0,0,true,'1970-01-01 00:16:39.999+00:00','','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');
INSERT INTO test_select_join_rel.t1 VALUES('1980-02-10 01:48:11.029+00:00',7,-20002,20000002,-200000000002,20873209.0220322201,22012110.113011921,true,'2016-07-17 20:12:00.12+00:00','test数据库语法查询测试！！！@TEST7-8','test数据库语法查询测试！！！@TEST7-9','test数据库语法查询测试！！！@TEST7-10','t','test数据库语法查询测试！！！@TEST7-12','中','test数据库语法查询测试！！！@TEST7-14','test数据库语法查询测试！！！@TEST7-15','test数据库语法查询测试！TEST7-16xaa','test数据库语法查询测试！！！@TEST7-17','test数据库语法查询测试！！！@TEST7-18',b'\xca','test数据库语法查询测试！！！@TEST7-20','test数据库语法查询测试！！！@TEST7-21','test数据库语法查询测试！！！@TEST7-22',-20002,20000002,-200000000002,555500.0055505,55505532.553015321,false,'test数据库语法查询测试！！！@TEST7-7','test数据库语法查询测试！！！@TEST7-8',b'\xee','test数据库语法查询测试！！！@TEST7-10','test数据库语法查询测试！！！@TEST7-11','test数据库语法查询测试！！！@TEST7-12','t3','test数据库语法查询测试！！！@TEST7-14','中','test数据库语法查询测试！！！@TEST7-16');
SELECT t1.code4, t11.code4 FROM test_select_join.t1 t1 JOIN test_select_join_rel.t1 t11 ON t1.code4 = t11.code4 order by t1.code4;
USE defaultdb;
DROP DATABASE IF EXISTS test_SELECT_join cascade;
DROP DATABASE IF EXISTS test_SELECT_join_rel cascade;