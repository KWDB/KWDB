--test comment
							
USE defaultdb;DROP DATABASE IF EXISTS test_comment;
CREATE TS DATABASE test_comment;
CREATE TABLE test_comment.t1(
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
---insert
INSERT INTO test_comment.t1 VALUES('1976-10-20 12:00:12.123',3,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');

COMMENT ON TABLE test_comment.t1 IS '《!!database TEST中文英文TABLE!!》';

---Add comment to a certain item and delete it.
---Whether the insert and delete is affected
---k_timestamp
COMMENT ON COLUMN test_comment.t1.k_timestamp IS '《!!database TEST中文英文COLUMN-k_timestamp!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN k_timestamp;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SELECT * FROM test_comment.t1 ORDER BY id;

------e1
COMMENT ON COLUMN test_comment.t1.e1 IS '《!!database TEST中文英文COLUMN-e1!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e1;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;

------e2
COMMENT ON COLUMN test_comment.t1.e2 IS '《!!database TEST中文英文COLUMN-e2!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e2;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e3
COMMENT ON COLUMN test_comment.t1.e3 IS '《!!database TEST中文英文COLUMN-e3!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e3;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e4
COMMENT ON COLUMN test_comment.t1.e4 IS '《!!database TEST中文英文COLUMN-e4!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e4;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e5
COMMENT ON COLUMN test_comment.t1.e5 IS '《!!database TEST中文英文COLUMN-e5!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e5;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e6
COMMENT ON COLUMN test_comment.t1.e6 IS '《!!database TEST中文英文COLUMN-e6!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e6;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e7
COMMENT ON COLUMN test_comment.t1.e7 IS '《!!database TEST中文英文COLUMN-e7!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e7;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e8
COMMENT ON COLUMN test_comment.t1.e8 IS '《!!database TEST中文英文COLUMN-e8!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e8;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e9
COMMENT ON COLUMN test_comment.t1.e9 IS '《!!database TEST中文英文COLUMN-e9!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e9;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e10
COMMENT ON COLUMN test_comment.t1.e10 IS '《!!database TEST中文英文COLUMN-e10!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e10;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e11
COMMENT ON COLUMN test_comment.t1.e11 IS '《!!database TEST中文英文COLUMN-e11!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e11;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e12
COMMENT ON COLUMN test_comment.t1.e12 IS '《!!database TEST中文英文COLUMN-e12!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e12;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e13
COMMENT ON COLUMN test_comment.t1.e13 IS '《!!database TEST中文英文COLUMN-e13!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e13;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e14
COMMENT ON COLUMN test_comment.t1.e14 IS '《!!database TEST中文英文COLUMN-e14!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e14;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e15
COMMENT ON COLUMN test_comment.t1.e15 IS '《!!database TEST中文英文COLUMN-e15!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e15;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e16
COMMENT ON COLUMN test_comment.t1.e16 IS '《!!database TEST中文英文COLUMN-e16!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e16;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e17
COMMENT ON COLUMN test_comment.t1.e17 IS '《!!database TEST中文英文COLUMN-e17!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e17;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e18
COMMENT ON COLUMN test_comment.t1.e18 IS '《!!database TEST中文英文COLUMN-e18!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e18;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e19
COMMENT ON COLUMN test_comment.t1.e19 IS '《!!database TEST中文英文COLUMN-e19!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e19;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e20
COMMENT ON COLUMN test_comment.t1.e20 IS '《!!database TEST中文英文COLUMN-e20!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e20;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e21
COMMENT ON COLUMN test_comment.t1.e21 IS '《!!database TEST中文英文COLUMN-e21!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e21;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------e22
COMMENT ON COLUMN test_comment.t1.e22 IS '《!!database TEST中文英文COLUMN-e22!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP COLUMN e22;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;

------code1
COMMENT ON COLUMN test_comment.t1.code1 IS '《!!database TEST中文英文COLUMN-code1!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code1;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code2
COMMENT ON COLUMN test_comment.t1.code2 IS '《!!database TEST中文英文COLUMN-code2!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code2;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code3
COMMENT ON COLUMN test_comment.t1.code3 IS '《!!database TEST中文英文COLUMN-code3!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code3;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code4
COMMENT ON COLUMN test_comment.t1.code4 IS '《!!database TEST中文英文COLUMN-code4!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code4;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code5
COMMENT ON COLUMN test_comment.t1.code5 IS '《!!database TEST中文英文COLUMN-code5!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code5;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code6
COMMENT ON COLUMN test_comment.t1.code6 IS '《!!database TEST中文英文COLUMN-code6!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code6;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code7
COMMENT ON COLUMN test_comment.t1.code7 IS '《!!database TEST中文英文COLUMN-code7!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code7;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code8
COMMENT ON COLUMN test_comment.t1.code8 IS '《!!database TEST中文英文COLUMN-code8!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code8;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code9
COMMENT ON COLUMN test_comment.t1.code9 IS '《!!database TEST中文英文COLUMN-code9!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code9;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code10
COMMENT ON COLUMN test_comment.t1.code10 IS '《!!database TEST中文英文COLUMN-code10!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code10;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code11
COMMENT ON COLUMN test_comment.t1.code11 IS '《!!database TEST中文英文COLUMN-code11!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code11;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code12
COMMENT ON COLUMN test_comment.t1.code12 IS '《!!database TEST中文英文COLUMN-code12!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code12;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code13
COMMENT ON COLUMN test_comment.t1.code13 IS '《!!database TEST中文英文COLUMN-code13!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code13;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code14
COMMENT ON COLUMN test_comment.t1.code14 IS '《!!database TEST中文英文COLUMN-code14!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code14;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code15
COMMENT ON COLUMN test_comment.t1.code15 IS '《!!database TEST中文英文COLUMN-code15!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code15;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
------code16
COMMENT ON COLUMN test_comment.t1.code16 IS '《!!database TEST中文英文COLUMN-code16!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE test_comment.t1 DROP TAG code16;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM test_comment.t1 WITH COMMENT] ORDER BY column_name;

SHOW CREATE TABLE test_comment.t1;

USE defaultdb;DROP DATABASE IF EXISTS test_comment;

create database db1 comment '《!!database TEST中文英文COLUMN-code16!!》';
create ts database tsdb1 comment '《!!database TEST中文英文COLUMN-code16!!》';
select "comment" from [show databases with comment] where database_name='db1' or database_name='tsdb1';

create table db1.t1(a int comment 'aaa' comment 'aaa');

create table db1.t1(a int not null comment 't1.a', b string comment='t1.b') comment 't1';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM db1.t1 WITH COMMENT] ORDER BY column_name;
SET sql_safe_updates = FALSE;ALTER TABLE db1.t1 DROP column b;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM db1.t1 WITH COMMENT] ORDER BY column_name;
comment on column db1.t1.a is '《!!database TEST中文英文COLUMN-code16!!》';
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM db1.t1 WITH COMMENT] ORDER BY column_name;
comment on column db1.t1.a is null;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM db1.t1 WITH COMMENT] ORDER BY column_name;

CREATE TABLE tsdb1.t1(
                                k_timestamp TIMESTAMPTZ NOT NULL comment 't1.ts',
                                id INT NOT NULL comment 't1.id',
                                e1 INT2 comment 't1.col1',
                                e2 INT comment 't1.col2',
                                e3 INT8 comment 't1.col3',
                                e4 FLOAT4 comment 't1.col4',
                                e5 FLOAT8 comment 't1.col5',
                                e6 BOOL comment 't1.col6',
                                e7 TIMESTAMPTZ comment 't1.col7',
                                e8 CHAR(1023) comment 't1.col8',
                                e9 NCHAR(255) comment 't1.col9',
                                e10 VARCHAR(4096) comment 't1.col10',
                                e11 CHAR comment 't1.col11',
                                e12 CHAR(255) comment 't1.col12',
                                e13 NCHAR comment 't1.col13',
                                e14 NVARCHAR(4096) comment 't1.col14',
                                e15 VARCHAR(1023) comment 't1.col15',
                                e16 NVARCHAR(200) comment 't1.col16',
                                e17 NCHAR(255) comment 't1.col17',
                                e18 CHAR(200) comment 't1.col18',
                                e19 VARBYTES comment 't1.col19',
                                e20 VARBYTES(60) comment 't1.col20',
                                e21 VARCHAR comment 't1.col21',
                                e22 NVARCHAR comment 't1.col22')
    ATTRIBUTES (
            code1 INT2 NOT NULL comment 't1.tag1' ,code2 INT comment 't1.tag2',code3 INT8 comment 't1.tag3',
            code4 FLOAT4 comment 't1.tag4' ,code5 FLOAT8 comment 't1.tag5',
            code6 BOOL comment 't1.tag6',
            code7 VARCHAR comment 't1.tag7',code8 VARCHAR(128) NOT NULL comment 't1.tag8',
            code9 VARBYTES comment 't1.tag9',code10 VARBYTES(60) comment 't1.tag10',
            code11 VARCHAR comment 't1.tag11',code12 VARCHAR(60) comment 't1.tag12',
            code13 CHAR(2) comment 't1.tag13',code14 CHAR(1023) NOT NULL comment 't1.tag14',
            code15 NCHAR comment 't1.tag15',code16 NCHAR(254) NOT NULL comment 't1.tag16')
PRIMARY TAGS(code1,code14,code8,code16) comment 'tsdb1.t1';

show create table tsdb1.t1;
SELECT column_name,"comment" FROM [SHOW COLUMNS FROM tsdb1.t1 WITH COMMENT] ORDER BY column_name;

DROP DATABASE IF EXISTS db1;
DROP DATABASE IF EXISTS tsdb1;