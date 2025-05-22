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
    ATTRIBUTES (code1 INT2 NOT NULL,code2 INT,code3 INT8,code4 FLOAT4 ,code5 FLOAT8,code6 BOOL,code7 VARCHAR,code8 VARCHAR(128) NOT NULL,code9 VARBYTES,code10 VARBYTES(60),code11 VARCHAR,code12 VARCHAR(60),code13 CHAR(2),code14 CHAR(1023) NOT NULL,code15 NCHAR,code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code1,code14,code8,code16);
INSERT INTO test_select_subquery.t1 VALUES(0,1,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');


CREATE TABLE test_select_subquery.t2(
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
    ATTRIBUTES (code1 INT2 NOT NULL,code2 INT,code3 INT8,code4 FLOAT4 ,code5 FLOAT8,code6 BOOL,code7 VARCHAR,code8 VARCHAR(128) NOT NULL,code9 VARBYTES,code10 VARBYTES(60),code11 VARCHAR,code12 VARCHAR(60),code13 CHAR(2),code14 CHAR(1023) NOT NULL,code15 NCHAR,code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code1,code14,code8,code16);

SELECT e9 FROM test_select_subquery.t1 GROUP BY e9 HAVING min(e9)>=(select min(e9) from test_select_subquery.t2 WHERE id=9);

explain SELECT e9 FROM test_select_subquery.t1 GROUP BY e9 HAVING min(e9)>=(select min(e9) from test_select_subquery.t2 WHERE id=9);

DROP DATABASE IF exists test_select_subquery cascade;