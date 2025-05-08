DROP DATABASE IF EXISTS test_select_last_add cascade;
CREATE ts DATABASE test_select_last_add;

DROP TABLE IF EXISTS test_select_last_add.testlastnew;
CREATE TABLE test_select_last_add.testlastnew(
                                                 k_timestamp TIMESTAMPTZ NOT NULL,
                                                 id INT NOT NULL,
                                                 e1 CHAR(1023),
                                                 e2 NCHAR(255),
                                                 e3 VARCHAR(4096),
                                                 e4 NVARCHAR(4096))
    ATTRIBUTES (
            idp INT2 NOT NULL,
			code1 VARCHAR(128),
            code3 CHAR(1023),
            code4 NCHAR(254))
PRIMARY TAGS(idp);

INSERT INTO test_select_last_add.testlastnew VALUES(1,1,null,'1','1','1',1,null,'1','1');
INSERT INTO test_select_last_add.testlastnew VALUES(2,2,null,null,'2','2',2,null,null,'2');

explain SELECT last(*,'1970-01-01 00:00:00.000') FROM test_select_last_add.testlastnew ;
SELECT last(*,'1970-01-01 00:00:00.000') FROM test_select_last_add.testlastnew ;
DROP DATABASE test_select_last_add cascade;

DROP DATABASE IF EXISTS t1 cascade;
create ts database t1;
create table t1.d1(k_timestamp timestamptz not null ,e1 bigint  not null, e2 char(20) not null , e3 timestamp  not null , e4 int not null, e5 smallint not null, e6 float not null, e7 bigint not null, e8 smallint not null, e9 float  not null, e10 double not null )  tags(t1_d1 int not null ) primary tags(t1_d1);
INSERT INTO t1.d1  VALUES (1667590000000, 444444444, 'a', 1667597776000, 98, 1, 499.999, 111111111, 10, 10.10, 0.0001,0);
INSERT INTO t1.d1  VALUES  (1667591000000,111111111, 'b', 1667597777111, 100, 1, 99.999, 2147483648, 100, -2000.2022, -700000.707077,1);
INSERT INTO t1.d1  VALUES   (1667592000000, 222222222, 'c', 1667597778112, 99, 1, 299.999, 111111111, 6351, 10.10, 2147483646.6789,2);
INSERT INTO t1.d1  VALUES (1667592010000, 333333333, 'd', 1667597779000, 98, 1, 55.999, 2222222, 12240, 10.100003, 32766.222,3);
INSERT INTO t1.d1  VALUES (1667592600000, 333333333, 'd', 1667597779000, 98, 1, 20.999, 1234567, 210, 1435345.10, 0.0001,4);
select time_bucket_gapfill(k_timestamp,300),interpolate(last(e1),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
select time_bucket_gapfill(k_timestamp,300),interpolate(last(e4),'null') from t1.d1 group by time_bucket_gapfill(k_timestamp,300) order by time_bucket_gapfill(k_timestamp,300);
DROP DATABASE t1 cascade;



DROP DATABASE IF EXISTS test_select_last_add cascade;
CREATE ts DATABASE test_select_last_add;
CREATE TABLE test_select_last_add.t1(                k_timestamp TIMESTAMPTZ NOT NULL,                id INT NOT NULL,                e1 INT2,                e2 INT,                e3 INT8,                e4 FLOAT4,                e5 FLOAT8,                e6 BOOL,                e7 TIMESTAMPTZ,                e8 CHAR(1023),                e9 NCHAR(255),                e10 VARCHAR(4096),                e11 CHAR,                e12 CHAR(255),                e13 NCHAR,                e14 NVARCHAR(4096),                e15 VARCHAR(1023),                 e16 NVARCHAR(200),                e17 NCHAR(255),                e18 CHAR(200),                           e19 VARBYTES,                e20 VARBYTES(60),                e21 VARCHAR,                e22 NVARCHAR) ATTRIBUTES (            code1 INT2 NOT NULL,code2 INT,code3 INT8,            code4 FLOAT4 ,code5 FLOAT8,            code6 BOOL,            code7 VARCHAR,code8 VARCHAR(128) NOT NULL,            code9 VARBYTES,code10 VARBYTES(60),            code11 VARCHAR,code12 VARCHAR(60),            code13 CHAR(2),code14 CHAR(1023) NOT NULL,            code15 NCHAR,code16 NCHAR(254) NOT NULL) PRIMARY TAGS(code1,code14,code8,code16);
INSERT INTO test_select_last_add.t1 VALUES(-62167219200000,1,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');
SELECT last(code1,'0000-01-01 00:00:00')  LE FROM test_select_last_add.t1;

SELECT last(k_timestamp,'297000000-1-1') FROM test_select_last_add.t1 group by id;
SELECT last(k_timestamp,'test') FROM test_select_last_add.t1 group by id;

DROP DATABASE test_select_last_add cascade;
