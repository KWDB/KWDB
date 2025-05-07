drop database test1 CASCADE;
create ts database test1;
CREATE TABLE test1.t1(k_timestamp TIMESTAMP not null, a int, b int) TAGS (tag1 int not null,tag2 int) PRIMARY TAGS (tag1);
insert into test1.t1 values('2024-07-08 07:17:22.988', 1, 2,2,2);
insert into test1.t1 values('2024-07-08 07:17:30.745', 11, 2,21,2);
insert into test1.t1 values('2024-07-08 07:17:34.781', 21, 21,31,2);
insert into test1.t1 values('2024-07-08 07:17:39.178', 31, 21,33,3);
insert into test1.t1 values('2024-07-08 07:17:42.866', 41, 41,43,23);
insert into test1.t1 values('2024-07-09 07:17:42.866', 410, 410,43,230);

select * from test1.t1 order by k_timestamp;
select last(a) from test1.t1;
SELECT last(a, '2024-07-08 07:17:42.866'), last(b, '2024-07-08 07:17:42.866') FROM test1.t1;
SELECT last(a, '2024-07-08 07:17:41.866'), last(b, '2024-07-07 07:17:42.866') FROM test1.t1;
SELECT sum(tag1),count(a),last(a, '2024-07-07 07:17:41.866'), last(b, '2024-07-09 07:17:42.866') FROM test1.t1;
SELECT first(a),first(tag2),last(tag1),count(a),last(a, '2024-07-07 07:17:41.866'), last(b, '2024-07-09 07:17:42.866') FROM test1.t1;
select last(*) from test1.t1;
SELECT first(tag1),last(a,'2024-07-08 07:17:28.247'),first(b),last(b,'2024-07-08 07:17:32.047')  FROM test1.t1;
SELECT last(a, '2024-07-08 07:17:42.866'), last(b, '2024-07-08 07:17:42.866') FROM test1.t1 where k_timestamp < '2024-07-08 07:17:39.178';

SELECT max(a),max(tag1),count(a),last(a, '2024-07-07 07:17:41.866'), last(b, '2024-07-09 07:17:42.866') FROM test1.t1;

select last(a,'2024-07-11 07:17:39.178+00:00'),last(tag1,'2024-07-09 07:17:43.178+00:00'),tag1 from test1.t1 group by tag1 order by tag1;

CREATE TABLE test1.t2(k_timestamp TIMESTAMP not null, a int, b varchar(30)) TAGS (tag1 varchar(30) not null,tag2 int) PRIMARY TAGS (tag1);
insert into test1.t2 values('2024-07-08 07:17:47.718', 1, '2','2',2);
insert into test1.t2 values('2024-07-08 07:17:51.918', 11, '2','21',2);
insert into test1.t2 values('2024-07-08 07:17:58.26', 21, '21','31',2);
insert into test1.t2 values('2024-07-08 07:18:02.775', 31, '21','33',3);
insert into test1.t2 values('2024-07-08 07:18:06.833', 41, '41','43',23);
select * from test1.t2  order by k_timestamp;
select last(*) from test1.t2;
SELECT first(tag1),last(a,'2024-07-08 07:18:02.247'),count(tag2),last(tag1),last(tag2,'2024-07-18 07:17:51.047'),count(tag1),last(b,'2024-07-18 07:18:40.047')  FROM test1.t2;
SELECT last(a,'2024-07-08 07:18:06.833')  FROM test1.t2;
SELECT last(a,'2024-07-08 07:18:02.247'),last(tag1),last(tag2,'2024-07-18 07:17:51.047'),count(tag1)  FROM test1.t2;
use defaultdb;
drop database test1 CASCADE;

DROP DATABASE test_select_last_add cascade;
CREATE ts DATABASE test_select_last_add;
-----------建表t1
CREATE TABLE test_select_last_add.t1(
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
INSERT INTO test_select_last_add.t1 VALUES(0,1,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');
INSERT INTO test_select_last_add.t1 VALUES(1,2,0,0,0,0,0,true,999999,' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',' ',0,0,0,0,0,TRUE,' ',' ',' ',' ',' ',' ',' ',' ',' ',' ');
SELECT   last(code1,'0000-01-01 00:00:00')  LE FROM test_select_last_add.t1;
INSERT INTO test_select_last_add.t1 VALUES(-62167219200000,0,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');
SELECT   last(code1,'0000-01-01 00:00:00')  LE FROM test_select_last_add.t1;
DROP DATABASE test_select_last_add cascade;

