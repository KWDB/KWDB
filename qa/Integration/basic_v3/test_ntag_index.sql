create ts database test_ntag_hash;
use test_ntag_hash;
create table test_ntag_hash.t1(
                                  k_timestamp timestamptz not null,
                                  e1 int2  not null,
                                  e2 int
) ATTRIBUTES (code0 int2 not null,code1 int2, code2 int4, code3 int8, code4 int8 ,code5 int8, val1 float4,val2 float8, location VARCHAR, color VARCHAR(128) not null,age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code0);
--单列索引：code2和code3分别建立索引，code2插入数据之前创建索引，code3插入之后创建索引后，查询数据是否正确
--1)创建索引后插入数据
CREATE INDEX index_1 ON t1(code1);
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:33.123',0,0,  100,100,200,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:33.223',20002,1000002,  200, 200,300,600,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:33.323',0,1,  300,200,300,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:33.423',20002,1000002,  500, 200,600,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:33.523',0,2,  600,300,300,300,600,700,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:33.623',20002,1000002,  700, 200,600,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:33.723',0,3,  800,300,300,300,600,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:33.823',20002,1000002,  900, 100,200,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:33.923',0,4,  901,200,300,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:35.923',20002,1000002,  902, 100,600,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:36.923',0,5,  903,200,300,300,300,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:37.223',20002,1000002,  907, 100,200,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:38.123',0,6,  905,300,300,300,600,700,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:39.223',20002,1000002,  906, 200,600,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:51.123',0,6,  908,200,300,300,600,700,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t1 VALUES('2024-2-21 16:32:51.223',20002,1000002,  909, 200,600,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');

SHOW INDEXES FROM t1;
--基本功能验证
CREATE INDEX index_2 ON t1(code2); --测试创建已有数据的索引
SHOW INDEXES FROM t1;
-- 基本查询
select * from t1 where code1 = 200 order by code0;
select * from t1 where code1 = 200 or code1 = 300 order by code0;
select * from t1 where code1 IN(200,300) order by code0;
select * from t1 where code2 = 600 order by code0;
select * from t1 where code3 = 600 order by code0;
select * from t1 where code1 = 200 or code2 = 600 order by code0;
select * from t1 where code1 = 200 and code2 = 600 order by code0;
-- 查询ptag
select code0 from t1 where code2 = 600 order by code0;
select code0 from t1 where code2 IN(300,500,700) order by code0;
-- 存在不适用tag索引的查询
select * from t1 where code1 = 200 and code3 = 600 order by code0;
select * from t1 where code1 = 200 or code3 = 600 order by code0;
explain select * from t1 where code1 = 200 and code3 = 600;
explain select * from t1 where code1 = 200 or code3 = 600;
-- 删除索引
DROP INDEX index_1;
SHOW INDEXES FROM t1;
-- 更改索引名字
ALTER INDEX index_2 RENAME TO index_3;
SHOW INDEXES FROM t1;
ALTER INDEX index_3 RENAME TO index_2;
SHOW INDEXES FROM t1;
CREATE INDEX index_1 ON t1(code1);
SHOW INDEXES FROM t1;
--删除
DELETE FROM t1 where code0 = 100;
select * from t1 where code1 = 100 order by code0;
DELETE FROM t1 where code0 = 901;
select * from t1 where code1 = 200 order by code0;
DELETE FROM t1 where code0 = 903;
select * from t1 where code2 = 300 order by code0;

--更新索引数据
--通过ptag查询更新普通tag的值
UPDATE t1 SET code1=600 where code0=900;
select * from t1 where code1 = 600 order by code0;
UPDATE t1 SET code2=600 where code0=900;
select * from t1 where code2 = 600 order by code0;
select * from t1 where code1 = 600 order by code0;

-- code0：primary tag
-- code1，code2，code3，code4，code5：普通tag
-- e1,e2,e3：普通列
-- code1：建立索引index1
-- code2：建立索引index2
-- code3：建立索引index3
-- code4，code5：建立索引index4
-- code1，code2，code3：建立索引index5
CREATE INDEX index_3 ON t1(code3);
CREATE INDEX index_4 ON t1(code4,code5);
CREATE INDEX index_5 ON t1(code1,code2,code3);
SHOW INDEXES FROM t1;
--SELECT
select e1,code1 from t1 where code1 = 100 and e1 > 1;
select e1,code1 from t1 where code1 > 100 and code4 = 400 and code5 = 500 and e1 > 1 order by code1;
select e1,code1,code0 from t1 where code1 > 100 and code4 = 400 and code5 = 500 and e1 > 1 order by code0;
select e1,code1 from t1 where code1 = 100 and code2 = 200 and code4 = 400 and code5 = 500 and e1 > 1;
select e1,code1,code0 from t1 where code1 = 100 and code2 = 200 and code4 = 400 and code5 = 500 and e1 > 1;
select e1,code1,code0 from t1 where code1 = 100 and code2 = 200 and code4 = 400 and code5 = 500 and code0=907 and e1 > 1;
select e1,code1 from t1 where code1 = 100 and code0 > 100 and e1 > 1;
select e1,code1 from t1 where code1 = 100 and code0 = 100 and e1 > 1;
select e1,code1 from t1 where code1 = 100 or e1 > 1 order by code1;
select e1,code1 from t1 where code1 = 100 or code2 = 100;
select e1,code1,code0 from t1 where code1 = 100 or code2 = 100 or code4 = 400 or code5 = 500 order by code0;
select e1,code1 from t1 where code1 = 100 or code2 = 100  or code0 = 600 order by code1;
select e1,code1 from t1 where (code1 = 100 or code2 = 100) and (code1 = 200 or code3 = 300);
select e1,code1 from t1 where (code1 = 100 or code0 = 100) and (code2 = 200 or code0 = 300);
select e1,code1 from t1 where (code1 = 100 or code2 = 100) and code1 = 200;
select e1,code1 from t1 where (code1 = 100 or code2 = 100) and code3 = 300;
select e1,code1 from t1 where (code1 = 100 or code2 = 100) and e1 > 100;
select e1,code1 from t1 where (code1 = 100 or code2 = 100) and (code1 = 200 or e1 > 100);
select e1,code1 from t1 where (code1 = 100 or code2 = 100) and (code1 = 200 or code0 = 300);

--DELETE
DELETE FROM t1 where code0 = 905;
select * from t1 where code2 = 300 order by code0;
DELETE FROM t1 where code0 = 908;
select * from t1 where code4=600 and code5 = 700 order by code0;
DELETE FROM t1 where code0 = 907;
select * from t1 where code1=100 and code2 = 200 and code3 = 300;
select * from t1 where code1 = 100 order by code0;

--UPDATE
UPDATE t1 set code1 = 600 where code0 = 906;
select * from t1 where code1 = 600 order by code0;
select * from t1 where code2 = 600 order by code0;
select * from t1 where code3 = 300 order by code0;
select * from t1 where code1 = 600 and code2 = 600 and code3 = 300 order by code0;
select * from t1 where code4 = 500 and code5 = 600 order by code0;
UPDATE t1 set code4 = 600 where code0 = 902;
select * from t1 where code1 = 100 order by code0;
select * from t1 where code2 = 600 order by code0;
select * from t1 where code3 = 300 order by code0;
select * from t1 where code1 = 100 and code2 = 600 and code3 = 300;
select * from t1 where code4 = 600 and code5 = 500 order by code0;
drop table t1;

--更改版本后查询:新增tag列
create table test_ntag_hash.t2(
                                  k_timestamp timestamptz not null,
                                  e1 int2  not null,
                                  e2 int
) ATTRIBUTES (code0 int2 not null,code1 int2, code2 int4, code3 int8, code4 int8 ,code5 int8, val1 float4,val2 float8, location VARCHAR, color VARCHAR(128) not null,age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code0);
--单列索引：code2和code3分别建立索引，code2插入数据之前创建索引，code3插入之后创建索引后，查询数据是否正确
CREATE INDEX index_1 ON t2(code1);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:33.123',0,0,  100,100,200,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:33.223',20002,1000002,  200, 200,300,600,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:33.323',0,1,  300,200,300,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:33.423',20002,1000002,  500, 200,600,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:33.523',0,2,  600,300,300,300,600,700,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:33.623',20002,1000002,  700, 200,600,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
CREATE INDEX index_2 ON t2(code2);
SHOW INDEXES FROM t2;
show tags from t2;

ALTER TABLE t2 ADD TAG code9 INT2;
show create table t2;
show columns from t2;
show tags from t2;
SHOW INDEXES FROM t2;
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:33.723',0,3,  800,300,300,300,600,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',1);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:33.823',20002,1000002,  900, 100,200,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',2);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:33.923',0,4,  901,200,300,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',3);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:35.923',20002,1000002,  902, 100,600,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',4);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:36.923',0,5,  903,200,300,300,300,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',5);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:37.223',20002,1000002,  907, 100,200,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',6);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:38.123',0,6,  905,300,300,300,600,700,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',7);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:39.223',20002,1000002,  906, 200,600,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',8);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:51.123',0,6,  908,200,300,300,600,700,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',9);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:51.223',20002,1000002,  909, 200,600,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',10);

-- 基本查询
select * from t2 order by code0;
select * from t2 where code1 = 200 order by code0;
select * from t2 where code1 = 200 or code1 = 300 order by code0;
select * from t2 where code1 IN(200,300) order by code0;
select * from t2 where code2 = 600 order by code0;
select * from t2 where code3 = 600 order by code0;
select * from t2 where code1 = 200 or code2 = 600 order by code0;
select * from t2 where code1 = 200 and code2 = 600 order by code0;
-- 查询ptag
select code0 from t2 where code2 = 600 order by code0;
select code0 from t2 where code2 IN(300,500,700) order by code0;
select * from t2 where code1 = 200 and code3 = 600 order by code0;
select * from t2 where code1 = 200 or code3 = 600 order by code0;
explain select * from t2 where code1 = 200 and code3 = 600;
explain select * from t2 where code1 = 200 or code3 = 600;
-- 删除索引
DROP INDEX index_1;
SHOW INDEXES FROM t2;
-- 更改索引名字
ALTER INDEX index_2 RENAME TO index_3;
SHOW INDEXES FROM t2;
ALTER INDEX index_3 RENAME TO index_2;
SHOW INDEXES FROM t2;
CREATE INDEX index_1 ON t2(code1);
SHOW INDEXES FROM t2;

SHOW INDEXES FROM t2;
--SELECT
select e1,code1 from t2 where code1 = 100 and e1 > 1;
select e1,code1 from t2 where code1 > 100 and code4 = 400 and code5 = 500 and e1 > 1;
select e1,code1,code0 from t2 where code1 > 100 and code4 = 400 and code5 = 500 and e1 > 1 order by code0;
select e1,code1 from t2 where code1 = 100 and code2 = 200 and code4 = 400 and code5 = 500 and e1 > 1;
select e1,code1,code0 from t2 where code1 = 100 and code2 = 200 and code4 = 400 and code5 = 500 and e1 > 1 order by code0;
select e1,code1,code0 from t2 where code1 = 100 and code2 = 200 and code4 = 400 and code5 = 500 and code0=907 and e1 > 1;
select e1,code1 from t2 where code1 = 100 and code0 > 100 and e1 > 1;
select e1,code1 from t2 where code1 = 100 and code0 = 100 and e1 > 1;
select e1,code1,code0 from t2 where code1 = 100 or e1 > 1 order by code0;
select e1,code1 from t2 where code1 = 100 or code2 = 100 order by e1;
select e1,code1,code0 from t2 where code1 = 100 or code2 = 100 or code4 = 400 or code5 = 500 order by code0;
select e1,code1,code0 from t2 where code1 = 100 or code2 = 100  or code0 = 600 order by code0;
select e1,code1,code0 from t2 where (code1 = 100 or code2 = 100) and (code1 = 200 or code3 = 300) order by code0;
select e1,code1,code0 from t2 where (code1 = 100 or code0 = 100) and (code2 = 200 or code0 = 300) order by code0;
select e1,code1 from t2 where (code1 = 100 or code2 = 100) and code1 = 200;
select e1,code1,code0 from t2 where (code1 = 100 or code2 = 100) and code3 = 300 order by code0;
select e1,code1 from t2 where (code1 = 100 or code2 = 100) and e1 > 100;
select e1,code1 from t2 where (code1 = 100 or code2 = 100) and (code1 = 200 or e1 > 100);
select e1,code1 from t2 where (code1 = 100 or code2 = 100) and (code1 = 200 or code0 = 300);

--新增metrics列，tag无变化，验证索引的正确性
alter table t2 add if not exists e3 int8;
SHOW INDEXES FROM t2;
show create table t2;
show columns from t2;
show tags from t2;
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-22 16:32:33.923',0,3,1,  201,200,300,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',1);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-22 16:32:35.923',20002,1000002,2,  202, 200,600,300,700,800,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',2);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-22 16:32:36.923',0,5,3,  203,200,300,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',3);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-22 16:32:37.223',20002,1000002,5,  205, 100,200,300,400,500,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',4);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-22 16:32:38.123',0,6,6,  206,200,300,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',5);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-22 16:32:39.223',20002,1000002,7,  207, 200,600,300,700,800,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',6);
--基本功能验证
SHOW INDEXES FROM t2;
-- 基本查询
select * from t2 where code1 = 200 order by code0;
select * from t2 where code1 = 200 or code1 = 300 order by code0;
select * from t2 where code1 IN(200,300) order by code0;
select * from t2 where code2 = 600 order by code0;
select * from t2 where code3 = 600 order by code0;
select * from t2 where code1 = 200 or code2 = 600 order by code0;
select * from t2 where code1 = 200 and code2 = 600 order by code0;
-- 查询ptag
select code0 from t2 where code2 = 600 order by code0;
select code0 from t2 where code2 IN(300,500,700) order by code0;
select * from t2 where code1 = 200 and code3 = 600 order by code0;
select * from t2 where code1 = 200 or code3 = 600 order by code0;
explain select * from t2 where code1 = 200 and code3 = 600;
explain select * from t2 where code1 = 200 or code3 = 600;
-- 删除索引
DROP INDEX index_1;
SHOW INDEXES FROM t2;
-- 更改索引名字
ALTER INDEX index_2 RENAME TO index_3;
SHOW INDEXES FROM t2;
ALTER INDEX index_3 RENAME TO index_2;
SHOW INDEXES FROM t2;
CREATE INDEX index_1 ON t2(code1);
SHOW INDEXES FROM t2;

SHOW INDEXES FROM t2;
--SELECT
select e1,code1 from t2 where code1 = 100 and e1 > 1;
select e1,code1 from t2 where code1 > 100 and code4 = 400 and code5 = 500 and e1 > 1;
select e1,code1,code0 from t2 where code1 > 100 and code4 = 400 and code5 = 500 and e1 > 1 order by code0;
select e1,code1 from t2 where code1 = 100 and code2 = 200 and code4 = 400 and code5 = 500 and e1 > 1;
select e1,code1,code0 from t2 where code1 = 100 and code2 = 200 and code4 = 400 and code5 = 500 and e1 > 1 order by code0;
select e1,code1,code0 from t2 where code1 = 100 and code2 = 200 and code4 = 400 and code5 = 500 and code0=907 and e1 > 1;
select e1,code1 from t2 where code1 = 100 and code0 > 100 and e1 > 1 order by code0;
select e1,code1 from t2 where code1 = 100 and code0 = 100 and e1 > 1;
select e1,code1,code0 from t2 where code1 = 100 or e1 > 1 order by code0;
select e1,code1,code2,code0 from t2 where code1 = 100 or code2 = 100 order by code0;
select e1,code1,code0 from t2 where code1 = 100 or code2 = 100 or code4 = 400 or code5 = 500 order by code0;
select e1,code1,code0 from t2 where code1 = 100 or code2 = 100  or code0 = 600 order by code0;
select e1,code1,code0 from t2 where (code1 = 100 or code2 = 100) and (code1 = 200 or code3 = 300) order by code0;
select e1,code1,code0 from t2 where (code1 = 100 or code0 = 100) and (code2 = 200 or code0 = 300) order by code0;
select e1,code1 from t2 where (code1 = 100 or code2 = 100) and code1 = 200;
select e1,code1,code0 from t2 where (code1 = 100 or code2 = 100) and code3 = 300 order by code0;
select e1,code1 from t2 where (code1 = 100 or code2 = 100) and e1 > 100;
select e1,code1 from t2 where (code1 = 100 or code2 = 100) and (code1 = 200 or e1 > 100);
select e1,code1 from t2 where (code1 = 100 or code2 = 100) and (code1 = 200 or code0 = 300);

--删除tag列
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-22 16:32:33.923',0,3,1,  201,200,300,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',1);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-22 16:32:35.923',20002,1000002,2,  202, 200,600,300,700,800,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',2);
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-22 16:32:36.923',0,5,3,  203,200,300,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely',3);
select * from t2 where code9 = 1 order by code0;
SHOW INDEXES FROM t2;
DROP INDEX index_x;
SHOW INDEXES FROM t2;

ALTER TABLE t2 DROP TAG code9;
SHOW INDEXES FROM t2;
show create table t2;
show columns from t2;
show tags from t2;
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:38.123',0,6,2,  305,200,300,300,600,700,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:39.223',20002,1000002,3,  306, 200,600,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
-- 基本查询
select * from t2 order by code0;
select * from t2 where code1 = 200 order by code0;
select * from t2 where code1 = 200 or code1 = 300 order by code0;
select * from t2 where code1 IN(200,300) order by code0;
select * from t2 where code2 = 600 order by code0;
select * from t2 where code3 = 600 order by code0;
select * from t2 where code1 = 200 or code2 = 600 order by code0;
select * from t2 where code1 = 200 and code2 = 600 order by code0;
-- 查询ptag
select code0 from t2 where code2 = 600 order by code0;
select code0 from t2 where code2 IN(300,500,700) order by code0;
-- 存在不适用tag索引的查询
select * from t2 where code1 = 200 and code3 = 600 order by code0;
select * from t2 where code1 = 200 or code3 = 600 order by code0;
explain select * from t2 where code1 = 200 and code3 = 600;
explain select * from t2 where code1 = 200 or code3 = 600;
-- 删除索引
DROP INDEX index_1;
SHOW INDEXES FROM t2;
-- 更改索引名字
ALTER INDEX index_2 RENAME TO index_3;
SHOW INDEXES FROM t2;
ALTER INDEX index_3 RENAME TO index_2;
SHOW INDEXES FROM t2;
CREATE INDEX index_1 ON t2(code1);
SHOW INDEXES FROM t2;

SHOW INDEXES FROM t2;
--SELECT
select e1,code1 from t2 where code1 = 100 and e1 > 1;
select e1,code1 from t2 where code1 > 100 and code4 = 400 and code5 = 500 and e1 > 1;
select e1,code1,code0 from t2 where code1 > 100 and code4 = 400 and code5 = 500 and e1 > 1 order by code0;
select e1,code1 from t2 where code1 = 100 and code2 = 200 and code4 = 400 and code5 = 500 and e1 > 1;
select e1,code1,code0 from t2 where code1 = 100 and code2 = 200 and code4 = 400 and code5 = 500 and e1 > 1 order by code0;
select e1,code1,code0 from t2 where code1 = 100 and code2 = 200 and code4 = 400 and code5 = 500 and code0=907 and e1 > 1;
select e1,code1 from t2 where code1 = 100 and code0 > 100 and e1 > 1 order by code0;
select e1,code1 from t2 where code1 = 100 and code0 = 100 and e1 > 1 order by code0;
select e1,code1,code0 from t2 where code1 = 100 or e1 > 1 order by code0;
select e1,code1,code2,code0 from t2 where code1 = 100 or code2 = 100 order by code0;
select e1,code1,code0 from t2 where code1 = 100 or code2 = 100 or code4 = 400 or code5 = 500 order by code0;
select e1,code1,code0 from t2 where code1 = 100 or code2 = 100 or code0 = 600 order by code0;
select e1,code1,code0 from t2 where (code1 = 100 or code2 = 100) and (code1 = 200 or code3 = 300) order by code0;
select e1,code1,code0 from t2 where (code1 = 100 or code0 = 100) and (code2 = 200 or code0 = 300) order by code0;
select e1,code1 from t2 where (code1 = 100 or code2 = 100) and code1 = 200;
select e1,code1,code0 from t2 where (code1 = 100 or code2 = 100) and code3 = 300 order by code0;
select e1,code1,code0 from t2 where (code1 = 100 or code2 = 100) and e1 > 100 order by code0;
select e1,code1 from t2 where (code1 = 100 or code2 = 100) and (code1 = 200 or e1 > 100);
select e1,code1 from t2 where (code1 = 100 or code2 = 100) and (code1 = 200 or code0 = 300);

CREATE INDEX index_3 ON t2(code3);
CREATE INDEX index_4 ON t2(val1);
CREATE INDEX index_5 ON t2(val2);
CREATE INDEX index_6 ON t2(age);
CREATE INDEX index_7 ON t2(sex);
CREATE INDEX index_8 ON t2(type);
SHOW INDEXES FROM t2;
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:38.123',0,6,2,  705,200,300,300,600,700,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
INSERT INTO test_ntag_hash.t2 VALUES('2024-2-21 16:32:39.223',20002,1000002,3,  706, 200,600,300,500,600,100.0,200.0,'beijing','red','2','社会性别女','1','cuteandlovely');
select * from t2 where code3 = 600 order by code0;
select * from t2 where val1 = 100.0 order by code0;
select * from t2 where val2 = 200.0 order by code0;
select * from t2 where age = '2' order by code0;
select * from t2 where sex = '社会性别女' order by code0;
select * from t2 where type = 'cuteandlovely' order by code0;
DROP INDEX index_1;
DROP INDEX index_2;
DROP INDEX index_3;
DROP INDEX index_4;
DROP INDEX index_5;
DROP INDEX index_6;
DROP INDEX index_7;
DROP INDEX index_8;
SHOW INDEXES FROM t2;
-- 多次创建并删除索引
CREATE INDEX index_1 ON t2(code1);
SHOW INDEXES FROM t2;
select * from t2 where code1 = 200 order by code0;
select * from t2 where code1 = 200 and code2 = 200 order by code0;
select * from t2 where code1 = 200 and code0 = 100 order by code0;
DROP INDEX index_1;
SHOW INDEXES FROM t2;
CREATE INDEX index_1 ON t2(code1);
SHOW INDEXES FROM t2;
select * from t2 where code1 = 200 order by code0;
select * from t2 where code1 = 200 and code2 = 200 order by code0;
select * from t2 where code1 = 200 and code0 = 100 order by code0;
DROP INDEX index_1;
SHOW INDEXES FROM t2;
CREATE INDEX index_1 ON t2(code1);
SHOW INDEXES FROM t2;
select * from t2 where code1 = 200 order by code0;
select * from t2 where code1 = 200 and code2 = 200 order by code0;
select * from t2 where code1 = 200 and code0 = 100 and e1 >1;
DROP INDEX index_1;
SHOW INDEXES FROM t2;

--多次更新索引，功能正常
CREATE INDEX index_1 ON t2(code1);
SHOW INDEXES FROM t2;
select * from t2 where code1 = 200 order by code0;
select * from t2 where code1 = 200 and code2 = 300 order by code0;
ALTER INDEX index_1 RENAME TO index_2;
SHOW INDEXES FROM t2;
select * from t2 where code1 = 200 order by code0;
select * from t2 where code1 = 200 and code2 = 300 order by code0;
ALTER INDEX index_2 RENAME TO index_3;
select * from t2 where code1 = 200 order by code0;
select * from t2 where code1 = 200 and code2 = 300 order by code0;
SHOW INDEXES FROM t2;
CREATE INDEX index_1 ON t2(code1);
SHOW INDEXES FROM t2;
select * from t2 where code1 = 200 order by code0;
select * from t2 where code1 = 200 and code2 = 300 order by code0;
ALTER INDEX index_1 RENAME TO index_2;
SHOW INDEXES FROM t2;
select * from t2 where code1 = 200 order by code0;
select * from t2 where code1 = 200 and code2 = 300 order by code0;
ALTER INDEX index_2 RENAME TO index_3;
select * from t2 where code1 = 200 order by code0;
select * from t2 where code1 = 200 and code2 = 300 order by code0;
DROP INDEX index_3;
SHOW INDEXES FROM t2;

export into csv "nodelocal://1/ntag/t2" from table test_ntag_hash.t2;
drop table t2;
import table create using 'nodelocal://1/ntag/t2/meta.sql' csv data ('nodelocal://1/ntag/t2');
SHOW INDEXES FROM t2;
select * from t2 where code1 = 100 order by code0;
explain select * from t2 where code1 = 100;
drop table test_ntag_hash.t2;

CREATE TABLE test_ntag_hash.t3(k_timestamp timestamptz NOT NULL, e1 int2 ) ATTRIBUTES (code1 int NOT NULL,code2 int NOT NULL,t_int2_1 int2, t_int4_1 int, t_int8_1 int8, t_float4_1 float4, t_float8_1 float8, t_bool_1 bool, t_charn_1 char(1023), t_ncharn_1 nchar(254),t_char_1 char, t_nchar_1 nchar,t_varcharn_1 varchar(256), t_varchar_1 varchar, t_varbytesn_1 varbytes(256),t_varbytes_1 varbytes) primary tags(code1,code2) activetime 10s;
ALTER TABLE t3 ADD TAG code3 INT2;
ALTER TABLE t3 ADD TAG code5 INT2;
INSERT INTO test_ntag_hash.t3 (k_timestamp,e1, code1, code2,code3,code5, t_int2_1,t_int4_1,t_int8_1,t_float4_1,t_float8_1,t_bool_1,t_charn_1,t_ncharn_1,t_char_1,t_nchar_1,t_varcharn_1,t_varchar_1,t_varbytesn_1,
                               t_varbytes_1) VALUES ('2024-10-21 01:30:00.123',1,1002,1002,1,1,1,1,1,1.1,1.1,true,'cn','ncn','c','n','vcn','vc','vbn','b');
CREATE INDEX index_1 ON test_ntag_hash.t3(t_int4_1,t_int8_1,code3,code5);
INSERT INTO test_ntag_hash.t3 (k_timestamp,e1, code1, code2,code3,code5, t_int2_1,t_int4_1,t_int8_1,t_float4_1,t_float8_1,t_bool_1,t_charn_1,t_ncharn_1,t_char_1,t_nchar_1,t_varcharn_1,t_varchar_1,t_varbytesn_1,t_varbytes_1) VALUES ('2024-10-21 01:30:00.123',1,1003,1003,1,1,1,1,1,1.1,1.1,true,'cn','ncn','c','n','vcn','vc','vbn','b');
select * from t3 where t_int4_1=1 and t_int8_1=1 and code3=1 and code5=1 order by code1;
select * from t3 where code5=1 order by code1;

drop index index_1;
ALTER TABLE t3 DROP TAG code5;
ALTER TABLE t3 ADD TAG code5 INT2;
INSERT INTO test_ntag_hash.t3 (k_timestamp,e1, code1, code2,code3,code5, t_int2_1,t_int4_1,t_int8_1,t_float4_1,t_float8_1,t_bool_1,t_charn_1,t_ncharn_1,t_char_1,t_nchar_1,t_varcharn_1,t_varchar_1,t_varbytesn_1,t_varbytes_1) VALUES ('2024-10-21 01:30:00.123',1,1006,1006,3,5,2,4,8,1.1,1.1,true,'cn','ncn','c','n','vcn','vc','vbn','b');
CREATE INDEX index_2 ON test_ntag_hash.t3(t_int8_1,code3,code5);
explain select * from t3 where t_int8_1=8 and code3=3 and code5=5;
select * from t3 where t_int8_1=8 and code3=3 and code5=5;

use defaultdb;
drop database test_ntag_hash cascade;