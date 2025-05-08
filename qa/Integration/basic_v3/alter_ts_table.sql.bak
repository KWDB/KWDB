drop database if exists test_alter cascade;
create ts database test_alter;

--basic test
create table test_alter.t1(ts timestamp not null, a int) tags(b int not null) primary tags(b);
insert into test_alter.t1 values(1672531201000, 111, 1);
insert into test_alter.t1 values(1672531202000, 222, 1);
insert into test_alter.t1 values(1672531203000, 333, 2);
insert into test_alter.t1 values(1672531204000, 444, 3);
insert into test_alter.t1 values(1672531205000, 555, 3);
select * from test_alter.t1 order by ts, b;

set sql_safe_updates = false;
SET CLUSTER SETTING ts.dedup.rule = 'merge';

alter table test_alter.t1 add column c int;
select * from test_alter.t1 order by ts, b;
alter table test_alter.t1 drop column c;
select * from test_alter.t1 order by ts, b;
alter table test_alter.t1 add column c int;
select * from test_alter.t1 order by ts, b;
insert into test_alter.t1 values(1672531205000, 556, NULL, 3);
insert into test_alter.t1 values(1672531206000, 666, 11, 1);
insert into test_alter.t1 values(1672531207000, 777, NULL, 2);
insert into test_alter.t1 values(1672531207000, NULL, 0, 2);
insert into test_alter.t1 values(1672531208000, 888, 33, 3);
select * from test_alter.t1 order by ts, b;

alter table test_alter.t1 add column d float;
select * from test_alter.t1 order by ts, b;
alter table test_alter.t1 drop column d;
select * from test_alter.t1 order by ts, b;
alter table test_alter.t1 add column d float;
select * from test_alter.t1 order by ts, b;

insert into test_alter.t1 values(1672531209000, 999, 44, 1.1, 1);
insert into test_alter.t1 values(1672531209000, NULL, 45, 2.2, 2);
insert into test_alter.t1 values(1672531209000, 1111, 46, NULL, 3);
insert into test_alter.t1 values(1672531209000, NULL, NULL, 4.4, 4);
select * from test_alter.t1 order by ts, b;

alter table test_alter.t1 add column e char(20);
select * from test_alter.t1 order by ts, b;
alter table test_alter.t1 drop column e;
select * from test_alter.t1 order by ts, b;
alter table test_alter.t1 add column e char(20);
select * from test_alter.t1 order by ts, b;

insert into test_alter.t1 values(1672531210000, 3333, 55, 5.5, 'chartest1', 1);
insert into test_alter.t1 values(1672531210000, 4444, 56, 6.6, 'chartest2', 2);
insert into test_alter.t1 values(1672531210000, 5555, 57, 7.7, 'chartest3', 3);
select * from test_alter.t1 order by ts, b;

alter table test_alter.t1 add column f varchar(100);
select * from test_alter.t1 order by ts, b;
alter table test_alter.t1 drop column f;
select * from test_alter.t1 order by ts, b;
select ts, a from test_alter.t1 order by ts, a;
alter table test_alter.t1 add column f varchar(100);
select * from test_alter.t1 order by ts, b;

insert into test_alter.t1 values(1672531211000, 6666, 66, 8.8, 'chartest1', 'aa', 1);
insert into test_alter.t1 values(1672531211000, 7777, 67, 9.9, 'chartest2', 'bbbbb', 2);
insert into test_alter.t1 values(1672531211000, 8888, 68, 0.1, 'chartest3', 'ccccccccccc', 3);
select * from test_alter.t1 order by ts, b;

-- ZDP-32174: add tag serial2 No error reported
create table test_alter.t2(
                            k_timestamp timestamp not null,
                            e1 int2
) tags (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float4, attr5 double, attr6 bool, attr11 char, attr12 char(254), attr13 nchar, attr14 nchar(254), attr15 varchar, attr16 varchar(1023)) primary tags(attr1);
alter table test_alter.t2 add tag a1 timestamp;
alter table test_alter.t2 add tag a2 timestamptz;
alter table test_alter.t2 add tag a3 bytea;
alter table test_alter.t2 add tag a4 blob;
alter table test_alter.t2 add tag a5 string;
alter table test_alter.t2 add tag a6 decimal;
alter table test_alter.t2 add tag a7 time;
alter table test_alter.t2 add tag a8 data;
alter table test_alter.t2 add tag a9 serial2;
show tags from test_alter.t2;
alter table test_alter.t2 drop tag a1;
alter table test_alter.t2 drop tag a2;
alter table test_alter.t2 drop tag a3;
alter table test_alter.t2 drop tag a4;
alter table test_alter.t2 drop tag a5;
alter table test_alter.t2 drop tag a6;
alter table test_alter.t2 drop tag a7;
alter table test_alter.t2 drop tag a8;
alter table test_alter.t2 drop tag a9;
show tags from test_alter.t2;

-- ZDP-32110: When the tag is named repeatedly, the error is "duplicate column name"
create table test_alter.t3(
                            k_timestamp timestamp not null,
                            e1 int2
) tags (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float4, attr5 double, attr6 bool, attr11 char, attr12 char(254), attr13 nchar, attr14 nchar(254), attr15 varchar, attr16 varchar(1023)) primary tags(attr1);
alter table test_alter.t3 add tag attr17_a1 smallint;
select pg_sleep(1);
alter table test_alter.t3 add tag attr17_a1 smallint;
select pg_sleep(1);
alter table test_alter.t3 drop tag attr17_a1;
select pg_sleep(1);
alter table test_alter.t3 add tag attr17_a1 smallint;
select pg_sleep(1);
alter table test_alter.t3 add tag attr17_a1 smallint;
select pg_sleep(1);

-- ZDP-32020
create table test_alter.t4(
                            k_timestamp timestamptz not null,
                            e1 int8,
                            e2 int4,
                            e3 int2,
                            e4 float4,
                            e5 float8,
                            e6 bool,
                            e7 timestamp,
                            e8 char(50),
                            e9 nchar(50),
                            e10 varchar(50),
                            e11 char,
                            e12 nchar,
                            e13 varchar,
                            e14 varbytes,
                            e15 varbytes(50),
                            e16 timestamptz
) tags (code1 int2 not null, code2 int, code3 int8, flag bool, val1 float, val2 float8, location varchar, color varchar(65536), age char, sex char(1023), year nchar, type nchar(254)) primary tags(code1);

alter table test_alter.t4 add tag attr17_a1 smallint;
alter table test_alter.t4 add tag attr18_a1 int;
alter table test_alter.t4 add tag attr19_a1 bigint;
alter table test_alter.t4 add tag attr20_a1 float4;
alter table test_alter.t4 add tag attr21_a1 double;
alter table test_alter.t4 add tag attr22_a1 bool;
alter table test_alter.t4 add tag attr25_a1 char;
alter table test_alter.t4 add tag attr26_a1 char(254);
alter table test_alter.t4 add tag attr27_a1 nchar;
alter table test_alter.t4 add tag attr28_a1 nchar(10);
alter table test_alter.t4 add tag attr29_a1 nchar(254);
alter table test_alter.t4 add tag attr30_a1 varchar;
alter table test_alter.t4 add tag attr31_a1 varchar(1023);
insert into test_alter.t4 (k_timestamp, e1, code1, attr17_a1, attr18_a1, attr19_a1, attr20_a1, attr21_a1, attr22_a1, attr25_a1, attr26_a1, attr27_a1, attr28_a1, attr29_a1, attr30_a1, attr31_a1) values ('2024-01-01t00:00:03+00:00',1,1, 1, 733, 969, 4593.82364066433, -3412.391887694972, false, 'y', 'e', 'x', 'w', 'r', 'f','l');
delete from test_alter.t4 where code1 = 1;
alter table test_alter.t4 add column if not exists c1 int8 null;
alter table test_alter.t4 add column if not exists c2 int4 null;
alter table test_alter.t4 add column if not exists c3 int2 null;
alter table test_alter.t4 add column if not exists c4 float4 null;
alter table test_alter.t4 add column if not exists c5 float8 null;
alter table test_alter.t4 add column if not exists c6 bool null;
alter table test_alter.t4 add column if not exists c7 timestamp null;
alter table test_alter.t4 add column if not exists c8 char(50) null;
alter table test_alter.t4 add column if not exists c9 nchar(50) null;
alter table test_alter.t4 add column if not exists c10 varchar(50) null;
alter table test_alter.t4 add column if not exists c11 char null;
alter table test_alter.t4 add column if not exists c12 nchar null;
alter table test_alter.t4 add column if not exists c13 varchar null;
alter table test_alter.t4 add column if not exists c14 varbytes null;
alter table test_alter.t4 add column if not exists c15 varbytes(50) null;
alter table test_alter.t4 add column if not exists c16 timestamptz null;
insert into test_alter.t4(k_timestamp, code1, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16) values ('2024-01-01t00:00:00+01:00', 2, 461, 849,123, -3623.3289748095276, -9555.69188725292, true, '2024-01-23 09:53:25', 'h', 't', 'y', 'o', 'b', 'k', 'e', '4', '2024-01-23 09:53:25');
insert into test_alter.t4 (k_timestamp, e1, code1, attr17_a1, attr18_a1, attr19_a1, attr20_a1, attr21_a1, attr22_a1, attr25_a1, attr26_a1, attr27_a1, attr28_a1, attr29_a1, attr30_a1, attr31_a1) values ('2024-01-01t00:00:03+00:00',1,1, 1, 733, 969, 4593.82364066433, -3412.391887694972, false, 'y', 'e', 'x', 'w', 'r', 'f','l');

-- ZDP-32018: No error reported for add column decimal
create table test_alter.t5(
                            k_timestamp timestamptz not null,
                            e1 int8,
                            e2 int4,
                            e3 int2,
                            e4 float4,
                            e5 float8,
                            e6 bool,
                            e7 timestamp,
                            e8 char(50),
                            e9 nchar(50),
                            e10 varchar(50),
                            e11 char,
                            e12 nchar,
                            e13 varchar,
                            e14 varbytes,
                            e15 varbytes(50),
                            e16 timestamptz
) tags (code1 int2 not null, code2 int, code3 int8, flag bool, val1 float, val2 float8, location varchar, color varchar(65536), age char, sex char(1023), year nchar, type nchar(254)) primary tags(code1);
alter table test_alter.t5 add column c1 decimal;

-- ZDP-31995: Show tag values error after inserting data
create table test_alter.t6(
                            k_timestamp timestamp not null,
                            e1 int2  not null
) tags (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float4, attr5 double, attr6 bool, attr11 char, attr12 char(254), attr13 nchar, attr14 nchar(254), attr15 varchar, attr16 varchar(1023)) primary tags(attr1);
alter table test_alter.t6 add tag attr27_a1 nchar;
show tag values from test_alter.t6;
alter table test_alter.t6 drop tag attr27_a1;
show tag values from test_alter.t6;
alter table test_alter.t6 add tag attr27_a1 nchar;
insert into test_alter.t6 (k_timestamp, e1, attr1, attr27_a1) values ('2018-10-10 10:00:00',1,2,'1');
show tag values from test_alter.t6;
alter table test_alter.t6 drop tag attr27_a1;
show tag values from test_alter.t6;

-- ZDP-32001: After adding fields, show tag values execution reported an error
create table test_alter.t7(
                            k_timestamp timestamptz not null,
                            e1 int8,
                            e2 int4,
                            e3 int2,
                            e4 float4,
                            e5 float8,
                            e6 bool,
                            e7 timestamp,
                            e8 char(50),
                            e9 nchar(50),
                            e10 varchar(50),
                            e11 char,
                            e12 nchar,
                            e13 varchar,
                            e14 varbytes,
                            e15 varbytes(50),
                            e16 timestamptz
) tags (code1 int2 not null, code2 int, code3 int8, flag bool, val1 float, val2 float8, location varchar, color varchar(65536), age char, sex char(1023), year nchar, type nchar(254)) primary tags(code1);
alter table test_alter.t7 add if not exists c1 int8;
show tag values from test_alter.t7;

-- ZDP-31999: After inserting a value into the tag, adding a field crashes
create table test_alter.t8(
                            k_timestamp timestamp not null,
                            e1 int2  not null
) tags (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float4, attr5 double, attr6 bool, attr11 char, attr12 char(254), attr13 nchar, attr14 nchar(254), attr15 varchar, attr16 varchar(1023)) primary tags(attr1);
insert into test_alter.t8 (k_timestamp, e1, attr1, attr2, attr3, attr4, attr5, attr6, attr11, attr12, attr13, attr14, attr15, attr16) values ('2024-01-01t00:00:00+00:00', 935, 1, 733, 969, 4593.82364066433, -3412.391887694972, false, 'y', 'e', 'x', 'w', 'r', 'f');
insert into test_alter.t8 (k_timestamp, e1, attr1, attr2, attr3, attr4, attr5, attr6, attr11, attr12, attr13, attr14, attr15, attr16) values ('2024-01-01t00:00:00+01:00', 935, 2, 733, 969, 4593.82364066433, -3412.391887694972, false, 'y', 'e', 'x', 'w', 'r', 'f');
insert into test_alter.t8 (k_timestamp, e1, attr1, attr2, attr3, attr4, attr5, attr6, attr11, attr12, attr13, attr14, attr15, attr16) values ('2024-01-01t00:00:00+02:00', 935, 3, 733, 969, 4593.82364066433, -3412.391887694972, false, 'y', 'e', 'x', 'w', 'r', 'f');
alter table test_alter.t8 add c1 int8 null;
alter table test_alter.t8 drop c1;
alter table test_alter.t8 add c1 int8 null;


-- ZDP-32772: Table level lifecycle has been set for the table, and column errors have been added
CREATE TABLE test_alter.newtable_2 (
	column1 timestamptz NOT NULL,
	column2 varchar NULL
) TAGS (
	 newtag1 varchar NOT NULL )
 PRIMARY TAGS (
	newtag1 )
 RETENTIONS 100d;
ALTER TABLE test_alter.newtable_2 ADD column4 varchar NULL;


-- ZDP-33096: Inserting data after repeatedly adding or deleting fields crashes
create table test_alter.t9(
                             k_timestamp timestamptz not null,
                             e1 int8,
                             e2 int4,
                             e3 int2,
                             e4 float4,
                             e5 float8,
                             e6 bool,
                             e7 timestamp,
                             e8 char(50),
                             e9 nchar(50),
                             e10 varchar(50),
                             e11 char,
                             e12 nchar,
                             e13 varchar,
                             e14 varbytes,
                             e15 varbytes(50),
                             e16 timestamptz
) tags (code1 int2 not null, code2 int, code3 int8, flag bool, val1 float, val2 float8, location varchar, color varchar(65536), age char, sex char(1023), year nchar, type nchar(254)) primary tags(code1);

set sql_safe_updates = false;

alter table test_alter.t9 drop column e1;
alter table test_alter.t9 drop column e3;
alter table test_alter.t9 drop column e4;
alter table test_alter.t9 drop column e5;
alter table test_alter.t9 drop column e6;
alter table test_alter.t9 drop column e7;
alter table test_alter.t9 drop column e8;
alter table test_alter.t9 drop column e9;
alter table test_alter.t9 drop column e10;
alter table test_alter.t9 drop column e11;
alter table test_alter.t9 drop column e12;
alter table test_alter.t9 drop column e13;
alter table test_alter.t9 drop column e14;
alter table test_alter.t9 drop column e15;
alter table test_alter.t9 drop column e16;

alter table test_alter.t9 add if not exists e1 int2;
alter table test_alter.t9 add if not exists e2 int8;
alter table test_alter.t9 add if not exists e3 int4;
alter table test_alter.t9 add if not exists e4 float8;
alter table test_alter.t9 add if not exists e5 float4;
alter table test_alter.t9 add if not exists e6 timestamp;
alter table test_alter.t9 add if not exists e7 bool;
alter table test_alter.t9 add if not exists e8 nchar(50);
alter table test_alter.t9 add if not exists e9 char(50);
alter table test_alter.t9 add if not exists e10 char;
alter table test_alter.t9 add if not exists e11 varchar(50);
alter table test_alter.t9 add if not exists e12 varchar;
alter table test_alter.t9 add if not exists e13 nchar;
alter table test_alter.t9 add if not exists e14 nvarchar(100);
alter table test_alter.t9 add if not exists e15 nvarchar;
alter table test_alter.t9 add if not exists e16 varbytes;
alter table test_alter.t9 add if not exists e17 varbytes(50);
alter table test_alter.t9 add if not exists e18 varbytes;
alter table test_alter.t9 add if not exists e19 varbytes(60);

alter table test_alter.t9 drop column e1;
alter table test_alter.t9 drop column e3;
alter table test_alter.t9 drop column e4;
alter table test_alter.t9 drop column e5;
alter table test_alter.t9 drop column e6;
alter table test_alter.t9 drop column e7;
alter table test_alter.t9 drop column e8;
alter table test_alter.t9 drop column e9;
alter table test_alter.t9 drop column e10;
alter table test_alter.t9 drop column e11;
alter table test_alter.t9 drop column e12;
alter table test_alter.t9 drop column e13;
alter table test_alter.t9 drop column e14;
alter table test_alter.t9 drop column e15;
alter table test_alter.t9 drop column e16;
alter table test_alter.t9 drop column e17;
alter table test_alter.t9 drop column e18;
alter table test_alter.t9 drop column e19;

alter table test_alter.t9 add if not exists e1 int2;
alter table test_alter.t9 add if not exists e2 int8;
alter table test_alter.t9 add if not exists e3 int4;
alter table test_alter.t9 add if not exists e4 float8;
alter table test_alter.t9 add if not exists e5 float4;
alter table test_alter.t9 add if not exists e6 timestamp;
alter table test_alter.t9 add if not exists e7 bool;
alter table test_alter.t9 add if not exists e8 nchar(50);
alter table test_alter.t9 add if not exists e9 char(50);
alter table test_alter.t9 add if not exists e10 char;
alter table test_alter.t9 add if not exists e11 varchar(50);
alter table test_alter.t9 add if not exists e12 varchar;
alter table test_alter.t9 add if not exists e13 nchar;
alter table test_alter.t9 add if not exists e14 nvarchar(100);
alter table test_alter.t9 add if not exists e15 nvarchar;
alter table test_alter.t9 add if not exists e16 varbytes;
alter table test_alter.t9 add if not exists e17 varbytes(50);
alter table test_alter.t9 add if not exists e18 varbytes;
alter table test_alter.t9 add if not exists e19 varbytes(60);

alter table test_alter.t9 drop column e1;
alter table test_alter.t9 drop column e3;
alter table test_alter.t9 drop column e4;
alter table test_alter.t9 drop column e5;
alter table test_alter.t9 drop column e6;
alter table test_alter.t9 drop column e7;
alter table test_alter.t9 drop column e8;
alter table test_alter.t9 drop column e9;
alter table test_alter.t9 drop column e10;
alter table test_alter.t9 drop column e11;
alter table test_alter.t9 drop column e12;
alter table test_alter.t9 drop column e13;
alter table test_alter.t9 drop column e14;
alter table test_alter.t9 drop column e15;
alter table test_alter.t9 drop column e16;
alter table test_alter.t9 drop column e17;
alter table test_alter.t9 drop column e18;
alter table test_alter.t9 drop column e19;

alter table test_alter.t9 add if not exists e1 int2;
alter table test_alter.t9 add if not exists e2 int8;
alter table test_alter.t9 add if not exists e3 int4;
alter table test_alter.t9 add if not exists e4 float8;
alter table test_alter.t9 add if not exists e5 float4;
alter table test_alter.t9 add if not exists e6 timestamp;
alter table test_alter.t9 add if not exists e7 bool;
alter table test_alter.t9 add if not exists e8 nchar(50);
alter table test_alter.t9 add if not exists e9 char(50);
alter table test_alter.t9 add if not exists e10 char;
alter table test_alter.t9 add if not exists e11 varchar(50);
alter table test_alter.t9 add if not exists e12 varchar;
alter table test_alter.t9 add if not exists e13 nchar;
alter table test_alter.t9 add if not exists e14 nvarchar(100);
alter table test_alter.t9 add if not exists e15 nvarchar;
alter table test_alter.t9 add if not exists e16 varbytes;
alter table test_alter.t9 add if not exists e17 varbytes(50);
alter table test_alter.t9 add if not exists e18 varbytes;
alter table test_alter.t9 add if not exists e19 varbytes(60);

alter table test_alter.t9 drop column e1;
alter table test_alter.t9 drop column e3;
alter table test_alter.t9 drop column e4;
alter table test_alter.t9 drop column e5;
alter table test_alter.t9 drop column e6;
alter table test_alter.t9 drop column e7;
alter table test_alter.t9 drop column e8;
alter table test_alter.t9 drop column e9;
alter table test_alter.t9 drop column e10;
alter table test_alter.t9 drop column e11;
alter table test_alter.t9 drop column e12;
alter table test_alter.t9 drop column e13;
alter table test_alter.t9 drop column e14;
alter table test_alter.t9 drop column e15;
alter table test_alter.t9 drop column e16;
alter table test_alter.t9 drop column e17;
alter table test_alter.t9 drop column e18;
alter table test_alter.t9 drop column e19;

insert into test_alter.t9(k_timestamp, e2, code1, code2) values(100023, 1, 1, 1);
insert into test_alter.t9(k_timestamp, e2, e14, e15, e16, code1, code2) values(100023, 1, '1', '1', '1', 1, 1);


-- ZDP-33457
CREATE TABLE test_alter.t10(
k_timestamp TIMESTAMPTZ not null,
 e1 INT8,
 e2 INT4,
 e3 INT2,
 e4 FLOAT4,
 e5 FLOAT8,
 e6 BOOL,
 e7 TIMESTAMP,
 e8 CHAR(50),
 e9 NCHAR(50),
 e10 VARCHAR(50),
 e11 CHAR,
 e12 NCHAR,
 e13 VARCHAR,
 e14 varbytes,
 e15 varbytes(50),
 e16 TIMESTAMPTZ
 ) tagS (code1 int2 not null,code2 int ,code3 int8 ,flag BOOL ,val1 float ,val2 float8 ,location VARCHAR ,color VARCHAR(65536) ,age CHAR ,sex CHAR(1023) ,year NCHAR ,type NCHAR(254) ) PRIMARY TAGS(code1);

INSERT INTO test_alter.t10 values (50000008,9223372036854775807,2147483647,32767,2.712882,3.14159267890796,true,50000011,'test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','中','test时间精度通用查询测试！！！@TEST1',b'\xaa',b'\xaabbccdd',50000011,4,2147483647,9223372036854775807,true,2.712882,3.14159267890796,'test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','1','test时间精度通用查询测试！！！@TEST1');
INSERT INTO test_alter.t10 values (50000009,-9223372036854775808,-2147483648,-32768,-2.712882,-3.14159267890796,true,50000011,'test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','中','test时间精度通用查询测试！！！@TEST1',b'\xaa',b'\xaabbccdd',50000011,5,2147483647,9223372036854775807,true,2.712882,3.14159267890796,'test时间精度通用查询测试！！！@TEST1','test时间精度通用查询测试！！！@TEST1','t','test时间精度通用查询测试！！！@TEST1','1','test时间精度通用查询测试！！！@TEST1');
INSERT INTO test_alter.t10 values (50000010,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,6,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

select * from test_alter.t10 where code1 = 6;

ALTER TABLE test_alter.t10 DROP COLUMN e1;
ALTER TABLE test_alter.t10 DROP COLUMN e3;
ALTER TABLE test_alter.t10 DROP COLUMN e4;
ALTER TABLE test_alter.t10 DROP COLUMN e5;
ALTER TABLE test_alter.t10 DROP COLUMN e6;
ALTER TABLE test_alter.t10 DROP COLUMN e7;
ALTER TABLE test_alter.t10 DROP COLUMN e8;

select * from test_alter.t10 where code1 = 6;

-- ZDP-34366
create table test_alter.t11(ts timestamp not null, a varchar(10), b int) tags(c int not null) primary tags(c);
insert into test_alter.t11 values(1672531201000, '111', 100, 1);

SET CLUSTER SETTING ts.dedup.rule = 'merge';
alter table test_alter.t11 add column d int;
alter table test_alter.t11 drop column d;
insert into test_alter.t11 values(1672531201000, NULL, 101, 1);
select * from test_alter.t11;

-- ZDP-34369
create table test_alter.t12(
k_timestamp timestamp not null,
e1 int2
) tagS (attr1 smallint not null,attr2 int ,attr3 bigint ,attr4 float4 ,attr5 double ,attr6 BOOL ,attr7 VARBYTES ,attr8 VARBYTES(1023) ,attr11 char ,attr12 char(254) ,attr13 nchar ,attr14 nchar(254) ,attr15 varchar , attr16 varchar(1023)) PRIMARY TAGS(attr1);
INSERT INTO test_alter.t12 (k_timestamp, e1,attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, attr11, attr12, attr13, attr14, attr15, attr16) VALUES ('2024-01-01T00:00:00+00:00', 935, 1, 733, 969, 4593.82364066433, -3412.391887694972, False, '9', '0', 'Y', 'e', 'x', 'W', 'r', 'F');
INSERT INTO test_alter.t12 (k_timestamp, e1,attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, attr11, attr12, attr13, attr14, attr15, attr16) VALUES ('2024-01-01T00:01:00+00:00', 935, 2, 733, 969, 4593.82364066433, -3412.391887694972, False, '9', '0', 'Y', 'e', 'x', 'W', 'r', 'F');
INSERT INTO test_alter.t12 (k_timestamp, e1,attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, attr11, attr12, attr13, attr14, attr15, attr16) VALUES ('2024-01-01T00:02:00+00:00', 935, 3, 733, 969, 4593.82364066433, -3412.391887694972, False, '9', '0', 'Y', 'e', 'x', 'W', 'r', 'F');
alter table test_alter.t12 add tag attr17_a1 smallint;
alter table test_alter.t12 add tag attr18_a1 int;
alter table test_alter.t12 add tag attr19_a1 bigint;
alter table test_alter.t12 add tag attr20_a1 float4;
alter table test_alter.t12 add tag attr21_a1 double;
alter table test_alter.t12 add tag attr22_a1 bool;
alter table test_alter.t12 add tag attr23_a1 VARBYTES;
alter table test_alter.t12 add tag attr24_a1 VARBYTES(10);
alter table test_alter.t12 add tag attr25_a1 char;
alter table test_alter.t12 add tag attr26_a1 char(254);
alter table test_alter.t12 add tag attr27_a1 nchar;
alter table test_alter.t12 add tag attr28_a1 nchar(10);
alter table test_alter.t12 add tag attr29_a1 nchar(254);
alter table test_alter.t12 add tag attr30_a1 varchar;
alter table test_alter.t12 add tag attr31_a1 varchar(1023);
select attr17_a1,attr18_a1,attr19_a1,attr20_a1,attr21_a1,attr22_a1,attr23_a1,attr24_a1,attr27_a1,attr28_a1,attr29_a1,attr30_a1,attr31_a1 from test_alter.t12 order by k_timestamp;

create table test_alter.t13(
k_timestamp timestamp not null,
e1 int2
) tags (attr1 smallint not null,attr2 varchar(32)) PRIMARY TAGS(attr1);;
insert into test_alter.t13 (k_timestamp, e1,attr1, attr2) VALUES('2024-01-01T00:00:00+00:00', 935, 1,'1111');
insert into test_alter.t13 (k_timestamp, e1,attr1) VALUES('2024-01-02T00:00:00+00:00', 935, 2);
insert into test_alter.t13 (k_timestamp, e1,attr1, attr2) VALUES('2024-01-03T00:00:00+00:00', 935, 3,'3333');
select * from test_alter.t13 order by k_timestamp,attr1;


-- ZDP-35837
create table test_alter.t14(
                k_timestamp timestamp not null,
                e1 int2 ) tagS (attr1 smallint not null,attr2 int ,attr3 bigint ,attr4 float4 ,attr5 double ,attr6 BOOL ,attr7 VARBYTES ,attr8 VARBYTES(1023) ,attr11 char ,attr12 char(254) ,attr13 nchar ,attr14 nchar(254) ,attr15 varchar , attr16 varchar(1023)) PRIMARY TAGS(attr1);
INSERT INTO test_alter.t14 (k_timestamp, e1,attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, attr11, attr12, attr13, attr14, attr15, attr16) VALUES ('2024-01-01T00:00:00+00:00', 935, 1, 733, 969, 4593.82364066433, -3412.391887694972, False, '9', '0', 'Y', 'e', 'x', 'W', 'r', 'F');
INSERT INTO test_alter.t14 (k_timestamp, e1,attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, attr11, attr12, attr13, attr14, attr15, attr16) VALUES ('2024-01-01T00:01:00+00:00', 935, 2, 733, 969, 4593.82364066433, -3412.391887694972, False, '9', '0', 'Y', 'e', 'x', 'W', 'r', 'F');
INSERT INTO test_alter.t14 (k_timestamp, e1,attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, attr11, attr12, attr13, attr14, attr15, attr16) VALUES ('2024-01-01T00:02:00+00:00', 935, 3, 733, 969, 4593.82364066433, -3412.391887694972, False, '9', '0', 'Y', 'e', 'x', 'W', 'r', 'F');

alter table test_alter.t14 add tag attr23_a1 VARBYTES;
alter table test_alter.t14 add tag attr24_a1 VARBYTES(10);
select pg_sleep(1);

INSERT INTO test_alter.t14 (k_timestamp, e1,attr1,attr23_a1,attr24_a1) VALUES ('2024-01-01T00:00:03+00:00',1,6,'F','l');
INSERT INTO test_alter.t14 (k_timestamp, e1,attr1,attr23_a1,attr24_a1) VALUES ('2024-01-01T00:00:04+00:00',1,7,'E','l');
INSERT INTO test_alter.t14 (k_timestamp, e1,attr1,attr23_a1,attr24_a1) VALUES ('2024-01-01T00:00:05+00:00',1,8,'S','o');
INSERT INTO test_alter.t14 (k_timestamp, e1,attr1,attr23_a1,attr24_a1) VALUES ('2024-01-01T00:00:06+00:00',1,9,NULL,NULL);
show tag values from test_alter.t14;


drop database test_alter cascade;


--- bug-37687:rename ts table to relational database
CREATE DATABASE rdb1;
CREATE TS DATABASE tsdb1;
CREATE TABLE tsdb1.t_r_table1(
                                 ts TIMESTAMPTZ NOT NULL,
                                 col1 varchar NOT NULL,
                                 col2 varchar NOT NULL
)
    ATTRIBUTES (
tag1 INT NOT NULL,
tag2 INT
)
PRIMARY TAGS(tag1);
ALTER TABLE tsdb1.t_r_table1 RENAME TO rdb1.t_r_table1;
CREATE TABLE rdb1.t1(a int);
ALTER TABLE rdb1.t1 RENAME TO tsdb1.test;
DROP DATABASE tsdb1;
DROP DATABASE rdb1;


-- bug-39786: agg query after add column
create ts database db1;
CREATE TABLE db1.ts_t1(
    k_timestamp timestamptz NOT NULL,
    e1 int2,
    e2 int,
    e3 int8,
    e4 float4,
    e5 float8,
    e6 bool,
    e7 timestamptz,
    e8 char(1023),
    e9 nchar(255),
    e10 char(810),
    e11 char,
    e12 char(812),
    e13 nchar,
    e14 char(814),
    e15 nchar(215),
    e16 char(816),
    e17 nchar(217),
    e18 char(418),
    e19 varchar(256),
    e20 char(420),
    e21 char(221),
    e22 char(422),
    i1_1 SMALLINT NULL,
    i1_2 SMALLINT NULL,
    i1_3 SMALLINT NULL,
    i1_4 SMALLINT NULL,
    i2_1 INT NULL,
    i2_2 INT NULL,
    f1_1 FLOAT4 NULL,
    f1_2 FLOAT4 NULL,
    f1 FLOAT8 NULL,
    f2 FLOAT8 NULL,
    c1_1 CHAR NULL,
    c1_2 CHAR NULL,
    c1_3 CHAR NULL,
    c1_4 CHAR NULL,
    n1_1 NCHAR NULL,
    n1_2 NCHAR NULL,
    n1_3 NCHAR NULL,
    n1_4 NCHAR NULL,
    nv1_1 NVARCHAR NULL,
    nv1_2 NVARCHAR NULL,
    nv1_3 NVARCHAR NULL,
    nv1_4 NVARCHAR NULL,
    v1_1 VARCHAR NULL,
    v1_2 VARCHAR NULL,
    v1_3 VARCHAR NULL,
    v1_4 VARCHAR NULL,
    v1_5 VARCHAR NULL,
    v1_6 VARCHAR NULL,
    v1_7 VARCHAR NULL,
    v1_8 VARCHAR NULL,
    v1_9 VARCHAR NULL,
    t1_1 TIMESTAMP NULL,
    tz_1 TIMESTAMPtz NULL) ATTRIBUTES (
    code1 int NOT NULL,
    flag int NOT NULL,
    color nchar(200) NOT NULL,
    t1 smallint,
    t2 int,
    t3 bigint,
    t4 float,
    t5 double) primary tags(code1, flag, color) activetime 10s;

INSERT INTO db1.ts_t1 (k_timestamp, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22, code1,  flag, color) VALUES ('2024-06-01 00:00:00.000',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,800,800,'color');
INSERT INTO db1.ts_t1 (k_timestamp, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22, code1,  flag, color) VALUES ('2024-06-01 01:00:00.000',100,100,1.88,1.88,1,'2024-06-14 01:47:56.286+00:00','e1','e1','e1','1','1','1','1','1','1','1','1','1','1','1','1',800,800,'color');
INSERT INTO db1.ts_t1 (k_timestamp, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22, code1,  flag, color) VALUES ('2024-06-01 02:00:00.000',800,800,8.88,8.88,0,'2024-06-14 02:47:56.286+00:00','e8','e9','e10','8','e12','8','e14','e15','e16','e17','e18','e19','e20','e21','e22',800,800,'color');
select * from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';

ALTER TABLE db1.ts_t1 ADD COLUMN ac1 INT8 NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac2 INT4 NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac3 INT2 NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac4 FLOAT4 NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac5 FLOAT8 NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac6 BOOL NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac7 TIMESTAMP NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac8 CHAR(50) NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac9 NCHAR(50) NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac10 VARCHAR(50) NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac11 CHAR NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac12 NCHAR NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac13 VARCHAR NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac16 TIMESTAMPTZ NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac17 NVARCHAR NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac18 NVARCHAR(50) NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac19 VARBYTES NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac20 VARBYTES(50) NULL;
ALTER TABLE db1.ts_t1 ADD COLUMN ac21 geometry NULL;

select count(ac6),max(ac6),min(ac6),first(ac6),last(ac6),first_row(ac6),last_row(ac6) from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';
select count(ac8),max(ac8),min(ac8),first(ac8),last(ac8),first_row(ac8),last_row(ac8) from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';
select count(ac9),max(ac9),min(ac9),first(ac9),last(ac9),first_row(ac9),last_row(ac9) from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';
select count(ac10),max(ac10),min(ac10),first(ac10),last(ac10),first_row(ac10),last_row(ac10) from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';
select count(ac11),max(ac11),min(ac11),first(ac11),last(ac11),first_row(ac11),last_row(ac11) from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';
select count(ac12),max(ac12),min(ac12),first(ac12),last(ac12),first_row(ac12),last_row(ac12) from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';
select count(ac13),max(ac13),min(ac13),first(ac13),last(ac13),first_row(ac13),last_row(ac13) from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';
select count(ac16),max(ac16),min(ac16),first(ac16),last(ac16),first_row(ac16),last_row(ac16) from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';
select k_timestamp,ac17 from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000' order by k_timestamp;
select count(ac17),max(ac17),min(ac17),first(ac17),last(ac17),first_row(ac17),last_row(ac17) from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';
select k_timestamp,ac18 from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000' order by k_timestamp;
select count(ac18),max(ac18),min(ac18),first(ac18),last(ac18),first_row(ac18),last_row(ac18) from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';
select k_timestamp,ac19 from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000' order by k_timestamp;
select count(ac19),max(ac19),min(ac19),first(ac19),last(ac19),first_row(ac19),last_row(ac19) from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';
select k_timestamp,ac20 from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000' order by k_timestamp;
select count(ac20),max(ac20),min(ac20),first(ac20),last(ac20),first_row(ac20),last_row(ac20) from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000';
select k_timestamp,ac21 from db1.ts_t1 where k_timestamp >= '2024-06-01 00:00:00.000' and k_timestamp <= '2024-06-01 02:00:00.000' order by k_timestamp;

drop database db1 cascade;

-- bug ZDP-41249
create ts database test;
CREATE TABLE test.t1(ts timestamp not null, e1 int, e2 int) tags(tag1 int not null) primary tags(tag1);

INSERT INTO test.t1 VALUES ('2024-06-01 00:00:00.000', 1, NULL, 1);
INSERT INTO test.t1 VALUES ('2024-06-02 00:00:00.000', 1, 1, 1);
INSERT INTO test.t1 VALUES ('2024-06-03 00:00:00.000', 1, 2, 1);
INSERT INTO test.t1 VALUES ('2024-06-04 00:00:00.000', 1, NULL, 1);
select first(e2), last(e2), first_row(e2), last_row(e2) from test.t1;

ALTER TABLE test.t1 DROP COLUMN e1;
select first(e2), last(e2), first_row(e2), last_row(e2) from test.t1;

drop database test cascade;