--basic test tag smallint
drop database if exists test_alter cascade;
create ts database test_alter;
create table test_alter.t1(k_timestamp timestamptz not null,e1 int2) tags (ptag smallint not null, attr1 smallint,attr2 smallint, attr3 smallint, attr4 smallint)primary tags(ptag);

insert into test_alter.t1(k_timestamp,e1,ptag, attr1,attr2, attr3, attr4) values(1672531211001, 1, 1, 32767, 32767, 32767, 32767);
select * from test_alter.t1 order by k_timestamp,ptag;

--case1 tag smallint -> int
alter table test_alter.t1 alter tag attr1 type int;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211002, 1, 2, 2147483647);
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--case2 tag smallint -> bigint
alter table test_alter.t1 alter tag attr2 type bigint;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211003, 1, 3, 9223372036854775807);
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

--case3 tag smallint -> varchar -> char -> varchar -> float
alter table test_alter.t1 alter tag attr3 type varchar(32);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr3) values(1672531211004, 1, 4, '9223372036854775808');
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;

alter table test_alter.t1 alter tag attr3 type char(32);
select pg_sleep(1);
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;
alter table test_alter.t1 alter tag attr3 type varchar(32);
select pg_sleep(1);
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;
alter table test_alter.t1 alter tag attr3 type float8;
select pg_sleep(1);
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;

--basic test tag int/bigint
drop database if exists test_alter cascade;
create ts database test_alter;
create table test_alter.t1(k_timestamp timestamptz not null,e1 int2) tags (ptag smallint not null, attr1 int, attr2 int, attr3 int, attr4 int)primary tags(ptag);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1,attr2, attr3, attr4) values(1672531211001, 1, 1, 2147483647, 2147483647, 2147483647, 2147483647);
select * from test_alter.t1 order by k_timestamp,ptag;

--case4 tag int -> bigint
alter table test_alter.t1 alter tag attr1 type bigint;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211002, 1, 2, 9223372036854775807);
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--case5 tag int -> varchar
alter table test_alter.t1 alter tag attr2 type varchar(32);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211003, 1, 3, '9223372036854775808');
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

--case6 tag bigint -> varchar
alter table test_alter.t1 alter tag attr1 type varchar(32);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211004, 1, 4, '9223372036854775809');
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--basic test tag float/double
drop database if exists test_alter cascade;
create ts database test_alter;
create table test_alter.t1(k_timestamp timestamptz not null,e1 int2) tags (ptag smallint not null, attr1 float4, attr2 float4, attr3 float4, attr4 float4)primary tags(ptag);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1,attr2, attr3, attr4) values(1672531211001, 1, 1, 2.712882, 2.712882, 2.712882, 2.712882);
select * from test_alter.t1 order by k_timestamp,ptag;

--case7 tag float -> double
alter table test_alter.t1 alter tag attr1 type double;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211002, 1, 2, 3.14159267890796);
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--case8 tag float -> varchar
alter table test_alter.t1 alter tag attr2 type varchar(32);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211003, 1, 3, '3.14159267890797');
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

--case9 tag double -> varchar
alter table test_alter.t1 alter tag attr1 type varchar(32);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211004, 1, 4, '3.14159267890798');
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--basic test tag char/nchar/varchar
drop database if exists test_alter cascade;
create ts database test_alter;
create table test_alter.t1(k_timestamp timestamptz not null,e1 int2) tags (ptag smallint not null, attr1 char(50),attr2 char(50), attr3 nchar(50), attr4 nchar(50))primary tags(ptag);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1,attr2, attr3, attr4) values(1672531211001, 1, 1, 'test时间精度通用查询测试！！！@TEST1', 'test时间精度通用查询测试！！！@TEST1', 'test时间精度通用查询测试！！！@TEST1', 'test时间精度通用查询测试！！！@TEST1');
select * from test_alter.t1 order by k_timestamp,ptag;

--case10 char -> char -> nchar
alter table test_alter.t1 alter tag attr1 type char(100);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211008, 1, 8, 'test时间精度通用查询测试！！！@TEST8');
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;
alter table test_alter.t1 alter tag attr1 type nchar(50);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211002, 1, 2, 'test时间精度通用查询测试！！！@TEST2');
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--case11 char -> varchar
alter table test_alter.t1 alter tag attr2 type varchar(50);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211003, 1, 3, 'test时间精度通用查询测试！！！@TEST3');
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

--case12 nchar -> nchar -> char
alter table test_alter.t1 alter tag attr3 type nchar(60);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr3) values(1672531211009, 1, 9, 'test时间精度通用查询测试！！！@TEST9');
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;
alter table test_alter.t1 alter tag attr3 type char(250);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr3) values(1672531211004, 1, 4, 'test时间精度通用查询测试！！！@TEST4');
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;

--case13 nchar -> varchar
alter table test_alter.t1 alter tag attr4 type varchar(200);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr4) values(1672531211005, 1, 5, 'test时间精度通用查询测试！！！@TEST5');
select k_timestamp,e1,ptag, attr4 from test_alter.t1 order by k_timestamp,ptag;

--case14 varchar -> varchar -> char
alter table test_alter.t1 alter tag attr2 type varchar(60);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211010, 1, 10, 'test时间精度通用查询测试！！！@TEST10');
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;
alter table test_alter.t1 alter tag attr2 type char(100);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211006, 1, 6, 'test时间精度通用查询测试！！！@TEST6');
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

--case15 varchar -> nchar
alter table test_alter.t1 alter tag attr4 type nchar(200);
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr4) values(1672531211007, 1, 7, 'test时间精度通用查询测试！！！@TEST7');
select k_timestamp,e1,ptag, attr4 from test_alter.t1 order by k_timestamp,ptag;

--basic test tag varchar->numeric
drop database if exists test_alter cascade;
create ts database test_alter;
create table test_alter.t1(k_timestamp timestamptz not null,e1 int2) tags (ptag smallint not null, attr1 varchar(32),attr2 varchar(32), attr3 varchar(32), attr4 varchar(32), attr5 varchar(32))primary tags(ptag);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1,attr2, attr3, attr4,attr5)values(1672531211001, 1, 1, '-32768', '-2147483648', '-9223372036854775808', '-2.712882', '-3.14159267890796');
select * from test_alter.t1 order by k_timestamp,ptag;

--case16 varchar -> smallint
alter table test_alter.t1 alter tag attr1 type smallint;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr1) values(1672531211002, 1, 2, 32767);
select k_timestamp,e1,ptag, attr1 from test_alter.t1 order by k_timestamp,ptag;

--case17 varchar -> int
alter table test_alter.t1 alter tag attr2 type int;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr2) values(1672531211003, 1, 3, 2147483647);
select k_timestamp,e1,ptag, attr2 from test_alter.t1 order by k_timestamp,ptag;

--case18 varchar -> bigint
alter table test_alter.t1 alter tag attr3 type bigint;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr3) values(1672531211004, 1, 4, 9223372036854775807);
select k_timestamp,e1,ptag, attr3 from test_alter.t1 order by k_timestamp,ptag;

--case19 varchar -> float
alter table test_alter.t1 alter tag attr4 type float4;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr4) values(1672531211005, 1, 5, 2.712882);
select k_timestamp,e1,ptag, attr4 from test_alter.t1 order by k_timestamp,ptag;

--case20 varchar -> double
alter table test_alter.t1 alter tag attr5 type float8;
select pg_sleep(1);
insert into test_alter.t1(k_timestamp,e1,ptag, attr5) values(1672531211006, 1, 6, 3.14159267890796);
select k_timestamp,e1,ptag, attr5 from test_alter.t1 order by k_timestamp,ptag;

--case21 smallint -> int/bigint/varchar
create table test_alter.t2(ts timestamp not null, a smallint, b smallint, c smallint) tags(attr int not null) primary tags(attr);
insert into test_alter.t2 values(1672531211005, 100, 200, 300, 1);
alter table test_alter.t2 alter column a type int;
alter table test_alter.t2 alter column b type bigint;
alter table test_alter.t2 alter column c type varchar(120);
insert into test_alter.t2 values(1672531211015, 65536, 65539,  'test时间精度！！！@TEST2', 1);
select * from test_alter.t2 order by ts,attr;

--case22 int -> bigint/varchar | bigint -> varchar
create table test_alter.t3(ts timestamp not null, a int, b int, c bigint) tags(attr int not null) primary tags(attr);
insert into test_alter.t3 values(1672531211005, 1000, 2000,  30000, 1);
alter table test_alter.t3 alter column a type bigint;
alter table test_alter.t3 alter column b type varchar(120);
alter table test_alter.t3 alter column c type varchar(50);
insert into test_alter.t3 values(1672531211015, 9999999, 'test！！！@TEST1', 'test时间精度！！！@TEST2',  1);
select * from test_alter.t3 order by ts,attr;

--case23 float -> double/varchar | double -> varchar
create table test_alter.t4(ts timestamp not null, a float4, b float4, c double) tags(attr int not null) primary tags(attr);
insert into test_alter.t4 values(1672531211005, 2.7128, 3.712826,  3.14159267890796, 1);
alter table test_alter.t4 alter column a type double;
alter table test_alter.t4 alter column b type varchar(120);
alter table test_alter.t4 alter column c type varchar(120);
insert into test_alter.t4 values(1672531211105, 3.14159267890796, '3.14159267890796',  'test时间精度通用查询测试！！！@TEST3', 1);
select * from test_alter.t4 order by ts,attr;

--case24 char -> nchar/varchar/nvarchar
create table test_alter.t5(ts timestamp not null, a char(32), b char(64), c char(128)) tags(attr int not null) primary tags(attr);
insert into test_alter.t5 values(1672531211005, 'test！！！@TEST1', 'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
alter table test_alter.t5 alter column a type nchar(32);
alter table test_alter.t5 alter column b type varchar(64);
alter table test_alter.t5 alter column c type nvarchar(128);
insert into test_alter.t5 values(1672531211105, 'test！！！@TEST1', 'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
select * from test_alter.t5 order by ts,attr;

--case25 varchar -> smallint/int/bigint/float/double/char/nchar/nvarchar
create table test_alter.t7(ts timestamp not null, a varchar(32), b varchar(64), c varchar(128), d varchar(128),  e varchar(128),  f  varchar(128),  g varchar(128), h varchar(128)) tags(attr int not null) primary tags(attr);
insert into test_alter.t7 values(1672531211005, '999', '99999', '99999999', '3.712882', '3.14159267890796', 'test！！！@TEST1', 'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
alter table test_alter.t7 alter column a type smallint;
alter table test_alter.t7 alter column b type int;
alter table test_alter.t7 alter column c type bigint;
alter table test_alter.t7 alter column d type float;
alter table test_alter.t7 alter column e type double;
alter table test_alter.t7 alter column  f type char(128);
alter table test_alter.t7 alter column  g type nchar(128);
alter table test_alter.t7 alter column  h type nvarchar(128);
insert into test_alter.t7 values(1672531211015, 999, 99999, 99999999, 3.712882, 3.14159267890796, 'test！！！@TEST1', 'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
select * from test_alter.t7 order by ts,attr;

--case26 nvarchar -> char/nchar/varchar
create table test_alter.t8(ts timestamp not null, a nvarchar(32), b nvarchar(64), c nvarchar(32)) tags(attr int not null) primary tags(attr);
insert into test_alter.t8 values(1672531211005, 'test！！！@TEST1', 'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
alter table test_alter.t8 alter column a type char(128);
alter table test_alter.t8 alter column b type nchar(128);
alter table test_alter.t8 alter column c type varchar(128);
insert into test_alter.t8 values(1672531211015, 'test！！！@TEST1', 'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
select * from test_alter.t8 order by ts,attr;

create table test_alter.t9(ts timestamp not null, a char, b nchar(64)) tags(attr int not null) primary tags(attr);
insert into test_alter.t9 values(1672531211005,  't',  'test时间精度通用查询测试！！！@TEST3', 1);
alter table test_alter.t9 alter column a type char(128);
alter table test_alter.t9 alter column b type nchar(128);
insert into test_alter.t9 values(1672531211015,  'test时间精度！！！@TEST2',  'test时间精度通用查询测试！！！@TEST3', 1);
select * from test_alter.t9 order by ts,attr;

-- float->double int->varchar char->varchar varchar->char
SET CLUSTER SETTING ts.dedup.rule = 'merge';
create table test_alter.t10(ts timestamp not null, a float4, b int, c char(64), d varchar(64)) tags(attr int not null) primary tags(attr);
insert into test_alter.t10 values(1672531211005, 3.14, 65536,  'test时间精度通用查询测试！！！@TEST3', '335545', 1);
alter table test_alter.t10 alter column a type double;
alter table test_alter.t10 alter column b type varchar(120);
alter table test_alter.t10 alter column c type varchar(120);
alter table test_alter.t10 alter column d type int;
insert into test_alter.t10 values(1672531211005, null, null,  null, null, 1);
select * from test_alter.t10 order by ts,attr;

-- fixed -> str when check alter valid or not
create table test_alter.t11(ts timestamp not null, a char(10)) tags(attr int not null) primary tags(attr);
insert into test_alter.t11 values(1000, '3456', 1);
insert into test_alter.t11 values(1000, '123', 2);
insert into test_alter.t11 values(2000, '12.3', 1);
insert into test_alter.t11 values(2000, '12', 3);
alter table test_alter.t11 alter column a type varchar(100);
alter table test_alter.t11 alter column a type float4;
select * from test_alter.t11 order by ts,attr;

create table test_alter.t12(ts timestamp not null, a char(10)) tags(attr int not null) primary tags(attr);
insert into test_alter.t12 values(1672531211005, '3456', 1);
insert into test_alter.t12 values(1672531211015, '12.3', 1);
alter table test_alter.t12 alter column a type varchar(100);

alter table test_alter.t12 alter column a type float4;
insert into test_alter.t12 values(1672531211005, null, 1);
insert into test_alter.t12 values(1672531211015, null, 1);
select * from test_alter.t12 order by ts;

-- fixed: ZDP-35471
create table test_alter.t13(ts timestamp not null, a char) tags(attr int not null) primary tags(attr);
insert into test_alter.t13 values(1672531211005, 'k', 1);
ALTER TABLE test_alter.t13 ALTER COLUMN a SET DATA TYPE VARCHAR;
ALTER TABLE test_alter.t13 ALTER COLUMN a SET DATA TYPE NVARCHAR;
select * from test_alter.t13;

-- cast_check varchar->int2
create table test_alter.t14(ts timestamp not null, a varchar(20)) tags(ptag int not null, attr1 varchar(100)) primary tags(ptag);
insert into test_alter.t14 values(1000000001, '1a', 1, 'a');
insert into test_alter.t14 values(1000000002, NULL, 1, 'a');
insert into test_alter.t14 values(1000000003, 'b2', 1, 'a');
insert into test_alter.t14 values(1000000004, '444', 2, 'b');
insert into test_alter.t14 values(1000000005, '9999999999', 2, 'b');
insert into test_alter.t14 values(1000000006, '-9999999999', 2, 'c');
insert into test_alter.t14 values(1000000007, '8', 3, 'c');
insert into test_alter.t14 values(1000000008, '3.45', 3, 'c');
insert into test_alter.t14 values(1000000009, NULL, 3, 'c');
select * from test_alter.t14 order by ts,ptag;;
select * from test_alter.t14 where cast_check(a as int2)=false order by ts,ptag;
alter table test_alter.t14 alter column a type int2;
select * from test_alter.t14 order by ts,ptag;;

-- cast_check varchar->float4
create table test_alter.t15(ts timestamp not null, a varchar(20)) tags(ptag int not null, attr1 varchar(100)) primary tags(ptag);
insert into test_alter.t15 values(1000000001, NULL, 1, 'a');
insert into test_alter.t15 values(1000000002, '3.1415926', 1, 'a');
insert into test_alter.t15 values(1000000003, 'b2.1', 1, 'a');
insert into test_alter.t15 values(1000000004, '4', 2, 'b');
insert into test_alter.t15 values(1000000005, NULL, 2, 'b');
insert into test_alter.t15 values(1000000006, '1.2abc', 2, 'c');
insert into test_alter.t15 values(1000000007, NULL, 3, 'c');
insert into test_alter.t15 values(1000000008, '-1.23', 3, 'c');
insert into test_alter.t15 values(1000000009, '3.4e+100', 3, 'c');
insert into test_alter.t15 values(1000000010, '3.4e+38', 4, 'd');
select * from test_alter.t15 order by ts,ptag;;
select * from test_alter.t15 where cast_check(a as float4)=false order by ts,ptag;
alter table test_alter.t15 alter column a type float4;
select * from test_alter.t15 order by ts,ptag;

-- invalid data: char->varchar->int
create table test_alter.t16(ts timestamp not null, a char(50)) tags(ptag int not null, attr1 varchar(100)) primary tags(ptag);
insert into test_alter.t16 values(1000000001, '1a', 1, 'aa');
insert into test_alter.t16 values(1000000002, '2b', 1, 'aa');
insert into test_alter.t16 values(1000000003, '4', 2, 'bb');
insert into test_alter.t16 values(1000000004, '3', 2, 'bb');
insert into test_alter.t16 values(1000000005, '6', 3, 'cc');
insert into test_alter.t16 values(1000000006, '9999999999999', 3, 'cc');
select * from test_alter.t16 order by ts,ptag;;
alter table test_alter.t16 alter column a type varchar;
alter table test_alter.t16 alter column a type int;
select * from test_alter.t16 order by ts,ptag;;


-- invalid data: varchar->bigint & query in time span
create table test_alter.t17(k_timestamp timestamp not null, e1 varchar, e2 varchar(1000)) tags (t1 int2 not null, t2 int) primary tags(t1);

insert into test_alter.t17 (k_timestamp ,e1,t1) values('2022-06-06 10:10:01','100',6);
insert into test_alter.t17 (k_timestamp ,e1,t1) values('2022-06-06 10:10:02','200',6);
insert into test_alter.t17 (k_timestamp ,e1,t1) values('2022-06-06 10:10:03','a1中@',6);
insert into test_alter.t17 (k_timestamp ,e1,t1) values('2022-06-06 10:10:04','1.234',6);
insert into test_alter.t17 (k_timestamp ,e1,t1) values('2022-06-06 10:10:05','-1.234',6);
insert into test_alter.t17 (k_timestamp ,e1,t1) values('2022-06-06 10:10:06','中文',6);
insert into test_alter.t17 (k_timestamp ,e1,t1) values('2022-06-06 10:10:07','123456789012345678901',6);
insert into test_alter.t17 (k_timestamp ,e1,t1) values('2022-06-06 10:10:08','-123456789012345678901',6);
select * from test_alter.t17;

ALTER TABLE test_alter.t17 ALTER COLUMN e1 TYPE BIGINT;
select * from test_alter.t17;
select * from test_alter.t17 where k_timestamp = '2022-06-06 10:10:02';
select * from test_alter.t17 where k_timestamp > '2022-06-06 10:10:02';
select count(e1),max(e1),min(e1),avg(e1),sum(e1),first(e1),last(e1),first_row(e1),last_row(e1) from test_alter.t17;

--ZDP-41335
CREATE TABLE test_alter.ts_t1 (
  k_timestamp TIMESTAMPTZ NOT NULL,
  e1 INT2 NULL,
  e2 INT4 NULL,
  e3 INT8 NULL,
  e4 FLOAT4 NULL,
  e5 FLOAT8 NULL,
  e6 BOOL NULL,
  e7 TIMESTAMPTZ NULL,
  e8 CHAR(1023) NULL,
  e9 NCHAR(255) NULL,
  e10 CHAR(810) NULL,
  e11 CHAR NULL,
  e12 CHAR(812) NULL,
  e13 NCHAR NULL,
  e14 CHAR(814) NULL,
  e15 NCHAR(215) NULL,
  e16 CHAR(816) NULL,
  e17 NCHAR(217) NULL,
  e18 CHAR(418) NULL,
  e19 VARCHAR(256) NULL,
  e20 CHAR(420) NULL,
  e21 CHAR(221) NULL,
  e22 CHAR(422) NULL,
  i3 INT8 NULL,
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
  nv1_1 NVARCHAR(63) NULL,
  nv1_2 NVARCHAR(63) NULL,
  nv1_3 NVARCHAR(63) NULL,
  nv1_4 NVARCHAR(63) NULL,
  v1_1 VARCHAR(254) NULL,
  v1_2 VARCHAR(254) NULL,
  v1_3 VARCHAR(254) NULL,
  v1_4 VARCHAR(254) NULL,
  v1_5 VARCHAR(254) NULL,
  v1_6 VARCHAR(254) NULL,
  v1_7 VARCHAR(254) NULL,
  v1_8 VARCHAR(254) NULL,
  v1_9 VARCHAR(254) NULL,
  t1_1 TIMESTAMP NULL,
  tz_1 TIMESTAMPTZ NULL
) TAGS (
  code1 INT4 NOT NULL,
  flag INT4 NOT NULL,
  color NCHAR(200) NOT NULL,
  t1 INT2,
  t2 INT4,
  t3 INT8,
  t4 FLOAT8,
  t5 FLOAT8 ) PRIMARY TAGS(code1, flag, color);

ALTER TABLE test_alter.ts_t1 ALTER COLUMN i3 SET DATA TYPE varchar(20);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN f1_1 SET DATA TYPE DOUBLE;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN f1_2 SET DATA TYPE VARCHAR(30);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN f2 SET DATA TYPE VARCHAR(30);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN c1_1 SET DATA TYPE CHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN c1_2 SET DATA TYPE NCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN c1_3 SET DATA TYPE VARCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN c1_4 SET DATA TYPE NVARCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN n1_1 SET DATA TYPE CHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN n1_2 SET DATA TYPE NCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN n1_3 SET DATA TYPE VARCHAR;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN n1_4 SET DATA TYPE NVARCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN nv1_1 SET DATA TYPE CHAR(300);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN nv1_2 SET DATA TYPE NCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN nv1_3 SET DATA TYPE VARCHAR;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN nv1_4 SET DATA TYPE NVARCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_1 SET DATA TYPE CHAR(1023);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_2 SET DATA TYPE NCHAR(254);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_3 SET DATA TYPE VARCHAR(300);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_4 SET DATA TYPE NVARCHAR(254);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_5 SET DATA TYPE SMALLINT;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_6 SET DATA TYPE INT;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_7 SET DATA TYPE BIGINT;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_8 SET DATA TYPE FLOAT4;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_9 SET DATA TYPE FLOAT;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN t1_1 SET DATA TYPE TIMESTAMPtz;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN tz_1 SET DATA TYPE TIMESTAMP;

ALTER TABLE test_alter.ts_t1 ALTER COLUMN i3 SET DATA TYPE varchar(20);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN f1_1 SET DATA TYPE DOUBLE;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN f1_2 SET DATA TYPE VARCHAR(30);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN f2 SET DATA TYPE VARCHAR(30);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN c1_1 SET DATA TYPE CHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN c1_2 SET DATA TYPE NCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN c1_3 SET DATA TYPE VARCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN c1_4 SET DATA TYPE NVARCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN n1_1 SET DATA TYPE CHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN n1_2 SET DATA TYPE NCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN n1_3 SET DATA TYPE VARCHAR;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN n1_4 SET DATA TYPE NVARCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN nv1_1 SET DATA TYPE CHAR(300);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN nv1_2 SET DATA TYPE NCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN nv1_3 SET DATA TYPE VARCHAR;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN nv1_4 SET DATA TYPE NVARCHAR(100);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_1 SET DATA TYPE CHAR(1023);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_2 SET DATA TYPE NCHAR(254);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_3 SET DATA TYPE VARCHAR(300);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_4 SET DATA TYPE NVARCHAR(254);
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_5 SET DATA TYPE SMALLINT;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_6 SET DATA TYPE INT;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_7 SET DATA TYPE BIGINT;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_8 SET DATA TYPE FLOAT4;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN v1_9 SET DATA TYPE FLOAT;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN t1_1 SET DATA TYPE TIMESTAMPtz;
ALTER TABLE test_alter.ts_t1 ALTER COLUMN tz_1 SET DATA TYPE TIMESTAMP;

drop database test_alter cascade;
SET CLUSTER SETTING ts.dedup.rule = 'override';