--basic test tag smallint
drop database if exists test_alter cascade;
create ts database test_alter;

--case21 smallint -> int/bigint/varchar
create table test_alter.t2(ts timestamp not null, a smallint, b smallint, c smallint) tags(attr int not null) primary tags(attr);
insert into test_alter.t2 values(1672531211005, 666, 11, 113, 1);
insert into test_alter.t2 values(1672531211006, 555, 22, 224, 1);
insert into test_alter.t2 values(1672531211007, 333, 33, NULL, 1);
insert into test_alter.t2 values(1672531211008, 444, 44, NULL, 1);
insert into test_alter.t2 values(1672531211009, 111, 55, NULL, 1);
alter table test_alter.t2 alter column a type int;
alter table test_alter.t2 alter column b type bigint;
alter table test_alter.t2 alter column c type varchar(120);
select pg_sleep(1);
select first(a),first(b),first(c),last(a),last(b),last(c) from test_alter.t2;
select count(c),max(c),min(c),first(c),last(c),first_row(c),last_row(c) from test_alter.t2;

--case22 int -> bigint/varchar | bigint -> varchar
create table test_alter.t3(ts timestamp not null, a int, b int, c bigint) tags(attr int not null) primary tags(attr);
insert into test_alter.t3 values(1672531211005, 1111, 1112,  111112, 1);
insert into test_alter.t3 values(1672531211006, 2222, 2223,  222223, 1);
alter table test_alter.t3 alter column a type bigint;
alter table test_alter.t3 alter column b type varchar(120);
alter table test_alter.t3 alter column c type varchar(50);
select pg_sleep(1);
select first(a),first(b),first(c),last(a),last(b),last(c) from test_alter.t3;

--case23 float -> double/varchar | double -> varchar
create table test_alter.t4(ts timestamp not null, a float4, b float4, c double) tags(attr int not null) primary tags(attr);
insert into test_alter.t4 values(1672531211005, 12.7128, 13.712826,  13.14159267890796, 1);
insert into test_alter.t4 values(1672531211006, 22.7128, 23.712826,  23.14159267890796, 1);
alter table test_alter.t4 alter column a type double;
alter table test_alter.t4 alter column b type varchar(120);
alter table test_alter.t4 alter column c type varchar(120);
select pg_sleep(1);
select first(a),first(b),first(c),last(a),last(b),last(c) from test_alter.t4;

--case24 char -> nchar/varchar/nvarchar
create table test_alter.t5(ts timestamp not null, a char(32), b char(64), c char(128)) tags(attr int not null) primary tags(attr);
insert into test_alter.t5 values(1672531211005, '1test@TEST1', '1test@TEST2',  '1test@TEST3', 1);
insert into test_alter.t5 values(1672531211106, '2test@TEST1', '2test@TEST2',  '2test@TEST3', 1);
alter table test_alter.t5 alter column a type nchar(32);
alter table test_alter.t5 alter column b type varchar(64);
alter table test_alter.t5 alter column c type nvarchar(128);
select pg_sleep(1);
select first(a),first(b),first(c),last(a),last(b),last(c) from test_alter.t5;

--case25 varchar -> smallint/int/bigint/float/double/char/nchar/nvarchar
create table test_alter.t7(ts timestamp not null, a varchar(32), b varchar(64), c varchar(128), d varchar(128),  e varchar(128),  f  varchar(128),  g varchar(128), h varchar(128)) tags(attr int not null) primary tags(attr);
insert into test_alter.t7 values(1672531211005, '1999', '199999', '199999999', '13.712882', '13.14159267890796', '1test@TEST1', '1test@TEST2',  '1test@TEST3', 1);
insert into test_alter.t7 values(1672531211006, '2999', '299999', '299999999', '23.712882', '23.14159267890796', '2test@TEST1', '2test@TEST2',  '2test@TEST3', 1);
alter table test_alter.t7 alter column a type smallint;
alter table test_alter.t7 alter column b type int;
alter table test_alter.t7 alter column c type bigint;
alter table test_alter.t7 alter column d type float;
alter table test_alter.t7 alter column e type double;
alter table test_alter.t7 alter column  f type char(128);
alter table test_alter.t7 alter column  g type nchar(128);
alter table test_alter.t7 alter column  h type nvarchar(128);
select pg_sleep(1);
select first(a),first(b),first(c),first(d),first(e),first(f),first(g),first(h),last(a),last(b),last(c),last(d),last(e),last(f),last(g),last(h) from test_alter.t7;

--case26 nvarchar -> char/nchar/varchar
create table test_alter.t8(ts timestamp not null, a nvarchar(32), b nvarchar(64), c nvarchar(32)) tags(attr int not null) primary tags(attr);
insert into test_alter.t8 values(1672531211005, '1test@TEST1', '1test@TEST2',  '1test@TEST3', 1);
insert into test_alter.t8 values(1672531211006, '2test@TEST1', '2test@TEST2',  '2test@TEST3', 1);

alter table test_alter.t8 alter column a type char(128);
alter table test_alter.t8 alter column b type nchar(128);
alter table test_alter.t8 alter column c type varchar(128);
select pg_sleep(1);
select first(a),first(b),first(c),last(a),last(b),last(c) from test_alter.t8;


--case27 change fixed length
create table test_alter.t9(ts timestamp not null, a char, b nchar(64)) tags(attr int not null) primary tags(attr);
insert into test_alter.t9 values(1672531211005,  't',  '1test@TEST3', 1);
insert into test_alter.t9 values(1672531211006,  'a',  '2tes@TEST3', 1);
alter table test_alter.t9 alter column a type char(128);
alter table test_alter.t9 alter column b type nchar(128);
select pg_sleep(1);
select first(a),first(b),last(a),last(b) from test_alter.t9;


--case28 merge policy
-- float->double int->varchar char->varchar varchar->char
create table test_alter.t10(ts timestamp not null, a float4, b int, c char(64), d varchar(64)) tags(attr int not null) primary tags(attr);
insert into test_alter.t10 values(1672531211005, 13.14, 165536,  '1test@TEST3', '1335545', 1);
insert into test_alter.t10 values(1672531211006, 23.14, 265536,  '2test@TEST3', '2335545', 1);
alter table test_alter.t10 alter column a type double;
alter table test_alter.t10 alter column b type varchar(120);
alter table test_alter.t10 alter column c type varchar(120);
alter table test_alter.t10 alter column d type int;
select pg_sleep(1);
select first(a),first(b),first(c),first(d),last(a),last(b),last(c),last(d) from test_alter.t10;

-- cast invalid data to NULL
create table test_alter.t11(ts timestamp not null, a varchar(50)) tags(ptag int not null, attr1 varchar(100)) primary tags(ptag);
insert into test_alter.t11 values(1000000001, '1a', 1, 'aa');
insert into test_alter.t11 values(1000000002, '2b', 1, 'aa');
insert into test_alter.t11 values(1000000003, '4', 2, 'bb');
insert into test_alter.t11 values(1000000004, '3', 2, 'bb');
insert into test_alter.t11 values(1000000005, '6', 3, 'cc');
insert into test_alter.t11 values(1000000006, '9999999999999', 3, 'cc');
select count(a),max(a),min(a),first(a),last(a),first_row(a),last_row(a) from test_alter.t11;
alter table test_alter.t11 alter column a type int;
select pg_sleep(1);
select first_row(a) from test_alter.t11;
select last_row(a) from test_alter.t11;
select count(a),max(a),min(a),sum(a),first(a),last(a),first_row(a),last_row(a) from test_alter.t11;


drop database test_alter cascade;
