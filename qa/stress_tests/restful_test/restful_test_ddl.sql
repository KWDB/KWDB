create ts database test_alter;
create table test_alter.t1(ts timestamp not null, a int) tags(b int not null) primary tags(b);
alter table test_alter.t1 add column c int;
alter table test_alter.t1 drop column c;
alter table test_alter.t1 add column c int;
alter table test_alter.t1 add column d float;
alter table test_alter.t1 drop column d;
alter table test_alter.t1 add column d float;
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
alter table test_alter.t2 add tag a8 date;
create table test_alter.t3(
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
) tags (code1 int2 not null, code2 int, code3 int8, flag bool, val1 float, val2 float8, location varchar, color varchar(65534), age char, sex char(1023), year nchar, type nchar(254)) primary tags(code1);

alter table test_alter.t3 add tag attr17_a1 smallint;
alter table test_alter.t3 add tag attr18_a1 int;
alter table test_alter.t3 add tag attr19_a1 bigint;
alter table test_alter.t3 add tag attr20_a1 float4;
alter table test_alter.t3 add tag attr21_a1 double;
alter table test_alter.t3 add tag attr22_a1 bool;
alter table test_alter.t3 add tag attr25_a1 char;
alter table test_alter.t3 add tag attr26_a1 char(254);
alter table test_alter.t3 add tag attr27_a1 nchar;
alter table test_alter.t3 add tag attr28_a1 nchar(10);
alter table test_alter.t3 add tag attr29_a1 nchar(254);
alter table test_alter.t3 add tag attr30_a1 varchar;
alter table test_alter.t3 add tag attr31_a1 varchar(1023);
drop table test_alter.t3;
drop table test_alter.t2;
drop table test_alter.t1;
create table test_alter.t4(ts timestamp not null, a varchar(32), b varchar(64), c varchar(128), d varchar(128),  e varchar(128),  f  varchar(128),  g varchar(128), h varchar(128)) tags(attr int not null) primary tags(attr);
alter table test_alter.t4 alter column a type smallint;
alter table test_alter.t4 alter column b type int;
alter table test_alter.t4 alter column c type bigint;
alter table test_alter.t4 alter column d type float;
alter table test_alter.t4 alter column e type double;
alter table test_alter.t4 alter column  f type char(128);
alter table test_alter.t4 alter column  g type nchar(128);
alter table test_alter.t4 alter column  h type nvarchar(128);
drop table test_alter.t4;
create ts database test;
create table test.tt1 (k_timestamp timestamp not null,ser_id timestamp(6) not null ,logon_date float not null)attributes (t1_attribute varchar not null) primary tags(t1_attribute);
create table test.tt2 (k_timestamp timestamp not null,ser_id timestamptz(6) not null ,logon_date float not null)attributes (t1_attribute varchar not null) primary tags(t1_attribute);
create table test.tt3 (k_timestamp timestamp not null,ser_id timestamptz(7) not null ,logon_date float not null)attributes (t1_attribute varchar not null) primary tags(t1_attribute);
create table test.logon (k_timestamp timestamp not null,ser_id INT4 not null ,logon_date float not null)attributes (t1_attribute varchar not null) primary tags(t1_attribute);
create ts database test;
create table test.S(k_timestamp timestamp not null,A INT4 not null)attributes (t1_attribute varchar not null) primary tags(t1_attribute);
create table test.S1(k_timestamp timestamp not null,A INT4 not null,b INT4 not null)attributes (t1_attribute varchar not null) primary tags(t1_attribute);
create table test.S2(k_timestamp timestamp not null,A INT4 not null)attributes (t1_attribute varchar not null) primary tags(t1_attribute);
drop database test cascade;

CREATE ts DATABASE test_savedata1;
CREATE TABLE test_savedata1.d1(k_timestamp timestamp not null,e1 int2 not null)
    attributes (t1_attribute int not null) primary tags(t1_attribute);

create TABLE test_savedata1.d2(k_timestamp timestamp not null,e1 int4 not null)
    attributes (t1_attribute int not null) primary tags(t1_attribute);

create table test_savedata1.nst1(ts timestamp not null, e1 int, e2 float not null, e3 bool, e4 int8, e5 char(10) not null, e6 nchar(10),
                       e7 char(10), e8 char(10) not null) attributes (tag1 int not null) primary tags(tag1);

create ts database test;
create table test.tb(
                        k_timestamp timestamptz not null,a1 int2,a2 int4,a3 int8,a4 float4,a5 float8,a6 double,
                        a7 char,a8 char(10),a9 nchar,a10 nchar(10),a11 bool,a12 varbytes,a13 varbytes(10),a14 timestamp)
    tags(t1 int2 not null,t2 int4,t3 int8,t4 float4,t5 float8,t6 double,t7 char(1),t8 char(10),
 t9 nchar(1),t10 nchar(10),t11 bool,t12 varbytes(1),t13 varbytes(10)) primary tags(t1);

create ts database test_insert;
create table test_insert.tb(k_timestamp timestamp not null ,e1 int2) tags(tag1 int not null) primary  tags(tag1);

drop database if exists ts_db cascade;
create ts database ts_db;
create table ts_db.t1(
                         k_timestamp timestamptz not null,
                         e1 int2  not null,
                         e2 int,
                         e3 int8 not null,
                         e4 float4,
                         e5 float8 not null,
                         e6 bool,
                         e7 timestamptz not null,
                         e8 char(1023),
                         e9 nchar(255) not null,
                         e10 nchar(200),
                         e11 char not null,
                         e12 nchar(200),
                         e13 nchar not null,
                         e14 nchar(200),
                         e15 nchar(200) not null,
                         e16 varbytes(200),
                         e17 nchar(200) not null,
                         e18 nchar(200),e19 varbytes not null,
                         e20 varbytes(1023),
                         e21 varbytes(200) not null,
                         e22 varbytes(200)
) ATTRIBUTES (code1 int2 not null,code2 int,code3 int8,flag BOOL not null,val1 float4,val2 float8,location nchar(200),color nchar(200) not null,name varbytes,state varbytes(1023),tall varbytes(200),screen varbytes(200),age CHAR,sex CHAR(1023),year NCHAR,type NCHAR(254)) primary tags(code1,flag,color);


drop database if exists test_insert2 cascade;
create ts database test_insert2;
create table test_insert2.t1(ts timestamptz not null,e1 timestamp,e2 int2,e3 int4,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(64),e10 nchar,e11 nchar(64),e16 varbytes, e17 varbytes(64))tags (tag1 bool not null,tag2 smallint, tag3 int,tag4 bigint, tag5 float4, tag6 double, tag7 varbytes, tag8 varbytes(64), tag11 char,tag12 char(64), tag13 nchar, tag14 nchar(64))primary tags(tag1);
