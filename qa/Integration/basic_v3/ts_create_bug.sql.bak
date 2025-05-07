-- ZDP-25983
DROP DATABASE IF EXISTS default_expr_db;
CREATE DATABASE default_expr_db;
use default_expr_db;
CREATE TABLE default_expr_tb1 (id int default unique_rowid(), name string);
CREATE TABLE default_expr_tb2 (id int8 default unique_rowid(), name string);
insert into default_expr_tb2(name) values ('aaa');
use defaultdb;
DROP DATABASE IF EXISTS default_expr_db;

--ZDP-21907
create ts database "NULL";
create table "NULL".test(ts timestamp not null, a int) tags(b int not null) primary tags(b);
insert into "NULL".test values ('2023-07-03 10:58:52.111',1, 11);
select * from "NULL".test;

CREATE ts DATABASE "null";
CREATE TABLE "null".null(ts timestamp not null,"null" int) tags(b int not null) primary tags(b);
INSERT INTO "null".null VALUES ('2023-07-03 10:58:52.111',1, 22);
select * from "null".null;

create ts database "int";
create table "int".test(ts timestamp not null, a int) tags(b int not null) primary tags(b);
insert into "int".test values ('2023-07-03 10:58:52.111',1, 33);
select * from "int".test;

create ts database "nul";
create table "nul".test(ts timestamp not null, a int) tags(b int not null) primary tags(b);
insert into "nul".test values ('2023-07-03 10:58:52.111',1, 44);
select * from "nul".test;

create ts database NULL;
use defaultdb;
drop database "NULL" cascade;
drop database "null" cascade;
drop database "int" cascade;
drop database "nul" cascade;
drop database NULL cascade;

-- ZDP-19734
create ts database db1;
use db1;
create table t1(k_timestamp timestamp not null,e1 varchar(100)) tags (a int not null) primary tags(a);
insert into t1 values('2018-10-10 10:00:00','null', 1);
--select e1 from t1;
use defaultdb;
drop database db1;

-- ZDP-30236
create ts database db1;
use db1;
create table db1.t1 (k_timestamp timestamptz not null,
                     e1 int2 not null,
                     e2 int,
                     e3 int8 not null,
                     e4 float,
                     e5 float8 not null,
                     e6 bool,
                     e7 timestamp not null,
                     e8 char(1023),
                     e9 nchar(255) not null,
                     e10 varchar(4096),
                     e11 char not null,
                     e12 varchar(255),
                     e13 nchar not null,
                     e14 varchar,
                     e15 nvarchar(4096) not null,
                     e16 varbytes,
                     e17 nvarchar(255) not null,
                     e18 nvarchar,
                     e19 varbytes not null,
                     e20 varbytes(1023),
                     e21 varbytes(4096) not null,
                     e22 varbytes(254),
                     e23 timestamp not null) ATTRIBUTES (
code1 int2 not null,
code1 int2) PRIMARY TAGS(code1);
use defaultdb;
drop database db1;

-- ZDP-30263
CREATE TS DATABASE test;
CREATE TABLE test.tb1(k_timestamp timestamptz not null,
                      e1 int2,
                      e2 int,
                      e3 int8,
                      e4 float,
                      e5 float8,
                      e6 bool,
                      e7 timestamp,
                      e8 char(1023),
                      e9 nchar(255),
                      e10 varchar(4096),
                      e11 char,
                      e12 varchar(255),
                      e13 nchar,
                      e14 varchar,
                      e15 nvarchar(4096),
                      e16 varbytes,
                      e17 nvarchar(255),
                      e18 nvarchar,
                      e19 varbytes,
                      e20 varbytes(1023),
                      e21 varbytes(4096),
                      e22 varbytes(254),
                      e23 timestamptz)
    TAGS (code timestamptz not null) primary tags (code);
use defaultdb;
drop database test;

-- ZDP-30430
create ts database db1;
create table db1.t2(k_timestamp timestamptz not null,e1 int2)attributes(code1 int not null,code2 timestamp)PRIMARY TAGS(code1);
create table db1.t1 (k_timestamp timestamptz not null,e1 int2)attributes(code1 int not null,code2 SERIAL2 not null)PRIMARY TAGS(code1);
use defaultdb;
drop database db1;

-- ZDP-30461
create ts database db1;
use db1;
create table db1.t1(k_timestamp timestamptz not null,e1 int2 not null unique,e2 double)attributes(code1 int2 not null) PRIMARY TAGS(code1);
create table db1.t1(k_timestamp timestamptz not null,e1 int2 not null,e2 double,constraint ck1 check(e1))attributes(code1 int2 not
null) PRIMARY TAGS(code1);
use defaultdb;
drop database db1;

-- ZDP-30860
create ts database db1;
use db1;
create table db1.t2()attributes(code1 int not null)PRIMARY TAGS(code1);
use defaultdb;
drop database db1;

-- ZDP-30910
create ts database db1;
create table db1.tt1(a int);
use defaultdb;
drop database db1;

-- ZDP-30931
create ts database db1;
--ERROR: SERIAL column is not supported in timeseries table
create table db1.t1 (k_timestamp timestamptz not null,e1 SERIAL2)attributes(code1 int2 not null)PRIMARY TAGS(code1);
create table db1.t1 (k_timestamp timestamptz not null,e1 SERIAL2)attributes(code1 int2 not null)PRIMARY TAGS(code1);
create table db1.t1 (k_timestamp timestamptz not null,e1 SMALLSERIAL)attributes(code1 int not null)PRIMARY TAGS(code1);
create table db1.t1 (k_timestamp timestamptz not null,e1 SERIAL4)attributes(code1 int not null)PRIMARY TAGS(code1);
create table db1.t1 (k_timestamp timestamptz not null,e1 SERIAL)attributes(code1 int not null)PRIMARY TAGS(code1);
create table db1.t1 (k_timestamp timestamptz not null,e1 SERIAL8)attributes(code1 int not null)PRIMARY TAGS(code1);
create table db1.t1 (k_timestamp timestamptz not null,e1 BIGSERIAL)attributes(code1 int not null)PRIMARY TAGS(code1);
--ERROR: unsupported column type in timeseries table
create table db1.t1 (k_timestamp timestamptz not null,e1 TIME)attributes(code1 int not null)PRIMARY TAGS(code1);
create table db1.t1 (k_timestamp timestamptz not null,e1 DATE)attributes(code1 int not null)PRIMARY TAGS(code1);
create table db1.t1 (k_timestamp timestamptz not null,e1 STRING)attributes(code1 int not null)PRIMARY TAGS(code1);
create table db1.t1 (k_timestamp timestamptz not null,e1 STRING(20))attributes(code1 int not null)PRIMARY TAGS(code1);
--ERROR
create table db1.t1 (k_timestamp timestamptz not null,e1 CLOB)attributes(code1 int not null)PRIMARY TAGS(code1);
--ERROR: unsupported column type decimal in timeseries table
create table db1.t1 (k_timestamp timestamptz not null,e1 DECIMAL)attributes(code1 int not null)PRIMARY TAGS(code1);
use defaultdb;
drop database db1;

-- ZDP-30935
create ts database db1;
create table db1.t1 (k_timestamp timestamptz not null,e1 BLOB)attributes(code1 int not null)PRIMARY TAGS(code1);
create table db1.t2 (k_timestamp timestamptz not null,e1 BYTEA)attributes(code1 int not null)PRIMARY TAGS(code1);
create table db1.t3 (k_timestamp timestamptz not null,e1 CHARACTER)attributes(code1 int not null)PRIMARY TAGS(code1);
create table db1.t4 (k_timestamp timestamptz not null,e1 CHARACTER VARYING)attributes(code1 int not null)PRIMARY TAGS(code1);
create table db1.t5 (k_timestamp timestamptz not null,e1 CHARACTER(20))attributes(code1 int not null)PRIMARY TAGS(code1);
create table db1.t6 (k_timestamp timestamptz not null,e1 CHARACTER VARYING(20))attributes(code1 int not null)PRIMARY TAGS(code1);
use defaultdb;
drop database db1;

-- ZDP-30937
create ts database db1;
create table db1.t1 (k_timestamp timestamptz not null,e1 int)attributes(code1 int not null)PRIMARY TAGS(code1);
begin;
select * from db1.t1;
end;
use defaultdb;
drop database db1;

-- ZDP-31545
CREATE TS DATABASE db1;
use db1;
CREATE TABLE db1.tb01(k_timestamp timestamptz not null, e1 int2) TAGS (code1 bool not null,code2 varchar not null)PRIMARY TAGS(code1,code2);
CREATE TABLE db1.tb02(k_timestamp timestamptz not null, e1 int2) TAGS (code1 bool not null,code2 varchar)PRIMARY TAGS(code1);
show create table db1.tb01;
show create table db1.tb02;
CREATE TABLE db1.tb_varchar(k_timestamp timestamptz not null, e1 varchar,e2 varchar(30))TAGS (code1 bool not null) primary tags (code1);
show create table db1.tb_varchar;
show columns from db1.tb_varchar;
drop database db1;

--ZDP-32318
drop database if exists db1 cascade;
create database db1;
use db1;
select count(*) from pg_catalog.pg_class;
use defaultdb;
alter database db1 rename to db2;
use db2;
select count(*) from pg_catalog.pg_class;
use defaultdb;
drop database if exists db2 cascade;

--ZDP-33046
create role test;
drop role test;

create user test_u1;
create user test_u2;
create user test_u3;
drop user test_u1, test_u2, test_u3;

--ZDP-33019
CREATE TS DATABASE test;
CREATE TABLE test."!@#$%%^中文" (k_timestamp timestamptz not null, e1 int2, e2 int4, e3 double) tags (code1 bool not null, code2 int2) primary tags (code1);
CREATE TABLE test."!@#$%%^" (k_timestamp timestamptz not null, e1 int2, e2 int4, e3 double) tags (code1 bool not null, code2 int2) primary tags (code1);
ALTER TABLE test."!@#$%%^" RENAME TO test."!@#$%%^中文";
CREATE TABLE test.ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt(k_timestamp timestamptz not null, e1 int2) tags(code1 bool not null, code2 int2) primary tags (code1);
CREATE TABLE test.tb1(k_timestamp timestamptz not null, e1 int2, e2 int4, e3 double) tags (code1 bool not null, code2 int2) primary tags (code1);
ALTER TABLE test.tb1 RENAME TO test.ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt;
DROP DATABASE test;
