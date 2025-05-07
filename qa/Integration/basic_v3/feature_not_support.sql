create ts database d1;
use d1;

--expected success
create table t_ok (ts timestamp not null, a int not null) tags (loc varchar(20) not null) primary tags(loc);
insert into t_ok values ('2020-1-1 12:00:00.000',1, 'p1');

--unsupported DML
--expected error
update t_ok set a=2 where 1=1;
upsert into t_ok values ('2020-1-1 12:00:00.000',2);

--unsupported DDL
--expected error
create view t_view as select * from t_ok;
create temp table t_temp(k_timestamp timestamp not null,e1 int2 not null)attributes(e2 int not null) primary tags(e2);
create table t_check(k_timestamp timestamp not null,e22 varbytes(254) check (length(e22)<100)) ATTRIBUTES (code1 int2 not null) primary tags(code1);

CREATE ts DATABASE d_lifetime RETENTIONS 100000000000000 YEAR;
ALTER ts DATABASE d1 set RETENTIONS = 10000year;

--expected success
CREATE ts DATABASE d_ok RETENTIONS 10 DAY;
SELECT RETENTIONS from d_ok.information_schema.schemata where schema_name='public';
ALTER  ts DATABASE d_ok set RETENTIONS = 100year;
SELECT RETENTIONS from d_ok.information_schema.schemata where schema_name='public';

use defaultdb;
drop database d1 cascade;
drop database d_ok;

--expected error
create ts database d_error;
use d_error;
create table t (ts timestamp not null, a int not null) tags (loc varchar(20) not null) primary tags(loc);
insert into t values ('2020-1-1 12:00:00.000',1, 'p1');

-- --expected error
-- truncate ct;
-- create index on ct(a);
-- CREATE STATISTICS s1 FROM ct;
-- ALTER TABLE ct SPLIT at select 1;
-- ALTER TABLE ct UNSPLIT at select 1;
-- ALTER TABLE ct EXPERIMENTAL_RELOCATE select array[1],2;
-- ALTER TABLE ct CONFIGURE ZONE USING range_min_bytes = 16777216;
-- EXPERIMENTAL SCRUB TABLE ct;
-- ALTER TABLE ct SCATTER;
-- COPY ct FROM STDIN;
-- ALTER TABLE ct RENAME TO ct1;
--
-- --expected error
-- SHOW PARTITIONS from table ct;
SHOW PARTITIONS from database d_error;

-- SHOW RANGES from table ct;
SHOW RANGES from database d_error;

--SHOW STATISTICS FOR TABLE ct;

--SHOW INDEXES FROM ct;
SHOW INDEXES FROM DATABASE d_error;

--SHOW keys FROM ct;
SHOW keys FROM DATABASE d_error;

--SHOW CONSTRAINTS FROM ct;


SHOW ZONE CONFIGURATION FOR DATABASE d_error;
-- SHOW ZONE CONFIGURATION FOR TABLE ct;
--
-- SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ct;

drop database d_error;

create database db_test1;
use db_test1;
create schema db1sch1;
set search_path=db1sch1;

CREATE TABLE test1constraints (id INT PRIMARY KEY,date TIMESTAMP NOT NULL,priority INT DEFAULT 1,customer_id INT UNIQUE,status STRING DEFAULT 'open',CHECK (priority BETWEEN 1 AND 5),CHECK (status in ('open', 'in progress', 'done', 'cancelled')),FAMILY (id, date, priority, customer_id, status));
SHOW CONSTRAINTS FROM test1constraints;

CREATE TABLE test1index (a int PRIMARY KEY);
SHOW INDEXES FROM test1index;

CREATE TABLE test1partition (a int) PARTITION BY NOTHING;
SHOW PARTITIONS FROM TABLE test1partition;

use defaultdb;
drop database db_test1 cascade;


--- unsupported feature on alter ts table
create ts database db1;
use db1;
CREATE TABLE t1 (
                    k_timestamp TIMESTAMPTZ NOT NULL,
                    value VARCHAR(254) NOT NULL
) TAGS (
id INT8 NOT NULL,
location VARCHAR(1024) NOT NULL ) PRIMARY TAGS(id);
ALTER TABLE db1.t1 ADD column1 varchar NULL;
ALTER TABLE db1.t1 ALTER COLUMN column1 SET NOT NULL;
ALTER TABLE db1.t1 ALTER COLUMN column1 DROP NOT NULL;
ALTER TABLE db1.t1 ALTER COLUMN column1 SET DEFAULT 'a';
ALTER TABLE db1.t1 ALTER COLUMN column1 DROP STORED;
select * from  t1;
DROP TABLE t1;
DROP DATABASE db1 cascade;

--ZDP-45589
drop database if exists tsdb cascade;
create ts database tsdb;
create table tsdb.t1(a timestamptz not null,e1 nchar)tags(c int not null)primary tags(c);
insert into tsdb.t1 values(now(),s,1);
drop database if exists tsdb cascade;