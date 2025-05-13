--- test_case_rename_database
create ts database tsdb;
alter database tsdb rename to tsdb1;
show databases;
create table tsdb1.test1(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
alter database tsdb rename to tsdb1;
alter database tsdb1 rename to tsdb;
show databases;
select * from tsdb1.test1;
select * from tsdb.test1;
drop database tsdb;

-- test_case_rename_column
create ts database tsdb;
create table tsdb.test1(ts timestamp not null, a int) tags(tag1 int not null,tag2 int) primary tags(tag1);
insert into tsdb.test1 (ts,a,tag1,tag2)values ('2024-03-06 10:05:00',1,1,1);
alter table tsdb.test1 rename column ts to ts1;
alter table tsdb.test1 rename column a to b;
alter table tsdb.test1 rename column tag1 to tag11;
alter table tsdb.test1 rename column tag2 to tag22;
show columns from tsdb.test1;
select * from tsdb.test1 order by tag1;
insert into tsdb.test1 (ts1,b,tag1,tag2)values ('2024-03-06 10:06:00',1,2,2);
alter table tsdb.test1 rename column ts1 to ts;
alter table tsdb.test1 rename column b to a;
select * from tsdb.test1 order by tag1;
drop database tsdb;

-- test_case_rename_tag
create ts database tsdb;
create table tsdb.test1(ts timestamp not null, a int) tags(tag1 int not null,tag2 int) primary tags(tag1);
insert into tsdb.test1 (ts,a,tag1,tag2)values ('2024-03-06 10:05:00',1,1,1);
alter table tsdb.test1 rename tag tag2 to tag22;
alter table tsdb.test1 rename tag tag1 to tag11;
alter table tsdb.test1 rename tag ts to ts1;
alter table tsdb.test1 rename tag a to b;
show columns from tsdb.test1;
select * from tsdb.test1 order by tag1;
insert into tsdb.test1 (ts,a,tag1,tag22)values ('2024-03-06 10:06:00',1,2,2);
alter table tsdb.test1 rename tag tag22 to tag2;
alter table tsdb.test1 rename tag tag11 to tag1;
select * from tsdb.test1 order by tag1;
drop database tsdb;

--test_case_invalid_name
create ts database "!@#$%%^中文";
create ts database "!@#$%%^";
alter database "!@#$%%^"  rename to "!@#$%%^中文";
drop database "!@#$%%^";
create ts database tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt;
create ts database test;
alter database test rename to tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt;

create table test.t1(ts timestamp not null, "!@#$%%^中文" int) tags(tag1 int not null,tag2 int) primary tags(tag1);
create table test.t1(ts timestamp not null, a int) tags(tag1 int not null,tag2 int) primary tags(tag1);
alter table test.t1 rename column a to "!@#$%%^中文";
alter table test.t1 rename column a to ttttttttttttttttttttttttttttttt;
alter table test.t1 rename tag tag1 to "!@#$%%^中文";
alter table test.t1 rename tag tag1 to ttttttttttttttttttttttttttttttt;
drop database test;

--test_case_exist_name
DROP DATABASE IF EXISTS test CASCADE;
CREATE TS DATABASE test ;
CREATE TABLE test.tb1 (k_timestamp timestamptz not null, e1 int2 , e2 int4 , e3 double) tags (code1 bool not null, code2 int2 not null, code3 double) primary tags (code1, code2);
ALTER TABLE test.tb1 RENAME TAG code1 to code2;
ALTER TABLE test.tb1 RENAME TAG code3 to e1;
ALTER TABLE test.tb1 RENAME COLUMN e1 to e2;
ALTER TABLE test.tb1 RENAME COLUMN e1 to code1;
drop database test;

