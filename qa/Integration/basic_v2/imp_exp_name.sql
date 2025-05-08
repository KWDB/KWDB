--test_case001 basic export and import database with schema support name uppercase;
create ts database "TSDB";
use "TSDB";
create table t1(ts timestamp not null,a int, b int) tags(tag1 int not null,
tag2 int) primary tags(tag1);
insert into t1 values('2022-01-01 00:00:01+00:00',11,11,33,44);
insert into t1 values('2022-01-02 00:00:01+00:00',22,22,33,44);
insert into t1 values('2022-01-03 00:00:01+00:00',33,33,33,44);
insert into t1 values('2022-01-04 00:00:01+00:00',44,44,33,44);
insert into t1 values('2022-01-05 00:00:01+00:00',55,55,44,44);
insert into t1 values('2022-01-06 00:00:01+00:00',66,44,44,44);
insert into t1 values('2022-01-07 00:00:01+00:00',77,44,55,44);
insert into t1 values('2022-01-08 00:00:01+00:00',88,22,66,66);
insert into t1 values('2022-01-09 00:00:01+00:00',99,33,66,77);

export into csv "nodelocal://1/db/tb1" from database "TSDB";
use defaultdb;
drop database "TSDB";
show databases;
import database csv data ("nodelocal://1/db/tb1");
show databases;
use "TSDB";
select * from t1 order by a;


use defaultdb;
create ts database "tsDB";
use "tsDB";
create table t1(ts timestamp not null,a int, b int) tags(tag1 int not null,
tag2 int) primary tags(tag1);
insert into t1 values('2022-01-01 00:00:01+00:00',11,11,33,44);
insert into t1 values('2022-01-02 00:00:01+00:00',22,22,33,44);
insert into t1 values('2022-01-03 00:00:01+00:00',33,33,33,44);
insert into t1 values('2022-01-04 00:00:01+00:00',44,44,33,44);
insert into t1 values('2022-01-05 00:00:01+00:00',55,55,44,44);
insert into t1 values('2022-01-06 00:00:01+00:00',66,44,44,44);
insert into t1 values('2022-01-07 00:00:01+00:00',77,44,55,44);
insert into t1 values('2022-01-08 00:00:01+00:00',88,22,66,66);
insert into t1 values('2022-01-09 00:00:01+00:00',99,33,66,77);

export into csv "nodelocal://1/db/tb2" from database "tsDB";
use defaultdb;
drop database "tsDB";
show databases;
import database csv data ("nodelocal://1/db/tb2");
show databases;
use "tsDB";
select * from t1 order by a;
drop database "tsDB";
drop database "TSDB";

CREATE TS DATABASE db;
USE db;
CREATE TABLE db.t1("time" timestamptz not null,e8 char(1023), e9 nchar(255))tags(i int not null) primary tags(i);
INSERT INTO db.t1 values('2023-12-1 12:00:12.000+00:00',null, '测试',1);
EXPORT INTO CSV "nodelocal://1/time" FROM TABLE db.t1;
IMPORT INTO db.t1("time",e8,e9,i) CSV DATA  ("nodelocal://1/time/");
DROP DATABASE db;
