CREATE ts DATABASE d_test PARTITION INTERVAL 100000000000000 YEAR;
CREATE ts DATABASE d_test PARTITION INTERVAL 1001Y;
CREATE ts DATABASE d_test PARTITION INTERVAL 13000MONTH;
CREATE ts DATABASE d_test PARTITION INTERVAL 13000MON;
CREATE ts DATABASE d_test PARTITION INTERVAL 100000000000000W;
CREATE ts DATABASE d_test PARTITION INTERVAL 100000000000000WEEK;
CREATE ts DATABASE d_test PARTITION INTERVAL 100000000000000DAY;
CREATE ts DATABASE d_test PARTITION INTERVAL 100000000000000D;
CREATE ts DATABASE d_test PARTITION INTERVAL 100000000000000H;
CREATE ts DATABASE d_test PARTITION INTERVAL 100000000000000HOUR;
CREATE ts DATABASE d_test PARTITION INTERVAL 100000000000000M;
CREATE ts DATABASE d_test PARTITION INTERVAL 100000000000000MINUTE;
CREATE ts DATABASE d_test PARTITION INTERVAL 100000000000000S;
CREATE ts DATABASE d_test PARTITION INTERVAL 100000000000000SECOND;

create ts database tsdb PARTITION INTERVAL 2d;
alter ts database tsdb set PARTITION INTERVAL = 10d;
create table tsdb.t(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag);
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) PARTITION INTERVAL 15d;

SELECT PARTITION_INTERVAL from tsdb.information_schema.schemata where schema_name='public' order by RETENTIONS;
SHOW CREATE TABLE tsdb.t;
alter table tsdb.t set PARTITION INTERVAL = 25d;
DROP database tsdb cascade;

create ts database tsdb PARTITION INTERVAL 1d;
SELECT PARTITION_INTERVAL from tsdb.information_schema.schemata where schema_name='public' order by RETENTIONS;
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag);
SHOW CREATE TABLE tsdb.t1;
insert into tsdb.t1 values('2019-12-11 01:23:05', 1, 100);
insert into tsdb.t1 values('2020-12-12 11:23:28', 1, 400);
insert into tsdb.t1 values('2021-12-16 01:23:49', 1, 100);
insert into tsdb.t1 values('2022-01-16 01:23:51', 1, 100);
insert into tsdb.t1 values('2021-02-16 01:23:39', 1, 200);
insert into tsdb.t1 values('2023-03-16 01:23:53', 1, 100);
insert into tsdb.t1 values('2020-11-16 01:23:24', 1, 800);
insert into tsdb.t1 values('2020-10-16 01:23:19', 1, 100);
insert into tsdb.t1 values('2020-09-16 01:23:13', 1, 100);
insert into tsdb.t1 values('2020-12-11 01:23:26', 1, 600);
insert into tsdb.t1 values('2020-12-12 11:23:29', 1, 100);
insert into tsdb.t1 values('2020-12-16 01:23:32', 1, 100);
insert into tsdb.t1 values('2021-01-16 01:23:37', 1, 100);
insert into tsdb.t1 values('2021-02-16 01:23:40', 1, 300);
insert into tsdb.t1 values('2021-03-16 01:23:43', 1, 100);
insert into tsdb.t1 values('2020-11-16 01:23:25', 1, 500);
insert into tsdb.t1 values('2020-10-16 01:23:20', 1, 100);
insert into tsdb.t1 values('2020-09-16 01:23:14', 1, 100);

select * from tsdb.t1 order by ts;

DROP database tsdb cascade;
