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

create ts database tsdb PARTITION INTERVAL 10d;
alter ts database tsdb set PARTITION INTERVAL = 15S;
alter ts database tsdb set PARTITION INTERVAL = 15SECOND;
alter ts database tsdb set PARTITION INTERVAL = 15MINUTE;
alter ts database tsdb set PARTITION INTERVAL = 15M;
alter ts database tsdb set PARTITION INTERVAL = 15H;
alter ts database tsdb set PARTITION INTERVAL = 15HOUR;
alter ts database tsdb set PARTITION INTERVAL = 100000000000000D;
alter ts database tsdb set PARTITION INTERVAL = 1001Y;
alter ts database tsdb set PARTITION INTERVAL = 13000MON;
alter ts database tsdb set PARTITION INTERVAL = 100000000000000WEEK;
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) PARTITION INTERVAL 100000000000000 YEAR;
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) PARTITION INTERVAL 1001Y;
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) PARTITION INTERVAL 13000MONTH;
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) PARTITION INTERVAL 100000000000000W;
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) PARTITION INTERVAL 100000000000000DAY;
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) PARTITION INTERVAL 100000000000000H;
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) PARTITION INTERVAL 100000000000000M;
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) PARTITION INTERVAL 100000000000000S;

create table tsdb.t(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag); --t: 10d

alter table tsdb.t set PARTITION INTERVAL = 15S;
alter table tsdb.t set PARTITION INTERVAL = 15SECOND;
alter table tsdb.t set PARTITION INTERVAL = 15MINUTE;
alter table tsdb.t set PARTITION INTERVAL = 15M;
alter table tsdb.t set PARTITION INTERVAL = 15H;
alter table tsdb.t set PARTITION INTERVAL = 15HOUR;
alter table tsdb.t set PARTITION INTERVAL = 100000000000000D;
alter table tsdb.t set PARTITION INTERVAL = 1001Y;
alter table tsdb.t set PARTITION INTERVAL = 13000MON;
alter table tsdb.t set PARTITION INTERVAL = 100000000000000WEEK;

create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag) PARTITION INTERVAL 15d;  --t1: 15d
SELECT PARTITION_INTERVAL from tsdb.information_schema.schemata where schema_name='public' order by RETENTIONS;
SHOW CREATE TABLE tsdb.t;
SHOW CREATE TABLE tsdb.t1;
alter ts database tsdb set PARTITION INTERVAL = 15d; -- tsdb, t1: 15d, t:10d
SELECT PARTITION_INTERVAL from tsdb.information_schema.schemata where schema_name='public' order by RETENTIONS;
SHOW CREATE TABLE tsdb.t;
SHOW CREATE TABLE tsdb.t1;
alter table tsdb.t1 set PARTITION INTERVAL = 25d;  -- t1: 25d
create table tsdb.t2(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag); --t2: 15d
SHOW CREATE TABLE tsdb.t1;
SHOW CREATE TABLE tsdb.t2;
alter table tsdb.t2 set PARTITION INTERVAL = 10s;  -- t2: 15d
SHOW CREATE TABLE tsdb.t2;
DROP database tsdb cascade;

create ts database tsdb;
SELECT PARTITION_INTERVAL from tsdb.information_schema.schemata where schema_name='public' order by RETENTIONS;
create table tsdb.t1(ts timestamp not null, a int) tags(ptag int not null) primary tags(ptag);  --t1: 1d
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

alter table tsdb.t1 set PARTITION INTERVAL = 10d;
SHOW CREATE TABLE tsdb.t1;
insert into tsdb.t1 values('2019-12-04 01:23:04', 2, 200);
insert into tsdb.t1 values('2020-12-11 11:23:27', 2, 200);
insert into tsdb.t1 values('2020-12-14 01:23:30', 2, 200);
insert into tsdb.t1 values('2020-12-15 01:23:31', 2, 200);
insert into tsdb.t1 values('2021-12-16 11:23:50', 2, 200);
insert into tsdb.t1 values('2020-12-20 01:23:33', 2, 200);
insert into tsdb.t1 values('2020-12-30 01:23:34', 2, 100);
insert into tsdb.t1 values('2021-01-02 01:23:35', 2, 300);
insert into tsdb.t1 values('2021-02-16 01:23:41', 2, 100);
insert into tsdb.t1 values('2023-03-16 01:23:54', 2, 400);
insert into tsdb.t1 values('2020-11-01 01:23:21', 2, 100);
insert into tsdb.t1 values('2020-10-12 01:23:18', 2, 500);
insert into tsdb.t1 values('2020-09-28 01:23:17', 2, 100);

alter table tsdb.t1 set PARTITION INTERVAL = 1mon;
SHOW CREATE TABLE tsdb.t1;
insert into tsdb.t1 values('2021-08-04 01:23:47', 3, 100);
insert into tsdb.t1 values('2020-07-11 11:23:08', 3, 700);
insert into tsdb.t1 values('2020-07-12 01:23:11', 3, 100);
insert into tsdb.t1 values('2021-02-15 01:23:38', 3, 600);
insert into tsdb.t1 values('2021-06-16 11:23:45', 3, 400);
insert into tsdb.t1 values('2021-09-20 01:23:48', 3, 100);

alter table tsdb.t1 set PARTITION INTERVAL = 1d;
SHOW CREATE TABLE tsdb.t1;
insert into tsdb.t1 values('2020-08-04 01:23:12', 4, 100);
insert into tsdb.t1 values('2020-07-11 11:23:09', 4, 200);
insert into tsdb.t1 values('2021-07-12 01:23:46', 4, 100);
insert into tsdb.t1 values('2020-02-15 01:23:06', 4, 500);
insert into tsdb.t1 values('2019-06-16 11:23:02', 4, 100);
insert into tsdb.t1 values('2020-09-20 01:23:15', 4, 300);
insert into tsdb.t1 values('2021-01-02 01:23:36', 4, 700);
insert into tsdb.t1 values('2023-02-16 01:23:52', 4, 100);
insert into tsdb.t1 values('2021-03-16 01:23:44', 4, 900);
insert into tsdb.t1 values('2020-11-01 01:23:22', 4, 800);

alter table tsdb.t1 set PARTITION INTERVAL = 2year;
SHOW CREATE TABLE tsdb.t1;
insert into tsdb.t1 values('2018-08-04 01:23:01', 5, 500);
insert into tsdb.t1 values('2020-07-11 11:23:10', 5, 100);
insert into tsdb.t1 values('2019-07-12 01:23:03', 5, 200);
insert into tsdb.t1 values('2020-02-15 01:23:07', 5, 300);
insert into tsdb.t1 values('2024-06-16 11:23:55', 5, 100);
insert into tsdb.t1 values('2020-09-20 01:23:16', 5, 600);
insert into tsdb.t1 values('2026-01-02 01:23:56', 5, 400);
insert into tsdb.t1 values('2021-02-16 01:23:42', 5, 100);
insert into tsdb.t1 values('2027-03-16 01:23:57', 5, 500);
insert into tsdb.t1 values('2020-11-01 01:23:23', 5, 900);

select * from tsdb.t1 order by ts;

DROP database tsdb cascade;