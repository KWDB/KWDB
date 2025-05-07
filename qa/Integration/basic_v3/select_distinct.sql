drop database if exists tsdb_distinct CASCADE;
create ts database tsdb_distinct;
create table tsdb_distinct.t1(ts timestamp not null,a int, b int, c char(32), d varchar(32), e varbytes(32)) tags(tag1 int not null, tag2 int) primary tags(tag1);
insert into tsdb_distinct.t1 values('2018-10-10 10:00:00',11,11,'char11','varchar11',b'\x00\x00\x00\xab', 33,44);
insert into tsdb_distinct.t1 values('2018-10-10 10:00:01',22,22,'char22','varchar22',b'\x00\x00\x00\xab', 33,44);
insert into tsdb_distinct.t1 values('2018-10-10 10:00:02',11,11,'char11','varchar11',b'\x00\x00\x00\xab', 33,44);
insert into tsdb_distinct.t1 values('2018-10-10 10:00:03',22,22,'char22','varchar22',b'\x00\x00\x00\xab', 33,44);
insert into tsdb_distinct.t1 values('2018-10-10 10:00:04',33,66,null,null,null,55,44);
insert into tsdb_distinct.t1 values('2018-10-10 10:00:05',33,55,'char33','varchar33',b'\x00\x00\x00\xac', 44,44);
insert into tsdb_distinct.t1 values('2018-10-10 10:00:06',22,44,'char33','varchar33',b'\x00\x00\x00\xac', 44,44);
insert into tsdb_distinct.t1 values('2018-10-10 10:00:07',33,55,null,null,null,55,44);

select distinct a from tsdb_distinct.t1 order by a;
select distinct c from tsdb_distinct.t1 order by c;
select distinct d from tsdb_distinct.t1 order by d;
select distinct e from tsdb_distinct.t1 order by e;
select sum(distinct b) from tsdb_distinct.t1;
select a, sum(distinct b) from tsdb_distinct.t1 group by a order by a;

create table tsdb_distinct.t2(ts timestamp not null, deviceid int) tags(tag1 int not null) primary tags(tag1);

insert into tsdb_distinct.t2 (ts, deviceid, tag1) VALUES ('2023-07-04 01:01:21.019252', 2, 1);
insert into tsdb_distinct.t2 (ts, deviceid, tag1) VALUES ('2023-07-04 06:58:44.065634', 2, 1);
insert into tsdb_distinct.t2 (ts, deviceid, tag1) VALUES ('2023-07-04 07:21:10.870064', 3, 1);
insert into tsdb_distinct.t2 (ts, deviceid, tag1) VALUES ('2023-07-04 15:58:21.088385', 6, 1);
insert into tsdb_distinct.t2 (ts, deviceid, tag1) VALUES ('2023-07-05 03:43:25.819149', 3, 2);

select distinct deviceid from tsdb_distinct.t2 order by deviceid;
drop database if exists tsdb_distinct CASCADE;
