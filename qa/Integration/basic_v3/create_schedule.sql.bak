create ts database tsdb;
create table tsdb.t1(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
create table tsdb.t2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
insert into tsdb.t1 values('2024-01-24 10:33:36',101,1);
select * from tsdb.t1;
create schedule s1 for sql 'insert into tsdb.t111 select * from tsdb.t2' recurring '@hourly' with schedule options first_run=now;
create schedule s1 for sql 'insert into tsdb.t2 select * from tsdb.t1' recurring '@hourly' with schedule options first_run=now;
select pg_sleep(60);
select * from tsdb.t2;
select name from [show schedules] order by name;
drop schedule s1;
select name from [show schedules] order by name;
create schedule s1 for sql 'insert into tsdb.t2 values(1,1)' recurring '@hourly';
create schedule s1 for sql 'insert into tsdb.t2(ts,a) select * from tsdb.t1' recurring '@hourly';
create schedule s1 for sql 'insert into tsdb.t2(ts,a) select ts,a from tsdb.t1' recurring '@hourly';
create schedule s1 for sql 'insert into tsdb.t2 select ts,a,tag1 from tsdb.t1' recurring '@hourly';
DROP SCHEDULE s1;
drop database tsdb;


