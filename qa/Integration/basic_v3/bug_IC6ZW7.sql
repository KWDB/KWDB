drop database if exists tsdb cascade;
create ts database tsdb;
create table tsdb.t1(
  k_timestamp timestamp not null,
  c1 varbytes)
attributes (
  tag1 varchar not null)
primary tags(tag1);

insert into tsdb.t1 values ('2023-04-10 07:18:30+00:00', b'_39247', 'device_1');
insert into tsdb.t1 values ('2023-04-10 07:18:40+00:00', b'\_39247', 'device_1');
insert into tsdb.t1 values ('2023-04-10 07:18:50+00:00', b'\\_39247', 'device_1');

select * from tsdb.t1;

drop database tsdb cascade;