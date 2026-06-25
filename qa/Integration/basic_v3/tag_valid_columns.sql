create ts database benchmark;
CREATE sparse TABLE benchmark.cpu2 (
  k_timestamp TIMESTAMPTZ(6) NOT NULL,
u1 INT8,
u2 INT8,
u3 INT8,
u4 INT8,
u5 INT8,
u6 INT8,
u7 INT8,
u8 INT8,
u9 INT8,
u10 INT8,
u11 INT8,
u12 INT8,
u13 INT8,
u14 INT8,
u15 INT8,
u16 INT8,
u17 INT8,
u18 INT8,
u19 INT8,
u20 INT8) TAGS (
  hostname CHAR(30) NOT NULL
)
PRIMARY TAGS(hostname);
select * from benchmark.cpu2;
select * from benchmark.cpu2 where hostname='host_0';
insert into benchmark.cpu2 (k_timestamp,u1, hostname) values('2022-01-01 00:00:00',59,'host_0');
select * from benchmark.cpu2 where hostname='host_0';
select * from benchmark.cpu2 where hostname='host_1';
select * from benchmark.cpu2;
insert into benchmark.cpu2 (k_timestamp,u1, u2, hostname) values('2022-01-01 00:00:00', 59, 60, 'host_0');
select * from benchmark.cpu2 where hostname='host_0';
select * from benchmark.cpu2 where hostname='host_1';
select * from benchmark.cpu2;

create table benchmark.t1(ts timestamp not null, a int) tags(b int not null) primary tags(b);
insert into benchmark.t1 values(1672531201000, 111, 1);
select * from benchmark.t1 order by ts, b;

drop database benchmark cascade;