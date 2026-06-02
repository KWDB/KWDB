drop database if exists test_ts_view_rel cascade;
drop database if exists test_ts_view cascade;

create ts database test_ts_view;
use test_ts_view;

create table sensor_data(ts timestamp not null, temperature float8, humidity int4, status varchar(16))
tags (device_id varchar(16) not null, site varchar(16))
primary tags(device_id);

create table sensor_event(ts timestamp not null, event_level int4, event_name varchar(24))
tags (device_id varchar(16) not null)
primary tags(device_id);

insert into sensor_data values
('2024-01-01 00:00:00', 21.5, 40, 'ok', 'd1', 'north'),
('2024-01-01 00:01:00', 22.0, 42, 'ok', 'd1', 'north'),
('2024-01-01 00:02:00', 18.0, 55, 'warn', 'd2', 'south');

insert into sensor_event values
('2024-01-01 00:01:00', 2, 'threshold', 'd1'),
('2024-01-01 00:02:00', 3, 'offline', 'd2'),
('2024-01-01 00:05:00', 1, 'restore', 'd5');

create view v_sensor_basic as
select ts, temperature, humidity, device_id, site
from sensor_data
where temperature >= 20;

select * from v_sensor_basic order by ts, device_id;

alter table sensor_data add column voltage float8;
alter table sensor_data add tag line varchar(16);

insert into sensor_data values
('2024-01-01 00:03:00', 23.5, 39, 'ok', 220.5, 'd3', 'north', 'l1'),
('2024-01-01 00:04:00', 24.2, 38, 'ok', 221.0, 'd4', 'north', 'l2');

insert into sensor_event values ('2024-01-01 00:03:00', 2, 'overheat', 'd3');

select * from v_sensor_basic order by ts, device_id;
show create view v_sensor_basic;

create view v_sensor_after_alter as
select ts, temperature, voltage, device_id, site, line
from sensor_data
where line is not null;

select * from v_sensor_after_alter order by ts, device_id;

create view v_sensor_agg as
select count(*) as cnt, max(temperature) as max_temperature, max(voltage) as max_voltage, device_id
from sensor_data
group by device_id;

select * from v_sensor_agg order by device_id;

create view v_sensor_event_join as
select d.ts, d.temperature, d.device_id, e.event_level, e.event_name
from sensor_data d
join sensor_event e on d.device_id = e.device_id and d.ts = e.ts
where e.event_level >= 2;

select * from v_sensor_event_join order by ts, device_id;

create database test_ts_view_rel;
use test_ts_view_rel;

create table device_meta(device_id varchar(16) primary key, owner varchar(32), active bool);
create table site_meta(site varchar(16) primary key, region varchar(16));

insert into device_meta values
('d1', 'team-a', true),
('d2', 'team-b', false),
('d3', 'team-c', true),
('d4', 'team-d', true);

insert into site_meta values ('north', 'r1'), ('south', 'r2');

create view v_sensor_rel_join as
select d.ts, d.temperature, d.voltage, d.device_id, m.owner, s.region
from test_ts_view.sensor_data d
join device_meta m on d.device_id = m.device_id
join site_meta s on d.site = s.site
where m.active = true;

select * from v_sensor_rel_join order by ts, device_id;
show create view v_sensor_rel_join;

use test_ts_view;

create view v_tsdb_rel_join as
select d.ts, d.temperature, d.device_id, m.owner
from sensor_data d
join test_ts_view_rel.device_meta m on d.device_id = m.device_id
where d.temperature >= 22;

select * from v_tsdb_rel_join order by ts, device_id;

show create view v_tsdb_rel_join;

alter view v_tsdb_rel_join rename to v_tsdb_rel_join_new;
select * from v_tsdb_rel_join_new order by ts, device_id;
show tables;
show create view v_tsdb_rel_join_new;

create materialized view m_view as
select d.ts, d.temperature, d.device_id, m.owner
from sensor_data d
join test_ts_view_rel.device_meta m on d.device_id = m.device_id
where d.temperature >= 22;

create temp view t_view as
select d.ts, d.temperature, d.device_id, m.owner
from sensor_data d
join test_ts_view_rel.device_meta m on d.device_id = m.device_id
where d.temperature >= 22;

show tables;

use test_ts_view_rel;
create materialized view m_view as
select * from device_meta;

alter materialized view m_view rename to test_ts_view.m_view_new;

use defaultdb;
drop database test_ts_view_rel cascade;
drop database test_ts_view cascade;
