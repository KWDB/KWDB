drop database if exists test_ts_view_rel cascade;
drop database if exists test_ts_view cascade;

create ts database test_ts_view;
use test_ts_view;

create table sensor_data(ts timestamp not null, temperature float8, humidity int4, status varchar(16))
    tags (device_id varchar(16) not null, site varchar(16))
primary tags(device_id);


insert into sensor_data values
                            ('2024-01-01 00:00:00', 21.5, 40, 'ok', 'd1', 'north'),
                            ('2024-01-01 00:01:00', 22.0, 42, 'ok', 'd1', 'north'),
                            ('2024-01-01 00:02:00', 18.0, 55, 'warn', 'd2', 'south');

insert into sensor_data values
                            ('2024-01-01 00:03:00', 23.5, 39, 'ok', 'd3', 'north'),
                            ('2024-01-01 00:04:00', 24.2, 38, 'ok',  'd4', 'north');

alter table sensor_data add column voltage float8;

create database test_ts_view_rel;
use test_ts_view_rel;

create table device_meta(device_id varchar(16) primary key, owner varchar(32), active bool);

insert into device_meta values
                            ('d1', 'team-a', true),
                            ('d2', 'team-b', false),
                            ('d3', 'team-c', true),
                            ('d4', 'team-d', true);



use test_ts_view;

select d.ts, d.temperature, d.device_id, m.owner
from sensor_data d
         join test_ts_view_rel.device_meta m on d.device_id = m.device_id
where d.temperature >= 22 order by d.ts;

select * from sensor_data where device_id='d1';
select pg_sleep(2);


select * from sensor_data where device_id='d1';
select d.ts, d.temperature, d.device_id, m.owner
from sensor_data d
         join test_ts_view_rel.device_meta m on d.device_id = m.device_id
where d.temperature >= 22 order by d.ts;
use defaultdb;
drop database test_ts_view_rel cascade;
drop database test_ts_view cascade;
