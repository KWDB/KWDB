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

CREATE TS DATABASE IF NOT EXISTS test_view_ts;
CREATE DATABASE IF NOT EXISTS test_view_rel;
CREATE TABLE test_view_ts.sensor_data(ts TIMESTAMP(3) NOT NULL, temperature DOUBLE, humidity INT4, status VARCHAR(16)) TAGS(device_id VARCHAR(16) NOT NULL, site VARCHAR(16)) PRIMARY TAGS(device_id);
CREATE TABLE test_view_rel.device_meta(device_id VARCHAR(16) PRIMARY KEY, owner VARCHAR(32), active BOOL, install_date DATE, last_reading TIMESTAMP, battery_level DECIMAL(5,2), location GEOMETRY);
CREATE TABLE test_view_rel.site_meta(site VARCHAR(16) PRIMARY KEY, region VARCHAR(32), area DECIMAL(10,2), manager VARCHAR(32), contact_phone VARCHAR(16));
CREATE TABLE test_view_rel.sensor_type(sensor_type_id INT4 PRIMARY KEY, sensor_name VARCHAR(32), unit VARCHAR(16), precision INT2, valid_range JSONB, calibration_info JSONB);

CREATE VIEW test_view_ts.v_four_join AS SELECT s.ts, s.temperature, s.humidity, s.status, d.device_id, d.owner, d.active, d.battery_level, d.install_date, st.region, st.area, st.manager, sn.sensor_name, sn.unit FROM test_view_ts.sensor_data s JOIN test_view_rel.device_meta d ON s.device_id = d.device_id JOIN test_view_rel.site_meta st ON s.site = st.site JOIN test_view_rel.sensor_type sn ON sn.sensor_type_id = 1 WHERE d.active = true AND s.temperature >= 22 ORDER BY s.ts, s.device_id;
SELECT device_id, owner, region, manager, sensor_name, COUNT(*) AS readings FROM test_view_ts.v_four_join GROUP BY device_id, owner, region, manager, sensor_name ORDER BY device_id;

CREATE TS DATABASE IF NOT EXISTS test_comment;
use test_comment;
CREATE TABLE IF NOT EXISTS test_comment.t1(
                                              k_timestamp TIMESTAMPTZ NOT NULL,
                                              id INT NOT NULL,
                                              e1 INT2,
                                              e2 INT,
                                              e3 INT8,
                                              e4 FLOAT4,
                                              e5 FLOAT8,
                                              e6 BOOL,
                                              e7 TIMESTAMPTZ,
                                              e8 CHAR(1023),
    e9 NCHAR(255),
    e10 VARCHAR(4096),
    e11 CHAR,
    e12 CHAR(255),
    e13 NCHAR,
    e14 NVARCHAR(4096),
    e15 VARCHAR(1023),
    e16 NVARCHAR(200),
    e17 NCHAR(255),
    e18 CHAR(200),
    e19 VARBYTES,
    e20 VARBYTES(60),
    e21 VARCHAR,
    e22 NVARCHAR
    ) ATTRIBUTES (code1 INT2 NOT NULL,code2 INT,code3 INT8,code4 FLOAT4 ,code5 FLOAT8,code6 BOOL,code7 VARCHAR,code8 VARCHAR(128) NOT NULL,code9 VARBYTES,code10 VARBYTES(60),code11 VARCHAR,code12 VARCHAR(60),code13 CHAR(2),code14 CHAR(1023) NOT NULL,code15 NCHAR,code16 NCHAR(254) NOT NULL)
    PRIMARY TAGS(code1,code14,code8,code16);

INSERT INTO test_comment.t1 VALUES(0,1,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');
INSERT INTO test_comment.t1 VALUES(1,2,0,0,0,0,0,true,999999,'          ','          ','          ',' ','          ',' ',' ','          ','          ','          ',' ',' ','          ','          ','          ',0,0,0,0,0,TRUE,'          ',' ',' ','          ','          ','          ','  ','          ',' ','          ');
INSERT INTO test_comment.t1 VALUES('1976-10-20 12:00:12.123',3,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',-10001,10000001,-100000000001,1047200.00312001,-1109810.113011921,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST3-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST3-14','中','test数据库语法查询测试！！！@TEST3-16');
INSERT INTO test_comment.t1 VALUES('1979-2-28 11:59:01.999',4,20002,20000002,200000000002,-20873209.0220322201,-22012110.113011921,false,123,'test数据库语法查询测试！！！@TEST4-8','test数据库语法查询测试！！！@TEST4-9','test数据库语法查询测试！！！@TEST4-10','t','test数据库语法查询测试！！！@TEST4-12','中','test数据库语法查询测试！！！@TEST4-14','test数据库语法查询测试！！！@TEST4-15','test数据库语法查询测试！TEST4-16xaa','test数据库语法查询测试！！！@TEST4-17','test数据库语法查询测试！！！@TEST4-18',b'\xca','test数据库语法查询测试！！！@TEST4-20','test数据库语法查询测试！！！@TEST4-21','test数据库语法查询测试！！！@TEST4-22',20002,-20000002,200000000002,-20873209.0220322201,22012110.113011921,true,'test数据库语法查询测试！！！@TEST4-7','test数据库语法查询测试！！！@TEST4-8',b'\xbb','test数据库语法查询测试！！！@TEST4-10','test数据库语法查询测试！！！@TEST4-11','test数据库语法查询测试！！！@TEST4-12','t3','test数据库语法查询测试！！！@TEST4-14','中','test数据库语法查询测试！！！@TEST4-16');
INSERT INTO test_comment.t1 VALUES(318193261000,5,30003,30000003,300000000003,-33472098.11312001,-39009810.333011921,true,'2015-3-12 10:00:00.234','test数据库语法查询测试！！！@TEST5-8','test数据库语法查询测试！！！@TEST5-9','test数据库语法查询测试！！！@TEST5-10','t','test数据库语法查询测试！！！@TEST5-12','中','test数据库语法查询测试！！！@TEST5-14','test数据库语法查询测试！！！@TEST5-15','test数据库语法查询测试！TEST5-16xaa','test数据库语法查询测试！！！@TEST5-17','test数据库语法查询测试！！！@TEST5-18',b'\xca','test数据库语法查询测试！！！@TEST5-20','test数据库语法查询测试！！！@TEST5-21','test数据库语法查询测试！！！@TEST5-22',-30003,30000003,-300000000003,33472098.11312001,-39009810.333011921,false,'test数据库语法查询测试！！！@TEST5-7','test数据库语法查询测试！！！@TEST5-8',b'\xcc','test数据库语法查询测试！！！@TEST5-10','test数据库语法查询测试！！！@TEST5-11','test数据库语法查询测试！！！@TEST5-12','t3','test数据库语法查询测试！！！@TEST5-14','中','test数据库语法查询测试！！！@TEST5-16');
INSERT INTO test_comment.t1 VALUES(318993291090,6,-10001,10000001,-100000000001,1047200.00312001,1109810.113011921,false,'2023-6-23 05:00:00.55','test数据库语法查询测试！！！@TEST6-8','test数据库语法查询测试！！！@TEST6-9','test数据库语法查询测试！！！@TEST6-10','t','test数据库语法查询测试！！！@TEST6-12','中','test数据库语法查询测试！！！@TEST6-14','test数据库语法查询测试！！！@TEST6-15','test数据库语法查询测试！TEST6-16xaa','test数据库语法查询测试！！！@TEST6-17','test数据库语法查询测试！！！@TEST6-18',b'\xca','test数据库语法查询测试！！！@TEST6-20','test数据库语法查询测试！！！@TEST6-21','test数据库语法查询测试！！！@TEST6-22',10001,-10000001,100000000001,424721.022311,4909810.11301191,true,'test数据库语法查询测试！！！@TEST6-7','test数据库语法查询测试！！！@TEST6-8',b'\xdd','test数据库语法查询测试！！！@TEST6-10','test数据库语法查询测试！！！@TEST6-11','test数据库语法查询测试！！！@TEST6-12','t3','test数据库语法查询测试！！！@TEST6-14','中','test数据库语法查询测试！！！@TEST6-16');
INSERT INTO test_comment.t1 VALUES(318995291029,7,-20002,20000002,-200000000002,20873209.0220322201,22012110.113011921,true,'2016-7-17 20:12:00.12','test数据库语法查询测试！！！@TEST7-8','test数据库语法查询测试！！！@TEST7-9','test数据库语法查询测试！！！@TEST7-10','t','test数据库语法查询测试！！！@TEST7-12','中','test数据库语法查询测试！！！@TEST7-14','test数据库语法查询测试！！！@TEST7-15','test数据库语法查询测试！TEST7-16xaa','test数据库语法查询测试！！！@TEST7-17','test数据库语法查询测试！！！@TEST7-18',b'\xca','test数据库语法查询测试！！！@TEST7-20','test数据库语法查询测试！！！@TEST7-21','test数据库语法查询测试！！！@TEST7-22',-20002,20000002,-200000000002,555500.0055505,55505532.553015321,false,'test数据库语法查询测试！！！@TEST7-7','test数据库语法查询测试！！！@TEST7-8',b'\xee','test数据库语法查询测试！！！@TEST7-10','test数据库语法查询测试！！！@TEST7-11','test数据库语法查询测试！！！@TEST7-12','t3','test数据库语法查询测试！！！@TEST7-14','中','test数据库语法查询测试！！！@TEST7-16');
INSERT INTO test_comment.t1 VALUES(318995302501,8,-30003,30000003,-300000000003,33472098.11312001,39009810.333011921,false,4565476,'test数据库语法查询测试！！！@TEST8-8','test数据库语法查询测试！！！@TEST8-9','test数据库语法查询测试！！！@TEST8-10','t','test数据库语法查询测试！！！@TEST8-12','中','test数据库语法查询测试！！！@TEST8-14','test数据库语法查询测试！！！@TEST8-15','test数据库语法查询测试！TEST8-16xaa','test数据库语法查询测试！！！@TEST8-17','test数据库语法查询测试！！！@TEST8-18',b'\xca','test数据库语法查询测试！！！@TEST8-20','test数据库语法查询测试！！！@TEST8-21','test数据库语法查询测试！！！@TEST8-22',30003,-30000003,300000000003,6900.0012345,6612.1215,true,'test数据库语法查询测试！！！@TEST8-7','test数据库语法查询测试！！！@TEST8-8',b'\xff','test数据库语法查询测试！！！@TEST8-10','test数据库语法查询测试！！！@TEST8-11','test数据库语法查询测试！！！@TEST8-12','t3','test数据库语法查询测试！！！@TEST8-14','中','test数据库语法查询测试！！！@TEST8-16');
INSERT INTO test_comment.t1 VALUES('2001-12-9 09:48:12.30',9,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,30000,null,null,null,null,true,null,'test数据库语法查询测试！！！@TESTnull',null,null,null,null,null,'test数据库语法查询测试！！！@TESTnull',null,'test数据库语法查询测试！！！@TESTnull');
INSERT INTO test_comment.t1 VALUES('2002-2-22 10:48:12.899',10,32767,-2147483648,9223372036854775807,-99999999991.9999999991,9999999999991.999999999991,true,'2020-10-1 12:00:01.0','test数据库语法查询测试！！！@TEST10-8','test数据库语法查询测试！！！@TEST10-9','test数据库语法查询测试！！！@TEST10-10','t','test数据库语法查询测试！！！@TEST10-12','中','test数据库语法查询测试！！！@TEST10-14','test数据库语法查询测试！！！@TEST10-15','test数据库语法查询测试！TEST10-16xaa','test数据库语法查询测试！！！@TEST10-17','test数据库语法查询测试！！！@TEST10-18',b'\xca','test数据库语法查询测试！！！@TEST10-20','test数据库语法查询测试！！！@TEST10-21','test数据库语法查询测试！！！@TEST10-22',1,111,1111111,1472011.12345,1109810.113011921,false,'test数据库语法查询测试！！！@TEST10-7','test数据库语法查询测试！！！@TEST10-8',b'\xcc','test数据库语法查询测试！！！@TEST10-10','test数据库语法查询测试！！！@TEST10-11','test数据库语法查询测试！！！@TEST10-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16');
INSERT INTO test_comment.t1 VALUES('2003-10-1 11:48:12.1',11,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',2,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST11-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16');
INSERT INTO test_comment.t1 VALUES('2004-9-9 00:00:00.9',12,12000,12000000,120000000000,-12000021.003125,-122209810.1131921,true,'2129-3-1 12:00:00.011','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','t','aaaaaabbbbbbcccccc','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','c','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'aaaaaabbbbbbcccccc','b','z','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','aaaaaabbbbbbcccccc','ty','aaaaaabbbbbbcccccc','u','aaaaaabbbbbbcccccc');
INSERT INTO test_comment.t1 VALUES('2004-12-31 12:10:10.911',13,23000,23000000,230000000000,-23000088.665120604,-122209810.1131921,true,'2020-12-31 23:59:59.999','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','T','SSSSSSDDDDDDKKKKKK','B','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','V','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK',20002,20000002,200000000002,1047200.00312001,1109810.113011921,false,'SSSSSSDDDDDDKKKKKK','O','P','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','SSSSSSDDDDDDKKKKKK','WL','SSSSSSDDDDDDKKKKKK','N','SSSSSSDDDDDDKKKKKK');
INSERT INTO test_comment.t1 VALUES('2008-2-29 2:10:10.111',14,32767,34000000,340000000000,-43000079.07812032,-122209810.1131921,true,'1975-3-11 00:00:00.0','1234567890987654321','1234567890987654321','1234567890987654321','1','1234567890987654321','2','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','1234567890987654321','9','1234567890987654321','1234567890987654321','1234567890987654321',-10001,-10000001,-100000000001,1047200.00312001,1109810.113011921,false,'1234567890987654321','8','7','1234567890987654321','1234567890987654321','1234567890987654321','65','1234567890987654321','4','1234567890987654321');
INSERT INTO test_comment.t1 VALUES('2012-02-29 1:10:10.000',15,-32767,-34000000,-340000000000,43000079.07812032,122209810.1131921,true,'2099-9-1 11:01:00.111','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','1','数据库语法查询测试','2','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','9','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试',10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'数据库语法查询测试','8','7','数据库语法查询测试','数据库语法查询测试','数据库语法查询测试','65','数据库语法查询测试','4','数据库语法查询测试');
INSERT INTO test_comment.t1 VALUES(1344618710110,16,11111,-11111111,111111111111,-11111.11111,11111111.11111111,false,'2017-12-11 09:10:00.200',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',0,0,0,0,0,false,e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'',e'\'');
INSERT INTO test_comment.t1 VALUES(1374618710110,17,-11111,11111111,-111111111111,11111.11111,-11111111.11111111,true,'2036-2-3 10:10:00.089',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',0,0,0,0,0,false,e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ',e'\ ');
INSERT INTO test_comment.t1 VALUES(1574618710110,18,22222,-22222222,222222222222,-22222.22222,22222222.22222222,false,'2012-1-1 12:12:00.049' ,e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',0,0,0,0,0,false,e'\\\\\\\\',e'\\\\\\\\',e'\ ',e'\\\\\\\\',e'\\\\\\\\',e'\\\\\\\\',e'\\',e'\\\\\\\\',e'\\',e'\\\\\\\\');
INSERT INTO test_comment.t1 VALUES(1874618710110,19,-22222,22222222,-222222222222,22222.22222,-22222222.22222222,true,'1980-6-27 19:17:00.123','\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,e'\\'  ,'\\\\\\\\' ,e'\\'  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,0,0,0,0,0,false,'\\\\\\\\' ,'\\\\\\\\' ,' '  ,'\\\\\\\\' ,'\\\\\\\\' ,'\\\\\\\\' ,'\ ' ,'\\\\\\\\' ,e'\\'  ,'\\\\\\\\');
INSERT INTO test_comment.t1 VALUES(9223372036000,20,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,e'\\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,e'\\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,e'\\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');

CREATE VIEW IF NOT EXISTS test_comment.v_ts_full AS
SELECT * FROM test_comment.t1;
SELECT * FROM v_ts_full ORDER BY id;

drop database test_comment cascade;