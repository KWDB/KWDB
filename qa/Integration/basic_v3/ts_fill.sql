drop database if exists ts_db cascade;
create ts database ts_db;
use ts_db;

create table metric_ts(k_timestamp timestamp not null, data1 bool, data2 int2, data3 int4, data4 int8, data5 float4, data6 float8,
                       data7 char(4), data8 nchar(4), data9 varchar(4), data10 nvarchar(4), data11 varbytes(4)) tags (device_id char(40) not null, alias char(40)) primary tags(device_id);
create table metric_ts2(k_timestamp timestamp not null, data1 float8, data2 char(40)) tags (device_id char(40) not null, alias char(40)) primary tags(device_id);

-- where error
select * from ts_db.metric_ts FILL(EXACT);
select * from ts_db.metric_ts where device_id = 'device1' FILL(EXACT);
select * from ts_db.metric_ts where k_timestamp = '2025-12-19 10:00:02.200' FILL(EXACT);
select * from ts_db.metric_ts where device_id in ('device1') and k_timestamp = '2025-12-19 10:00:02.200' FILL(EXACT);
select * from ts_db.metric_ts where device_id = 'device1' and k_timestamp = '2025-12-19 10:00:02.200' and data2=22 FILL(EXACT);

-- group by error
select * from ts_db.metric_ts where device_id = 'device1' and k_timestamp = '2025-12-19 10:00:02.200' FILL(EXACT) group by k_timestamp;

-- projection error
select max(k_timestamp) from ts_db.metric_ts where device_id = 'device1' and k_timestamp = '2025-12-19 10:00:02.200' FILL(EXACT)
select k_timestamp, data1+data2 from ts_db.metric_ts where device_id = 'device1' and k_timestamp = '2025-12-19 10:00:02.200' FILL(EXACT)

-- table error
select * from (select * from ts_db.metric_ts) where device_id = 'device1' and k_timestamp = '2025-12-19 10:00:02.200' FILL(EXACT);

-- join error
select t1.k_timestamp, t1.data1, t1.data2 from ts_db.metric_ts t1, ts_db.metric_ts2 t2 where t1.device_id = 'device1' and t1.k_timestamp = '2025-12-19 10:00:02.200' FILL(EXACT);

-- union error
select * from ts_db.metric_ts2 union (select * from ts_db.metric_ts where device_id = 'device1' and k_timestamp = '2025-12-19 10:00:02.200' FILL(EXACT));

-- with error
with sql1 as (select * from ts_db.metric_ts where k_timestamp = '2025-12-19 10:00:02.15' and device_id = 'device2' FILL(previous)) select * from sql1;

-- from subquery
select * from (select * from ts_db.metric_ts) where k_timestamp='2026-01-01' and device_id='device1' FILL(EXACT);

-- uncorrelated Subquery
select * from  ts_db.metric_ts where k_timestamp='2026-01-01' and device_id=(select device_id from ts_db.metric_ts2 limit 1) FILL(EXACT);

-- correlated Subquery
select * from  ts_db.metric_ts t1 where k_timestamp='2026-01-01' and device_id=(select device_id from ts_db.metric_ts2 t2 where t1.k_timestamp=t2.k_timestamp limit 1) FILL(EXACT);


-- EXACT FILL
insert into metric_ts values('2025-12-19 10:00:02.150', false, 1, 11, 111, 11.11, 111.111, 'd7', 'd8', 'd9', 'd10', 'd11', 'device1', 'tianjin');
insert into metric_ts values('2025-12-19 10:00:02.190', true, 2, 22, 222, 22.22, 222.222, 'd71', 'd81', 'd91', 'd101', 'd102', 'device1', 'tianjin');
insert into metric_ts values('2025-12-19 10:00:02.480', false, 3, 33, 333, 33.33, 333.333, 'd72', 'd82', 'd92', 'd102', 'd103', 'device1', 'tianjin');

select * from ts_db.metric_ts;
select * from ts_db.metric_ts where device_id = 'device1' and k_timestamp = '2025-12-19 10:00:02.200' FILL(EXACT);
select * from ts_db.metric_ts where device_id = 'device1' and k_timestamp = '2025-12-19 10:00:02.150' FILL(EXACT);

select * from ts_db.metric_ts where device_id = 'device1' and k_timestamp = '2025-12-19 10:00:02.150' FILL(EXACT, 1000);
select * from ts_db.metric_ts where device_id = 'device1' and k_timestamp = '2025-12-19 10:00:02.150' FILL(EXACT, 1000, 1000);
select * from ts_db.metric_ts where device_id = 'device1' and k_timestamp = '2025-12-19 10:00:02.150' FILL(EXACT, 'test_const');
select * from ts_db.metric_ts where device_id = 'device1' and k_timestamp = '2025-12-19 10:00:02.150' FILL(EXACT, 1000, 1000, 'test_const');

-- PREVIOUS FILL
insert into metric_ts values('2025-12-19 10:00:02.050', false, 1, 11, 111, 11.11, 111.111, 'd7', 'd8', 'd9', 'd10', 'd11', 'device22', 'beijing');
insert into metric_ts values('2025-12-19 10:00:02.110', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'device22', 'beijing');
insert into metric_ts values('2025-12-19 10:00:02.150', false, 1, 11, 111, 11.11, 111.111, 'd7', 'd8', 'd9', 'd10', 'd11', 'device2', 'beijing');
insert into metric_ts values('2025-12-19 10:00:02.150', false, 11, 111, 1111, 11.111, 111.1111, 'd71', 'd81', 'd91', 'd101', 'd111', 'device21', 'beijing');

select * from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200';
-- bool
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id = 'device22' and k_timestamp = '2025-12-19 10:00:02.100' FILL(PREVIOUS);
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id;
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 0);
-- int2
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device22' and k_timestamp = '2025-12-19 10:00:02.100' FILL(PREVIOUS);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id;
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 0);
-- int4
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device22' and k_timestamp = '2025-12-19 10:00:02.100' FILL(PREVIOUS);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id;
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 0);
-- int8
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device22' and k_timestamp = '2025-12-19 10:00:02.100' FILL(PREVIOUS);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id;
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 0);
-- float4
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device22' and k_timestamp = '2025-12-19 10:00:02.100' FILL(PREVIOUS);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id;
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 0);
-- float8
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device22' and k_timestamp = '2025-12-19 10:00:02.100' FILL(PREVIOUS);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id;
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 0);
-- char
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id = 'device22' and k_timestamp = '2025-12-19 10:00:02.100' FILL(PREVIOUS);
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 0);
-- nchar
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id = 'device22' and k_timestamp = '2025-12-19 10:00:02.100' FILL(PREVIOUS);
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 0);
-- varchar
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id = 'device22' and k_timestamp = '2025-12-19 10:00:02.100' FILL(PREVIOUS);
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id;
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 0);
-- nvarchar
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id = 'device22' and k_timestamp = '2025-12-19 10:00:02.100' FILL(PREVIOUS);
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id;
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 0);
-- varbytes
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id = 'device22' and k_timestamp = '2025-12-19 10:00:02.100' FILL(PREVIOUS);
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 0);

select * from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
insert into metric_ts values('2025-12-19 10:00:02.190', true, 2, 22, 222, 22.22, 222.222, 'd71', 'd81', 'd91', 'd101', 'd102', 'device2', 'beijing');
insert into metric_ts values('2025-12-19 10:00:02.190', true, 21, 221, 2221, 22.221, 222.2221, 'd711', 'd811', 'd911', 'd111', 'd112', 'device21', 'beijing');
-- bool
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id;
-- int2
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id;
-- int4
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id;
-- int8
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id;
-- float4
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id;
-- float8
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id;
-- char
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id;
-- nchar
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id;
-- varchar
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id;
-- nvarchar
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id;
-- varbytes
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id in ('device2', 'device21') and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id;

select k_timestamp, device_id, data2, data3 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.490';
insert into metric_ts values('2025-12-19 10:00:02.470', false, NULL, 22, 222, 22.22, 222.222, 'd71', 'd81', 'd91', 'd101', 'd102', 'device2', 'beijing');
insert into metric_ts values('2025-12-19 10:00:02.480',true, 2, NULL, 222, 22.22, 222.222, 'd71', 'd81', 'd91', 'd101', 'd102', 'device2', 'beijing');
select k_timestamp, device_id, data2, data3 from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.490' FILL(PREVIOUS, 50);

select * from ts_db.metric_ts where device_id = 'device2';

-- error
select * from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, -1);
select * from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 1000, 1000);
select * from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 'test_const');
select * from ts_db.metric_ts where device_id = 'device2' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 1000, 1000, 'test_const');

-- NEXT FILL
insert into metric_ts values('2025-12-19 10:00:02.150', false, 1, 11, 111, 11.11, 111.111, 'd7', 'd8', 'd9', 'd10', 'd11', 'device3', 'shanghai');
insert into metric_ts values('2025-12-19 10:00:02.150', false, 12, 112, 1112, 11.112, 111.1112, 'd72', 'd82', 'd92', 'd102', 'd112', 'device32', 'shanghai');
insert into metric_ts values('2025-12-19 10:00:02.500', true, 2, 22, 222, 22.22, 222.222, 'd71', 'd81', 'd91', 'd101', 'd102', 'device3', 'shanghai');
insert into metric_ts values('2025-12-19 10:00:02.500', true, 22, 222, 2222, 22.222, 222.2222, 'd712', 'd812', 'd912', 'd121', 'd122', 'device32', 'shanghai');
insert into metric_ts values('2025-12-19 10:00:02.800', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'device33', 'shanghai');
insert into metric_ts values('2025-12-19 10:00:02.900', true, 22, 222, 2222, 22.222, 222.2222, 'd712', 'd812', 'd912', 'd121', 'd122', 'device33', 'shanghai');

select * from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200';
-- bool
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id = 'device33' and k_timestamp = '2025-12-19 10:00:02.800' FILL(NEXT);
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id in ('device3', 'device32') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id;
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 0);
-- int2
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device33' and k_timestamp = '2025-12-19 10:00:02.800' FILL(NEXT);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id in ('device3', 'device32') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id;
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 0);
-- int4
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device33' and k_timestamp = '2025-12-19 10:00:02.800' FILL(NEXT);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id in ('device3', 'device32') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id;
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 0);
-- in8
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device33' and k_timestamp = '2025-12-19 10:00:02.800' FILL(NEXT);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id in ('device3', 'device32') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id;
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 0);
-- float4
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device33' and k_timestamp = '2025-12-19 10:00:02.800' FILL(NEXT);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id in ('device3', 'device32') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id;
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 0);
-- float8
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device33' and k_timestamp = '2025-12-19 10:00:02.800' FILL(NEXT);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id in ('device3', 'device32') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id;
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 0);
-- char
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id = 'device33' and k_timestamp = '2025-12-19 10:00:02.800' FILL(NEXT);
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id in ('device3', 'device32') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 0);
-- nchar
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id = 'device33' and k_timestamp = '2025-12-19 10:00:02.800' FILL(NEXT);
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id in ('device3', 'device32') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 0);
-- varchar
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id = 'device33' and k_timestamp = '2025-12-19 10:00:02.800' FILL(NEXT);
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id in ('device3', 'device32') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id;
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 0);
-- nvarchar
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id = 'device33' and k_timestamp = '2025-12-19 10:00:02.800' FILL(NEXT);
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id in ('device3', 'device32') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id;
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 0);
-- varbytes
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id = 'device33' and k_timestamp = '2025-12-19 10:00:02.800' FILL(NEXT);
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id in ('device3', 'device32') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 0);

select * from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
insert into metric_ts values('2025-12-19 10:00:02.230', false, 3, 33, 333, 33.33, 333.333, 'd72', 'd82', 'd92', 'd102', 'd103', 'device3', 'shanghai');
insert into metric_ts values('2025-12-19 10:00:02.230', false, 33, 333, 3333, 33.333, 333.3333, 'd723', 'd823', 'd923', 'd132', 'd133', 'device33', 'shanghai');
-- bool
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id in ('device3', 'device33') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id;
-- int2
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id in ('device3', 'device33') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id;
-- int4
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id in ('device3', 'device33') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id;
-- int8
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id in ('device3', 'device33') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id;
-- float4
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id in ('device3', 'device33') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id;
-- float8
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id in ('device3', 'device33') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id;
-- char
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id in ('device3', 'device33') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id;
-- nchar
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id in ('device3', 'device33') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id;
-- varchar
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id in ('device3', 'device33') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id;
-- nvarchar
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id in ('device3', 'device33') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id;
-- varbytes
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id in ('device3', 'device33') and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id;

select k_timestamp, device_id, data2, data3 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.480';
insert into metric_ts values('2025-12-19 10:00:02.490', true, 4, NULL, 444, 44.44, 444.444, 'd74', 'd84', 'd94', 'd104', 'd105', 'device3', 'shanghai');
select k_timestamp, device_id, data2, data3 from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.480' FILL(NEXT, 50);

select * from ts_db.metric_ts where device_id = 'device3';

-- error
select * from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, -1);
select * from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 1000, 1000);
select * from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 'test_const');
select * from ts_db.metric_ts where device_id = 'device3' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 1000, 1000, 'test_const');

-- CLOSER FILL
insert into metric_ts values('2025-12-19 10:00:02.150', false, 1, 11, 111, 11.11, 111.111, 'd7', 'd8', 'd9', 'd10', 'd11', 'device4', 'shenzhen');
insert into metric_ts values('2025-12-19 10:00:02.150', false, 13, 113, 1113, 11.113, 111.1113, 'd73', 'd83', 'd93', 'd103', 'd113', 'device43', 'shenzhen');
insert into metric_ts values('2025-12-19 10:00:02.500', true, 2, 22, 222, 22.22, 222.222, 'd71', 'd81', 'd91', 'd101', 'd102', 'device4', 'shenzhen');
insert into metric_ts values('2025-12-19 10:00:02.500', true, 23, 223, 2223, 22.223, 222.2223, 'd713', 'd813', 'd913', 'd131', 'd132', 'device43', 'shenzhen');
insert into metric_ts values('2025-12-19 10:00:02.800', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'device44', 'shenzhen');
insert into metric_ts values('2025-12-19 10:00:02.900', true, 23, 223, 2223, 22.223, 222.2223, 'd713', 'd813', 'd913', 'd131', 'd132', 'device44', 'shenzhen');

select * from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400';
-- bool
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id = 'device44' and k_timestamp = '2025-12-19 10:00:02.800' FILL(CLOSER);
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id;
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER, 0);
-- int2
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device44' and k_timestamp = '2025-12-19 10:00:02.800' FILL(CLOSER);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id;
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER, 0);
-- int4
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device44' and k_timestamp = '2025-12-19 10:00:02.800' FILL(CLOSER);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id;
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER, 0);
-- int8
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device44' and k_timestamp = '2025-12-19 10:00:02.800' FILL(CLOSER);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id;
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER, 0);
-- float4
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device44' and k_timestamp = '2025-12-19 10:00:02.800' FILL(CLOSER);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id;
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER, 0);
-- float8
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device44' and k_timestamp = '2025-12-19 10:00:02.800' FILL(CLOSER);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id;
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER, 0);
-- char
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id = 'device44' and k_timestamp = '2025-12-19 10:00:02.800' FILL(CLOSER);
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER, 0);
-- nchar
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id = 'device44' and k_timestamp = '2025-12-19 10:00:02.800' FILL(CLOSER);
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER, 0);
-- varchar
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id = 'device44' and k_timestamp = '2025-12-19 10:00:02.800' FILL(CLOSER);
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id;
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER, 0);
-- nvarchar
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id = 'device44' and k_timestamp = '2025-12-19 10:00:02.800' FILL(CLOSER);
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id;
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER, 0);
-- varbytes
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id = 'device44' and k_timestamp = '2025-12-19 10:00:02.800' FILL(CLOSER);
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER, 0);

select * from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
insert into metric_ts values('2025-12-19 10:00:02.400', false, 3, 33, 333, 33.33, 333.333, 'd72', 'd82', 'd92', 'd102', 'd103', 'device4', 'shenzhen');
insert into metric_ts values('2025-12-19 10:00:02.400', false, 33, 333, 3333, 33.333, 333.3333, 'd723', 'd823', 'd923', 'd132', 'd133', 'device43', 'shenzhen');
-- bool
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id, data1 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id;
-- int2
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id;
-- int4
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id;
-- int8
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id;
-- float4
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id;
-- float8
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id;
-- char
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id, data7 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id;
-- nchar
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id, data8 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id;
-- varchar
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id, data9 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id;
-- nvarchar
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id, data10 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id;
-- varbytes
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id, data11 from ts_db.metric_ts where device_id in ('device4', 'device43') and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id;

select k_timestamp, device_id, data2, data3 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.450';
insert into metric_ts values('2025-12-19 10:00:02.430', true, 4, NULL, 444, 44.44, 444.444, 'd74', 'd84', 'd94', 'd104', 'd105', 'device4', 'shenzhen');
select k_timestamp, device_id, data2, data3 from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.450' FILL(CLOSER, 100);

select * from ts_db.metric_ts where device_id = 'device4';

-- error
select * from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CLOSER, -1);
select * from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CLOSER, 1000, 1000);
select * from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CLOSER, 'test_const');
select * from ts_db.metric_ts where device_id = 'device4' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CLOSER, 1000, 1000, 'test_const');

-- LINEAR FILL
insert into metric_ts values('2025-12-19 10:00:02.150', false, 1, 11, 111, 11.11, 111.111, 'd7', 'd8', 'd9', 'd10', 'd11', 'device5', 'sichuan');
insert into metric_ts values('2025-12-19 10:00:02.150', false, 14, 114, 1114, 11.114, 111.1114, 'd74', 'd84', 'd94', 'd104', 'd114', 'device54', 'sichuan');
insert into metric_ts values('2025-12-19 10:00:02.400', true, 2, 22, 222, 22.22, 222.222, 'd71', 'd81', 'd91', 'd101', 'd102', 'device5', 'sichuan');
insert into metric_ts values('2025-12-19 10:00:02.400', true, 24, 224, 2224, 22.224, 222.2224, 'd714', 'd814', 'd914', 'd141', 'd142', 'device54', 'sichuan');
insert into metric_ts values('2025-12-19 10:00:02.800', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'device55', 'sichuan');
insert into metric_ts values('2025-12-19 10:00:02.900', true, 24, 224, 2224, 22.224, 222.2224, 'd714', 'd814', 'd914', 'd141', 'd142', 'device55', 'sichuan');

select * from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500';
select * from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);

insert into metric_ts values('2025-12-19 10:00:02.600', false, 3, 33, 333, 33.33, 333.333, 'd72', 'd82', 'd92', 'd102', 'd103', 'device5', 'sichuan');
insert into metric_ts values('2025-12-19 10:00:02.600', false, 34, 334, 3334, 33.334, 333.3334, 'd724', 'd824', 'd924', 'd142', 'd143', 'device54', 'sichuan');
-- int2
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device55' and k_timestamp = '2025-12-19 10:00:02.800' FILL(LINEAR);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR) order by device_id;
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR, 0);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id;
-- int4
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device55' and k_timestamp = '2025-12-19 10:00:02.800' FILL(LINEAR);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR) order by device_id;
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR, 0);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id;
-- int8
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device55' and k_timestamp = '2025-12-19 10:00:02.800' FILL(LINEAR);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR) order by device_id;
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR, 0);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id;
-- float4
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device55' and k_timestamp = '2025-12-19 10:00:02.800' FILL(LINEAR);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR) order by device_id;
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR, 0);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id;
-- float8
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device55' and k_timestamp = '2025-12-19 10:00:02.800' FILL(LINEAR);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR) order by device_id;
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR, 0);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id;
-- NULL
select k_timestamp, device_id, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);
select k_timestamp, device_id, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR) order by device_id;
select k_timestamp, device_id, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR, 0);
select k_timestamp, device_id, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id;

insert into metric_ts values('2025-12-19 10:00:02.170', true, 4, NULL, 444, 44.44, 444.444, 'd74', 'd84', 'd94', 'd104', 'd105', 'device5', 'sichuan');
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id, data2 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id;
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id, data3 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id;
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id, data4 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id;
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id, data5 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id;
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id, data6 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id;
select k_timestamp, device_id, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts where device_id in ('device5', 'device54') and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id;

select * from ts_db.metric_ts where device_id = 'device5';

-- error
select * from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.200' FILL(LINEAR, -1);
select * from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.200' FILL(LINEAR, 0, -1);
select * from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.200' FILL(LINEAR, 1000);
select * from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.200' FILL(LINEAR, 'test_const');
select * from ts_db.metric_ts where device_id = 'device5' and k_timestamp = '2025-12-19 10:00:02.200' FILL(LINEAR, 1000, 1000, 'test_const');

-- CONSTANT FILL
create table metric_ts3(k_timestamp timestamp not null, data1 bool, data2 int2, data3 int4, data4 int8, data5 float4, data6 float8,
                        data7 char(4), data8 nchar(4), data9 varchar(4), data10 nvarchar(4), data11 varbytes(4)) tags (device_id char(40) not null, alias char(40)) primary tags(device_id);
insert into metric_ts3 values('2026-01-01 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'device6', 'guangzhou');
insert into metric_ts3 values('2026-01-01 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'device65', 'guangzhou');

select * from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200';
-- bool
select k_timestamp, device_id, data1 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2026-01-01 00:00:00' FILL(CONSTANT, true);
select k_timestamp, device_id, data1 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id, data1 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id;
select k_timestamp, device_id, data1 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1);
select k_timestamp, device_id, data1 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1) order by device_id;
select k_timestamp, device_id, data1 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const');
select k_timestamp, device_id, data1 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const') order by device_id;
-- int2
select k_timestamp, device_id, data2 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2026-01-01 00:00:00' FILL(CONSTANT, 1);
select k_timestamp, device_id, data2 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1);
select k_timestamp, device_id, data2 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1) order by device_id;
select k_timestamp, device_id, data2 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483600);
select k_timestamp, device_id, data2 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483600) order by device_id;
select k_timestamp, device_id, data2 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id, data2 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id;
select k_timestamp, device_id, data2 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const');
select k_timestamp, device_id, data2 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const') order by device_id;
-- int4
select k_timestamp, device_id, data3 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2026-01-01 00:00:00' FILL(CONSTANT, 1);
select k_timestamp, device_id, data3 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1);
select k_timestamp, device_id, data3 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1) order by device_id;
select k_timestamp, device_id, data3 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483600);
select k_timestamp, device_id, data3 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483600) order by device_id;
select k_timestamp, device_id, data3 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483648);
select k_timestamp, device_id, data3 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483648) order by device_id;
select k_timestamp, device_id, data3 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id, data3 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id;
select k_timestamp, device_id, data3 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const');
select k_timestamp, device_id, data3 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const') order by device_id;
-- int8
select k_timestamp, device_id, data4 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2026-01-01 00:00:00' FILL(CONSTANT, 1);
select k_timestamp, device_id, data4 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1);
select k_timestamp, device_id, data4 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1) order by device_id;
select k_timestamp, device_id, data4 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483600);
select k_timestamp, device_id, data4 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483600) order by device_id;
select k_timestamp, device_id, data4 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483648);
select k_timestamp, device_id, data4 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483648) order by device_id;
select k_timestamp, device_id, data4 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id, data4 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id;
select k_timestamp, device_id, data4 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const');
select k_timestamp, device_id, data4 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const') order by device_id;
-- float4
select k_timestamp, device_id, data5 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2026-01-01 00:00:00' FILL(CONSTANT, 1);
select k_timestamp, device_id, data5 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1);
select k_timestamp, device_id, data5 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1) order by device_id;
select k_timestamp, device_id, data5 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.2);
select k_timestamp, device_id, data5 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.2) order by device_id;
select k_timestamp, device_id, data5 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast(2.2 as float8));
select k_timestamp, device_id, data5 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast(2.2 as float8)) order by device_id;
select k_timestamp, device_id, data5 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id, data5 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id;
select k_timestamp, device_id, data5 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const');
select k_timestamp, device_id, data5 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const') order by device_id;
-- float8
select k_timestamp, device_id, data6 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2026-01-01 00:00:00' FILL(CONSTANT, 1);
select k_timestamp, device_id, data6 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1);
select k_timestamp, device_id, data6 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1) order by device_id;
select k_timestamp, device_id, data6 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.2);
select k_timestamp, device_id, data6 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.2) order by device_id;
select k_timestamp, device_id, data6 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id, data6 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id;
select k_timestamp, device_id, data6 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const');
select k_timestamp, device_id, data6 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const') order by device_id;
-- char
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2026-01-01 00:00:00' FILL(CONSTANT, 'test');
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test');
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test') order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as varbytes));
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as varbytes)) order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test1');
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test1') order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测');
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测') order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试');
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试') order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1111);
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1111) order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 11111);
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 11111) order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.22);
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.22) order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.222);
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.222) order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, true);
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, true) order by device_id;
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id, data7 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id;
-- nchar
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2026-01-01 00:00:00' FILL(CONSTANT, 'test');
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test');
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test') order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as varbytes));
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as varbytes)) order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test1');
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test1') order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试测试');
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试测试') order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试测试测');
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试测试测') order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1111);
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1111) order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 11111);
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 11111) order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.22);
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.22) order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.222);
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.222) order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, true);
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, true) order by device_id;
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id, data8 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id;
-- varbytes
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2026-01-01 00:00:00' FILL(CONSTANT, cast('test' as varbytes));
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as varbytes));
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as varbytes)) order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as bytes));
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as bytes)) order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test');
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test') order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test1');
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test1') order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测');
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测') order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试');
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试') order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1111);
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1111) order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 11111);
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 11111) order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.22);
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.22) order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.222);
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.222) order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, true);
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, true) order by device_id;
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id, data11 from ts_db.metric_ts3 where device_id in ('device6', 'device65') and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id ;

-- error
select * from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1000, 1000);
select * from ts_db.metric_ts3 where device_id = 'device6' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1000, 1000, 'test_const');

-- multi col as ptag
create table metric_ts_multi(k_timestamp timestamp not null, data1 bool, data2 int2, data3 int4, data4 int8, data5 float4, data6 float8,
                            data7 char(4), data8 nchar(4), data9 varchar(4), data10 nvarchar(4), data11 varbytes(4)) tags (device_id1 char(40) not null, device_id2 char(40) not null, alias char(40)) primary tags(device_id1, device_id2);

-- EXACT FILL
insert into metric_ts_multi values('2025-12-19 10:00:02.150', false, 1, 11, 111, 11.11, 111.111, 'd7', 'd8', 'd9', 'd10', 'd11', 'device1', 'device11', 'tianjin');
insert into metric_ts_multi values('2025-12-19 10:00:02.190', true, 2, 22, 222, 22.22, 222.222, 'd71', 'd81', 'd91', 'd101', 'd102', 'device1', 'device11', 'tianjin');
insert into metric_ts_multi values('2025-12-19 10:00:02.480', false, 3, 33, 333, 33.33, 333.333, 'd72', 'd82', 'd92', 'd102', 'd103', 'device1', 'device11', 'tianjin');

select * from ts_db.metric_ts_multi;
select * from ts_db.metric_ts_multi where device_id1 = 'device1' and device_id2 = 'device11' and k_timestamp = '2025-12-19 10:00:02.200' FILL(EXACT);
select * from ts_db.metric_ts_multi where device_id1 = 'device1' and device_id2 = 'device11' and k_timestamp = '2025-12-19 10:00:02.150' FILL(EXACT);

-- PREVIOUS FILL
insert into metric_ts_multi values('2025-12-19 10:00:02.150', false, 1, 11, 111, 11.11, 111.111, 'd7', 'd8', 'd9', 'd10', 'd11', 'device2', 'device22', 'beijing');
insert into metric_ts_multi values('2025-12-19 10:00:02.150', true, 11, 111, 1111, 11.111, 111.1111, 'd71', 'd81', 'd91', 'd101', 'd111', 'device21', 'device211', 'beijing');

select * from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200';
-- bool
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id1, device_id2;
-- int2
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id1, device_id2;
-- int4
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id1, device_id2;
-- int8
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id1, device_id2;
-- float4
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id1, device_id2;
-- float8
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id1, device_id2;
-- char
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id1, device_id2;
-- nchar
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id1, device_id2;
-- varchar
select k_timestamp, device_id1, device_id2, data9 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id1, device_id2, data9 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id1, device_id2;
-- nvarchar
select k_timestamp, device_id1, device_id2, data10 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id1, device_id2, data10 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id1, device_id2;
-- varbytes
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS);
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS) order by device_id1, device_id2;

select * from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
insert into metric_ts_multi values('2025-12-19 10:00:02.190', true, 2, 22, 222, 22.22, 222.222, 'd71', 'd81', 'd91', 'd101', 'd102', 'device2', 'device22', 'beijing');
insert into metric_ts_multi values('2025-12-19 10:00:02.190', false, 21, 221, 2221, 22.221, 222.2221, 'd711', 'd811', 'd911', 'd111', 'd112', 'device21', 'device211', 'beijing');
-- bool
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id1, device_id2;
-- int2
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id1, device_id2;
-- int4
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id1, device_id2;
-- int8
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id1, device_id2;
-- float4
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id1, device_id2;
-- float8
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id1, device_id2;
-- char
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id1, device_id2;
-- nchar
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id1, device_id2;
-- varchar
select k_timestamp, device_id1, device_id2, data9 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id1, device_id2, data9 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id1, device_id2;
-- nvarchar
select k_timestamp, device_id1, device_id2, data10 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id1, device_id2, data10 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id1, device_id2;
-- varbytes
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22' and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20);
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device2', 'device22'), ('device21', 'device211')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(PREVIOUS, 20) order by device_id1, device_id2;

select * from ts_db.metric_ts_multi where device_id1 = 'device2' and device_id2 = 'device22';

-- NEXT FILL
insert into metric_ts_multi values('2025-12-19 10:00:02.150', false, 1, 11, 111, 11.11, 111.111, 'd7', 'd8', 'd9', 'd10', 'd11', 'device3', 'device33', 'shanghai');
insert into metric_ts_multi values('2025-12-19 10:00:02.150', true, 12, 112, 1112, 11.112, 111.1112, 'd72', 'd82', 'd92', 'd102', 'd112', 'device32', 'device321', 'shanghai');
insert into metric_ts_multi values('2025-12-19 10:00:02.500', true, 2, 22, 222, 22.22, 222.222, 'd71', 'd81', 'd91', 'd101', 'd102', 'device3', 'device33', 'shanghai');
insert into metric_ts_multi values('2025-12-19 10:00:02.500', false, 22, 222, 2222, 22.222, 222.2222, 'd712', 'd812', 'd912', 'd121', 'd122', 'device32', 'device321', 'shanghai');

select * from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200';
-- bool
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device32', 'device321')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id1, device_id2;
-- int2
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device32', 'device321')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id1, device_id2;
-- int4
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device32', 'device321')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id1, device_id2;
-- in8
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device32', 'device321')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id1, device_id2;
-- float4
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device32', 'device321')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id1, device_id2;
-- float8
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device32', 'device321')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id1, device_id2;
-- char
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device32', 'device321')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id1, device_id2;
-- nchar
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device32', 'device321')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id1, device_id2;
-- varchar
select k_timestamp, device_id1, device_id2, data9 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id1, device_id2, data9 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device32', 'device321')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id1, device_id2;
-- nvarchar
select k_timestamp, device_id1, device_id2, data10 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id1, device_id2, data10 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device32', 'device321')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id1, device_id2;
-- varbytes
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT);
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device32', 'device321')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT) order by device_id1, device_id2;

select * from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
insert into metric_ts_multi values('2025-12-19 10:00:02.230', false, 3, 33, 333, 33.33, 333.333, 'd72', 'd82', 'd92', 'd102', 'd103', 'device3', 'device33', 'shanghai');
insert into metric_ts_multi values('2025-12-19 10:00:02.230', false, 33, 333, 3333, 33.333, 333.3333, 'd723', 'd823', 'd923', 'd132', 'd133', 'device33', 'device331', 'shanghai');
-- bool
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device33', 'device331')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id1, device_id2;
-- int2
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device33', 'device331')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id1, device_id2;
-- int4
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device33', 'device331')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id1, device_id2;
-- int8
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device33', 'device331')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id1, device_id2;
-- float4
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device33', 'device331')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id1, device_id2;
-- float8
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device33', 'device331')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id1, device_id2;
-- char
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device33', 'device331')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id1, device_id2;
-- nchar
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device33', 'device331')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id1, device_id2;
-- varchar
select k_timestamp, device_id1, device_id2, data9 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id1, device_id2, data9 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device33', 'device331')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id1, device_id2;
-- nvarchar
select k_timestamp, device_id1, device_id2, data10 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id1, device_id2, data10 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device33', 'device331')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id1, device_id2;
-- varbytes
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50);
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device3', 'device33'), ('device33', 'device331')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(NEXT, 50) order by device_id1, device_id2;

select k_timestamp, device_id1, device_id2, data2, data3 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.480';
insert into metric_ts_multi values('2025-12-19 10:00:02.490', true, 4, NULL, 444, 44.44, 444.444, 'd74', 'd84', 'd94', 'd104', 'd105', 'device3', 'device33', 'shanghai');
select k_timestamp, device_id1, device_id2, data2, data3 from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33' and k_timestamp = '2025-12-19 10:00:02.480' FILL(NEXT, 50);

select * from ts_db.metric_ts_multi where device_id1 = 'device3' and device_id2 = 'device33';

-- CLOSER FILL
insert into metric_ts_multi values('2025-12-19 10:00:02.150', false, 1, 11, 111, 11.11, 111.111, 'd7', 'd8', 'd9', 'd10', 'd11', 'device4', 'device44', 'shenzhen');
insert into metric_ts_multi values('2025-12-19 10:00:02.150', false, 13, 113, 1113, 11.113, 111.1113, 'd73', 'd83', 'd93', 'd103', 'd113', 'device43', 'device431', 'shenzhen');
insert into metric_ts_multi values('2025-12-19 10:00:02.500', true, 2, 22, 222, 22.22, 222.222, 'd71', 'd81', 'd91', 'd101', 'd102', 'device4', 'device44', 'shenzhen');
insert into metric_ts_multi values('2025-12-19 10:00:02.500', true, 23, 223, 2223, 22.223, 222.2223, 'd713', 'd813', 'd913', 'd131', 'd132', 'device43', 'device431', 'shenzhen');

select * from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.400';
-- bool
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id1, device_id2;
-- int2
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id1, device_id2;
-- int4
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id1, device_id2;
-- int8
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id1, device_id2;
-- float4
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id1, device_id2;
-- float8
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id1, device_id2;
-- char
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id1, device_id2;
-- nchar
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id1, device_id2;
-- varchar
select k_timestamp, device_id1, device_id2, data9 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id1, device_id2, data9 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id1, device_id2;
-- nvarchar
select k_timestamp, device_id1, device_id2, data10 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id1, device_id2, data10 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id1, device_id2;
-- varbytes
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER);
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.400' FILL(CLOSER) order by device_id1, device_id2;

select * from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
insert into metric_ts_multi values('2025-12-19 10:00:02.400', false, 3, 33, 333, 33.33, 333.333, 'd72', 'd82', 'd92', 'd102', 'd103', 'device4', 'device44', 'shenzhen');
insert into metric_ts_multi values('2025-12-19 10:00:02.400', false, 33, 333, 3333, 33.333, 333.3333, 'd723', 'd823', 'd923', 'd132', 'd133', 'device43', 'device431', 'shenzhen');
-- bool
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id1, device_id2;
-- int2
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id1, device_id2;
-- int4
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id1, device_id2;
-- int8
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id1, device_id2;
-- float4
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id1, device_id2;
-- float8
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id1, device_id2;
-- char
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id1, device_id2;
-- nchar
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id1, device_id2;
-- varchar
select k_timestamp, device_id1, device_id2, data9 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id1, device_id2, data9 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id1, device_id2;
-- nvarchar
select k_timestamp, device_id1, device_id2, data10 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id1, device_id2, data10 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id1, device_id2;
-- varbytes
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100);
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device4', 'device44'), ('device43', 'device431')) and k_timestamp = '2025-12-19 10:00:02.350' FILL(CLOSER, 100) order by device_id1, device_id2;

select k_timestamp, device_id1, device_id2, data2, data3 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.450';
insert into metric_ts_multi values('2025-12-19 10:00:02.430', true, 4, NULL, 444, 44.44, 444.444, 'd74', 'd84', 'd94', 'd104', 'd105', 'device4', 'device44', 'shenzhen');
select k_timestamp, device_id1, device_id2, data2, data3 from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44' and k_timestamp = '2025-12-19 10:00:02.450' FILL(CLOSER, 100);

select * from ts_db.metric_ts_multi where device_id1 = 'device4' and device_id2 = 'device44';

-- LINEAR FILL
insert into metric_ts_multi values('2025-12-19 10:00:02.150', false, 1, 11, 111, 11.11, 111.111, 'd7', 'd8', 'd9', 'd10', 'd11', 'device5', 'device55', 'sichuan');
insert into metric_ts_multi values('2025-12-19 10:00:02.150', false, 14, 114, 1114, 11.114, 111.1114, 'd74', 'd84', 'd94', 'd104', 'd114', 'device54', 'device541', 'sichuan');
insert into metric_ts_multi values('2025-12-19 10:00:02.400', true, 2, 22, 222, 22.22, 222.222, 'd71', 'd81', 'd91', 'd101', 'd102', 'device5', 'device55', 'sichuan');
insert into metric_ts_multi values('2025-12-19 10:00:02.400', true, 24, 224, 2224, 22.224, 222.2224, 'd714', 'd814', 'd914', 'd141', 'd142', 'device54', 'device541', 'sichuan');

select * from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.500';
select * from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);

insert into metric_ts_multi values('2025-12-19 10:00:02.600', false, 3, 33, 333, 33.33, 333.333, 'd72', 'd82', 'd92', 'd102', 'd103', 'device5', 'device55', 'sichuan');
insert into metric_ts_multi values('2025-12-19 10:00:02.600', false, 34, 334, 3334, 33.334, 333.3334, 'd724', 'd824', 'd924', 'd142', 'd143', 'device54', 'device541', 'sichuan');
-- int2
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id1, device_id2;
-- int4
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id1, device_id2;
-- int8
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id1, device_id2;
-- float4
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id1, device_id2;
-- float8
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id1, device_id2;
-- NULL
select k_timestamp, device_id1, device_id2, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR);
select k_timestamp, device_id1, device_id2, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.500' FILL(LINEAR, 0);
select k_timestamp, device_id1, device_id2, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id1, device_id2, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id1, device_id2;

insert into metric_ts_multi values('2025-12-19 10:00:02.170', true, 4, NULL, 444, 44.44, 444.444, 'd74', 'd84', 'd94', 'd104', 'd105', 'device5', 'device55', 'sichuan');
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55' and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200);
select k_timestamp, device_id1, device_id2, data1, data7, data8, data9, data10, data11 from ts_db.metric_ts_multi where (device_id1, device_id2) in (('device5', 'device55'), ('device54', 'device541')) and k_timestamp = '2025-12-19 10:00:02.250' FILL(LINEAR, 150, 200) order by device_id1, device_id2;

select * from ts_db.metric_ts_multi where device_id1 = 'device5' and device_id2 = 'device55';

-- CONSTANT FILL
create table metric_ts_multi2(k_timestamp timestamp not null, data1 bool, data2 int2, data3 int4, data4 int8, data5 float4, data6 float8,
                              data7 char(4), data8 nchar(4), data9 varchar(4), data10 nvarchar(4), data11 varbytes(4)) tags (device_id1 char(40) not null, device_id2 char(40) not null, alias char(40)) primary tags(device_id1, device_id2);
insert into metric_ts_multi2 values('2026-01-01 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'device6', 'device66', 'guangzhou');
insert into metric_ts_multi2 values('2026-01-01 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'device65', 'device651', 'guangzhou');

select * from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200';
-- bool
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1);
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const');
select k_timestamp, device_id1, device_id2, data1 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const') order by device_id1, device_id2;
-- int2
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1);
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483600);
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483600) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const');
select k_timestamp, device_id1, device_id2, data2 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const') order by device_id1, device_id2;
-- int4
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483600);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483600) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483648);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483648) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const');
select k_timestamp, device_id1, device_id2, data3 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const') order by device_id1, device_id2;
-- int8
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483600);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483600) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483648);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2147483648) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const');
select k_timestamp, device_id1, device_id2, data4 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const') order by device_id1, device_id2;
-- float4
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1);
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.2);
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.2) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast(2.2 as float8));
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast(2.2 as float8)) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const');
select k_timestamp, device_id1, device_id2, data5 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const') order by device_id1, device_id2;
-- float8
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1);
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.2);
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.2) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const');
select k_timestamp, device_id1, device_id2, data6 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test_const') order by device_id1, device_id2;
-- char
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test');
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test') order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as varbytes)));
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as varbytes)) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test1');
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test1') order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测');
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测') order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试');
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试') order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1111);
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1111) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 11111);
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 11111) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.22);
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.22) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.222);
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.222) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, true);
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, true) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id1, device_id2, data7 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id1, device_id2;
-- nchar
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test');
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test') order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as varbytes));
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as varbytes)) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test1');
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test1') order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试测试');
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试测试') order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试测试测');
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试测试测') order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1111);
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1111) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 11111);
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 11111) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.22);
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.22) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.222);
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.222) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, true);
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, true) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id1, device_id2, data8 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id1, device_id2;
-- varbytes
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as varbytes));
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as varbytes)) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as bytes));
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, cast('test' as bytes)) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test');
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test') order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test1');
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 'test1') order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测');
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测') order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试');
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, '测试') order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1111);
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 1111) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 11111);
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 11111) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.22);
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.22) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.222);
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, 2.222) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, true);
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, true) order by device_id1, device_id2;
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where device_id1 = 'device6' and device_id2 = 'device66' and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false);
select k_timestamp, device_id1, device_id2, data11 from ts_db.metric_ts_multi2 where (device_id1, device_id2) in (('device6', 'device66'), ('device65', 'device651')) and k_timestamp = '2025-12-19 10:00:02.200' FILL(CONSTANT, false) order by device_id1, device_id2;

-- -- bugfix
CREATE TS DATABASE select_fill_test;
CREATE TABLE select_fill_test.t1 (
k_timestamp TIMESTAMPTZ NOT NULL,
e1 TIMESTAMP,
e2 INT2,
e3 INT4,
e4 INT8,
e5 FLOAT4,
e6 FLOAT8,
e7 BOOL,
e8 CHAR,
e9 CHAR(64),
e10 NCHAR,
e11 NCHAR(64),
e12 VARCHAR,
e13 VARCHAR(64),
e14 NVARCHAR,
e15 NVARCHAR(64),
e16 VARBYTES,
e17 VARBYTES(64),
e18 VARBYTES,
e19 VARBYTES(64)
) TAGS (
tag1 BOOL ,
tag2 SMALLINT,
tag3 INT ,
tag4 BIGINT,
tag5 FLOAT4,
tag6 DOUBLE,
tag7 VARBYTES,
tag8 VARBYTES(64),
tag9 VARBYTES,
tag10 VARBYTES(64),
tag11 CHAR,
tag12 CHAR(64) ,
tag13 NCHAR,
tag14 NCHAR(64),
tag15 VARCHAR,
tag16 VARCHAR(64) NOT NULL
) PRIMARY TAGS (tag16);

INSERT INTO select_fill_test.t1(
    k_timestamp, e1, e2, e3, e4, e5, e6, e7, e8, e9,
    e10, e11, e12, e13, e14, e15, e16, e17, e18, e19,
    tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10,
    tag11, tag12, tag13, tag14, tag15, tag16
) VALUES
('2026-03-06 08:00:01+00', '2026-03-06 08:00:00', 123, 12345, 123456789012, 3.14, 3.1415926535, true, 'A', 'test_char64_001 ',
'中', '测试NCHAR64_001 ', 'varchar_test', 'varchar64_test_001', 'nvarchar测试', 'nvarchar64测试_001', 'abc', 'def', 'ghi', 'jkl',
false, 456, 45678, 456789012345, 6.28, 6.283185307, 'xyz', '123', '456', '789',
'B', 'tag_char64_001 ', '文', '标签NCHAR64_001 ', '标签varchar1', 'JISCO.BOF2.出钢温度1'), ('2026-03-06 08:00:03+00', '2026-03-06 09:00:00', -456, -45678, -456789012345, 1.23, 1.2345678901, false, 'B', 'test_char64_002 ',
'国', '测试NCHAR64_002 ', '数据测试', 'varchar64_test_002', 'nvarchar数据', 'nvarchar64测试_002', 'mno', 'pqr', 'stu', 'vwx',
true, -789, -78901, -789012345678, 2.46, 2.4691357802, '012', '345', '678', '901',
'C', 'tag_char64_002 ', '字', '标签NCHAR64_002 ', '标签varchar2', 'JISCO.BOF2.出钢温度1'), ('2026-03-06 08:00:05+00', NULL, NULL, 78901, 7890123456789, 5.67, 5.6789012345, true, 'C', 'test_char64_003 ',
'测', '测试NCHAR64_003 ', '类型测试', 'varchar64_test_003', 'nvarchar类型', 'nvarchar64测试_003', 'yz{', '|}~', '!"#', '$%&',
false, 101, 10112, 1011213141516, 11.34, 11.357802469, '()', '*+,', '-./', ':;<',
'D', 'tag_char64_003 ', '符', '标签NCHAR64_003 ', '标签varchar3', 'JISCO.BOF2.出钢温度1'), ('2026-03-06 08:00:07+00', '2026-03-06 11:00:00', -101, -10112, -1011213141516, 7.89, 7.8901234567, false, 'D', 'test_char64_004 ',
'试', NULL, NULL, 'varchar64_test_004', 'nvarchar数值', 'nvarchar64测试_004', '=>?', '@AB', 'CDE', 'FGH',
true, 202, 20223, 2022324252627, 15.78, 15.780246913, 'IJK', 'LMN', 'OPQ', 'RST',
'E', 'tag_char64_004 ', '测', '标签NCHAR64_004 ', '标签varchar4', 'JISCO.BOF2.出钢温度1'), ('2026-03-06 08:00:09+00', '2026-03-06 12:00:00', 303, 30334, 3033435363738, 9.01, 9.0123456789, true, 'E', 'test_char64_005 ',
'数', '测试NCHAR64_005 ', '布尔测试', 'varchar64_test_005', 'nvarchar布尔', 'nvarchar64测试_005', 'UVW', 'XYZ', '[\\]', '^_`', false,
-303, -30334, -3033435363738, 18.02, 18.024691357, 'abc', 'def', 'ghi', 'jkl', 'F', 'tag_char64_005 ', '试', '标签NCHAR64_005 ', '标签varchar5', 'JISCO.BOF2.出钢温度1'),
('2026-03-06 08:00:11+00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'JISCO.BOF2.出钢温度1'),
('2026-03-06 08:00:13+00', '2026-03-06 13:00:00', -404, -40445, -4044546474849, 2.34, 2.3456789012, false, 'F', 'test_char64_006 ',
'据', '测试NCHAR64_006 ', '字节测试', 'varchar64_test_006', 'nvarchar字节', 'nvarchar64测试_006', '{|}', '\x7e7f80', '\x818283', '\x848586',
true, 404, 40445, 4044546474849, 4.68, 4.6913578024, '\x878889', '\x8a8b8c', '\x8d8e8f', '\x909192',
'G', 'tag_char64_006 ', '数', '标签NCHAR64_006 ', '标签varchar6', 'JISCO.BOF2.出钢温度1');

INSERT INTO select_fill_test.t1(
    k_timestamp, e1, e2, e3, e4, e5, e6, e7, e8, e9,
    e10, e11, e12, e13, e14, e15, e16, e17, e18, e19,
    tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10,
    tag11, tag12, tag13, tag14, tag15, tag16
) VALUES
      ('2026-03-06 08:00:01+00', '2026-03-06 08:00:00', 123, 12345, 123456789012, 3.14, 3.1415926535, true, 'A', 'test_char64_001 ',
       '中', '测试NCHAR64_001 ', 'varchar_test', 'varchar64_test_001', 'nvarchar测试', 'nvarchar64测试_001', 'abc', 'def', 'ghi', 'jkl',
       false, 456, 45678, 456789012345, 6.28, 6.283185307, 'xyz', '123', '456', '789',
       'B', 'tag_char64_001 ', '文', '标签NCHAR64_001 ', '标签varchar1', 'JISCO.BOF2.出钢温度3'),
      ('2026-03-06 08:00:03+00', '2026-03-06 09:00:00', -456, -45678, -456789012345, 1.23, 1.2345678901, false, 'B', 'test_char64_002 ',
       '国', '测试NCHAR64_002 ', '数据测试', 'varchar64_test_002', 'nvarchar数据', 'nvarchar64测试_002', 'mno', 'pqr', 'stu', 'vwx',
       true, -789, -78901, -789012345678, 2.46, 2.4691357802, '012', '345', '678', '901',
       'C', 'tag_char64_002 ', '字', '标签NCHAR64_002 ', '标签varchar2', 'JISCO.BOF2.出钢温度3'),
      ('2026-03-06 08:00:05+00', NULL, NULL, 78901, 7890123456789, 5.67, 5.6789012345, true, 'C', 'test_char64_003 ',
       '测', '测试NCHAR64_003 ', '类型测试', 'varchar64_test_003', 'nvarchar类型', 'nvarchar64测试_003', 'yz{', '|}~', '!"#', '$%&',
       false, 101, 10112, 1011213141516, 11.34, 11.357802469, '()', '*+,', '-./', ':;<',
       'D', 'tag_char64_003 ', '符', '标签NCHAR64_003 ', '标签varchar3', 'JISCO.BOF2.出钢温度3'),
      ('2026-03-06 08:00:07+00', '2026-03-06 11:00:00', -101, -10112, -1011213141516, 7.89, 7.8901234567, false, 'D', 'test_char64_004 ',
       '试', NULL, NULL, 'varchar64_test_004', 'nvarchar数值', 'nvarchar64测试_004', '=>?', '@AB', 'CDE', 'FGH',
       true, 202, 20223, 2022324252627, 15.78, 15.780246913, 'IJK', 'LMN', 'OPQ', 'RST',
       'E', 'tag_char64_004 ', '测', '标签NCHAR64_004 ', '标签varchar4', 'JISCO.BOF2.出钢温度3'),
      ('2026-03-06 08:00:09+00', '2026-03-06 12:00:00', 303, 30334, 3033435363738, 9.01, 9.0123456789, true, 'E', 'test_char64_005 ',
       '数', '测试NCHAR64_005 ', '布尔测试', 'varchar64_test_005', 'nvarchar布尔', 'nvarchar64测试_005', 'UVW', 'XYZ', '[\\]', '^_`',
       false, -303, -30334, -3033435363738, 18.02, 18.024691357, 'abc', 'def', 'ghi', 'jkl',
       'F', 'tag_char64_005 ', '试', '标签NCHAR64_005 ', '标签varchar5', 'JISCO.BOF2.出钢温度3'),
      ('2026-03-06 08:00:11+00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'JISCO.BOF2.出钢温度3'),
      ('2026-03-08 08:00:13+00', '2026-03-06 13:00:00', -404, -40445, -4044546474849, 2.34, 2.3456789012, false, 'F', 'test_char64_006 ',
       '据', '测试NCHAR64_006 ', '字节测试', 'varchar64_test_006', 'nvarchar字节', 'nvarchar64测试_006', '{|}', '\x7e7f80', '\x818283', '\x848586',
       true, 404, 40445, 4044546474849, 4.68, 4.6913578024, '\x878889', '\x8a8b8c', '\x8d8e8f', '\x909192',
       'G', 'tag_char64_006 ', '数', '标签NCHAR64_006 ', '标签varchar6', 'JISCO.BOF2.出钢温度3');

CREATE PROCEDURE select_fill_test.pro1(a TIMESTAMP, b VARCHAR) $$BEGIN SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 from select_fill_test.t1 WHERE k_timestamp=a and tag16=b FILL(EXACT) ORDER BY tag16; END $$;

call select_fill_test.pro1('2026-03-06 08:00:08+00:00', 'JISCO.BOF2.出钢温度1');

CREATE PROCEDURE select_fill_test.pro2(a TIMESTAMP, b VARCHAR) $$BEGIN SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 FROM select_fill_test.t1 WHERE k_timestamp=a and tag16=b FILL(PREVIOUS) ORDER BY tag16; END $$;

call select_fill_test.pro2('2026-03-06 08:00:08+00:00', 'JISCO.BOF2.出钢温度1');

CREATE PROCEDURE select_fill_test.pro3(a TIMESTAMP, b VARCHAR) $$BEGIN SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 FROM select_fill_test.t1 WHERE k_timestamp=a and tag16=b FILL(NEXT) ORDER BY tag16; END $$;

call select_fill_test.pro3('2026-03-06 08:00:08+00:00', 'JISCO.BOF2.出钢温度1');

CREATE PROCEDURE select_fill_test.pro4(a TIMESTAMP, b VARCHAR) $$BEGIN SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 FROM select_fill_test.t1 WHERE k_timestamp=a and tag16=b FILL(CLOSER) ORDER BY tag16; END $$;

call select_fill_test.pro4('2026-03-06 08:00:08+00:00', 'JISCO.BOF2.出钢温度1');

CREATE PROCEDURE select_fill_test.pro5(a TIMESTAMP, b VARCHAR) $$BEGIN SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 FROM select_fill_test.t1 WHERE k_timestamp=a and tag16=b FILL(CONSTANT, 11) ORDER BY tag16; END $$;

call select_fill_test.pro5('2026-03-06 08:00:08+00:00', 'JISCO.BOF2.出钢温度1');

CREATE PROCEDURE select_fill_test.pro6(a TIMESTAMP, b VARCHAR) $$BEGIN SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 FROM select_fill_test.t1 WHERE k_timestamp=a and tag16=b FILL(LINEAR) ORDER BY tag16; END $$;

call select_fill_test.pro6('2026-03-06 08:00:08+00:00', 'JISCO.BOF2.出钢温度1');

prepare p1 as SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 from select_fill_test.t1 WHERE k_timestamp=$1 and tag16=$2 FILL(EXACT) ORDER BY tag16;

execute p1('2026-03-06 08:00:08+00:00', 'JISCO.BOF2.出钢温度1');

prepare p2 as SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 FROM select_fill_test.t1 WHERE k_timestamp=$1 and tag16=$2 FILL(PREVIOUS) ORDER BY tag16;

execute p2('2026-03-06 08:00:08+00:00', 'JISCO.BOF2.出钢温度1');

prepare p3 as SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 FROM select_fill_test.t1 WHERE k_timestamp=$1 and tag16=$2 FILL(NEXT) ORDER BY tag16;

execute p3('2026-03-06 08:00:08+00:00', 'JISCO.BOF2.出钢温度1');

prepare p4 as SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 FROM select_fill_test.t1 WHERE k_timestamp=$1 and tag16=$2 FILL(CLOSER) ORDER BY tag16;

execute p4('2026-03-06 08:00:08+00:00', 'JISCO.BOF2.出钢温度1');

prepare p5 as SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 FROM select_fill_test.t1 WHERE k_timestamp=$1 and tag16=$2 FILL(CONSTANT, $3) ORDER BY tag16;
prepare p5 as SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 FROM select_fill_test.t1 WHERE k_timestamp=$1 and tag16=$2 FILL(CONSTANT, 11) ORDER BY tag16;

execute p5('2026-03-06 08:00:08+00:00', 'JISCO.BOF2.出钢温度1');

prepare p6 as SELECT k_timestamp,tag16,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19 FROM select_fill_test.t1 WHERE k_timestamp=$1 and tag16=$2 FILL(LINEAR) ORDER BY tag16;

execute p6('2026-03-06 08:00:08+00:00', 'JISCO.BOF2.出钢温度1');

use defaultdb;
drop database select_fill_test cascade;
drop database ts_db cascade;
