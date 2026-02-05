---diff
SET CLUSTER SETTING ts.rows_per_block.max_limit=100;
SET CLUSTER SETTING ts.rows_per_block.min_limit=100;
drop database test7 CASCADE;
create ts database test7;
create table test7.t1_1(ts timestamp not null, value1 int,deviceid int,station_id int) tags (id1 int not null,id2 int) PRIMARY TAGS (id1);
insert into test7.t1_1  values('2024-09-05 07:26:17.843',1,1,2,1,2);
insert into test7.t1_1 values('2024-09-05 07:26:18.031', 11,1,2,1,2);
insert into test7.t1_1  values('2024-09-05 07:26:18.2', 14,1,2,1,2);
insert into test7.t1_1 values('2024-09-05 07:26:18.359', 94,1,2,1,2);
insert into test7.t1_1  values('2024-09-05 07:26:18.502', 14,2,2,1,2);
insert into test7.t1_1 values('2024-09-05 07:26:18.64', 24,2,2,1,2);
insert into test7.t1_1  values('2024-09-05 07:26:18.785', 51,2,2,1,2);
insert into test7.t1_1  values('2024-09-05 07:26:18.926',11,1,2,2,2);
insert into test7.t1_1 values('2024-09-05 07:26:19.059', 15,1,2,2,2);
insert into test7.t1_1  values('2024-09-05 07:26:19.19', 15,1,2,2,2);
insert into test7.t1_1 values('2024-09-05 07:26:19.33', 95,1,2,2,2);
insert into test7.t1_1  values('2024-09-05 07:26:19.484', 15,2,2,2,2);
insert into test7.t1_1 values('2024-09-05 07:26:19.628', 25,2,2,2,2);
insert into test7.t1_1  values('2024-09-05 07:26:19.764', 52,2,2,2,2);

SELECT diff(value1)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_1 order by ts;
SELECT diff(value1+deviceid)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_1 order by ts;
SELECT diff(abs(value1))  OVER (PARTITION BY id1 order by ts),id1, value1 FROM test7.t1_1 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts),id1, abs(deviceid) FROM test7.t1_1 order by ts;

-- error
SELECT lead(value1)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_1;

create table test7.t1_2(ts timestamp not null, value1 float,deviceid int,station_id int) tags (id1 bigint not null,id2 int) PRIMARY TAGS (id1);
insert into test7.t1_1  values('2024-09-05 07:26:17.843',1,1,2,1,2);
insert into test7.t1_1 values('2024-09-05 07:26:18.031', 11,1,2,1,2);
insert into test7.t1_1  values('2024-09-05 07:26:18.2', 14,1,2,1,2);
insert into test7.t1_1 values('2024-09-05 07:26:18.359', 94,1,2,1,2);
insert into test7.t1_2  values('2024-09-05 07:26:18.502', 14,2,2,2,2);
insert into test7.t1_2 values('2024-09-05 07:26:18.64', 24,2,2,2,2);
insert into test7.t1_2  values('2024-09-05 07:26:18.785', 51,2,2,2,2);
insert into test7.t1_2 values('2024-09-05 07:26:19.33', 104,1,2,2,2);
insert into test7.t1_2 values('2024-09-05 07:26:19.484', 124,2,2,2,2);
insert into test7.t1_2 values('2024-09-05 07:26:19.628', 144,1,5,2,2);
insert into test7.t1_2 values('2024-09-05 07:26:19.764', 154,2,7,2,2);
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_2 order by ts;
SELECT diff(abs(value1))  OVER (PARTITION BY id1 order by ts),id1, value1 FROM test7.t1_2 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts),id1, value1 FROM test7.t1_2;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts),id1, abs(deviceid) FROM test7.t1_2 order by ts;

insert into test7.t1_1  values('2024-09-05 07:26:17.843',1,1,2,1,2);
insert into test7.t1_1 values('2024-09-05 07:26:18.031', 11,1,2,1,2);
insert into test7.t1_1  values('2024-09-05 07:26:18.2', 14,1,2,1,2);
insert into test7.t1_1 values('2024-09-05 07:26:18.359', 94,1,2,1,2);
insert into test7.t1_1  values('2024-09-05 07:26:18.502', 14,2,2,1,2);
insert into test7.t1_1 values('2024-09-05 07:26:18.64', 24,2,2,1,2);
insert into test7.t1_1  values('2024-09-05 07:26:18.785', 51,2,2,1,2);
insert into test7.t1_1  values('2024-09-05 07:26:18.926',11,1,2,2,2);
insert into test7.t1_1 values('2024-09-05 07:26:19.059', 15,1,2,2,2);
insert into test7.t1_1  values('2024-09-05 07:26:19.19', 15,1,2,2,2);
insert into test7.t1_1 values('2024-09-05 07:26:19.33', 95,1,2,2,2);
insert into test7.t1_1  values('2024-09-05 07:26:19.484', 15,2,2,2,2);
insert into test7.t1_1 values('2024-09-05 07:26:19.628', 25,2,2,2,2);
insert into test7.t1_1  values('2024-09-05 07:26:19.764', 52,2,2,2,2);

---many diff
create table test7.t1_3(ts timestamp not null, value1 float,value2 int, deviceid int,station_id int) tags (id1 bigint not null,id2 int) PRIMARY TAGS (id1);
insert into test7.t1_3  values('2024-09-05 07:26:17.843',1,15,1,2,1,2);
insert into test7.t1_3 values('2024-09-05 07:26:18.031', 11,17,1,2,1,2);
insert into test7.t1_3  values('2024-09-05 07:26:18.2', 14,18,1,2,1,2);
insert into test7.t1_3 values('2024-09-05 07:26:18.359', 94,18,1,2,1,2);
insert into test7.t1_3  values('2024-09-05 07:26:18.502', 14,5,2,2,2,2);
insert into test7.t1_3 values('2024-09-05 07:26:18.64', 24,16,2,2,2,2);
insert into test7.t1_3  values('2024-09-05 07:26:18.785', 51,17,2,2,2,2);
insert into test7.t1_3 values('2024-09-05 07:26:18.926', 104,30,1,2,2,2);
insert into test7.t1_3 values('2024-09-05 07:26:19.059', 124,35,2,2,2,2);
insert into test7.t1_3 values('2024-09-05 07:26:19.19', 144,42,1,5,2,2);
insert into test7.t1_3 values('2024-09-05 07:26:19.33', 154,46,2,7,2,2);
insert into test7.t1_3 values('2024-09-05 07:26:19.484', 124,35,2,2,3,2);
insert into test7.t1_3 values('2024-09-05 07:26:19.628', 144,42,1,5,3,2);
insert into test7.t1_3 values('2024-09-05 07:26:19.764', 154,46,2,7,3,2);
insert into test7.t1_3 values('2024-09-05 07:27:19.764', 154,46,2,7,4,2);

SELECT diff(value1)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_3 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts) ,diff(value2)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_3 order by ts;
SELECT diff(abs(value1))  OVER (PARTITION BY id1 order by ts),id1, value1 FROM test7.t1_3 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts),id1, value1 FROM test7.t1_3 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts),value1,diff(value2)  OVER (PARTITION BY id1 order by ts),value2,id1 FROM test7.t1_3 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts),value1,diff(value2)  OVER (PARTITION BY id1 order by ts),value2,id1,abs(deviceid) FROM test7.t1_3 order by ts;

---any expression
create table test7.t1_4(ts timestamp not null, value1 float,value2 int, deviceid int,station_id int) tags (id1 varchar(64) not null,id2 int) PRIMARY TAGS (id1);
insert into test7.t1_4  values('2024-09-05 07:26:17.843',1,15,1,2,'1',2);
insert into test7.t1_4 values('2024-09-05 07:26:17.943', 11,17,1,2,'1',2);
insert into test7.t1_4  values('2024-09-05 07:26:18.143', 14,18,1,2,'1',2);
insert into test7.t1_4 values('2024-09-05 07:26:18.843', 94,18,1,2,'1',2);
insert into test7.t1_4  values('2024-09-05 07:26:18.890', 14,5,2,2,'2',2);
insert into test7.t1_4 values('2024-09-05 07:27:17.843', 24,16,2,2,'2',2);
insert into test7.t1_4  values('2024-09-05 07:27:27.843', 51,17,2,2,'2',2);
insert into test7.t1_4 values('2024-09-05 07:27:37.843', 104,30,1,2,'2',2);
insert into test7.t1_4 values('2024-09-05 07:28:17.843', 124,35,2,2,'2',2);
insert into test7.t1_4 values('2024-09-05 07:28:18.843', 144,42,1,5,'2',2);
insert into test7.t1_4 values('2024-09-05 07:28:19.843', 154,46,2,7,'2',2);
insert into test7.t1_4 values('2024-09-05 07:28:27.843', 124,35,2,2,'3',2);
insert into test7.t1_4 values('2024-09-05 07:28:37.843', 144,42,1,5,'3',2);
insert into test7.t1_4 values('2024-09-05 07:28:47.843', 154,46,2,7,'3',2);
insert into test7.t1_4 values('2024-09-05 07:28:57.843', 154,46,2,7,'3',2);

SELECT diff(value1)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_4 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts),diff(value2)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_4 order by ts;
SELECT diff(abs(value1))  OVER (PARTITION BY id1 order by ts),id1, value1 FROM test7.t1_4 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts),id1, value1 FROM test7.t1_4 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts),id1, abs(deviceid) FROM test7.t1_4 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts),value1,diff(value2)  OVER (PARTITION BY id1 order by ts),value2,id1 FROM test7.t1_4 order by ts;
SELECT diff(6)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_4 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_4 order by ts limit 2;
SELECT diff(round(value1))  OVER (PARTITION BY id1 order by ts) ,round(value1) FROM test7.t1_4 order by ts;
SELECT diff(value2|deviceid)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_4 order by ts;
SELECT diff(value2|deviceid)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_4 order by ts limit 10;
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts),id1, abs(deviceid) FROM test7.t1_4 order by ts limit 3;
SELECT diff(value2+deviceid)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_4 order by ts;
SELECT diff(value2-deviceid)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_4 order by ts;
SELECT diff(value2*deviceid)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_4 order by ts;
SELECT diff(value2/deviceid)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_4 order by ts;
SELECT diff(value2%deviceid)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_4 order by ts;

---two ptag
create table test7.t1_5(ts timestamp not null, value1 float,value2 int, deviceid int,station_id int) tags (id1 varchar(64) not null,id2 int not null) PRIMARY TAGS (id1,id2);
insert into test7.t1_5  values('2024-09-05 07:26:17.843',1,15,1,2,'1',2);
insert into test7.t1_5 values('2024-09-05 07:26:18.843', 11,17,1,2,'1',2);
insert into test7.t1_5  values('2024-09-05 07:26:18.943', 14,18,1,2,'1',2);
insert into test7.t1_5 values('2024-09-05 07:26:19.143', 94,18,1,2,'1',2);
insert into test7.t1_5  values('2024-09-05 07:26:20.143', 14,5,2,2,'2',2);
insert into test7.t1_5 values('2024-09-05 07:26:21.843', 24,16,2,2,'2',2);
insert into test7.t1_5  values('2024-09-05 07:26:22.843', 51,17,2,2,'2',2);
insert into test7.t1_5 values('2024-09-05 07:26:23.843', 104,30,1,2,'2',2);
insert into test7.t1_5 values('2024-09-05 07:26:24.843', 124,35,2,2,'2',2);
insert into test7.t1_5 values('2024-09-05 07:27:17.843', 144,42,1,5,'2',2);
insert into test7.t1_5 values('2024-09-05 07:27:18.843', 154,46,2,7,'2',2);
insert into test7.t1_5 values('2024-09-05 07:27:19.843', 124,35,2,2,'3',2);
insert into test7.t1_5 values('2024-09-05 07:28:17.843', 144,42,1,5,'3',2);
insert into test7.t1_5 values('2024-09-05 07:29:17.843', 154,46,2,7,'3',2);
insert into test7.t1_5 values('2024-09-05 07:36:17.843', 154,46,2,7,'3',2);
insert into test7.t1_5 values('2024-09-05 07:37:17.843', 121,35,2,2,'3',3);
insert into test7.t1_5 values('2024-09-05 07:39:18.843', 146,42,1,5,'3',3);
insert into test7.t1_5 values('2024-09-05 07:40:17.843', 148,46,2,7,'3',3);
insert into test7.t1_5 values('2024-09-05 07:46:17.843', 154,46,2,7,'3',3);

SELECT diff(value1)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_5 order by ts;
SELECT diff(value1),diff(value2)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_5 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1,id2 order by ts) FROM test7.t1_5 order by ts;
SELECT diff(abs(value1))  OVER (PARTITION BY id1,id2 order by ts),id1,id2 value1 FROM test7.t1_5 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1,id2 order by ts),id1,id2 value1 FROM test7.t1_5 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1,id2 order by ts),id1,id2, abs(deviceid) FROM test7.t1_5 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1,id2 order by ts),value1,diff(value2) OVER (PARTITION BY id1,id2 order by ts),value2,id1,id2 FROM test7.t1_5 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1,id2 order by ts),value1,diff(value2) OVER (PARTITION BY id1 order by ts),value2,id1,id2 FROM test7.t1_5 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1,id2 order by ts) as e,station_id,deviceid,id1,id2   FROM test7.t1_5 where station_id+deviceid
> 8 order by ts;
SELECT diff(value1)  OVER (PARTITION BY id1,id2 order by ts) as e,station_id   FROM test7.t1_5 where station_id  > 5 order by ts;

---include null
create table test7.t1_6(ts timestamp not null, value1 float,value2 int, value3 int, deviceid int,station_id int) tags (id1 varchar(64) not null,id2 int) PRIMARY TAGS (id1);
insert into test7.t1_6 (ts,value1,value2,deviceid,station_id,id1,id2)   values('2024-09-05 07:46:17.843',1,15,1,2,'1',2);
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_6 order by ts;
insert into test7.t1_6(ts,deviceid,station_id,id1,id2)  values('2024-09-05 07:49:17.843',1,2,'1',2);
insert into test7.t1_6  (ts,value1,value2,deviceid,station_id,id1,id2) values('2024-09-05 07:46:18.843',3,18,1,2,'1',2);
insert into test7.t1_6  (ts,value1,value2,deviceid,station_id,id1,id2)  values('2024-09-05 07:46:19.843',4,21,1,2,'1',2);
insert into test7.t1_6(ts,value1,station_id,id1,id2)  values('2024-09-05 07:49:17.843',7,2,'1',2);
insert into test7.t1_6  (ts,value1,value2,deviceid,station_id,id1,id2)  values('2024-09-05 07:50:17.843',12,15,1,2,'1',2);
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_6 order by ts;
SELECT diff(value2)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_6 order by ts;
SELECT diff(value3)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_6 order by ts;
insert into test7.t1_6  (ts,value1,value2,value3,deviceid,station_id,id1,id2)  values('2024-09-05 07:51:17.843',13,15,5,1,2,'1',2);
SELECT diff(value3)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_6 order by ts;
insert into test7.t1_6  (ts,value1,value2,value3,deviceid,station_id,id1,id2)  values('2024-09-05 07:51:19.843',18,19,8,1,2,'1',2);
select *  FROM test7.t1_6 order by ts;
SELECT diff(value3)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_6 order by ts;
SELECT value1,value2,value3,id1,diff(value3)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_6 order by ts;

---all type
create table test7.t1_7(ts timestamp not null, value1 float,value2 double, value3 int, value4 smallint ,value5 bigint,deviceid int,station_id int) tags (id1 varchar(64) not null,id2 int) PRIMARY TAGS (id1);
insert into test7.t1_7  values('2024-09-05 07:50:17.843',1,15,2,4,6,1,2,'1',2);
insert into test7.t1_7 values('2024-09-05 07:50:18.843', 11,17,4,6,8,1,2,'1',2);
insert into test7.t1_7  values('2024-09-05 07:50:19.843', 14,18,7,9,6,10,2,'1',2);
insert into test7.t1_7 values('2024-09-05 07:50:20.843', 94,18,8,10,11,1,2,'1',2);
insert into test7.t1_7  values('2024-09-05 07:50:21.943', 14,5,12,14,16,2,2,'2',2);
insert into test7.t1_7 values('2024-09-05 07:50:22.143', 24,16,21,41,61,2,2,'2',2);
insert into test7.t1_7  values('2024-09-05 07:50:22.243', 51,17,32,44,66,2,2,'2',2);
insert into test7.t1_7 values('2024-09-05 07:50:23.443', 104,30,52,54,76,1,2,'2',2);
insert into test7.t1_7 values('2024-09-05 07:50:25.843', 124,35,59,74,86,2,2,'2',2);
SELECT diff(value1)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_7 order by ts;
SELECT diff(value2)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_7 order by ts;
SELECT diff(value3)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_7 order by ts;
SELECT diff(value4)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_7 order by ts;
SELECT diff(value5)  OVER (PARTITION BY id1 order by ts) FROM test7.t1_7 order by ts;

create table test7.t1_8(ts timestamp not null, value1 float,value2 int, deviceid int,station_id int) tags (id1 bigint not null,id2 int) PRIMARY TAGS (id1);
insert into test7.t1_8  values('2024-09-03 07:39:38.036', 14,18,1,2,1,2);
insert into test7.t1_8 values('2024-09-03 07:39:38.136', 94,18,1,2,1,2);
insert into test7.t1_8  values('2024-09-03 07:39:38.89', 14,5,2,2,2,2);
insert into test7.t1_8 values('2024-09-03 07:39:39.113', 24,16,2,2,2,2);
insert into test7.t1_8  values('2024-09-03 07:39:39.348', 51,17,2,2,2,2);
insert into test7.t1_8 values('2024-09-03 07:39:39.57', 104,30,1,2,2,2);
insert into test7.t1_8 values('2024-09-03 07:39:39.77', 124,35,2,2,2,2);
insert into test7.t1_8 values('2024-09-03 07:39:40.751', 124,35,2,2,3,2);
insert into test7.t1_8 values('2024-09-03 07:39:40.967', 144,42,1,5,3,2);

SELECT e FROM (SELECT diff(value1) OVER (PARTITION BY id1 order by ts) AS e, ts, deviceid, abs(value2) AS e1 FROM test7.t1_8) AS temp WHERE (ts BETWEEN '2024-09-03 07:39:39' AND '2024-09-03 07:39:40') AND (experimental_strftime(ts, '%H:%M:%S') <= '23:59:59') AND (experimental_strftime(ts, '%H:%M:%S') >= '00:00:00') AND (e > 9) order by ts;

SELECT e FROM (SELECT diff(value1) OVER (PARTITION BY id1 order by ts) AS e, ts, deviceid, abs(value2) AS e1 FROM test7.t1_8) AS temp WHERE (ts BETWEEN '2024-09-03 07:39:39' AND '2024-09-03 07:39:40') AND (experimental_strftime(ts, '%H:%M:%S') <= '23:59:59') AND (experimental_strftime(ts, '%H:%M:%S') >= '00:00:00') AND (e > 11) order by ts;

SELECT sum(e) FROM (SELECT diff(value1) OVER (PARTITION BY id1 order by ts) AS e, ts, deviceid, abs(value2) AS e1 FROM test7.t1_8) AS temp
WHERE (ts BETWEEN '2024-09-03 07:39:39' AND '2024-09-03 07:39:40') AND (experimental_strftime(ts, '%H:%M:%S') <= '23:59:59') AND (experimental_strftime(ts, '%H:%M:%S') >= '00:00:00') AND (e > 9);

SELECT sum(e) FROM (SELECT diff(value1) OVER (PARTITION BY id1 order by ts) AS e, ts, deviceid, abs(value2) AS e1 FROM test7.t1_8) AS temp WHERE (ts BETWEEN '2024-09-03 07:39:39' AND '2024-09-03 07:39:40') AND (experimental_strftime(ts, '%H:%M:%S') <= '23:59:59') AND (experimental_strftime(ts, '%H:%M:%S') >= '00:00:00') AND (e > 11);

SELECT sum(e),sum(deviceid) FROM (SELECT diff(value1) OVER (PARTITION BY id1 order by ts) AS e, ts, deviceid, abs(value2) AS e1 FROM test7.t1_8 where abs(value2) > 1) AS temp WHERE (ts BETWEEN '2024-09-03 07:39:39' AND '2024-09-03 07:39:40') AND (experimental_strftime(ts, '%H:%M:%S') <= '23:59:59') AND (experimental_strftime(ts, '%H:%M:%S') >= '00:00:00') AND (e > 11) AND (e1 > 10) AND (deviceid > 12);

SELECT sum(e),sum(e2)  FROM (SELECT diff(value1) OVER (PARTITION BY id1 order by ts) AS e, diff(value2) OVER (PARTITION BY id1  order by ts) AS e2, ts,deviceid, abs(value2) AS e1 FROM test7.t1_8) AS temp WHERE (e >10)  and (deviceid> 0) and (e2 > 1) and (e1 > 1) and (e+e2> 27);

SELECT sum(e),sum(e2)  FROM (SELECT diff(value1) OVER (PARTITION BY id1 order by ts) AS e, diff(value2) OVER (PARTITION BY id1  order by ts) AS e2, ts,deviceid, abs(value2) AS e1 FROM test7.t1_8) AS temp WHERE (e >10);

SELECT sum(e),sum(e2)  FROM (SELECT diff(value1) OVER (PARTITION BY id1 order by ts) AS e, diff(value2) OVER (PARTITION BY id1
 order by ts) AS e2, ts,deviceid, abs(value2) AS e1 FROM test7.t1_8) AS temp WHERE (e >10);
 
SELECT sum(e),sum(e2)  FROM (SELECT diff(value1) OVER (PARTITION BY id1 order by ts) AS e, diff(value2) OVER (PARTITION BY id1
order by ts) AS e2, ts,deviceid, abs(value2) AS e1 FROM test7.t1_8) AS temp where deviceid > 1;

create table test7.t1_9(ts timestamp not null, value1 bigint,value2 smallint) tags (id1 varchar(64) not null,id2 int) PRIMARY TAGS (id1);
insert into test7.t1_9  values('2024-09-05 07:50:17.843',-32767,32767,'1',2);
insert into test7.t1_9  values('2024-09-05 07:51:18.843',32767,-32765,'1',2);
select ts,value1,value2,diff(value1) over (partition by id1 order by ts),diff(value2) over (partition by id1 order by ts)  from test7.t1_9 order by ts;
insert into test7.t1_9  values('2024-09-05 07:52:18.843',-9223372036854775807,-32765,'1',2);
select ts,value1,value2,diff(value1) over (partition by id1 order by ts) from test7.t1_9 order by ts;
select ts,value1 from test7.t1_9 order by ts;
insert into test7.t1_9  values('2024-09-05 07:53:18.843',9223372036854775807,32765,'1',2);
select ts,value1,value2,diff(value1) over (partition by id1 order by ts) from test7.t1_9 order by ts;
select ts,value1 from test7.t1_9 order by ts;
select ts,value1,value2,diff(value2) over (partition by id1 order by ts)  from test7.t1_9 order by ts;
select ts,value1,value2 from test7.t1_9 order by ts;
 
use defaultdb;
drop database test7 CASCADE;

drop database test CASCADE;
create database test;
use test;

create table test.te1 (value1 int, deviceid int);
insert into test.te1 values(null,2);
insert into test.te1 values(null,2);
insert into test.te1 values(3,2);
insert into test.te1 values(4,2);

---null as head
select diff(value1) over (partition by deviceid) from te1;

---devide
select diff(value1/deviceid) over (partition by deviceid) from te1;
select diff(value1/deviceid) over (partition by deviceid), diff(value1/deviceid) over (partition by deviceid) from te;

---has null
create table test.te2 (value1 int, deviceid int);
insert into test.te2 values(1,2);
insert into test.te2 values(null,2);
insert into test.te2 values(null,2);
insert into test.te2 values(4,2);
insert into test.te2 values(5,2);
insert into test.te2 values(null,2);
insert into test.te2 values(8,2);
insert into test.te2 values(5,2);
select diff(value1) over (partition by deviceid) from te2;

use defaultdb;
drop database test CASCADE;

CREATE ts DATABASE test_select_diff;
use test_select_diff;
CREATE TABLE test_select_diff.t1(
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
        e22 NVARCHAR)
    ATTRIBUTES (
        code1 INT2 NOT NULL,code2 INT,code3 INT8,
        code4 FLOAT4 ,code5 FLOAT8,
        code6 BOOL,
        code7 VARCHAR,code8 VARCHAR(128) NOT NULL,
        code9 VARBYTES,code10 VARBYTES(60),
        code11 VARCHAR,code12 VARCHAR(60),
        code13 CHAR(2),code14 CHAR(1023) NOT NULL,
        code15 NCHAR,code16 NCHAR(254) NOT NULL)
PRIMARY TAGS(code1,code14,code8,code16);

------设备0：0000边界时间戳数据
INSERT INTO test_select_diff.t1 VALUES('0000-1-1 00:00:00',1,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');
INSERT INTO test_select_diff.t1 VALUES('0000-1-2 00:01:00',2,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,0,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0');

------设备1：1,'test数据库语法查询测试！！！@TEST1-8'
INSERT INTO test_select_diff.t1 VALUES('2024-6-1 00:01:00',11,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,1,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST1-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST1-14','\'  ,'test数据库语法查询测试！！！@TEST1-16');
INSERT INTO test_select_diff.t1 VALUES('2024-6-1 6:00:00',12,-2,4,99,10.125,200.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,1,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST1-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST1-14','\'  ,'test数据库语法查询测试！！！@TEST1-16');

------设备2：2,'test数据库语法查询测试！！！@TEST2-8'
INSERT INTO test_select_diff.t1 VALUES('2024-6-3 00:01:00',21,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,2,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST2-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST2-14','\'  ,'test数据库语法查询测试！！！@TEST2-16');
INSERT INTO test_select_diff.t1 VALUES('2024-6-3 06:00:00',22,-2,4,99,10.125,200.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,2,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST2-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST2-14','\'  ,'test数据库语法查询测试！！！@TEST2-16');

------设备3：3,'test数据库语法查询测试！！！@TEST3-8'
INSERT INTO test_select_diff.t1 VALUES('2024-6-5 00:01:00',31,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,3,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST3-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST3-14','\'  ,'test数据库语法查询测试！！！@TEST3-16');
INSERT INTO test_select_diff.t1 VALUES('2024-6-5 06:00:00',32,-2,4,99,10.125,200.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,3,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST3-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST3-14','\'  ,'test数据库语法查询测试！！！@TEST3-16');

------设备4：4,'test数据库语法查询测试！！！@TEST4-8'
INSERT INTO test_select_diff.t1 VALUES('2024-6-7 00:01:00',41,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,4,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST4-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST4-14','\'  ,'test数据库语法查询测试！！！@TEST4-16');
INSERT INTO test_select_diff.t1 VALUES('2024-6-7 06:00:00',42,-2,4,99,10.125,200.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,4,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST4-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST4-14','\'  ,'test数据库语法查询测试！！！@TEST4-16');

------设备5：5,'test数据库语法查询测试！！！@TEST5-8'
INSERT INTO test_select_diff.t1 VALUES('2024-6-9 00:01:00',51,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,5,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST5-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST5-14','\'  ,'test数据库语法查询测试！！！@TEST5-16');
INSERT INTO test_select_diff.t1 VALUES('2024-6-9 06:00:00',52,-2,4,99,10.125,200.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,5,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST5-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST5-14','\'  ,'test数据库语法查询测试！！！@TEST5-16');

------设备6：6,'test数据库语法查询测试！！！@TEST6-8'
INSERT INTO test_select_diff.t1 VALUES('2024-6-12 00:01:00',61,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,6,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST6-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST6-14','\'  ,'test数据库语法查询测试！！！@TEST6-16');
INSERT INTO test_select_diff.t1 VALUES('2024-6-12 06:00:00',62,-2,4,99,10.125,200.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,6,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST6-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST6-14','\'  ,'test数据库语法查询测试！！！@TEST6-16');

------设备7：7,'test数据库语法查询测试！！！@TEST7-8'
INSERT INTO test_select_diff.t1 VALUES('2024-6-17 00:01:00',71,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,7,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST7-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST7-14','\'  ,'test数据库语法查询测试！！！@TEST7-16');
INSERT INTO test_select_diff.t1 VALUES('2024-6-17 06:00:00',72,-2,4,99,10.125,200.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,7,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST7-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST7-14','\'  ,'test数据库语法查询测试！！！@TEST7-16');

------设备8：8,'test数据库语法查询测试！！！@TEST8-8'
INSERT INTO test_select_diff.t1 VALUES('2024-6-22 00:01:00',81,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,8,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST8-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST8-14','\'  ,'test数据库语法查询测试！！！@TEST8-16');
INSERT INTO test_select_diff.t1 VALUES('2024-6-22 06:00:00',82,-2,4,99,10.125,200.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,8,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST8-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST8-14','\'  ,'test数据库语法查询测试！！！@TEST8-16');

------设备9：9,'test数据库语法查询测试！！！@TEST9-8'
INSERT INTO test_select_diff.t1 VALUES('2024-6-27 00:01:00',91,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,9,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST9-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST9-14','\'  ,'test数据库语法查询测试！！！@TEST9-16');
INSERT INTO test_select_diff.t1 VALUES('2024-6-27 06:00:00',92,-2,4,99,10.125,200.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,9,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST9-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST9-14','\'  ,'test数据库语法查询测试！！！@TEST9-16');

------设备10：10,'test数据库语法查询测试！！！@TEST10-8'
INSERT INTO test_select_diff.t1 VALUES('2024-6-30 23:00:00',101,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',10,0,0,0,0,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST10-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16');
INSERT INTO test_select_diff.t1 VALUES('2024-7-1 00:01:00',102,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',10,0,0,0,0,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST10-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST10-14','中','test数据库语法查询测试！！！@TEST10-16');

------设备11：11,'test数据库语法查询测试！！！@TEST11-8'
INSERT INTO test_select_diff.t1 VALUES('2024-7-06 24:00:00',111,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',11,0,0,0,0,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST11-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16');
INSERT INTO test_select_diff.t1 VALUES('2024-7-06 01:00:00',112,10001,10000001,100000000001,-1047200.00312001,-1109810.113011921,true,'2021-3-1 12:00:00.909','test数据库语法查询测试！！！@TEST3-8','test数据库语法查询测试！！！@TEST3-9','test数据库语法查询测试！！！@TEST3-10','t','test数据库语法查询测试！！！@TEST3-12','中','test数据库语法查询测试！！！@TEST3-14','test数据库语法查询测试！！！@TEST3-15','test数据库语法查询测试！TEST3-16xaa','test数据库语法查询测试！！！@TEST3-17','test数据库语法查询测试！！！@TEST3-18',b'\xca','test数据库语法查询测试！！！@TEST3-20','test数据库语法查询测试！！！@TEST3-21','test数据库语法查询测试！！！@TEST3-22',11,0,0,0,0,false,'test数据库语法查询测试！！！@TEST3-7','test数据库语法查询测试！！！@TEST11-8',b'\xaa','test数据库语法查询测试！！！@TEST3-10','test数据库语法查询测试！！！@TEST3-11','test数据库语法查询测试！！！@TEST3-12','t3','test数据库语法查询测试！！！@TEST11-14','中','test数据库语法查询测试！！！@TEST11-16');

------设备12：12,'test数据库语法查询测试！！！@TEST12-8'
INSERT INTO test_select_diff.t1 VALUES('2024-7-10 00:01:00',121,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,12,null,null,null,null,true,null,'test数据库语法查询测试！！！@TEST12-8',null,null,null,null,null,'test数据库语法查询测试！！！@TEST12-14',null,'test数据库语法查询测试！！！@TEST12-16');
INSERT INTO test_select_diff.t1 VALUES('2024-7-10 06:00:00',122,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',12,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST12-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST12-14','中','test数据库语法查询测试！！！@TEST12-16');

------设备13：13,'test数据库语法查询测试！！！@TEST13-8'
INSERT INTO test_select_diff.t1 VALUES('2024-7-13 00:01:00',131,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,13,null,null,null,null,true,null,'test数据库语法查询测试！！！@TEST13-8',null,null,null,null,null,'test数据库语法查询测试！！！@TEST13-14',null,'test数据库语法查询测试！！！@TEST13-16');
INSERT INTO test_select_diff.t1 VALUES('2024-7-13 06:00:00',132,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',13,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST13-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST13-14','中','test数据库语法查询测试！！！@TEST13-16');

------设备14：14,'test数据库语法查询测试！！！@TEST14-8'
INSERT INTO test_select_diff.t1 VALUES('2024-7-16 00:01:00',141,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,14,null,null,null,null,true,null,'test数据库语法查询测试！！！@TEST14-8',null,null,null,null,null,'test数据库语法查询测试！！！@TEST14-14',null,'test数据库语法查询测试！！！@TEST14-16');
INSERT INTO test_select_diff.t1 VALUES('2024-7-16 06:00:00',142,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',14,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST14-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST14-14','中','test数据库语法查询测试！！！@TEST14-16');

------设备15：15,'test数据库语法查询测试！！！@TEST15-8'
INSERT INTO test_select_diff.t1 VALUES('2024-7-19 00:01:00',151,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,15,null,null,null,null,true,null,'test数据库语法查询测试！！！@TEST15-8',null,null,null,null,null,'test数据库语法查询测试！！！@TEST15-14',null,'test数据库语法查询测试！！！@TEST15-16');
INSERT INTO test_select_diff.t1 VALUES('2024-7-19 06:00:00',152,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',15,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST15-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST15-14','中','test数据库语法查询测试！！！@TEST15-16');

------设备16：16,'test数据库语法查询测试！！！@TEST16-8'
INSERT INTO test_select_diff.t1 VALUES('2024-7-22 00:01:00',161,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,16,null,null,null,null,true,null,'test数据库语法查询测试！！！@TEST16-8',null,null,null,null,null,'test数据库语法查询测试！！！@TEST16-14',null,'test数据库语法查询测试！！！@TEST16-16');
INSERT INTO test_select_diff.t1 VALUES('2024-7-22 06:00:00',162,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',16,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST16-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST16-14','中','test数据库语法查询测试！！！@TEST16-16');

------设备17：17,'test数据库语法查询测试！！！@TEST17-8'
INSERT INTO test_select_diff.t1 VALUES('2024-7-26 00:01:00',171,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,17,null,null,null,null,true,null,'test数据库语法查询测试！！！@TEST17-8',null,null,null,null,null,'test数据库语法查询测试！！！@TEST17-14',null,'test数据库语法查询测试！！！@TEST17-16');
INSERT INTO test_select_diff.t1 VALUES('2024-7-26 06:00:00',172,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',17,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST17-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST17-14','中','test数据库语法查询测试！！！@TEST17-16');

------设备18：18,'test数据库语法查询测试！！！@TEST18-8'
INSERT INTO test_select_diff.t1 VALUES('2024-8-01 00:01:00',181,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,18,null,null,null,null,true,null,'test数据库语法查询测试！！！@TEST18-8',null,null,null,null,null,'test数据库语法查询测试！！！@TEST18-14',null,'test数据库语法查询测试！！！@TEST18-16');
INSERT INTO test_select_diff.t1 VALUES('2024-8-01 06:00:00',182,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',18,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST18-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST18-14','中','test数据库语法查询测试！！！@TEST18-16');

------设备19：19,'test数据库语法查询测试！！！@TEST19-8'
INSERT INTO test_select_diff.t1 VALUES('2024-8-05 00:01:00',191,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,19,null,null,null,null,true,null,'test数据库语法查询测试！！！@TEST19-8',null,null,null,null,null,'test数据库语法查询测试！！！@TEST19-14',null,'test数据库语法查询测试！！！@TEST19-16');
INSERT INTO test_select_diff.t1 VALUES('2024-8-06 06:00:00',192,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',19,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST19-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST19-14','中','test数据库语法查询测试！！！@TEST19-16');

------设备20：20,'test数据库语法查询测试！！！@TEST20-8'
INSERT INTO test_select_diff.t1 VALUES('2024-8-05 00:01:00',201,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,20,null,null,null,null,true,null,'test数据库语法查询测试！！！@TEST20-8',null,null,null,null,null,'test数据库语法查询测试！！！@TEST20-14',null,'test数据库语法查询测试！！！@TEST20-16');
INSERT INTO test_select_diff.t1 VALUES('2024-8-06 06:00:00',202,-32768,2147483647,-9223372036854775808,99999999991.9999999991,-9999999999991.999999999991,false,28372987421,'test数据库语法查询测试！！！@TEST11-8','test数据库语法查询测试！！！@TEST11-9','test数据库语法查询测试！！！@TEST11-10','t','test数据库语法查询测试！！！@TEST11-12','中','test数据库语法查询测试！！！@TEST11-14','test数据库语法查询测试！！！@TEST11-15','test数据库语法查询测试！TEST11-16xaa','test数据库语法查询测试！！！@TEST11-17','test数据库语法查询测试！！！@TEST11-18',b'\xca','test数据库语法查询测试！！！@TEST11-20','test数据库语法查询测试！！！@TEST11-21','test数据库语法查询测试！！！@TEST11-22',20,222,2222222,2221398001.0312001,2309810.89781,true,'test数据库语法查询测试！！！@TEST11-7','test数据库语法查询测试！！！@TEST20-8',b'\xcc','test数据库语法查询测试！！！@TEST11-10','test数据库语法查询测试！！！@TEST11-11','test数据库语法查询测试！！！@TEST11-12','t3','test数据库语法查询测试！！！@TEST20-14','中','test数据库语法查询测试！！！@TEST20-16');

-------2970边界时间戳数据
INSERT INTO test_select_diff.t1 VALUES('2970-1-1 00:00:00',211,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,21,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。。。' ,'\'  ,'\0\0中文te@@~eng TE./。。。');
INSERT INTO test_select_diff.t1 VALUES('2969-1-2 00:00:00',212,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',21,0,0,0,0,false,'','\0\0中文te@@~eng TE./。\0\\0\0','','','','','','\0\0中文te@@~eng TE./。。。','','\0\0中文te@@~eng TE./。。。');
INSERT INTO test_select_diff.t1 VALUES('2969-1-10 00:00:00',220,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,21,null,null,null,null,true,null,'\0\0中文te@@~eng TE./。\0\\0\0',null,null,null,null,null,'\0\0中文te@@~eng TE./。。。',null,'\0\0中文te@@~eng TE./。。。');

SELECT COUNT(*) FROM test_select_diff.t1;

select diff(NULL) over (partition by code1,code14,code8,code16) from test_select_diff.t1;

select k_timestamp,e1,diff(k_timestamp) over (partition by code1,code14,code8,code16) from test_select_diff.t1;

select id,k_timestamp,e1,diff(e1) over (partition by code1,code14,code8,code16 order by k_timestamp) diffe1 from test_select_diff.t1 where code1=1 or code1=2 order by diffe1, k_timestamp;

select id,k_timestamp,e1,diff(e1) over (partition by 12345 order by k_timestamp) from test_select_diff.t1 where code1=1 or code1=2 order by id;

select max(e1),diff(e1) over (partition by code1,code8,code14,code16) from test_select_diff.t1 group by e1,code1,code8,code14,code16 order by e1 limit 1;

select time_bucket(k_timestamp,'30day') tb,diff(e) over (partition by code1,code8,code14,code16 order by k_timestamp) diff from (select code1,code8,code14,code16,k_timestamp,e5,diff(e5) over (partition by code1,code8,code14,code16 order by k_timestamp) e from test_select_diff.t1 where k_timestamp between '0000-1-1 00:00:00' and '2970-1-1 00:00:00' and experimental_strftime(k_timestamp,'%H:%M:%S')<='24:00:00' and experimental_strftime(k_timestamp,'%H:%M:%S')>='00:00:00') order by tb;

-- mutil diff
CREATE TABLE space_data_1823973101770842113 (
    ts TIMESTAMPTZ NOT NULL,
    occurred TIMESTAMP NULL,
    pac FLOAT8 NULL,
    soc FLOAT8 NULL,
    availablechargedenergy FLOAT8 NULL,
    availabledischargedenergy FLOAT8 NULL,
    chargedenergy FLOAT8 NULL,
    dischargedenergy FLOAT8 NULL
) TAGS (
    rel_id NCHAR(50) NOT NULL,
    space_model_id NCHAR(50),
    station_id NCHAR(50),
    tenant_id NCHAR(50),
    space_node_id NCHAR(50),
    device_id NCHAR(50) ) PRIMARY TAGS(rel_id);

use defaultdb;
DROP DATABASE test_select_diff cascade;

CREATE ts DATABASE test_select_diff5;
CREATE TABLE test_select_diff5.t1(
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
                e22 NVARCHAR) 
ATTRIBUTES (
            code1 INT2 NOT NULL,code2 INT,code3 INT8,
            code4 FLOAT4 ,code5 FLOAT8,
                        code6 BOOL,
                        code7 VARCHAR,code8 VARCHAR(128) NOT NULL,
                        code9 VARBYTES,code10 VARBYTES(60),
                        code11 VARCHAR,code12 VARCHAR(60),
                        code13 CHAR(2),code14 CHAR(1023) NOT NULL,
                        code15 NCHAR,code16 NCHAR(254) NOT NULL) 
PRIMARY TAGS(code1,code14,code8,code16);
------设备2：2,'test数据库语法查询测试！！！@TEST2-8'
INSERT INTO test_select_diff5.t1 VALUES('2024-6-3 00:01:00',21,-1,1,-1,1.125,-2.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,2,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST2-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST2-14','\'  ,'test数据库语法查询测试！！！@TEST2-16');
INSERT INTO test_select_diff5.t1 VALUES('2024-6-3 06:00:00',22,-2,4,99,10.125,200.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,2,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST2-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST2-14','\'  ,'test数据库语法查询测试！！！@TEST2-16');
INSERT INTO test_select_diff5.t1 VALUES('2024-6-3 12:00:00',23,-5,40,199,30.125,205.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,2,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST2-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST2-14','\'  ,'test数据库语法查询测试！！！@TEST2-16');
INSERT INTO test_select_diff5.t1 VALUES('2024-6-3 18:00:00',24,5,-40,-199,50.125,305.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,2,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST2-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST2-14','\'  ,'test数据库语法查询测试！！！@TEST2-16');
INSERT INTO test_select_diff5.t1 VALUES('2024-6-3 24:00:00',25,12,403,222,33.125,444.125,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,2,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST2-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST2-14','\'  ,'test数据库语法查询测试！！！@TEST2-16');
INSERT INTO test_select_diff5.t1 VALUES('2024-6-3 01:00:00',26,38,289,123,9099.1,89.657,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,2,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST2-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST2-14','\'  ,'test数据库语法查询测试！！！@TEST2-16');
INSERT INTO test_select_diff5.t1 VALUES('2024-6-3 05:00:00',27,876,129,456,145.6,89.657,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,2,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST2-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST2-14','\'  ,'test数据库语法查询测试！！！@TEST2-16');
INSERT INTO test_select_diff5.t1 VALUES('2024-6-3 11:00:00',28,987,222,333,245.6,99.657,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,2,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST2-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST2-14','\'  ,'test数据库语法查询测试！！！@TEST2-16');
INSERT INTO test_select_diff5.t1 VALUES('2024-6-3 19:00:00',29,187,252,338,249.8,19.657,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,2,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST2-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST2-14','\'  ,'test数据库语法查询测试！！！@TEST2-16');
INSERT INTO test_select_diff5.t1 VALUES('2024-6-3 22:00:00',30,107,452,938,49.8,9.657,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,2,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\\0\0' ,'test数据库语法查询测试！！！@TEST2-8',' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'test数据库语法查询测试！！！@TEST2-14','\'  ,'test数据库语法查询测试！！！@TEST2-16');
select avg(LE) from (select diff(e1) over (partition by code1,code8,code14,code16 order by k_timestamp) as LE from test_select_diff5.t1) group by LE having le>=0 order by LE;

use defaultdb;
DROP DATABASE test_select_diff5 cascade;