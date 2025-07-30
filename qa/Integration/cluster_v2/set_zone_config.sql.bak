SET CLUSTER SETTING server.advanced_distributed_operations.enabled = true;
SET CLUSTER SETTING ts.dedup.rule = 'keep';
SET cluster setting server.time_until_store_dead = '1min15s';
SET CLUSTER SETTING ts.rows_per_block.max_limit = 10;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 50;
-- create
CREATE TS DATABASE tsdb;
CREATE TABLE tsdb.ts1(
ts timestamptz not null,e1 timestamp,e2 int2, e3 int4, e4 int8, e5 float4, e6 float8, e7 bool, e8 char, e9 char(64), e10 nchar, e11 nchar(64), e12 varchar, e13 varchar(64), e14 nvarchar, e15 nvarchar(64), e16 VARBYTES, e17 VARBYTES(64), e18 varbytes, e19 varbytes(64)
) TAGS (
tag1 bool, tag2 smallint, tag3 int, tag4 bigint, tag5 float4, tag6 double, tag7 VARBYTES, tag8 VARBYTES(64), tag9 varbytes, tag10 varbytes(64), tag11 char, tag12 char(64), tag13 nchar, tag14 nchar(64), tag15 varchar, tag16 varchar(64) not null
) PRIMARY TAGS(tag16);

-- insert
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:00+00:00', '2024-03-01 10:13:33', 939, 133, 496, 381.18652952325283, -4536.314125530661, False, 'Y', 'C', 'I', 'u', 'L', 'T', 'u', 'i', 'C', 'D', 'C', '6', False, 358, 13, 406, -3618.1734152846866, 6865.783731156127, 'A', '0', '6', '7', 'E', 'W', 'A', 'G', 'Z', 'r');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:01+00:00', '2024-03-01 10:13:33', 131, 202, 123, -4310.674272262582, 5686.927198819194, True, 'm', 'L', 'm', 'i', 'd', 'F', 'S', 'K', '1', '4', 'D', '0', False, 417, 935, 512, -6306.704987135536, 9218.741104708184, 'C', '4', 'D', '2', 'O', 'x', 'K', 'W', 'i', 'A');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:02+00:00', '2024-03-01 10:13:33', 564, 366, 358, 3348.866375467638, 4753.048035881542, True, 'H', 'x', 'Q', 'Z', 'z', 'G', 'b', 't', 'C', '6', 'C', '2', True, 936, 503, 517, 2666.423505757264, -6394.893019865491, 'F', 'E', 'F', '0', 'G', 'F', 'U', 'X', 'a', 'a');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:03+00:00', '2024-03-01 10:13:33', 999, 863, 573, 6239.44977652297, -2102.5098621138013, True, 'O', 'E', 'O', 'W', 'I', 'V', 'F', 'h', '8', '2', 'C', 'F', True, 468, 912, 229, 8646.807813334563, 1313.618617697719, 'F', '6', 'F', '6', 'n', 'N', 'Z', 'O', 'B', 'R');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:04+00:00', '2024-03-01 10:13:33', 304, 313, 522, -2.5688712901810504, -251.39891900363546, False, 'S', 'V', 'J', 'q', 'd', 'g', 'J', 'C', '9', '0', 'F', '3', False, 768, 398, 698, 9761.243104805795, 8592.884167599692, 'A', '2', 'D', '9', 'Z', 'Y', 'E', 'Y', 'e', 'w');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:05+00:00', '2024-03-01 10:13:33', 939, 133, 496, 381.18652952325283, -4536.314125530661, False, 'Y', 'C', 'I', 'u', 'L', 'T', 'u', 'i', 'C', 'D', 'C', '6', False, 358, 13, 406, -3618.1734152846866, 6865.783731156127, 'A', '0', '6', '7', 'E', 'W', 'A', 'G', 'Z', 'r');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:06+00:00', '2024-03-01 10:13:33', 131, 202, 123, -4310.674272262582, 5686.927198819194, True, 'm', 'L', 'm', 'i', 'd', 'F', 'S', 'K', '1', '4', 'D', '0', False, 417, 935, 512, -6306.704987135536, 9218.741104708184, 'C', '4', 'D', '2', 'O', 'x', 'K', 'W', 'i', 'A');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:07+00:00', '2024-03-01 10:13:33', 564, 366, 358, 3348.866375467638, 4753.048035881542, True, 'H', 'x', 'Q', 'Z', 'z', 'G', 'b', 't', 'C', '6', 'C', '2', True, 936, 503, 517, 2666.423505757264, -6394.893019865491, 'F', 'E', 'F', '0', 'G', 'F', 'U', 'X', 'a', 'a');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:08+00:00', '2024-03-01 10:13:33', 999, 863, 573, 6239.44977652297, -2102.5098621138013, True, 'O', 'E', 'O', 'W', 'I', 'V', 'F', 'h', '8', '2', 'C', 'F', True, 468, 912, 229, 8646.807813334563, 1313.618617697719, 'F', '6', 'F', '6', 'n', 'N', 'Z', 'O', 'B', 'R');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:09+00:00', '2024-03-01 10:13:33', 304, 313, 522, -2.5688712901810504, -251.39891900363546, False, 'S', 'V', 'J', 'q', 'd', 'g', 'J', 'C', '9', '0', 'F', '3', False, 768, 398, 698, 9761.243104805795, 8592.884167599692, 'A', '2', 'D', '9', 'Z', 'Y', 'E', 'Y', 'e', 'w');
INSERT INTO tsdb.ts1 select * from tsdb.ts1;
INSERT INTO tsdb.ts1 select * from tsdb.ts1;
INSERT INTO tsdb.ts1 select * from tsdb.ts1;
INSERT INTO tsdb.ts1 select * from tsdb.ts1;
INSERT INTO tsdb.ts1 select * from tsdb.ts1;
INSERT INTO tsdb.ts1 select * from tsdb.ts1;
INSERT INTO tsdb.ts1 select * from tsdb.ts1;
select count(*) from tsdb.ts1;
CREATE DATABASE rd1;
CREATE TABLE rd1.rt1(a int);

alter range default configure zone using constraints='[+region=r2]';
alter range liveness configure zone using constraints='[+region=r2]';
alter range meta configure zone using constraints='[+region=r2]';
alter range system configure zone using constraints='[+region=r2]';
alter range timeseries configure zone using constraints='[+region=r2]';
alter database rd1 configure zone using constraints='[+region=r2]';
alter database tsdb configure zone using constraints='[+region=r2]';
alter table rd1.rt1 configure zone using constraints='[+region=r2]';
alter table tsdb.ts1 configure zone using constraints='[+region=r2]';

alter range default configure zone using lease_preferences='[+region=r2]';
alter range liveness configure zone using lease_preferences='[+region=r2]';
alter range meta configure zone using lease_preferences='[+region=r2]';
alter range system configure zone using lease_preferences='[+region=r2]';
alter range timeseries configure zone using lease_preferences='[+region=r2]';
alter database rd1 configure zone using lease_preferences='[+region=r2]';
alter database tsdb configure zone using lease_preferences='[+region=r2]';
alter table rd1.rt1 configure zone using lease_preferences='[+region=r2]';
alter table tsdb.ts1 configure zone using lease_preferences='[+region=r2]';

alter range default configure zone using num_replicas = 5;
show  zone configuration for range default ;
alter database tsdb configure zone using num_replicas = 3;
show  zone configuration for database tsdb ;
alter table tsdb.ts1 configure zone using num_replicas = 5;
show  zone configuration for table tsdb.ts1 ;

alter table rd1.rt1 configure zone using ts_merge.days = 300s;
show all zone configurations ;

alter table tsdb.ts1 configure zone using ts_merge.days = 300s;
show all zone configurations ;

-- sleep: 30s
-- wait-all-replica-health: 300s
-- kill: c4
-- kill: c5
-- sleep: 30s

-- insert
INSERT INTO tsdb.ts1 select * from tsdb.ts1;
select count(*) from tsdb.ts1;