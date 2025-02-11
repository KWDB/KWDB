SET CLUSTER SETTING ts.rows_per_block.max_limit = 10;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 50;
SET cluster setting server.time_until_store_dead = '1min15s';

CREATE TS DATABASE tsdb;
CREATE TABLE tsdb.ts1(
                         ts timestamptz not null,e1 timestamp,e2 int2, e3 int4, e4 int8, e5 float4, e6 float8, e7 bool, e8 char, e9 char(64), e10 nchar, e11 nchar(64), e12 varchar, e13 varchar(64), e14 nvarchar, e15 nvarchar(64), e16 VARBYTES, e17 VARBYTES(64), e18 varbytes, e19 varbytes(64)
) TAGS (
tag1 bool, tag2 smallint, tag3 int, tag4 bigint, tag5 float4, tag6 double, tag7 VARBYTES, tag8 VARBYTES(64), tag9 varbytes, tag10 varbytes(64), tag11 char, tag12 char(64), tag13 nchar, tag14 nchar(64), tag15 varchar, tag16 varchar(64) not null
) PRIMARY TAGS(tag16);


INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:00+00:00', '2024-03-01 10:13:33', 939, 133, 496, 381.18652952325283, -4536.314125530661, False, 'Y', 'C', 'I', 'u', 'L', 'T', 'u', 'i', 'C', 'D', 'C', '6', False, 358, 13, 406, -3618.1734152846866, 6865.783731156127, 'A', '0', '6', '7', 'E', 'W', 'A', 'G', 'Z', 'r');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:01+00:00', '2024-03-01 10:13:33', 131, 202, 123, -4310.674272262582, 5686.927198819194, True, 'm', 'L', 'm', 'i', 'd', 'F', 'S', 'K', '1', '4', 'D', '0', False, 417, 935, 512, -6306.704987135536, 9218.741104708184, 'C', '4', 'D', '2', 'O', 'x', 'K', 'W', 'i', 'A');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:02+00:00', '2024-03-01 10:13:33', 564, 366, 358, 3348.866375467638, 4753.048035881542, True, 'H', 'x', 'Q', 'Z', 'z', 'G', 'b', 't', 'C', '6', 'C', '2', True, 936, 503, 517, 2666.423505757264, -6394.893019865491, 'F', 'E', 'F', '0', 'G', 'F', 'U', 'X', 'a', 'a');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:03+00:00', '2024-03-01 10:13:33', 999, 863, 573, 6239.44977652297, -2102.5098621138013, True, 'O', 'E', 'O', 'W', 'I', 'V', 'F', 'h', '8', '2', 'C', 'F', True, 468, 912, 229, 8646.807813334563, 1313.618617697719, 'F', '6', 'F', '6', 'n', 'N', 'Z', 'O', 'B', 'R');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:04+00:00', '2024-03-01 10:13:33', 304, 313, 522, -2.5688712901810504, -251.39891900363546, False, 'S', 'V', 'J', 'q', 'd', 'g', 'J', 'C', '9', '0', 'F', '3', False, 768, 398, 698, 9761.243104805795, 8592.884167599692, 'A', '2', 'D', '9', 'Z', 'Y', 'E', 'Y', 'e', 'w');

select count(*) from tsdb.ts1;

-- kill: c4
-- sleep: 10s
-- restart-with-ts-store: c4

INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:05+00:00', '2024-03-01 10:13:33', 558, 290, 692, -3401.496054468469, 7584.4174993787165, True, 'f', 'j', 'j', 'W', 'j', 'S', 't', 'P', '0', '7', '2', '7', True, 451, 243, 403, 6242.813244476303, -8411.184327891333, '2', 'F', 'D', 'C', 'q', 'e', 'a', 't', 'l', 'I');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:06+00:00', '2024-03-01 10:13:33', 30, 600, 750, -9322.677455416664, 3267.8120154597273, False, 'C', 'b', 'a', 'r', 'Q', 'r', 'N', 'C', 'F', '8', '7', 'B', True, 764, 538, 884, -460.68600840290674, -2843.4447150997185, '7', '9', '8', '3', 'k', 'M', 'X', 'L', 'e', 'B');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:07+00:00', '2024-03-01 10:13:33', 672, 944, 990, 8039.006233657263, -452.7330099401697, False, 'K', 'C', 'Z', 'v', 'F', 'M', 'p', 'X', '4', 'B', '3', '0', True, 402, 228, 951, 6524.391812714177, 7903.077732617439, '7', '3', '4', 'C', 'V', 'C', 'x', 'c', 'S', 'f');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:08+00:00', '2024-03-01 10:13:33', 541, 816, 100, 6587.635040327961, -5877.841456168977, True, 'S', 'x', 'D', 'G', 'H', 'K', 's', 'o', '0', 'D', 'D', '7', False, 682, 668, 564, -7752.64928854315, -5239.617655573612, 'A', '3', '7', '4', 'v', 'U', 'o', 'S', 'l', 'R');
INSERT INTO tsdb.ts1 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:09+00:00', '2024-03-01 10:13:33', 630, 900, 977, -4679.772368281976, 1852.1603881555366, True, 'E', 'G', 'v', 'v', 'J', 'o', 'a', 'D', '8', '6', '4', '5', False, 726, 372, 744, 6239.251279258919, 3092.1266121824974, 'D', '8', '3', '5', 'M', 'M', 'M', 'M', 'Z', 'r');

select count(*) from tsdb.ts1;


-- kill: c1,c2,c3,c4,c5
-- sleep: 10s

-- restart-with-ts-store: c1,c2,c3,c4,c5
select count(*) from tsdb.ts1;

CREATE TS DATABASE tsdb;
CREATE TABLE tsdb.ts2(
                         ts timestamptz not null,e1 timestamp,e2 int2, e3 int4, e4 int8, e5 float4, e6 float8, e7 bool, e8 char, e9 char(64), e10 nchar, e11 nchar(64), e12 varchar, e13 varchar(64), e14 nvarchar, e15 nvarchar(64), e16 VARBYTES, e17 VARBYTES(64), e18 varbytes, e19 varbytes(64)
) TAGS (
tag1 bool, tag2 smallint, tag3 int, tag4 bigint, tag5 float4, tag6 double, tag7 VARBYTES, tag8 VARBYTES(64), tag9 varbytes, tag10 varbytes(64), tag11 char, tag12 char(64), tag13 nchar, tag14 nchar(64), tag15 varchar, tag16 varchar(64) not null
) PRIMARY TAGS(tag16);

INSERT INTO tsdb.ts2 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:10+00:00', '2024-03-01 10:13:33', 198, 886, 301, -8276.960663964923, 1297.7806678936613, False, 'Z', 'W', 's', 'K', 'v', 'e', 'u', 'o', 'E', '9', '4', '6', True, 305, 928, 275, 1137.6091136229134, -5633.601664811967, 'F', 'E', 'A', 'C', 'N', 'I', 'Z', 'B', 'l', 'd');
INSERT INTO tsdb.ts2 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:11+00:00', '2024-03-01 10:13:33', 13, 257, 944, -3345.182779812776, 9122.858690447738, False, 'Y', 'A', 'E', 'I', 's', 'C', 'M', 'r', 'C', '0', '6', '0', False, 624, 889, 267, 1712.5761525224898, 2070.6116627396605, '1', '9', '7', 'B', 'C', 'd', 'K', 'A', 'C', 'C');
INSERT INTO tsdb.ts2 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:12+00:00', '2024-03-01 10:13:33', 433, 840, 660, -9805.643648927791, -5027.644977677721, True, 'D', 'E', 'G', 'c', 'R', 'P', 'F', 'H', 'B', '3', '1', 'B', True, 827, 927, 427, -3137.071228218937, -2681.0880311580213, '8', 'C', '5', 'F', 'i', 'v', 'd', 'd', 'N', 'O');
INSERT INTO tsdb.ts2 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:13+00:00', '2024-03-01 10:13:33', 557, 379, 648, 1815.2420611306425, -4368.325523386058, False, 'i', 'I', 'M', 'S', 'd', 'u', 'z', 't', 'E', '4', '9', 'A', True, 28, 790, 289, 2708.3031261793367, -4730.930062586838, '7', '5', '0', 'D', 'O', 'j', 'z', 'f', 'S', 'F');
INSERT INTO tsdb.ts2 (ts, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8, tag9, tag10, tag11, tag12, tag13, tag14, tag15, tag16) VALUES ('2024-01-01T00:00:14+00:00', '2024-03-01 10:13:33', 652, 820, 462, -9101.652475386127, -5182.946174259202, True, 'l', 'E', 'W', 'F', 'w', 'l', 'Q', 'R', '4', 'E', 'F', 'B', False, 502, 117, 658, -692.3099414165154, -7963.55265649622, '1', '4', 'E', 'A', 'L', 'E', 'V', 'k', 'g', 'j');

select count(*) from tsdb.ts2;
