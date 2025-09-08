create TS DATABASE TS_DB;
USE TS_DB;

create table TS_DB.d1(k_timestamp TIMESTAMP NOT NULL, a int8 not null, b timestamp not null) tags (t1_attribute int not null) primary tags(t1_attribute);
create table TS_DB.d2(k_timestamp TIMESTAMP NOT NULL, a int8 not null) tags (t2_attribute int not null) primary tags(t2_attribute);

INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:00', 1, 156384292, 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:01', 2, 1563842920, 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:02', 3, 12356987402, 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:03', 4, 102549672049, 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:04', 5, 1254369870546, 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:05', 6, 1846942576287, 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:06', 7, 1235405546970, 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:07', 8, -62167219200001, 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:08', 10, 12354055466259706, 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:09', 11, 9223372036854775807, 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:10', 12, 9223372036854, 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:11', 13, 31556995200001, 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:12', 14, '2020-12-30 18:52:14.111', 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:13', 15, '2020-12-30 18:52:14.000', 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:14', 16, '2020-12-30 18:52:14.1', 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:15', 17, '2020-12-30 18:52:14.26', 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:16', 18, '2023-01-0118:52:14', 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:17', 19, '2023010118:52:14', 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:18', 20, '2970-01-01 00:00:00.001', 1);
INSERT INTO TS_DB.d1 values ('2018-10-10 10:00:19', 21, '2970-01-01 00:00:00.002', 1);
INSERT INTO TS_DB.d2 values ('2018-10-10 10:00:20',  -9223372036855, 1);
INSERT INTO TS_DB.d2 values ('2018-10-10 10:00:21',  -9223372036854, 1);
INSERT INTO TS_DB.d2 values ('2018-10-10 10:00:22',  -1, 1);
INSERT INTO TS_DB.d2 values ('2018-10-10 10:00:23',  0, 1);
INSERT INTO TS_DB.d2 values ('2018-10-10 10:00:24',  1679647084556, 1);
INSERT INTO TS_DB.d2 values ('2018-10-10 10:00:25',  1679647084, 1);
INSERT INTO TS_DB.d2 values ('2018-10-10 10:00:26',  167964708, 1);
INSERT INTO TS_DB.d2 values ('2018-10-10 10:00:27',  9223372036854775807, 1);
INSERT INTO TS_DB.d2 values ('2018-10-10 10:00:28',  9223372036854, 1);
INSERT INTO TS_DB.d2 values ('2018-10-10 10:00:29',  9223372036855, 1);

-- select a from TS_DB.d1;

-- select a, cast(b as int) from TS_DB.d1;

-- select cast(a as timestamp) from TS_DB.d2 where a = -9223372036855;
-- select cast(a as timestamp) from TS_DB.d2 where a  = -9223372036854;
-- select cast(a as timestamp) from TS_DB.d2 where a  = -1;
-- select cast(a as timestamp) from TS_DB.d2 where a  = 0;
-- select cast(a as timestamp) from TS_DB.d2 where a  = 1679647084556;
-- select cast(a as timestamp) from TS_DB.d2 where a  = 1679647084;
-- select cast(a as timestamp) from TS_DB.d2 where a  = 167964708;
-- select cast(a as timestamp) from TS_DB.d2 where a  = 9223372036854775807;
-- select cast(a as timestamp) from TS_DB.d2 where a  = 9223372036854;
-- select cast(a as timestamp) from TS_DB.d2 where a  = 9223372036855;
--
-- select cast(timestamp'1677-09-20 00:12:43.146' as int);
-- select cast(timestamp'1677-09-21 00:12:43.146' as int);
-- select cast(timestamp'1969-12-31 23:59:59.999' as int);
-- select cast(timestamp'1970-01-01 00:00:00.000' as int);
-- select cast(timestamp'0' as int);
-- select cast(timestamp'2020-12-31 16:30:56.1' as int);
-- select cast(timestamp'2020-12-31 16:30:56.156' as int);
-- select cast(timestamp'2020-12-31 16:30:56.2132345' as int);
-- select cast(timestamp'2020-12-31' as int);
-- select cast(timestamp'2020-12-31 ' as int);
-- select cast(timestamp'20201231163056' as int);
-- select cast(timestamp'2262-04-12 07:47:16.854' as int);
-- select cast(timestamp'2262-04-12 07:47:16.855' as int);
--
-- select cast(-9223372036855 as timestamp);
-- select cast(-9223372036854 as timestamp);
-- select cast(-1 as timestamp);
-- select cast(0 as timestamp);
-- select cast(1679647084556 as timestamp);
-- select cast(1679647084 as timestamp);
-- select cast(167964708 as timestamp);
-- select cast(9223372036854775807 as timestamp);
-- select cast(9223372036854 as timestamp);
-- select cast(9223372036855 as timestamp);

drop table TS_DB.d1 cascade;
drop table TS_DB.d2 cascade;
drop database TS_DB cascade;