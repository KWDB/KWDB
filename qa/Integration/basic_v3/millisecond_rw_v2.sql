create TS DATABASE TS_DB;
USE TS_DB;

create table TS_DB.d1(k_timestamp TIMESTAMP NOT NULL, e1 int8 not null, e2 timestamp not null) tags (t1_attribute int not null) primary tags(t1_attribute);


INSERT INTO TS_DB.d1 values (1679882987000, 1, 156384292, 1);
INSERT INTO TS_DB.d1 values (1679882988000, 2, 1563842920, 1);
INSERT INTO TS_DB.d1 values (1679882989000, 3, 12356987402, 1);
INSERT INTO TS_DB.d1 values (1679882990000, 4, 102549672049, 1);
INSERT INTO TS_DB.d1 values (1679882991000, 5, 1254369870546, 1);
INSERT INTO TS_DB.d1 values (1679882992000, 6, 1846942576287, 1);
INSERT INTO TS_DB.d1 values (1679882993000, 7, 1235405546970, 1);
INSERT INTO TS_DB.d1 values (1679882994000, 8, 31556995200002, 1);
INSERT INTO TS_DB.d1 values (1679882995000, 10, 12354055466259706, 1);
INSERT INTO TS_DB.d1 values (1679882996000, 11, 9223372036854775807, 1);
INSERT INTO TS_DB.d1 values (1679882997000, 12, 9223372036854, 1);
INSERT INTO TS_DB.d1 values (1679882998000, 13, 31556995200001, 1);
INSERT INTO TS_DB.d1 values (1679882999000, 14, '2020-12-30 18:52:14.111', 1);
INSERT INTO TS_DB.d1 values (1679883000000, 15, '2020-12-30 18:52:14.000', 1);
INSERT INTO TS_DB.d1 values (1679883001000, 16, '2020-12-30 18:52:14.1', 1);
INSERT INTO TS_DB.d1 values (1679883002000, 17, '2020-12-30 18:52:14.26', 1);
INSERT INTO TS_DB.d1 values (1679883003000, 18, '2023-01-0118:52:14', 1);
INSERT INTO TS_DB.d1 values (1679883004000, 19, '2023010118:52:14', 1);
INSERT INTO TS_DB.d1 values (1679883005000, 20, '2970-01-01 00:00:01', 1);
INSERT INTO TS_DB.d1 values (1679883006000, 21, '2970-01-01 00:00:00.001', 1);

select * from TS_DB.d1 order by e1;

drop DATABASE TS_DB cascade;

--ZDP-44417
create ts database test_timebucket_gapfill_ns;
create table test_timebucket_gapfill_ns.tb(k_timestamp timestamptz(3) not null,e1 timestamp(3),
    e2 int2,e3 int,e4 int8,e5 float4,e6 float8,e7 bool,e8 char,e9 char(100),e10 nchar,
    e11 nchar(255),e12 varchar,e13 varchar(254),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),
    e17 nvarchar(4096),e18 varbytes,e19 varbytes(100),e20 varbytes,e21 varbytes(254),
    e22 varbytes(4096),e23 timestamp(6),e24 timestamp(9),e25 timestamptz(6),e26 timestamptz(9))
    tags (t1 int2 not null,t2 int,t3 int8,t4 bool not null,t5 float4,t6 float8,t7 char,
    t8 char(100) not null,t9 nchar,t10 nchar(254),t11 varchar,t12 varchar(128) not null,
    t13 varbytes,t14 varbytes(100),t15 varbytes,t16 varbytes(255)) primary tags(t1,t4,t8,t12);
insert into test_timebucket_gapfill_ns.tb values('0001-11-06 17:10:55.1231','1970-01-01 08:00:00.1234',
    700,7000,70000,700000.707,7000000.1010101,true,null,null,null,
    null,null,null,null,null,null,null,null,null,null,
    null,null,'1970-01-01 08:00:00.3453321','1973-10-10 18:00:00.1111111111',
    '2070-01-01 15:15:15.78812','1570-05-10 10:55:20.3457778',1,null,7000,false,70.7077,
    700.5675675,'a','test测试！！！@TEST1','e','\a',null,'vvvaa64_1','b','test测试1023_1','vwwws测试_1','aaabbb');
drop DATABASE test_timebucket_gapfill_ns cascade;