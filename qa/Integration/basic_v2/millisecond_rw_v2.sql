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