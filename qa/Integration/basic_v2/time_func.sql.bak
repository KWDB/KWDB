CREATE TS DATABASE t1;

CREATE TABLE t1.d1(k_timestamp  TIMESTAMP not null,e1 int8 not null, e2 timestamp not null) tags (code1 INT not null) primary tags(code1);

INSERT INTO t1.d1 VALUES('2018-10-10 10:00:00', 1, 156384292, 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:01', 2, 1563842920, 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:02', 3, 12356987402, 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:03', 4, 102549672049, 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:04', 5, 1254369870546, 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:05', 6, 1846942576287, 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:06', 7, 1235405546970, 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:07', 8, -62167219200001, 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:08', 10, 12354055466259706, 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:09', 11, 9223372036854775807, 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:10', 12, 9223372036854, 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:11', 13, 31556995200001, 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:12', 14, '2020-12-30 18:52:14.111', 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:13', 15, '2020-12-30 18:52:14.000', 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:14', 16, '2020-12-30 18:52:14.1', 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:15', 17, '2020-12-30 18:52:14.26', 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:16', 18, '2023-01-0118:52:14', 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:17', 19, '2023010118:52:14', 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:18', 20, '2970-01-01 00:00:00.001', 1);
INSERT INTO t1.d1 VALUES('2018-10-10 10:00:19', 21, '2970-01-01 00:00:00.002', 1);
select e1,e2, extract('year', e2), date_trunc('year',e2) from t1.d1 order by e1;
select e1,e2, extract('month', e2), date_trunc('month',e2) from t1.d1 order by e1;
select e1,e2, extract('week', e2), date_trunc('week',e2) from t1.d1 order by e1;
select e1,e2, extract('dayofyear', e2), date_trunc('week',e2) from t1.d1 order by e1;
select e1,e2, extract('dayofweek', e2), date_trunc('week',e2) from t1.d1 order by e1;
select e1,e2, extract('hour', e2), date_trunc('hour',e2) from t1.d1 order by e1;
select e1,e2, extract('minute', e2), date_trunc('minute',e2) from t1.d1 order by e1;
select e1,e2, extract('second', e2), date_trunc('second',e2) from t1.d1 order by e1;
select e1,e2, extract('year', e2), extract('month', e2), extract('week', e2), extract('dayofyear', e2), extract('dayofweek', e2), extract('hour', e2), extract('minute', e2), extract('second', e2) from t1.d1 order by e1;

select extract('year', timestamp'2020-12-31 12:30:00.000');
select extract('year', timestamp'2020-12-31 12:30:00');
select extract('year', timestamp'2020-12-31');
select extract('year', timestamp'2020');
select extract('year', timestamp'20201231123000000');
select extract('year', timestamp'20201231123000');

select extract('month', timestamp'2020-12-31 12:30:00.000');
select extract('month', timestamp'2020-12-31 12:30:00');
select extract('month', timestamp'2020-12-31');
select extract('month', timestamp'2020');
select extract('month', timestamp'20201231123000000');
select extract('month', timestamp'20201231123000');

select extract('week', timestamp'2020-12-31 12:30:00.000');
select extract('week', timestamp'2020-12-31 12:30:00');
select extract('week', timestamp'2020-12-31');
select extract('week', timestamp'2020');
select extract('week', timestamp'20201231123000000');
select extract('week', timestamp'20201231123000');

select extract('hour', timestamp'2020-12-31 12:30:00.000');
select extract('hour', timestamp'2020-12-31 12:30:00');
select extract('hour', timestamp'2020-12-31');
select extract('hour', timestamp'2020');
select extract('hour', timestamp'20201231123000000');
select extract('hour', timestamp'20201231123000');

select extract('minute', timestamp'2020-12-31 12:30:00.000');
select extract('minute', timestamp'2020-12-31 12:30:00');
select extract('minute', timestamp'2020-12-31');
select extract('minute', timestamp'2020');
select extract('minute', timestamp'20201231123000000');
select extract('minute', timestamp'20201231123000');

select extract('second', timestamp'2020-12-31 12:30:00.000');
select extract('second', timestamp'2020-12-31 12:30:00');
select extract('second', timestamp'2020-12-31');
select extract('second', timestamp'2020');
select extract('second', timestamp'20201231123000000');
select extract('second', timestamp'20201231123000');

select extract('year', 1234521648216);
select extract('month', 1234521648216);
select extract('day', 1234521648216);
select extract('hour', 1234521648216);
select extract('minute', 1234521648216);
select extract('month', -2023);
select extract('month', 084093248028402842384028340382408203492830498239048249020);
select extract('month', 18409324-23425);
select extract('month', 432048203942.29362);
select extract('month', 432048203942.);
select extract('month', 0xC9023472524AFE);

select e1,e2, date_trunc('second',e2) from t1.d1 order by e1;
select e1,e2, date_trunc('minute', e2) from t1.d1 order by e1;
select e1,e2, date_trunc('hour', e2) from t1.d1 order by e1;
select e1,e2, date_trunc('h', e2) from t1.d1 order by e1;
select e1,e2, date_trunc('day', e2) from t1.d1 order by e1;
select e1,e2, date_trunc('month', e2) from t1.d1 order by e1;
select e1,e2, date_trunc('week', e2) from t1.d1 order by e1;
select e1,e2, date_trunc('year', e2) from t1.d1 order by e1;
select e1,e2, date_trunc('quarter', e2) from t1.d1 order by e1;

select date_trunc('year', timestamp'2020-12-31 12:30:00.000');
select date_trunc('year', timestamp'2020-12-31 12:30:00');
select date_trunc('year', timestamp'2020-12-31');
select date_trunc('year', timestamp'2020');
select date_trunc('year', timestamp'20201231123000000');
select date_trunc('year', timestamp'20201231123000');

select date_trunc('month', timestamp'2020-12-31 12:30:00.000');
select date_trunc('month', timestamp'2020-12-31 12:30:00');
select date_trunc('month', timestamp'2020-12-31');
select date_trunc('month', timestamp'2020');
select date_trunc('month', timestamp'20201231123000000');
select date_trunc('month', timestamp'20201231123000');

select date_trunc('week', timestamp'2020-12-31 12:30:00.000');
select date_trunc('week', timestamp'2020-12-31 12:30:00');
select date_trunc('week', timestamp'2020-12-31');
select date_trunc('week', timestamp'2020');
select date_trunc('week', timestamp'20201231123000000');
select date_trunc('week', timestamp'20201231123000');

select date_trunc('hour', timestamp'2020-12-31 12:30:00.000');
select date_trunc('hour', timestamp'2020-12-31 12:30:00');
select date_trunc('hour', timestamp'2020-12-31');
select date_trunc('hour', timestamp'2020');
select date_trunc('hour', timestamp'20201231123000000');
select date_trunc('hour', timestamp'20201231123000');

select date_trunc('minute', timestamp'2020-12-31 12:30:00.000');
select date_trunc('minute', timestamp'2020-12-31 12:30:00');
select date_trunc('minute', timestamp'2020-12-31');
select date_trunc('minute', timestamp'2020');
select date_trunc('minute', timestamp'20201231123000000');
select date_trunc('minute', timestamp'20201231123000');

select date_trunc('second', timestamp'2020-12-31 12:30:00.000');
select date_trunc('second', timestamp'2020-12-31 12:30:00');
select date_trunc('second', timestamp'2020-12-31');
select date_trunc('second', timestamp'2020');
select date_trunc('second', timestamp'20201231123000000');
select date_trunc('second', timestamp'20201231123000');

select date_trunc('year', 1234521648216);
select date_trunc('month', 1234521648216);
select date_trunc('day', 1234521648216);
select date_trunc('hour', 1234521648216);
select date_trunc('minute', 1234521648216);
select date_trunc('month', -2023);
select date_trunc('month', 084093248028402842384028340382408203492830498239048249020);
select date_trunc('month', 18409324-23425);
select date_trunc('month', 432048203942.29362);
select date_trunc('month', 432048203942.);
select date_trunc('month', 0xC9023472524AFE);

drop database t1 cascade;
