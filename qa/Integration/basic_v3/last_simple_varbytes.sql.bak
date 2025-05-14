-- test varchar, varbytes, varbinary types
create ts database test_last;
use test_last;
create table test_last.tb (k_timestamp timestamp not null,e1 varchar,e2 varchar(32),e3 nvarchar,e4 nvarchar(32),e5 varbytes,e6 varbytes(32)) attributes (attr1 varchar(32) not null, attr2 int not null) primary tags(attr1,attr2);

-- empty table
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

-- 2018-05-05 (part1)
insert into test_last.tb values('2018-05-05 14:11:45.111','varchar_0505_1','varchar_0505_11','nvarchar_0505_1','nvarchar_0505_11','varbytes_1',b'\xaa\xbb\xdd', '', 1);
insert into test_last.tb values('2018-05-05 14:11:45.112','varchar_0505_2','varchar_0505_12','nvarchar_0505_2','nvarchar_0505_12','varbytes_2',b'\xaa\xbb\xdd\xdd', '', 1);
insert into test_last.tb values('2018-05-05 14:11:45.113','varchar_0505_3','varchar_0505_13','nvarchar_0505_3','nvarchar_0505_13','varbytes_3',b'\xaa\xbb\xdd\xdd\xdd', '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

-- 2018-05-06 (part2)
insert into test_last.tb values('2018-05-06 14:11:45.111','varchar_0506_1','varchar_0506_11','nvarchar_0506_1','nvarchar_0506_11','varbytes_1',b'\xaa\xbb\xdd', '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

insert into test_last.tb values('2018-05-06 15:11:45.112',null,'varchar_0506_12','nvarchar_0506_2','nvarchar_0506_12','varbytes_2',b'\xaa\xbb\xdd\xdd', '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

insert into test_last.tb values('2018-05-06 16:11:45.111','varchar_0506_3',null,'nvarchar_0506_3','nvarchar_0506_13','varbytes_3',b'\xaa\xbb\xdd', '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

insert into test_last.tb values('2018-05-06 17:11:45.111','varchar_0506_4','varchar_0506_14',null,'nvarchar_0506_14','varbytes_4',b'\xaa\xbb\xdd\xdd\xdd', '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

insert into test_last.tb values('2018-05-06 18:11:45.111','varchar_0506_5','varchar_0506_15','nvarchar_0506_5',null,'varbytes_5',b'\xaa\xbb\xdd', '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

insert into test_last.tb values('2018-05-06 19:11:45.111','varchar_0506_6','varchar_0506_16','nvarchar_0506_6','nvarchar_0506_16',null,b'\xaa\xbb\xdd\xdd', '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

insert into test_last.tb values('2018-05-06 20:11:45.111','varchar_0506_7','varchar_0506_17','nvarchar_0506_7','nvarchar_0506_17','varbytes_7',null, '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;


-- 2018-05-07
insert into test_last.tb values('2018-05-07 14:12:45.112', null, null, null, null,'varbytes_1',b'\xaa\xbb\xdd', '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

insert into test_last.tb values('2018-05-07 14:12:45.113', null, null, null, null, null, null, '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

insert into test_last.tb values('2018-05-07 15:11:45.114',null,'varchar_0507_13','nvarchar_0507_3','nvarchar_0507_13','varbytes_3',b'\xaa\xbb\xdd\xdd', '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

-- 2018-06-07
insert into test_last.tb values('2018-06-07 15:11:45.114', null, null, null, null, null, null, '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

-- 2019-06-07(multiple null partition)
insert into test_last.tb values('2019-06-07 15:11:45.114', null, null, null, null, null, null, '', 1);
insert into test_last.tb values('2019-06-07 15:11:45.114', null, null, null, null, null, null, '', 1);
insert into test_last.tb values('2020-07-07 15:11:45.114', null, null, null, null, null, null, '', 1);
insert into test_last.tb values('2023-07-07 15:11:45.114', null, null, null, null, null, null, '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

-- same time
insert into test_last.tb values('2023-07-07 15:11:45.114',null,'varchar_0707_13','nvarchar_0707_3','nvarchar_0707_13','varbytes_3',b'\xaa\xbb\xdd\xdd', '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

insert into test_last.tb values('2023-07-07 15:11:45.114',null,'varchar_0707_14','nvarchar_0707_4','nvarchar_0707_14','varbytes_4',b'\xaa\xbb\xdd\xdd', '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

-- disorder with null
insert into test_last.tb values('2023-07-07 15:11:45.113',null,'varchar_0707_15','nvarchar_0707_5','nvarchar_0707_15','varbytes_5',b'\xaa\xbb\xdd\xdd', '', 1);
select last(*) from tb;
select last_row(*) from tb;
select last(*) from tb where true;
select last_row(*) from tb where true;

-- select * from tb order by k_timestamp;
-- select last(*) from tb group by e1 order by k_timestamp;
-- select last_row(*) from tb group by e1 order by k_timestamp;

/* normal time-series table */ 
create table t(k_timestamp timestamp not null, x float, y int, z varchar(32)) tags(a int not null) primary tags(a);
insert into t values('2023-07-29 03:11:59.688', 1.0, 1, 'a', 1);
insert into t values('2023-07-29 03:12:59.688', 2.0, 2, 'b', 1);
insert into t values('2023-07-29 03:15:59.688', 3.0, 1, 'a', 1);
insert into t values('2023-07-29 03:18:59.688', 4.0, 2, 'b', 1);
insert into t values('2023-07-29 03:25:59.688', 5.0, 5, 'e', 1);
insert into t values('2023-07-29 03:35:59.688', 6.0, 6, 'e', 1);
insert into t values('2023-07-29 03:10:59.688', 0.1, 2, 'b', 1);
insert into t values('2023-07-29 03:26:59.688', 5.5, 5, 'e', 1);

select last(z) from t;
select last(*) from t;
select last_row(*) from t;
select last_row(*) from t;

select last(z) from t where true;
select last(*) from t where true;
select last_row(*) from t where true;
select last_row(*) from t where true;

drop table t;
drop table tb;
drop database test_last;
-- :drop connection;

