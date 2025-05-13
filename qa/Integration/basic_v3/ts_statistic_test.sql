set cluster setting sql.stats.tag_automatic_collection.enabled = false;
create ts database test;
use test;
Create table t1(k_timestamp timestamp not null,c1 int2,c2 int4,c3 float4,c4 float8,c5 char,c6 varchar(10),c7 int8,c8 nchar(10),c9 nvarchar(10),c10 varbytes,c11 timestamptz,c12 bool) tags (size int not null) primary tags (size) ;
set timezone = 8;
Insert into t1 values ('2024-1-1 1:00:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,2);
Insert into t1 values ('2024-1-1 1:01:00',2,200,0.2,0.1,'a','aaa',1000,'e','f','g','2024-1-1 1:00:00',true,2);
Insert into t1 values ('2024-1-1 2:00:00',3,200,0.3,0.1,'a','aaa',2000,'ee','ff','gg','2024-1-1 1:00:01',true,6);
Insert into t1 values ('2024-1-1 3:00:00',4,500,0.4,0.2,'b','bb',2000,'ee','ff','gg','2024-1-1 1:00:01',false,4);
Insert into t1 values ('2024-1-1 4:00:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,5);
Insert into t1 values ('2024-1-1 5:00:00',6,6,0.6,0.2,'b','bbb',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,6);
Insert into t1 values ('2024-1-1 6:00:00',7,7,0.7,0.3,'c','cc',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,7);
Insert into t1 values ('2024-1-1 7:00:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,8);
Insert into t1 values ('2024-1-1 8:00:00',9,9,0.9,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',true,9);
Insert into t1 values ('2024-1-1 9:00:00',10,10,1.0,0.3,'c','ccc',6000,'eeee','fffff','ggg','2024-1-1 1:00:05',false,10);
Insert into t1 values ('2024-1-1 10:00:00',null,null,null,null,null,null,6000,'eeee','fffff','ggggg','2024-1-1 1:00:06',true,10);
Insert into t1 values ('2024-1-1 11:00:00',null,null,null,null,null,null,10000,'eeee','fffff','ggggg','2024-1-1 1:00:06',false,10);
select * from t1 order by k_timestamp;

-------- Test normal columns
------ Test create multi column of statistic at same time
create statistics t1sall from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1sall';

-------- Test Create manually
------ Test create single column of statistic
---- Test normal column
-- test timestamp
-- select k_timestamp from t1 order by k_timestamp;
create statistics t1s0 on k_timestamp from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s0';
-- test Int
-- select c1 from t1 order by c1;
create statistics t1s1 on c1 from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s1';
-- select c2 from t1 order by c2;
create statistics t1s2 on c2 from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s2';
-- test float4
-- select c3 from t1 order by c3;
create statistics t1s3 on c3 from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s3';
-- test float8
-- select c4 from t1 order by c4;
create statistics t1s4 on c4 from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s4';
-- test char
-- select c5 from t1 order by c5;
create statistics t1s5 on c5 from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s5';
-- test varchar
-- select c6 from t1 order by c6;
create statistics t1s6 on c6 from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s6';
-- test int8
create statistics t1s7 on c7 from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s7';
-- test nchar
create statistics t1s8 on c8 from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s8';
-- test nvarchar
create statistics t1s9 on c9 from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s9';
-- test varbytes
create statistics t1s10 on c10 from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s10';
-- test timestamptz
create statistics t1s11 on c11 from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s11';
-- test bool
create statistics t1s12 on c12 from t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1s12';
-- select size from t1 order by size;
create statistics t1p1 on size from t1 ;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't1p1';
DELETE FROM system.table_statistics WHERE name = 't1s0' or name = 't1s1' or name = 't1s2' or name = 't1s3' or name = 't1s4' or name = 't1s5' or name = 't1s6' or name = 't1p1'
or name = 't1s7' or name = 't1s8' or name = 't1s9' or name = 't1s10' or name = 't1s11' or name = 't1s12' or name = 't1sall';

-------- Test Tag columns
Create table t2(k_timestamp timestamp not null,e1 int) tags (c1 int2,c2 int4,c3 float4,c4 float8,c5 char,c6 varchar(10),size int not null,c7 int8,c8 bool,c9 nchar,c10 varbytes) primary tags (size) ;
Insert into t2 values ('2024-1-1 1:00:00',1,1,100,0.1,0.1,'a','aa',2,1000,true,'a','a');
Insert into t2 values ('2024-1-1 1:01:00',2,2,200,0.2,0.1,'a','aaa',2,1000,false,'a','b');
Insert into t2 values ('2024-1-1 2:00:00',3,3,200,0.3,0.1,'a','aaa',6,2000,true,'a','c');
Insert into t2 values ('2024-1-1 3:00:00',4,4,500,0.4,0.2,'b','bb',4,2000,false,'a','cc');
Insert into t2 values ('2024-1-1 4:00:00',5,5,500,0.5,0.2,'b','bb',5,3000,true,'a','d');
Insert into t2 values ('2024-1-1 5:00:00',6,6,6,0.6,0.2,'b','bbb',6,3000,false,'a','d');
Insert into t2 values ('2024-1-1 6:00:00',7,7,7,0.7,0.3,'c','cc',7,4000,true,'a','e');
Insert into t2 values ('2024-1-1 7:00:00',8,8,8,0.8,0.3,'c','cc',8,4000,false,'a','f');
Insert into t2 values ('2024-1-1 8:00:00',9,9,9,0.9,0.3,'c','cc',9,5000,true,'a','h');
Insert into t2 values ('2024-1-1 9:00:00',10,10,10,1.0,0.3,'c','ccc',10,5000,false,'a','i');
Insert into t2 values ('2024-1-1 10:00:00',null,null,null,null,null,null,null,11,6000,true,'a','j');
Insert into t2 values ('2024-1-1 11:00:00',null,null,null,null,null,null,null,12,6000,false,'a','k');
Insert into t2 values ('2024-1-1 12:00:00',null,null,null,null,null,null,null,13,8000,null,'a','kk');
select * from t2 order by k_timestamp;
-- test timestamp
-- select k_timestamp from t2 order by k_timestamp;
create statistics t2s0 on k_timestamp from t2;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2s0';
-- test Int
-- select e1 from t2 order by e1;
create statistics t2se on e1 from t2;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2se';
-- select c1 from t2 order by c1;
create statistics t2s1 on c1 from t2;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2s1';
-- select c2 from t2 order by c2;
create statistics t2s2 on c2 from t2;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2s2';
-- test float4
-- select c3 from t2 order by c3;
create statistics t2s3 on c3 from t2;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2s3';
-- test float8
-- select c4 from t2 order by c4;
create statistics t2s4 on c4 from t2;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2s4';
-- test char
-- select c5 from t2 order by c5;
create statistics t2s5 on c5 from t2;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2s5';
-- test varchar
-- select c6 from t2 order by c6;
create statistics t2s6 on c6 from t2;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2s6';
-- test int8
create statistics t2s7 on c7 from t2;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2s7';
-- test bool
create statistics t2s8 on c8 from t2;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2s8';
-- test nchar
create statistics t2s9 on c9 from t2;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2s9';
-- test varbytes
create statistics t2s10 on c10 from t2;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2s10';

-- select size from t2 order by size;
create statistics t2p1 on size from t2 ;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't2p1';
DELETE FROM system.table_statistics WHERE name = 't2s0' or name = 't2se' or name = 't2s1' or name = 't2s2' or name = 't2s3' or name = 't2s4' or name = 't2s5' or name = 't2s6' or name = 't2p1'
                                       or name = 't2s7' or name = 't2s8' or name = 't2s9' or name = 't2s10';

-------- Test primary tag(rowCount=distinctCount)
Create table t3(k_timestamp timestamp not null,e1 int) tags (c1 smallint not null,c2 nchar(10) not null,c3 char not null,c4 varchar(10) not null,size int not null) primary tags (c1,c2,c3,c4) ;
Insert into t3 values ('2024-1-1 1:00:00',1,1,'100','a','aa',2);
Insert into t3 values ('2024-1-1 1:01:00',2,2,'200','a','aaa',2);
Insert into t3 values ('2024-1-1 2:00:00',3,2,'200','a','aaa',6);
Insert into t3 values ('2024-1-1 3:00:00',4,4,'500','b','bb',4);
Insert into t3 values ('2024-1-1 4:00:00',5,5,'500','b','bb',5);
Insert into t3 values ('2024-1-1 5:00:00',6,6,'6','b','bbb',6);
Insert into t3 values ('2024-1-1 6:00:00',7,7,'8','c','cc',7);
Insert into t3 values ('2024-1-1 7:00:00',8,8,'8','c','cc',8);
Insert into t3 values ('2024-1-1 8:00:00',9,9,'9','c','cc',9);
Insert into t3 values ('2024-1-1 9:00:00',10,10,'10','c','ccc',10);
select * from t3 order by k_timestamp;
create statistics t3all from t3;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't3all';
-- test timestamp
-- select k_timestamp from t3 order by k_timestamp;
create statistics t3s0 on k_timestamp from t3;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't3s0';
-- test Int
-- select e1 from t3 order by e1;
create statistics t3se on e1 from t3;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't3se';
-- test int
-- select c1 from t3 order by c1;
create statistics t3s1 on c1 from t3;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't3s1';
-- test nchar
-- select c2 from t3 order by c2;
create statistics t3s2 on c2 from t3;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't3s2';
-- test char
-- select c3 from t2 order by c3;
create statistics t3s3 on c3 from t3;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't3s3';
-- test varchar
-- select c4 from t2 order by c4;
create statistics t3s4 on c4 from t3;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't3s4';

-- test all PTag
create statistics t3s5 on c1,c2,c3,c4 from t3;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't3s5';

create statistics t3s6 on c4,c2,c1,c3 from t3;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 't3s6';

DELETE FROM system.table_statistics WHERE name = 't3s0' or name = 't3se' or name = 't3s1' or name = 't3s2' or name = 't3s3' or name = 't3s4' or name = 't3s5' or name = 't3all' or name = 't3s6';

-------- Test Create automatically
Create table t5(k_timestamp timestamp not null,c1 int,c2 int,c3 double) tags (c4 float, c5 char not null, c6 bool not null,size int not null) primary tags (c5, c6 ,size) ;
Insert into t5 values ('2024-1-1 1:00:00',1,100,0.1,0.1,'a',true,2);
Insert into t5 values ('2024-1-1 1:01:00',2,200,0.2,0.1,'a',false,2);
Insert into t5 values ('2024-1-1 2:00:00',3,200,0.3,0.1,'a',true,6);
Insert into t5 values ('2024-1-1 3:00:00',4,500,0.4,0.2,'b',false,4);
Insert into t5 values ('2024-1-1 4:00:00',5,500,0.5,0.2,'b',true,5);
Insert into t5 values ('2024-1-1 5:00:00',6,6,0.6,0.2,'b',false,6);
Insert into t5 values ('2024-1-1 6:00:00',7,7,0.7,0.3,'c',true,7);
Insert into t5 values ('2024-1-1 7:00:00',8,8,0.8,0.3,'c',false,8);
Insert into t5 values ('2024-1-1 8:00:00',9,9,0.9,0.3,'c',true,9);
Insert into t5 values ('2024-1-1 9:00:00',10,10,1.0,0.3,'c',false,10);
Insert into t5 values ('2024-1-1 10:00:00',null,null,null,'e','e',true,10);
select * from t5 order by k_timestamp;
-- select pg_sleep(70);
-- -- test does not contain statistics and is created by default
-- select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = '__auto__' and "columnIDs" = array[6,7,8];
-- TODO(zh): test create statistics triggered by changes in the number of rows
-- TODO(zh): test create statistics triggered by time

-- Test normal column group creation statistics
create statistics __ts_auto__ on [1,2,3,4,5] from t5;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = '__ts_auto__';

create statistics __ts_auto__ on [5,2,3,4,1] from t5;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = '__ts_auto__';

create statistics __ts_auto__ on [1,3,2,5,4] from t5;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = '__ts_auto__';

create statistics __ts_auto__ on [1,3,2,5,4,6,7] from t5;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = '__ts_auto__';

-- Test primary column group creation statistics
create statistics __ts_auto__ on [6,7,8] from t5;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = '__ts_auto__';

DELETE FROM system.table_statistics WHERE name = '__ts_auto__';


--- Test exception
-- test PTag
create statistics t3s6 on c1,c2 from t3;
create statistics t3s7 on c1,c2,c3 from t3;
create statistics t3s8 on c3,c4 from t3;
create statistics t3s9 on c1,c1,c1,c1 from t3;
--bug
create statistics t3s9 on c1,c2,c3,c4,c1 from t3;
create statistics t3s9 from t5 as of system time '1µs';

-- Test ADD/DELETE columns
CREATE ts DATABASE test_select;
CREATE TABLE test_select.t1(
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

INSERT INTO test_select.t1 VALUES('2000-1-1 1:00:00',20,1,-1,1,-2.125,1,false,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,''  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,-1,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\0\0' ,''  ,'\0\0中文te@@~eng TE./。\0\\0\0');
CREATE STATISTICS st0 FROM test_select.t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st0';

ALTER TABLE test_select.t1 ADD COLUMN c1 int null;
ALTER TABLE test_select.t1 ADD COLUMN c2 int null;
select pg_sleep(3);
INSERT INTO test_select.t1 VALUES('2000-1-1 2:00:00',20,1,-1,1,-2.125,2,true,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,''  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,' '  ,'中文te@@~eng TE./' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,null,1,-1,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\0\0' ,''  ,'\0\0中文te@@~eng TE./。\0\\0\0');
CREATE STATISTICS st0 FROM test_select.t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st0';

ALTER TABLE test_select.t1 DROP COLUMN e20;
ALTER TABLE test_select.t1 DROP COLUMN e21;
ALTER TABLE test_select.t1 DROP COLUMN e22;
select pg_sleep(3);
INSERT INTO test_select.t1 VALUES('2000-1-1 3:00:00',20,1,-1,1,-2.125,2,true,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,''  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,' '  ,1,1,-1,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\0\0' ,''  ,'\0\0中文te@@~eng TE./。\0\\0\0');
CREATE STATISTICS st0 FROM test_select.t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st0';

ALTER TABLE test_select.t1 DROP TAG code2;
ALTER TABLE test_select.t1 ADD TAG code17 int null;
ALTER TABLE test_select.t1 DROP TAG code3;
select pg_sleep(3);
INSERT INTO test_select.t1 VALUES('2000-1-1 4:00:00',20,1,-1,1,-2.125,2,true,'2020-1-1 12:00:00.000','\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,''  ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\'  ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,' '  ,1,0,0,0,0,false,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,' ','中文te@@~eng TE./。' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\0\0中文te@@~eng TE./。\0\\0\0' ,'\ ' ,'\0\0中文te@@~eng TE./。\0\0\0' ,''  ,'\0\0中文te@@~eng TE./。\0\\0\0',1);
CREATE STATISTICS st0 FROM test_select.t1;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st0';

-------- Test multi-columns distinct-count for primary tag
Create table t6(k_timestamp timestamp not null,c1 int,c2 int,c3 double) tags (c4 float, c5 char not null, c6 bool not null,size int not null) primary tags (c5, c6 ,size) ;
Insert into t6 values ('2024-1-1 1:00:00',1,100,0.1,0.1,'a',true,2);
Insert into t6 values ('2024-1-1 1:01:00',2,200,0.2,0.1,'a',false,2);
Insert into t6 values ('2024-1-1 2:00:00',3,200,0.3,0.1,'a',true,2);
Insert into t6 values ('2024-1-1 3:00:00',4,500,0.4,0.2,'b',false,4);
Insert into t6 values ('2024-1-1 4:00:00',5,500,0.5,0.2,'b',true,5);
Insert into t6 values ('2024-1-1 5:00:00',6,6,0.6,0.2,'b',true,5);
Insert into t6 values ('2024-1-1 6:00:00',7,7,0.7,0.3,'c',true,7);
Insert into t6 values ('2024-1-1 7:00:00',8,8,0.8,0.3,'c',false,8);
Insert into t6 values ('2024-1-1 8:00:00',9,9,0.9,0.3,'c',true,9);
Insert into t6 values ('2024-1-1 9:00:00',10,10,1.0,0.3,'c',false,10);

create statistics st_multiy on c1,c2 from t6;
create statistics st_multiy on c5,c6 from t6;
create statistics st_multiy1 on c5,c6,size from t6;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st_multiy1';
create statistics st_multiy2 on c5,size,c6 from t6;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st_multiy2';
create statistics st_multiy3 on size,c5,c6 from t6;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'st_multiy3';
create statistics auto_multiy on [6,7,8] from t6;
select "name","columnIDs","rowCount","distinctCount","nullCount" from system.table_statistics  where name = 'auto_multiy';


-- Test Sort histogram
--- Complete order
Create table t7(k_timestamp timestamp not null,c1 int2,c2 int4,c3 float4,c4 float8,c5 char,c6 varchar(10),c7 int8,c8 nchar(10),c9 nvarchar(10),c10 varbytes,c11 timestamptz,c12 bool) tags (size int not null) primary tags (size) ;
Insert into t7 values ('2024-1-1 1:00:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,1);
Insert into t7 values ('2024-1-1 1:10:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,1);
Insert into t7 values ('2024-1-1 1:20:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,2);
Insert into t7 values ('2024-1-1 1:30:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,2);
Insert into t7 values ('2024-1-1 1:40:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,2);
Insert into t7 values ('2024-1-1 2:00:00',2,200,0.2,0.1,'a','aaa',1000,'e','f','g','2024-1-1 1:00:00',true,3);
Insert into t7 values ('2024-1-1 2:30:00',2,200,0.2,0.1,'a','aaa',1000,'e','f','g','2024-1-1 1:00:00',true,4);
Insert into t7 values ('2024-1-1 3:00:00',3,200,0.3,0.1,'a','aaa',2000,'ee','ff','gg','2024-1-1 1:00:01',true,5);
Insert into t7 values ('2024-1-1 4:00:00',4,500,0.4,0.2,'b','bb',2000,'ee','ff','gg','2024-1-1 1:00:01',false,6);
Insert into t7 values ('2024-1-1 5:00:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,7);
Insert into t7 values ('2024-1-1 5:10:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,8);
Insert into t7 values ('2024-1-1 5:20:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,9);
Insert into t7 values ('2024-1-1 5:30:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,10);
Insert into t7 values ('2024-1-1 6:00:00',6,6,0.6,0.2,'b','bbb',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,11);
Insert into t7 values ('2024-1-1 7:00:00',7,7,0.7,0.3,'c','cc',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,12);
Insert into t7 values ('2024-1-1 7:30:00',7,7,0.7,0.3,'c','cc',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,12);
Insert into t7 values ('2024-1-1 7:50:00',7,7,0.7,0.3,'c','cc',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,12);
Insert into t7 values ('2024-1-1 8:00:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,13);
Insert into t7 values ('2024-1-1 8:10:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,14);
Insert into t7 values ('2024-1-1 8:20:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,14);
Insert into t7 values ('2024-1-1 8:30:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,14);
Insert into t7 values ('2024-1-1 8:50:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,15);
Insert into t7 values ('2024-1-1 9:00:00',9,9,0.9,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',true,16);
Insert into t7 values ('2024-1-1 10:00:00',10,10,1.0,0.3,'c','ccc',6000,'eeee','fffff','ggg','2024-1-1 1:00:05',false,17);
ALTER TABLE t7 INJECT STATISTICS '[
  {
    "columns": ["size"],
    "created_at": "2024-09-05",
    "row_count": 17,
    "distinct_count": 17,
    "null_count": 0,
    "sort_histogram_buckets": [
      {"row_count": 1,"unordered_row_count": 0,"ordered_entities": 1,"unordered_entities": 0,"upper_bound": "2023-12-31 17:00:00+00:00"},
      {"row_count": 6,"unordered_row_count": 0,"ordered_entities": 3,"unordered_entities": 0,"upper_bound": "2023-12-31 18:00:00+00:00"},
      {"row_count": 2,"unordered_row_count": 0,"ordered_entities": 2,"unordered_entities": 0,"upper_bound": "2023-12-31 19:00:00+00:00"},
      {"row_count": 1,"unordered_row_count": 0,"ordered_entities": 1,"unordered_entities": 0,"upper_bound": "2023-12-31 20:00:00+00:00"},
      {"row_count": 1,"unordered_row_count": 0,"ordered_entities": 1,"unordered_entities": 0,"upper_bound": "2023-12-31 21:00:00+00:00"},
      {"row_count": 4,"unordered_row_count": 0,"ordered_entities": 4,"unordered_entities": 0,"upper_bound": "2023-12-31 22:00:00+00:00"},
      {"row_count": 3,"unordered_row_count": 0,"ordered_entities": 1,"unordered_entities": 0,"upper_bound": "2023-12-31 23:00:00+00:00"},
      {"row_count": 4,"unordered_row_count": 0,"ordered_entities": 2,"unordered_entities": 0,"upper_bound": "2024-01-01 00:00:00+00:00"},
      {"row_count": 5,"unordered_row_count": 0,"ordered_entities": 3,"unordered_entities": 0,"upper_bound": "2024-01-01 01:00:00+00:00"},
      {"row_count": 1,"unordered_row_count": 0,"ordered_entities": 1,"unordered_entities": 0,"upper_bound": "2024-01-01 02:00:00+00:00"}
    ],
    "histo_col_type": "TIMESTAMPTZ"
  }]';
show sort_histogram for table t7;

--- Complete unordered
Create table t8(k_timestamp timestamp not null,c1 int2,c2 int4,c3 float4,c4 float8,c5 char,c6 varchar(10),c7 int8,c8 nchar(10),c9 nvarchar(10),c10 varbytes,c11 timestamptz,c12 bool) tags (size int not null) primary tags (size) ;
Insert into t8 values ('2024-1-1 10:00:00',10,10,1.0,0.3,'c','ccc',6000,'eeee','fffff','ggg','2024-1-1 1:00:05',false,17);
Insert into t8 values ('2024-1-1 9:00:00',9,9,0.9,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',true,16);
Insert into t8 values ('2024-1-1 8:50:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,15);
Insert into t8 values ('2024-1-1 8:30:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,14);
Insert into t8 values ('2024-1-1 8:20:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,14);
Insert into t8 values ('2024-1-1 8:10:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,14);
Insert into t8 values ('2024-1-1 8:00:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,13);
Insert into t8 values ('2024-1-1 7:50:00',7,7,0.7,0.3,'c','cc',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,12);
Insert into t8 values ('2024-1-1 7:30:00',7,7,0.7,0.3,'c','cc',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,12);
Insert into t8 values ('2024-1-1 7:00:00',7,7,0.7,0.3,'c','cc',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,12);
Insert into t8 values ('2024-1-1 6:00:00',6,6,0.6,0.2,'b','bbb',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,11);
Insert into t8 values ('2024-1-1 5:30:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,10);
Insert into t8 values ('2024-1-1 5:20:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,9);
Insert into t8 values ('2024-1-1 5:10:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,8);
Insert into t8 values ('2024-1-1 5:00:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,7);
Insert into t8 values ('2024-1-1 4:00:00',4,500,0.4,0.2,'b','bb',2000,'ee','ff','gg','2024-1-1 1:00:01',false,6);
Insert into t8 values ('2024-1-1 3:00:00',3,200,0.3,0.1,'a','aaa',2000,'ee','ff','gg','2024-1-1 1:00:01',true,5);
Insert into t8 values ('2024-1-1 2:30:00',2,200,0.2,0.1,'a','aaa',1000,'e','f','g','2024-1-1 1:00:00',true,4);
Insert into t8 values ('2024-1-1 2:00:00',2,200,0.2,0.1,'a','aaa',1000,'e','f','g','2024-1-1 1:00:00',true,3);
Insert into t8 values ('2024-1-1 1:40:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,2);
Insert into t8 values ('2024-1-1 1:30:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,2);
Insert into t8 values ('2024-1-1 1:20:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,2);
Insert into t8 values ('2024-1-1 1:10:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,1);
Insert into t8 values ('2024-1-1 1:00:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,1);
ALTER TABLE t8 INJECT STATISTICS '[
  {
    "columns": ["size"],
    "created_at": "2024-09-05",
    "row_count": 17,
    "distinct_count": 17,
    "null_count": 0,
    "sort_histogram_buckets": [
      {"row_count": 1,"unordered_row_count": 1,"ordered_entities": 0,"unordered_entities": 1,"upper_bound": "2023-12-31 17:00:00+00:00"},
      {"row_count": 6,"unordered_row_count": 5,"ordered_entities": 1,"unordered_entities": 2,"upper_bound": "2023-12-31 18:00:00+00:00"},
      {"row_count": 2,"unordered_row_count": 0,"ordered_entities": 2,"unordered_entities": 0,"upper_bound": "2023-12-31 19:00:00+00:00"},
      {"row_count": 1,"unordered_row_count": 0,"ordered_entities": 1,"unordered_entities": 0,"upper_bound": "2023-12-31 20:00:00+00:00"},
      {"row_count": 1,"unordered_row_count": 0,"ordered_entities": 1,"unordered_entities": 0,"upper_bound": "2023-12-31 21:00:00+00:00"},
      {"row_count": 4,"unordered_row_count": 0,"ordered_entities": 4,"unordered_entities": 0,"upper_bound": "2023-12-31 22:00:00+00:00"},
      {"row_count": 3,"unordered_row_count": 3,"ordered_entities": 0,"unordered_entities": 1,"upper_bound": "2023-12-31 23:00:00+00:00"},
      {"row_count": 4,"unordered_row_count": 3,"ordered_entities": 1,"unordered_entities": 1,"upper_bound": "2024-01-01 00:00:00+00:00"},
      {"row_count": 5,"unordered_row_count": 3,"ordered_entities": 2,"unordered_entities": 1,"upper_bound": "2024-01-01 01:00:00+00:00"},
      {"row_count": 1,"unordered_row_count": 0,"ordered_entities": 1,"unordered_entities": 0,"upper_bound": "2024-01-01 02:00:00+00:00"}
    ],
    "histo_col_type": "TIMESTAMPTZ"
  }]';
show sort_histogram for table t8;

--- Partial order
Insert into t7 values ('2024-1-1 1:09:00',10,10,1.0,0.3,'c','ccc',6000,'eeee','fffff','ggg','2024-1-1 1:00:05',false,1);
Insert into t7 values ('2024-1-1 1:08:00',9,9,0.9,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',true,1);
Insert into t7 values ('2024-1-1 1:06:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,1);
Insert into t7 values ('2024-1-1 1:05:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,1);
Insert into t7 values ('2024-1-1 1:03:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,1);
Insert into t7 values ('2024-1-1 2:10:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,4);
Insert into t7 values ('2024-1-1 2:20:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,4);
Insert into t7 values ('2024-1-1 2:45:00',7,7,0.7,0.3,'c','cc',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,5);
Insert into t7 values ('2024-1-1 2:55:00',7,7,0.7,0.3,'c','cc',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,5);
Insert into t7 values ('2024-1-1 5:21:00',7,7,0.7,0.3,'c','cc',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,10);
Insert into t7 values ('2024-1-1 5:22:00',6,6,0.6,0.2,'b','bbb',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,10);
Insert into t7 values ('2024-1-1 5:23:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,10);
Insert into t7 values ('2024-1-1 5:26:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,10);
Insert into t7 values ('2024-1-1 5:27:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,10);
Insert into t7 values ('2024-1-1 5:28:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,10);
Insert into t7 values ('2024-1-1 7:51:00',4,500,0.4,0.2,'b','bb',2000,'ee','ff','gg','2024-1-1 1:00:01',false,13);
Insert into t7 values ('2024-1-1 7:52:00',3,200,0.3,0.1,'a','aaa',2000,'ee','ff','gg','2024-1-1 1:00:01',true,13);
Insert into t7 values ('2024-1-1 7:53:00',2,200,0.2,0.1,'a','aaa',1000,'e','f','g','2024-1-1 1:00:00',true,13);
Insert into t7 values ('2024-1-1 7:55:00',2,200,0.2,0.1,'a','aaa',1000,'e','f','g','2024-1-1 1:00:00',true,13);
Insert into t7 values ('2024-1-1 7:56:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,13);
Insert into t7 values ('2024-1-1 7:58:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,13);
ALTER TABLE t7 INJECT STATISTICS '[
  {
    "columns": ["size"],
    "created_at": "2024-09-05",
    "row_count": 17,
    "distinct_count": 17,
    "null_count": 0,
    "sort_histogram_buckets": [
      {"row_count": 1,"unordered_row_count": 1,"ordered_entities": 0,"unordered_entities": 1,"upper_bound": "2023-12-31 17:00:00+00:00"},
      {"row_count": 11,"unordered_row_count": 7,"ordered_entities": 2,"unordered_entities": 1,"upper_bound": "2023-12-31 18:00:00+00:00"},
      {"row_count": 6,"unordered_row_count": 6,"ordered_entities": 0,"unordered_entities": 2,"upper_bound": "2023-12-31 19:00:00+00:00"},
      {"row_count": 1,"unordered_row_count": 0,"ordered_entities": 1,"unordered_entities": 0,"upper_bound": "2023-12-31 20:00:00+00:00"},
      {"row_count": 1,"unordered_row_count": 0,"ordered_entities": 1,"unordered_entities": 0,"upper_bound": "2023-12-31 21:00:00+00:00"},
      {"row_count": 10,"unordered_row_count": 7,"ordered_entities": 3,"unordered_entities": 1,"upper_bound": "2023-12-31 22:00:00+00:00"},
      {"row_count": 3,"unordered_row_count": 0,"ordered_entities": 1,"unordered_entities": 0,"upper_bound": "2023-12-31 23:00:00+00:00"},
      {"row_count": 10,"unordered_row_count": 7,"ordered_entities": 1,"unordered_entities": 1,"upper_bound": "2024-01-01 00:00:00+00:00"},
      {"row_count": 5,"unordered_row_count": 0,"ordered_entities": 3,"unordered_entities": 0,"upper_bound": "2024-01-01 01:00:00+00:00"},
      {"row_count": 1,"unordered_row_count": 0,"ordered_entities": 1,"unordered_entities": 0,"upper_bound": "2024-01-01 02:00:00+00:00"}
    ],
    "histo_col_type": "TIMESTAMPTZ"
  }]';
show sort_histogram for table t7;

set timezone = 0;
use default;
drop database test cascade;
drop database test_select cascade;
set cluster setting sql.stats.tag_automatic_collection.enabled = true;