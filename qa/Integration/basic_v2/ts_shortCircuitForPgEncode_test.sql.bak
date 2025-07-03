set cluster setting sql.pg_encode_short_circuit.enabled = true;

-- create database.
drop database if exists test cascade;
create database test;
use test;

create table test1(col1 smallint, col2 int, col3 bigint, col4 float, col5 bool, col6 varchar);
insert into test1 values(1000,1000000,100000000000000000,100000000000000000.101,true, 'test_c1'), (2000,2000000,200000000000000000,200000000000000000.202,true, 'test_c2');
drop database if exists test cascade;

drop database if exists test_ts cascade;
create ts database test_ts;
use test_ts;

create table ts_table
(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar)
    attributes (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float, attr5 bool, attr6 varchar)
               primary attributes (attr1);
insert into ts_table values('2023-05-31 10:00:00', 1000,1000000,100000000000000000,100000000000000000.101,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts'), ('2023-05-31 11:00:00', 2000,2000000,200000000000000000,200000000000000000.202,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts');

select 1699496698000::timestamp from ts_table;

-- Simple query of ordinary time series table.
select * from test_ts.ts_table;
select e1 from test_ts.ts_table where e2 < 2500000 group by e1 having e1 < 3000;
select attr2 from test_ts.ts_table where attr1 < 2000 group by attr2 having attr2 < 3000000;
select e1 from test_ts.ts_table order by e1 desc limit 1 offset 1;
select e1, attr1 from test_ts.ts_table order by e1 desc limit 1 offset 1;
select e1, attr2 from test_ts.ts_table order by attr2 desc limit 1 offset 1;
select e1, e2, e3, case e1 when 1000 then 10 when 2000 then 20 end as result from test_ts.ts_table where e3 < 300000000000000000;
select attr1, attr2, attr6, case attr6 when 'test_attr_c1' then 10 when 'test_attr_c2' then 20 end as result from test_ts.ts_table where e3 < 300000000000000000;
select e1, e2, e3, case e1 when 1000 then 10 when 1000 then 20 end as result from test_ts.ts_table where attr3 < 300000000000000000;
select attr1, attr6, case attr6 when 'test_attr_c1' then 10 when 'test_attr_c2' then 20 end as result from test_ts.ts_table where attr2 < 2000000;
drop database if exists test_ts cascade;

-- The impact of test type cast on short circuiting.
drop database if exists ts_db cascade;
create ts database ts_db;
create table ts_db.t1 (
                          kt timestamp not null,
                          ktz timestamptz,
                          i2 int2,
                          i4 int4,
                          i8 int8,
                          f4 float4,
                          d double,
                          bt varbytes,
                          c char,
                          nc nchar,
                          vc varchar,
                          nvc nvarchar,
                          b bool)
    tags (t1 int2 not null) primary tags(t1);

insert into ts_db.t1 values ('2022-01-01 11:22:33.456+08:00', '2022-01-01 11:22:33.456+08:00' , 0,0,0,0.0,0.0,'0','0','0','000','000',true,1);

select kt::int2, kt::int4, kt::int8, ktz::int2, ktz::int4, ktz::int8 from ts_db.t1;
select kt::float4, kt::double, ktz::float4, ktz::double from ts_db.t1;
select kt::char, ktz::char, kt::nchar, ktz::nchar,kt::char(10), ktz::char(10), kt::nchar(10), ktz::nchar(10), kt::varchar, ktz::varchar,kt::nvarchar, ktz::nvarchar from ts_db.t1;

select i2::int2, i2::int4, i2::int8, i4::int2, i4::int4, i4::int8, i8::int2, i8::int4, i8::int8 from ts_db.t1;
select i2::float4, i2::double, i4::float4, i4::double, i8::float4, i8::double from ts_db.t1;

select f4::int2, f4::int4, f4::int8, d::int2, d::int4, d::int8 from ts_db.t1;

insert into ts_db.t1 values ('2022-01-02 11:22:33.456+08:00', '2022-01-02 11:22:33.456+08:00' , -32768,-2147483648,-9223372036854774808,-9223372036854774808.1,-9223372036854774808.1,'1','1','1','2e3','2e3',false,1);
insert into ts_db.t1 values ('2022-01-03 11:22:33.456+08:00', '2022-01-03 11:22:33.456+08:00' , 32767,2147483647,9223372036854774807,9223372036854774807,9223372036854774807,'2','2','2','-2.2','-2.2',false,1);

select i2::int4, i2::int8, i4::int8 from ts_db.t1;
select i4::int2 from ts_db.t1;
select i8::int2 from ts_db.t1;
select i8::int4 from ts_db.t1;

select f4::int2 from ts_db.t1;
select f4::int4 from ts_db.t1;
select d::int2 from ts_db.t1;
select d::int4 from ts_db.t1;

select i2::char, i4::char,i8::char, i2::nchar, i4::nchar, i8::nchar, i2::char(10), i4::char(10), i8::char(10),i2::nchar(10), i4::nchar(10), i8::nchar(10), i2::varchar, i4::varchar, i8::varchar, i2::nvarchar, i4::nvarchar, i8::nvarchar from ts_db.t1;
select f4::char, d::char, f4::nchar, d::nchar, f4::char(10), d::char(10), f4::nchar(10), d::nchar(10), f4::varchar, d::varchar, f4::nvarchar, d::nvarchar from ts_db.t1;

select f4::double, d::float4 from ts_db.t1;

insert into ts_db.t1 values ('2022-01-04 11:22:33.456+08:00', '2022-01-04 11:22:33.456+08:00' , 1,1,1,1.0,1.0,'3','3','3','3.3e3','2.3e3',true,1);
insert into ts_db.t1 values ('2022-01-05 11:22:33.456+08:00', '2022-01-05 11:22:33.456+08:00' , 1,1,1,1.0,1.0,'3','3','3','3.3e-3','3.3e-3',true,1);

select c::int2, c::int4, c::int8, vc::int2, vc::int4, vc::int8 from ts_db.t1;

select c::float4, c::double, vc::float4, vc::double from ts_db.t1;

create table ts_db.t2 (
                          kt timestamp not null,
                          ktz timestamptz,
                          i2 int2,
                          i4 int4,
                          i8 int8,
                          f4 float4,
                          d double,
                          bt varbytes,
                          c char,
                          nc nchar,
                          vc varchar,
                          nvc nvarchar,
                          b bool)
    tags (t1 int2 not null) primary tags(t1);
insert into ts_db.t2 values ('2022-01-06 11:22:33.456+08:00', '2022-01-06 11:22:33.456+08:00' , 1,1,1641439353456,1641439353456.0,1641439353456.0,'3','3','3','2022-01-04 11:22:33.456+08:00','2022-01-04 11:22:33.456+08:00',true,1);
select vc::timestamp, vc::timestamptz, i8::timestamp, i8::timestamptz from ts_db.t2;
set time zone 8;
select vc::timestamp, vc::timestamptz, i8::timestamp, i8::timestamptz from ts_db.t2;

create table ts_db.t3 (kt timestamptz not null, s1 varchar(8) not null,s2 char(8),s3 varbytes(8)) tags (t1 int2 not null) primary tags(t1);
insert into ts_db.t3 values (now(), 'var', ' 你好2' , 'E''\\x01' , 1);
insert into ts_db.t3 values (now(), 'var', '\U' , 'E''\\x02' ,1);
select  s1, s2, s3 from ts_db.t3;

drop database ts_db cascade;

set cluster setting sql.pg_encode_short_circuit.enabled = false;

