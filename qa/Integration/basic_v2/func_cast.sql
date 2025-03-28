drop database ts_db cascade;
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

-- drop database ts_db cascade;
-- create database ts_db;
-- create table ts_db.t1 (
--     kt timestamp not null,
--     ktz timestamptz,
--     i2 int2,
--     i4 int4,
--     i8 int8,
--     f4 float4,
--     d double,
--     bt varbytes,
--     c char,
--     nc nchar,
--     vc varchar,
--     nvc nvarchar,
--     b bool, t1 int2);
    

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

create table ts_db.t3 (
    kt timestamp not null,
    ktz timestamptz,
    ktu timestamp(6),
    ktzu timestamptz(6),
    ktn timestamp(9),
    ktzn timestamptz(9))
    tags (t1 int2 not null) primary tags(t1);
insert into ts_db.t3 values ('2022-01-06 11:22:33.456+08:00', '2022-01-06 11:22:33.456+08:00','2022-01-06 11:22:33.456789+08:00', '2022-01-06 11:22:33.456789+08:00','2022-01-06 11:22:33.456789012+08:00', '2022-01-06 11:22:33.456789012+08:00',1 );
insert into ts_db.t3 values ('2022-01-06 11:22:33.006+08:00', '2022-01-06 11:22:33.006+08:00','2022-01-06 11:22:33.0007+08:00', '2022-01-06 11:22:33.0007+08:00','2022-01-06 11:22:33.000009+08:00', '2022-01-06 11:22:33.000009+08:00',2 );

select kt::char, ktz::char, kt::nchar, ktz::nchar,kt::char(10), ktz::char(10), kt::nchar(10), ktz::nchar(10), kt::varchar, ktz::varchar,kt::nvarchar, ktz::nvarchar from ts_db.t3 order by t1;
select ktu::char, ktzu::char, ktu::nchar, ktzu::nchar,ktu::char(10), ktzu::char(10), ktu::nchar(10), ktzu::nchar(10), ktu::varchar, ktzu::varchar,ktu::nvarchar, ktzu::nvarchar from ts_db.t3 order by t1;
select ktn::char, ktzn::char, ktn::nchar, ktzn::nchar,ktn::char(10), ktzn::char(10), ktn::nchar(10), ktzn::nchar(10), kt::varchar, ktzn::varchar,ktn::nvarchar, ktzn::nvarchar from ts_db.t3 order by t1;

set time zone 8;
select kt::char, ktz::char, kt::nchar, ktz::nchar,kt::char(10), ktz::char(10), kt::nchar(10), ktz::nchar(10), kt::varchar, ktz::varchar,kt::nvarchar, ktz::nvarchar from ts_db.t3 order by t1;
select ktu::char, ktzu::char, ktu::nchar, ktzu::nchar,ktu::char(10), ktzu::char(10), ktu::nchar(10), ktzu::nchar(10), ktu::varchar, ktzu::varchar,ktu::nvarchar, ktzu::nvarchar from ts_db.t3 order by t1;
select ktn::char, ktzn::char, ktn::nchar, ktzn::nchar,ktn::char(10), ktzn::char(10), ktn::nchar(10), ktzn::nchar(10), kt::varchar, ktzn::varchar,ktn::nvarchar, ktzn::nvarchar from ts_db.t3 order by t1;


drop database ts_db cascade;
