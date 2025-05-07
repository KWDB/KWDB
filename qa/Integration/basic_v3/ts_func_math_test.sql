-- create ts database and table
drop database if EXISTS ts_db cascade;
create ts database ts_db;
create table ts_db.t1(
k_timestamp timestamp not null,
e1 int2 not null,
e2 int not null,
e3 int8 not null,
e4 float4 not null,
e5 float8 not null,
e6 char(10) not null,
e7 char(10) not null,
e8 varbytes(20) not null,
e9 varbytes(20) not null) TAGS (code1 int2 not null) PRIMARY TAGS(code1);
create table ts_db.t2(
k_timestamp timestamp not null,
e1 float4 not null,
e2 float8 not null
) TAGS (code1 int2 not null) PRIMARY TAGS(code1);
create table ts_db.t3(
k_timestamp timestamp not null,
e1 float4 not null,
e2 float8 not null
) TAGS (code1 int2 not null) PRIMARY TAGS(code1);

-- insert data
insert into ts_db.t1 values('2018-10-10 10:00:00', -1000, 2000, 233, 123.45, 196.32, 'e2', 'a3', b'\x123', b'\xaa\xbb\xee', 100);
insert into ts_db.t1 values('2018-10-10 10:00:01', 0, 3000, 322, 0.0, 156.89, 'abc', 'e2f', b'\x1234', b'\xaa\xbb\xcc', 100);
insert into ts_db.t1 values('2018-10-10 10:00:02', 3000, 4000, 555, -98.63, 298.36, 'ab', 'cf', b'\xff', b'\xaa\xbb\x12',100);
insert into ts_db.t1 values('2018-10-10 10:00:03', 3000, 2000, 666, 56.98, 16.32, 'abc', '123', b'\x123', b'\xaa\xbb\xcc', 100);
insert into ts_db.t1 values('2018-10-10 10:00:04', 2000, 2000, 777, 0.5, 16.32, '1q', 'a1c', b'\xa1', b'\xa1\xb2\xe3', 100);
insert into ts_db.t2 values('2018-10-10 10:00:05', 123.45, 196.32, 100);
insert into ts_db.t2 values('2018-10-10 10:00:06', 56.98, 16.32, 100);
insert into ts_db.t2 values('2018-10-10 10:00:07', 0.5, 16.32, 100);
insert into ts_db.t3 values('2018-10-10 10:00:08', -98.63, 298.36, 100);
insert into ts_db.t3 values('2018-10-10 10:00:09', 98.63, -298.36, 100);

-- select
SELECT isnan(e4) FROM ts_db.t1;
SELECT isnan(e5) FROM ts_db.t1;

SELECT ln(e4) FROM ts_db.t1;
SELECT ln(e5) FROM ts_db.t1;

SELECT log(e4) FROM ts_db.t1;
SELECT log(e5) FROM ts_db.t1;
SELECT log(e4, e5) FROM ts_db.t1;
SELECT log(e5, e4) FROM ts_db.t1;
SELECT log(e1, e2) FROM ts_db.t2;
SELECT log(e2, e1) FROM ts_db.t2;
SELECT log(e1, e2) FROM ts_db.t3;
SELECT log(e2, e1) FROM ts_db.t3;

SELECT radians(e4) FROM ts_db.t1;
SELECT radians(e5) FROM ts_db.t1;

SELECT sign(e1) FROM ts_db.t1;
SELECT sign(e2) FROM ts_db.t1;
SELECT sign(e3) FROM ts_db.t1;
SELECT sign(e4) FROM ts_db.t1;
SELECT sign(e5) FROM ts_db.t1;

SELECT trunc(e4) FROM ts_db.t1;
SELECT trunc(e5) FROM ts_db.t1;

SELECT width_bucket(0, e1, e2, 3) FROM ts_db.t1;
SELECT width_bucket(0, e2, e3, 3) FROM ts_db.t1;
SELECT width_bucket(0, e1, e3, 3) FROM ts_db.t1;

SELECT mod(e1, e2) FROM ts_db.t1;
SELECT mod(e2, e1) FROM ts_db.t1;
SELECT mod(e4, e5) FROM ts_db.t1;
SELECT mod(e5, e4) FROM ts_db.t1;

SELECT pow(e1, e2) FROM ts_db.t1;
SELECT pow(e2, e1) FROM ts_db.t1;
SELECT pow(e4, e5) FROM ts_db.t1;
SELECT pow(e5, e4) FROM ts_db.t1;
SELECT pow(0,0) FROM ts_db.t1;

SELECT round(e4) FROM ts_db.t1;
SELECT round(e5) FROM ts_db.t1;

SELECT sin(e4) FROM ts_db.t1;
SELECT sin(e5) FROM ts_db.t1;

SELECT sqrt(e4) FROM ts_db.t1;
SELECT sqrt(e5) FROM ts_db.t1;

SELECT tan(e4) FROM ts_db.t1;
SELECT tan(e5) FROM ts_db.t1;

SELECT atan2(e4,e5) FROM ts_db.t1;
SELECT cbrt(e4) FROM ts_db.t1;
SELECT cot(e4) FROM ts_db.t1;
SELECT div(e1,e2) FROM ts_db.t1;
SELECT div(e4,e5) FROM ts_db.t1;
SELECT exp(e4) FROM ts_db.t1;

SELECT fnv32(e6,e7) FROM ts_db.t1;
SELECT fnv32(e8,e9) FROM ts_db.t1;
SELECT fnv32a(e6,e7) FROM ts_db.t1;
SELECT fnv32a(e8,e9) FROM ts_db.t1;

SELECT fnv64(e6,e7) FROM ts_db.t1;
SELECT fnv64(e8,e9) FROM ts_db.t1;
SELECT fnv64a(e6,e7) FROM ts_db.t1;
SELECT fnv64a(e8,e9) FROM ts_db.t1;

SELECT crc32c(e6,e7) FROM ts_db.t1;
SELECT crc32c(e8,e9) FROM ts_db.t1;
SELECT crc32ieee(e6,e7) FROM ts_db.t1;
SELECT crc32ieee(e8,e9) FROM ts_db.t1;

drop database if EXISTS ts_db cascade;