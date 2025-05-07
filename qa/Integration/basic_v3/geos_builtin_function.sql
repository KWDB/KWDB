create ts database tsdb;
use tsdb;
create table t(ts timestamptz not null, a geometry, b geometry) tags(ptag geometry not null) primary tags(ptag);
create table t(ts timestamptz not null, a geometry, b geometry) tags(ptag int not null, gtag geometry) primary tags(ptag);
create table t(ts timestamptz not null, a geometry, b geometry) tags(ptag int not null) primary tags(ptag);

insert into t values('2024-02-05 16:59:00.001 +0800', 'point(1.0, 1.0)', 'point(2.0, 2.0)', 1);
insert into t values('2024-02-05 16:59:00.001 +0800', 'point(1.0 1.0)', 'polygon(2.0 2.0, 2.0 1.0, 3.0 0.0, 3.0 2.0, 2.0 2.0)', 1);
insert into t values('2024-02-05 16:59:00.001 +0800', 'point(1.0 1.0)', 'point(2.0 2.0)', 1);
insert into t values('2024-02-05 16:59:00.002 +0800', 'point(1 1)', 'point(2 2)', 1);
insert into t values('2024-02-05 16:59:00.003 +0800', 'point(1.123 1.123)', 'point(2.123 2.123)', 1);
insert into t values('2024-02-05 16:59:00.004 +0800', 'point(1.1234567812345678 1.1234567812345678)', 'point(2.1234567812345678 2.1234567812345678)', 1);
insert into t values('2024-02-05 16:59:00.004 +0800', 'point(abc abc)', 'point(def def)', 1);
insert into t values('2024-02-05 16:59:00.004 +0800', 'point(* +)', 'point(! $)', 1);
insert into t values('2024-02-05 16:59:00.004 +0800', 'point(1)', 'point(2)', 1);
insert into t values('2024-02-05 16:59:00.004 +0800', 'linestring(1,1)', 'linestring(2,2)', 1);
insert into t values('2024-02-05 16:59:00.004 +0800', 'polygon((1,2,1))', 'polygon((2,3,2))', 1);
insert into t values('2024-02-05 16:59:00.005 +0800', 'point(1.0 1.0 2.0)', 'point(1.0 1.0 2.0)', 1);
insert into t values('2024-02-05 16:59:00.006 +0800', 'linestring(1 1 1, 2 2 2)', 'linestring(1 1 1, 2 2 2)', 1);
insert into t values('2024-02-05 16:59:00.006 +0800', 'point(1.0 1.0 ,1.0)', 'point(2 2)', 1);
insert into t values('2024-02-05 16:59:00.007 +0800', 'point(-1 -1)', 'point(-2 -2)', 1);
insert into t values('2024-02-05 16:59:00.007 +0800', 'point()', 'linestring()', 1);
insert into t values('2024-02-05 16:59:00.007 +0800', 'polygon()', 'polygon(())', 1);
insert into t values('2024-02-05 16:59:00.008 +0800', 'linestring(0 0, 1 1, 3 1)', 'linestring(0 0, 1 1, 3 1)', 1);
insert into t values('2024-02-05 16:59:00.009 +0800', 'linestring(0 0, 1 1, 2 2)', 'linestring(0 0, 1 1, 2 2)', 1);
insert into t values('2024-02-05 16:59:00.010 +0800', 'polygon((0 0, 4 0, 4 4, 0 4, 0 0),(1 1, 2 1, 2 2, 1 2, 1 1))', 'point(2 2)', 1);
insert into t values('2024-02-05 16:59:00.011 +0800', 'polygon((0 0, 4 0, 0 4, 0 0))', 'point(2 2)', 1);
insert into t values('2024-02-05 16:59:00.012 +0800', 'polygon((0 0, 4 0, 0 4, 0 0),(0 0, 5 0, 0 5, 0 0))', 'point(2 2)', 1);
insert into t values('2024-02-05 16:59:00.013 +0800', 'point(1.0 1.0)', NULL, 1);
insert into t values('2024-02-05 16:59:00.014 +0800', 'point(1.0 1.0)', 'linestring(0.0 0.0, 2.0 2.0)', 1);
insert into t values('2024-02-05 16:59:00.015 +0800', 'point(1.0 1.0)', 'polygon((2.0 2.0, 2.0 1.0, 3.0 0.0, 3.0 2.0, 2.0 2.0))', 1);
insert into t values('2024-02-05 16:59:00.016 +0800', 'linestring(1.0 1.0, 4.0 4.0)', 'polygon((2.0 2.0, 2.0 1.0, 3.0 1.0, 3.0 2.0, 2.0 2.0))', 1);
insert into t values('2024-02-05 16:59:00.017 +0800', 'linestring(0.0 0.0, 3.0 1.5)', 'polygon((2.0 2.0, 2.0 1.0, 3.0 1.0, 3.0 2.0, 2.0 2.0))', 1);
insert into t values('2024-02-05 16:59:00.018 +0800', 'polygon((2.0 2.0, 2.0 1.0, 3.0 1.0, 3.0 2.0, 2.0 2.0))', 'polygon((0.0 0.0, 4.0 0.0, 4.0 4.0, 0.0 4.0, 0.0 0.0))', 1);

select * from t;

select st_distance(a, b) from t;
select st_dwithin(a,b,1.0) from t;
select st_contains(b,a) from t;
select st_intersects(a,b) from t;
select st_equals(a,b) from t;
select st_touches(a,b) from t;
select st_covers(b,a) from t;
select st_area(a) from t;
select st_area(b) from t;
select st_distance(a, b::varchar) from t;

SELECT * FROM t WHERE ST_Contains(b, 'Point(2.0 2.0)');
SELECT * FROM t WHERE ST_Intersects(b, 'LINESTRING(0 0, 2 2)');

select * from t order by ts,b;
select * from t group by b order by b;
select count(*) from t group by st_area(a) order by st_area(a);
select * from t where st_area(b) > 1;
select * from t where b is null;
select last(a), last(b) from t;
select concat(a,'123456') from t;
select concat_agg(b) from t;

/*
alter table t add column c geometry;
alter table t rename c to c_new;
*/

use defaultdb;
drop database tsdb cascade;