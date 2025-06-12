use defaultdb;
drop database if exists d1 cascade;
create ts database d1;
use d1;
create table t1 (ts timestamp not null, e1 int2,e2 int4,e3 int8, e4 varchar, e5 float8) tags (tag1 int not null, tag2 int, tag3 varchar)primary tags (tag1);
insert into t1 values ('2024-06-06 06:00:00', 11 , 21, 31, '41', 51, 101, 201,'301');
insert into t1 values ('2024-06-07 06:00:00', 12 , 22, 32, '42', 52, 102, 202,'302');
insert into t1 values ('2024-06-08 06:00:00', 13 , 23, 33, '43', 53, 103, 203,'303');
insert into t1 values ('2024-06-09 06:00:00', 14 , 24, 34, '44', 54, 104, 204,'304');
insert into t1 values ('2024-06-10 06:00:00', 15 , 25, 35, '45', 55, 105, 205,'305');

--1. basic expr

explain select e1 from d1.t1 where    (e1 in (select 14))    and    ((e2 > 0 or (e2 = -1 and e3 = 34)))    and    (e4 = '44')    order by e1;

select e1 from d1.t1 where    (e1 in (select 14))    and    ((e2 > 0 or (e2 = -1 and e3 = 34)))    and    (e4 = '44')    order by e1;

explain select e1 from d1.t1 where    (ts > '2020-01-01 00:00:00' or ts < '1970-01-01 00:00:00')    and    ((tag1 > 0 or (tag2 = -1 and tag3 = '300')))    and    (e4 = '44')    order by e1;

select e1 from d1.t1 where    (ts > '2020-01-01 00:00:00' or ts < '1970-01-01 00:00:00')    and    ((tag1 > 0 or (tag2 = -1 and tag3 = '300')))    and    (e4 = '44')    order by e1;

--2. function expr

explain select e1 from d1.t1 where    (length(e4) != 0 or trunc(e5) = 2)    and    (to_english(e1) = 'one-two' or cos(e5) = 0)    order by e1;

select e1 from d1.t1 where    (length(e4) != 0 or trunc(e5) = 2)    and    (to_english(e1) = 'one-two' or cos(e5) = 0)    order by e1;

explain select e1 from d1.t1 where    (abs(e1) > 3 or length(e4) < 2 or trunc(e5) = 2)    and    (to_english(e1) = 'one-one' and sin(e5) = 0)    order by e1;

select e1 from d1.t1 where    (abs(e1) > 3 or length(e4) < 2 or trunc(e5) = 2)    and    (to_english(e1) = 'one-one' and sin(e5) = 0)    order by e1;

--3. null

explain select e1 from d1.t1 where    (e4 is not null and tag2 is not null or tag3 is not null)    and    (e1 is not null or e2 is not null and e3 is not null)    order by e1;

select e1 from d1.t1 where    (e4 is not null and tag2 is not null or tag3 is not null)    and    (e1 is not null or e2 is not null and e3 is not null)    order by e1;

explain select e1 from d1.t1 where    (e1 is not null or e2 is null and e3 is not null)    and    (e4 is not null and tag2 is not null or tag3 is null)    order by e1;

select e1 from d1.t1 where    (e1 is not null or e2 is null and e3 is not null)    and    (e4 is not null and tag2 is not null or tag3 is null)    order by e1;

--4. binary

explain select e1 from d1.t1 where    (e1-1 > 0 and e3*3 != 0 )    and    (tag1/2 > 50 or tag3 like '3%')    order by e1;

select e1 from d1.t1 where    (e1-1 > 0 and e3*3 != 0 )    and    (tag1/2 > 50 or tag3 like '3%')    order by e1;

explain select e1 from d1.t1 where    (e1+e2 > 0 and e3*e5 != 0 )    and    (e3/2 > 15 or e4 like '4%')    order by e1;

select e1 from d1.t1 where    (e1+e2 > 0 and e3*e5 != 0 )    and    (e3/2 > 15 or e4 like '4%')    order by e1;

--5. between and

explain select e1 from d1.t1 where    (ts between '2020-01-01 00:00:00' and '2024-07-01 00:00:00')    and    (e2 between 1 and 99 or e3 between 2 and 98 and e5 between 3 and 97)    order by e1;

select e1 from d1.t1 where    (ts between '2020-01-01 00:00:00' and '2024-07-01 00:00:00')    and    (e2 between 1 and 99 or e3 between 2 and 98 and e5 between 3 and 97)    order by e1;

explain select e1 from d1.t1 where    (e1 between 0 and 100)    and    (e2 between 1 and 99 or e3 between 2 and 98 and e5 between 3 and 97)    order by e1;

select e1 from d1.t1 where    (e1 between 0 and 100)    and    (e2 between 1 and 99 or e3 between 2 and 98 and e5 between 3 and 97)    order by e1;

--6. case when

explain select e1 from d1.t1 where    (case tag3 when '301' then 'yes' else 'no' end)='yes' or tag1 is not null    order by e1;

select e1 from d1.t1 where    (case tag3 when '301' then 'yes' else 'no' end)='yes' or tag1 is not null    order by e1;

explain select e1 from d1.t1 where    ((case e1 when 11 then 0 else -1 end)=0 or (case e5 when 51 then 0 else -1 end)=0)     and    ((case tag3 when '301' then 'yes' else 'no' end)='yes')    order by e1;

select e1 from d1.t1 where    ((case e1 when 11 then 0 else -1 end)=0 or (case e5 when 51 then 0 else -1 end)=0)     and    ((case tag3 when '301' then 'yes' else 'no' end)='yes')    order by e1;

--7. cast

explain select e1 from d1.t1 where    (length(cast(e1 as varchar)) >= 0)    and    cast(e3 as float) != 0    order by e1;

select e1 from d1.t1 where    (length(cast(e1 as varchar)) >= 0)    and    cast(e3 as float) != 0    order by e1;

explain select e1 from d1.t1 where    (e5 in (cast (51 as float8)))    and    (e4 like '%1' and cast(e3 as float) != 0 or cast(e1 as varchar) >= '11')    order by e1;

select e1 from d1.t1 where    (e5 in (cast (51 as float8)))    and    (e4 like '%1' and cast(e3 as float) != 0 or cast(e1 as varchar) >= '11')    order by e1;

--8. coalesce

explain select e1 from d1.t1 where (coalesce(e1, 12345) < 100 or coalesce(tag2, 1) > 12345) and coalesce(e5, 1.2) is not null    order by e1;

select e1 from d1.t1 where (coalesce(e1, 12345) < 100 or coalesce(tag2, 1) > 12345) and coalesce(e5, 1.2) is not null    order by e1;

explain select e1 from d1.t1 where (coalesce(tag2, 1) > 12345) and coalesce(e5, 1.2) is not null    order by e1;

select e1 from d1.t1 where (coalesce(tag2, 1) > 12345) and coalesce(e5, 1.2) is not null    order by e1;

--9. ifnull

explain select e1 from d1.t1 where (ifnull(e1, 12345) < 100 or ifnull(tag2, 1) > 12345) and ifnull(e5, 1.2) is not null    order by e1;

select e1 from d1.t1 where (ifnull(e1, 12345) < 100 or ifnull(tag2, 1) > 12345) and ifnull(e5, 1.2) is not null    order by e1;

explain select e1 from d1.t1 where ifnull(tag2, 1) > 12345 and ifnull(e5, 1.2) is not null    order by e1;

select e1 from d1.t1 where ifnull(tag2, 1) > 12345 and ifnull(e5, 1.2) is not null    order by e1;

--10. subquery

explain select e1 from d1.t1 as t2 where    (t2.e1 in (select t3.e1 from d1.t1 as t3))    and    ((e2 > 0 or (select max(e2) from d1.t1) is not null))    order by e1;

select e1 from d1.t1 as t2 where    (t2.e1 in (select t3.e1 from d1.t1 as t3))    and    ((e2 > 0 or (select max(e2) from d1.t1) is not null))    order by e1;

explain select e1 from d1.t1 as t2 where    (((select last(e2) from d1.t1) > 20 and e1 > 0))    and    ((select last(e2) from d1.t1 where (e2 > 0 or e2 < -1)) > 0 and e1 != 0 and tag1 is not null or tag2 is null )   order by e1;

select e1 from d1.t1 as t2 where    (((select last(e2) from d1.t1) > 20 and e1 > 0))    and    ((select last(e2) from d1.t1 where (e2 > 0 or e2 < -1)) > 0 and e1 != 0 and tag1 is not null or tag2 is null )   order by e1;

explain select e1 from d1.t1 as t2 where    e1 in (select e1 from d1.t1 where    (abs(e1) > 3 or length(e4) < 2 or trunc(e5) = 2)    and    (to_english(e1) = 'one-one' and sin(e5) = 0)    order by e1)  and e2 > 0 order by e1;

select e1 from d1.t1 as t2 where    e1 in (select e1 from d1.t1 where    (abs(e1) > 3 or length(e4) < 2 or trunc(e5) = 2)    and    (to_english(e1) = 'one-one' and sin(e5) = 0)    order by e1)  and e2 > 0 order by e1;

--11. mixture

--11.1
explain select e1 from d1.t1 where    (abs(e1) > 3 or length(e4) < 2 or trunc(e5) = 2)    and    (e4 is not null or e1+e2 > 0 )    and    (e1 between 0 and 100)    and    ((case e1 when 11 then 0 else -1 end)=0)    order by e1;

select e1 from d1.t1 where    (abs(e1) > 3 or length(e4) < 2 or trunc(e5) = 2)    and    (e4 is not null or e1+e2 > 0 )    and    (e1 between 0 and 100)    and    ((case e1 when 11 then 0 else -1 end)=0)    order by e1;

--11.2
explain select e1 from d1.t1 where    (e5 in (cast (51 as float8)))    and    (e4 like '%1' and cast(e3 as float) != 0 or cast(e1 as varchar) >= '11')    and    (ifnull(e1, 12345) < 100 or ifnull(tag2, 1) > 12345)    order by e1;

select e1 from d1.t1 where    (e5 in (cast (51 as float8)))    and    (e4 like '%1' and cast(e3 as float) != 0 or cast(e1 as varchar) >= '11')    and    (ifnull(e1, 12345) < 100 or ifnull(tag2, 1) > 12345)    order by e1;

--11.3
explain select e1 from d1.t1 as t2 where    (t2.e1 in (select t3.e1 from d1.t1 as t3))    and    (e5 in (cast (51 as float8)))    and    (e1 between 0 and 100)    order by e1;

select e1 from d1.t1 as t2 where    (t2.e1 in (select t3.e1 from d1.t1 as t3))    and    (e5 in (cast (51 as float8)))    and    (e1 between 0 and 100)    order by e1;

--12. mixture timeseries and relation

explain select e1 from d1.t1 where    (to_english(e1) = 'one-one' and sin(e5) > -1)    and    (ifnull(e5, 1.2) is not null or e4 like '%1' and cast(e3 as float) != 0)     order by e1;

select e1 from d1.t1 where    (to_english(e1) = 'one-one' and sin(e5) > -1)    and    (ifnull(e5, 1.2) is not null or e4 like '%1' and cast(e3 as float) != 0)     order by e1;

explain select e1 from d1.t1 where    (ts between '2020-01-01 00:00:00' and '2024-07-01 00:00:00')    and    e5 in (cast (51 as float8))    and    coalesce(e5, 1.2) is not null    order by e1;

select e1 from d1.t1 where    (ts between '2020-01-01 00:00:00' and '2024-07-01 00:00:00')    and    e5 in (cast (51 as float8))    and    coalesce(e5, 1.2) is not null    order by e1;

explain select e1 from d1.t1 as t2 where    (t2.e1 in (select t3.e1 from d1.t1 as t3))    and     (to_english(e1) = 'one-one' or sin(e5) > -1)     order by e1;

select e1 from d1.t1 as t2 where    (t2.e1 in (select t3.e1 from d1.t1 as t3))    and     (to_english(e1) = 'one-one' or sin(e5) > -1)     order by e1;

explain select e1 from d1.t1 as t2 where    ((e2 > 0 or (select max(e2) from d1.t1) is not null))    or    (abs(e1) > 3 or length(e4) < 2 or trunc(e5) = 2)    order by e1;

select e1 from d1.t1 as t2 where    ((e2 > 0 or (select max(e2) from d1.t1) is not null))    or    (abs(e1) > 3 or length(e4) < 2 or trunc(e5) = 2)    order by e1;

explain select e1 from d1.t1 as t2 where    exists (select abs(e2) from d1.t1 where length(e4)>0)    or    (abs(e1) > 3 or length(e4) < 2 or trunc(e5) = 2)    order by e1;

select e1 from d1.t1 as t2 where    exists (select abs(e2) from d1.t1 where length(e4)>0)    or    (abs(e1) > 3 or length(e4) < 2 or trunc(e5) = 2)    order by e1;

explain select e1 from d1.t1 as t2 where    exists (select abs(e2) from d1.t1 where length(e4)>0)    or    (to_english(e1) = 'one-one'  or length(e4) < 2 or trunc(e5) = 2)    order by e1;

select e1 from d1.t1 as t2 where    exists (select abs(e2) from d1.t1 where length(e4)>0)    or    (to_english(e1) = 'one-one'  or length(e4) < 2 or trunc(e5) = 2)    order by e1;

explain select t2.e1 from d1.t1 as t2 join t1 as t3 on  (t2.e1 in (select d1.t1.e1 from t1))    or    (to_english(t2.e1) = 'one-one'  or length(t3.e4) < 2 or trunc(t3.e5) = 2)    order by e1;

select t2.e1 from d1.t1 as t2 join t1 as t3 on  (t2.e1 in (select d1.t1.e1 from t1))    or    (to_english(t2.e1) = 'one-one'  or length(t3.e4) < 2 or trunc(t3.e5) = 2)    order by e1;

explain select e1 from d1.t1 where    (ts > '2020-01-01 00:00:00' and e1 + e2 < 1000)    and    e5 in (cast (51 as float8))    and    coalesce(e5, 1.2) is not null    order by e1;

select e1 from d1.t1 where    (ts > '2020-01-01 00:00:00' and e1 + e2 < 1000)    and    e5 in (cast (51 as float8))    and    coalesce(e5, 1.2) is not null    order by e1;


explain SELECT * FROM t1
        WHERE
            (e1 > 10 AND e1 < 20 AND e2 > 20 AND e2 < 30 AND e3 > 30)
           OR
                (e4 = '41' OR e4 = '42' OR e5 > 50 OR e5 < 40 OR e5 BETWEEN 45 AND 55)
                AND
                (tag1 IN (101, 102) AND tag2 IN (201, 202) AND tag3 LIKE '3%')
                AND
                (e1 NOT IN (SELECT MIN(e1) FROM t1) OR e1 NOT IN (SELECT MAX(e1) FROM t1))
                AND
                (e2 != (SELECT AVG(e2) FROM t1) OR e3 != (SELECT AVG(e3) FROM t1))
                AND
                (e4 NOT LIKE '4%' OR e4 NOT LIKE '5%' OR e4 IS NOT NULL)
                AND
                (e5 != (SELECT STDDEV(e5) FROM t1) OR e5 != (SELECT SUM(e5) FROM t1))
                AND
                (tag1 != (SELECT MIN(tag1) FROM t1) OR tag1 != (SELECT MAX(tag1) FROM t1)) order by e1;


SELECT * FROM t1
WHERE
    (e1 > 10 AND e1 < 20 AND e2 > 20 AND e2 < 30 AND e3 > 30)
   OR
        (e4 = '41' OR e4 = '42' OR e5 > 50 OR e5 < 40 OR e5 BETWEEN 45 AND 55)
        AND
        (tag1 IN (101, 102) AND tag2 IN (201, 202) AND tag3 LIKE '3%')
        AND
        (e1 NOT IN (SELECT MIN(e1) FROM t1) OR e1 NOT IN (SELECT MAX(e1) FROM t1))
        AND
        (e2 != (SELECT AVG(e2) FROM t1) OR e3 != (SELECT AVG(e3) FROM t1))
        AND
        (e4 NOT LIKE '4%' OR e4 NOT LIKE '5%' OR e4 IS NOT NULL)
        AND
        (e5 != (SELECT STDDEV(e5) FROM t1) OR e5 != (SELECT SUM(e5) FROM t1))
        AND
        (tag1 != (SELECT MIN(tag1) FROM t1) OR tag1 != (SELECT MAX(tag1) FROM t1)) order by e1;


explain SELECT * FROM t1
        WHERE
                    e1 > 10 AND e1 < 20 AND e2 > 20
           OR
                (e3 > 30 AND e4 = '41')
                AND
                (e5 > 50 OR tag1 = 101)
                AND
                (tag2 IN (201, 202) OR tag3 LIKE '3%')
        order by e1;


SELECT * FROM t1
WHERE
            e1 > 10 AND e1 < 20 AND e2 > 20
   OR
        (e3 > 30 AND e4 = '41')
        AND
        (e5 > 50 OR tag1 = 101)
        AND
        (tag2 IN (201, 202) OR tag3 LIKE '3%')
order by e1;


explain SELECT * FROM t1
        WHERE
                e1 > (SELECT AVG(e1) FROM t1) AND e2 < (SELECT MAX(e2) FROM t1)
          AND
            (e3 > 30 OR e3 < 20)
          AND
            (e4 LIKE '4%' OR e4 LIKE '5%')
          AND
            (e5 > 50 OR tag1 = 101)
        order by e1;


SELECT * FROM t1
WHERE
        e1 > (SELECT AVG(e1) FROM t1) AND e2 < (SELECT MAX(e2) FROM t1)
  AND
    (e3 > 30 OR e3 < 20)
  AND
    (e4 LIKE '4%' OR e4 LIKE '5%')
  AND
    (e5 > 50 OR tag1 = 101)
order by e1;


explain SELECT * FROM t1
        WHERE
                    e1 > 10 AND e1 < 20 AND e2 > (SELECT AVG(e2) FROM t1)
           OR
                (e3 > 30 AND e4 LIKE '4%')
                AND
                (e5 BETWEEN 50 AND 60 OR tag1 = 101)
                AND
                (tag2 IN (201, 202) OR tag3 IS NOT NULL)
        order by e1;

SELECT * FROM t1
WHERE
            e1 > 10 AND e1 < 20 AND e2 > (SELECT AVG(e2) FROM t1)
   OR
        (e3 > 30 AND e4 LIKE '4%')
        AND
        (e5 BETWEEN 50 AND 60 OR tag1 = 101)
        AND
        (tag2 IN (201, 202) OR tag3 IS NOT NULL)
order by e1;


explain SELECT * FROM t1
        WHERE
                    abs(e1 - 10) < 5 AND e2 > 20 AND e3 < 30
           OR
                (e3 > 30 OR e3 < 20)
                AND
                (e4 LIKE '4%' OR e5 > abs(50 - (SELECT AVG(e5) FROM t1)))
                AND
                (abs(tag1 - 100) < 5 OR tag2 IN (201, 202))
        order by e1;

SELECT * FROM t1
WHERE
            abs(e1 - 10) < 5 AND e2 > 20 AND e3 < 30
   OR
        (e3 > 30 OR e3 < 20)
        AND
        (e4 LIKE '4%' OR e5 > abs(50 - (SELECT AVG(e5) FROM t1)))
        AND
        (abs(tag1 - 100) < 5 OR tag2 IN (201, 202))
order by e1;


explain SELECT * FROM t1
        WHERE
                    abs(e1 - (SELECT AVG(e1) FROM t1)) < 5 AND e2 > 20 AND e3 < (SELECT MAX(e3) FROM t1)
           OR
                (e3 > 30 OR e3 < 20)
                AND
                (e4 LIKE '4%' OR e5 > abs(50 - (SELECT STDDEV(e5) FROM t1)))
                AND
                (abs(tag1 - 100) < 5 OR tag2 IN (201, 202))
        order by e1;

SELECT * FROM t1
WHERE
            abs(e1 - (SELECT AVG(e1) FROM t1)) < 5 AND e2 > 20 AND e3 < (SELECT MAX(e3) FROM t1)
   OR
        (e3 > 30 OR e3 < 20)
        AND
        (e4 LIKE '4%' OR e5 > abs(50 - (SELECT STDDEV(e5) FROM t1)))
        AND
        (abs(tag1 - 100) < 5 OR tag2 IN (201, 202))
order by e1;

explain SELECT * FROM t1
        WHERE
                    abs(e1 - (SELECT AVG(e1) FROM t1)) < 5 AND e2 > 20 AND e3 < (SELECT MAX(e3) FROM t1)
           OR
                (e3 > (SELECT MIN(e3) FROM t1) OR e4 NOT LIKE '4%')
                AND
                (e5 > (SELECT AVG(e5) FROM t1) OR abs(e5 - (SELECT STDDEV(e5) FROM t1)) > 10)
                AND
                (tag1 = 101 OR abs(tag2 - (SELECT AVG(tag2) FROM t1)) < 5)
        order by e1;

SELECT * FROM t1
WHERE
            abs(e1 - (SELECT AVG(e1) FROM t1)) < 5 AND e2 > 20 AND e3 < (SELECT MAX(e3) FROM t1)
   OR
        (e3 > (SELECT MIN(e3) FROM t1) OR e4 NOT LIKE '4%')
        AND
        (e5 > (SELECT AVG(e5) FROM t1) OR abs(e5 - (SELECT STDDEV(e5) FROM t1)) > 10)
        AND
        (tag1 = 101 OR abs(tag2 - (SELECT AVG(tag2) FROM t1)) < 5)
order by e1;


explain SELECT * FROM t1
        WHERE
                    abs(e1 - (SELECT AVG(e1) FROM t1)) < 5 AND e2 > 20 AND e3 < (SELECT MAX(e3) FROM t1)
           OR
                (e3 > (SELECT MIN(e3) FROM t1) OR e4 NOT LIKE '4%')
                AND
                (e5 > (SELECT AVG(e5) FROM t1) OR abs(e5 - (SELECT STDDEV(e5) FROM t1)) > 10)
                AND
                (tag1 = 101 OR e4 IN (SELECT DISTINCT e4 FROM t1 WHERE e4 LIKE '4%'))
        order by e1;

SELECT * FROM t1
WHERE
            abs(e1 - (SELECT AVG(e1) FROM t1)) < 5 AND e2 > 20 AND e3 < (SELECT MAX(e3) FROM t1)
   OR
        (e3 > (SELECT MIN(e3) FROM t1) OR e4 NOT LIKE '4%')
        AND
        (e5 > (SELECT AVG(e5) FROM t1) OR abs(e5 - (SELECT STDDEV(e5) FROM t1)) > 10)
        AND
        (tag1 = 101 OR e4 IN (SELECT DISTINCT e4 FROM t1 WHERE e4 LIKE '4%'))
order by e1;


explain SELECT * FROM t1
        WHERE
            (e1 > (SELECT AVG(e1) FROM t1) AND abs(e2 - 20) < 10)
           OR
                (e3 > 30 AND e4 LIKE '4%')
                AND
                (e5 > 50 OR abs(e5 - (SELECT AVG(e5) FROM t1)) > 5)
                AND
                (tag1 = 101 OR abs(tag1 - (SELECT AVG(tag1) FROM t1)) < 10)
        order by e1;

SELECT * FROM t1
WHERE
    (e1 > (SELECT AVG(e1) FROM t1) AND abs(e2 - 20) < 10)
   OR
        (e3 > 30 AND e4 LIKE '4%')
        AND
        (e5 > 50 OR abs(e5 - (SELECT AVG(e5) FROM t1)) > 5)
        AND
        (tag1 = 101 OR abs(tag1 - (SELECT AVG(tag1) FROM t1)) < 10)
order by e1;


explain SELECT * FROM t1
        WHERE
            (e1 > (SELECT AVG(e1) FROM t1) AND abs(e2 - 20) < 10)
           OR
                (e3 > 30 AND e4 LIKE '4%')
                AND
                (e5 > 50 OR abs(e5 - (SELECT AVG(e5) FROM t1)) > 5)
                AND
                (tag1 = 101 OR abs(tag1 - (SELECT AVG(tag1) FROM t1)) < 10)
        ORDER BY e1;

SELECT * FROM t1
WHERE
    (e1 > (SELECT AVG(e1) FROM t1) AND abs(e2 - 20) < 10)
   OR
        (e3 > 30 AND e4 LIKE '4%')
        AND
        (e5 > 50 OR abs(e5 - (SELECT AVG(e5) FROM t1)) > 5)
        AND
        (tag1 = 101 OR abs(tag1 - (SELECT AVG(tag1) FROM t1)) < 10)
ORDER BY e1;

explain SELECT * FROM t1
        WHERE
            (e1 > 10 AND e2 < (SELECT MAX(e2) FROM t1))
           OR
                (e3 < 20 OR e4 NOT LIKE '%5')
                AND
                (e5 BETWEEN 40 AND 60 OR abs(e5 - 50) < 5)
                AND
                (tag2 IN (200, 300) OR tag3 LIKE '3%')
        ORDER BY e1;

SELECT * FROM t1
WHERE
    (e1 > 10 AND e2 < (SELECT MAX(e2) FROM t1))
   OR
        (e3 < 20 OR e4 NOT LIKE '%5')
        AND
        (e5 BETWEEN 40 AND 60 OR abs(e5 - 50) < 5)
        AND
        (tag2 IN (200, 300) OR tag3 LIKE '3%')
ORDER BY e1;


explain SELECT * FROM t1
        WHERE
            (e1 > (SELECT MIN(e1) FROM t1) AND e2 > 20)
           OR
                (e3 > (SELECT AVG(e3) FROM t1) OR e4 = '41')
                AND
                (e5 > 50 AND abs(e5 - (SELECT STDDEV(e5) FROM t1)) < 15)
                AND
                (tag1 != 101 OR tag2 > (SELECT MIN(tag2) FROM t1))
        ORDER BY e1;

SELECT * FROM t1
WHERE
    (e1 > (SELECT MIN(e1) FROM t1) AND e2 > 20)
   OR
        (e3 > (SELECT AVG(e3) FROM t1) OR e4 = '41')
        AND
        (e5 > 50 AND abs(e5 - (SELECT STDDEV(e5) FROM t1)) < 15)
        AND
        (tag1 != 101 OR tag2 > (SELECT MIN(tag2) FROM t1))
ORDER BY e1;

explain SELECT * FROM t1
        WHERE
            (e1 BETWEEN 10 AND 20 AND e2 < (SELECT AVG(e2) FROM t1))
           OR
                (e3 > 30 OR e3 < (SELECT MIN(e3) FROM t1))
                AND
                (e5 > (SELECT MAX(e5) FROM t1) OR e5 < 40)
                AND
                (tag1 = 101 OR abs(tag2 - 200) < 5)
        ORDER BY e1;

SELECT * FROM t1
WHERE
    (e1 BETWEEN 10 AND 20 AND e2 < (SELECT AVG(e2) FROM t1))
   OR
        (e3 > 30 OR e3 < (SELECT MIN(e3) FROM t1))
        AND
        (e5 > (SELECT MAX(e5) FROM t1) OR e5 < 40)
        AND
        (tag1 = 101 OR abs(tag2 - 200) < 5)
ORDER BY e1;


explain SELECT * FROM t1
        WHERE
            (e1 > 15 AND abs(e2 - (SELECT AVG(e2) FROM t1)) < 5)
           OR
                (e3 > 35 AND e4 NOT LIKE '%2')
                AND
                (e5 > 55 OR e5 < (SELECT MIN(e5) FROM t1))
                AND
                (tag1 != 100 OR tag3 IN ('301', '401'))
        ORDER BY e1;

SELECT * FROM t1
WHERE
    (e1 > 15 AND abs(e2 - (SELECT AVG(e2) FROM t1)) < 5)
   OR
        (e3 > 35 AND e4 NOT LIKE '%2')
        AND
        (e5 > 55 OR e5 < (SELECT MIN(e5) FROM t1))
        AND
        (tag1 != 100 OR tag3 IN ('301', '401'))
ORDER BY e1;


explain SELECT * FROM t1
        WHERE
            (e1 > 10 AND e2 = (SELECT MIN(e2) FROM t1))
           OR
                (e3 = (SELECT MAX(e3) FROM t1) OR e4 = '41')
                AND
                (e5 > 50 OR e5 < 20)
                AND
                (tag1 IN (101, 102) OR abs(tag1 - 100) < 5)
        ORDER BY e1;

SELECT * FROM t1
WHERE
    (e1 > 10 AND e2 = (SELECT MIN(e2) FROM t1))
   OR
        (e3 = (SELECT MAX(e3) FROM t1) OR e4 = '41')
        AND
        (e5 > 50 OR e5 < 20)
        AND
        (tag1 IN (101, 102) OR abs(tag1 - 100) < 5)
ORDER BY e1;

explain SELECT * FROM t1
        WHERE
            (e1 < 20 AND e2 > (SELECT AVG(e2) FROM t1))
           OR
                (e3 < 30 OR e4 NOT LIKE '4%')
                AND
                (e5 BETWEEN 45 AND 55 OR abs(e5 - 50) < 10)
                AND
                (tag2 != 201 OR tag3 NOT LIKE '3%')
        ORDER BY e1;

SELECT * FROM t1
WHERE
    (e1 < 20 AND e2 > (SELECT AVG(e2) FROM t1))
   OR
        (e3 < 30 OR e4 NOT LIKE '4%')
        AND
        (e5 BETWEEN 45 AND 55 OR abs(e5 - 50) < 10)
        AND
        (tag2 != 201 OR tag3 NOT LIKE '3%')
ORDER BY e1;


explain SELECT * FROM t1
        WHERE
            (e1 > 15 AND e3 < (SELECT MIN(e3) FROM t1))
           OR
                (e2 > 25 OR e4 IS NULL)
                AND
                (e5 > (SELECT AVG(e5) FROM t1) OR e5 < (SELECT STDDEV(e5) FROM t1))
                AND
                (tag1 = 101 OR tag2 BETWEEN 200 AND 300)
        ORDER BY e1;

SELECT * FROM t1
WHERE
    (e1 > 15 AND e3 < (SELECT MIN(e3) FROM t1))
   OR
        (e2 > 25 OR e4 IS NULL)
        AND
        (e5 > (SELECT AVG(e5) FROM t1) OR e5 < (SELECT STDDEV(e5) FROM t1))
        AND
        (tag1 = 101 OR tag2 BETWEEN 200 AND 300)
ORDER BY e1;


explain SELECT * FROM t1
        WHERE
            (e1 < 25 AND e2 != (SELECT MAX(e2) FROM t1))
           OR
                (e3 > 35 OR e4 LIKE '%4')
                AND
                (e5 > 55 OR e5 < (SELECT MIN(e5) FROM t1))
                AND
                (tag1 != 105 OR abs(tag2 - 205) < 10)
        ORDER BY e1;

SELECT * FROM t1
WHERE
    (e1 < 25 AND e2 != (SELECT MAX(e2) FROM t1))
   OR
        (e3 > 35 OR e4 LIKE '%4')
        AND
        (e5 > 55 OR e5 < (SELECT MIN(e5) FROM t1))
        AND
        (tag1 != 105 OR abs(tag2 - 205) < 10)
ORDER BY e1;

set cluster setting ts.sql.query_opt_mode=0110;

explain select e1 from d1.t1 where e2 > 0 and e4 = '44' order by e1;

select e1 from d1.t1 where e2 > 0 and e4 = '44' order by e1;

set cluster setting ts.sql.query_opt_mode=1110;

explain select e1 from d1.t1 where e2 > 0 and e4 = '44' order by e1;

select e1 from d1.t1 where e2 > 0 and e4 = '44' order by e1;

drop database if exists d1 cascade;
