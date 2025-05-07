drop database if exists only_tag cascade;
create ts database only_tag;
use only_tag;
create table table1(ts timestamp not null, a int, b int ) tags (ptag1 int not null, t1 int, t2 int, t3 int) primary tags (ptag1);
insert into table1 values ('2024-9-12 10:00:00', 1, 2, 100, 1000, 10000, 10000),('2024-9-12 10:00:01', 1, 2, 100, 1000, 10000, 10000),('2024-9-12 10:00:02', 2, 2, 200, 2000, 20000, 20000),('2024-9-12 10:00:03', 3, 2, 300, 3000, 30000, 30000),('2024-9-12 10:00:04', 4, 2, 400, 4000, 40000, 40000),('2024-9-12 10:00:05', 5, 2, 500, 5000, 50000, 50000),('2024-9-12 10:00:06', 6, 2, 600, 6000, 60000, 60000),('2024-9-12 10:00:07', 7, 2, 700, 7000, 70000, 70000);


create table table2(ts timestamp not null, a int, b int ) tags (ptag1 int not null, t1 int not null, t2 int, t3 int) primary tags (ptag1, t1);
insert into table2 values ('2024-9-12 10:00:00', 1, 2, 100, 1000, 10000, 10000),('2024-9-12 10:00:01', 1, 2, 100, 1000, 10000, 10000),('2024-9-12 10:00:02', 2, 2, 200, 2000, 20000, 20000),('2024-9-12 10:00:03', 3, 2, 300, 3000, 30000, 30000),('2024-9-12 10:00:04', 4, 2, 400, 4000, 40000, 40000),('2024-9-12 10:00:05', 5, 2, 500, 5000, 50000, 50000),('2024-9-12 10:00:06', 6, 2, 600, 6000, 60000, 60000),('2024-9-12 10:00:07', 7, 2, 700, 7000, 70000, 70000);


create table table3(ts timestamp not null, a int) tags (ptag1 int not null, t1 double, t2 double, t3 double, t4 char(10)) primary tags (ptag1);
insert into table3 values 
('2024-9-12 10:00:00', 1, 100, 1000, 10000, 10000, '1000'),('2024-9-12 10:00:02', 2, 200, 2000, 20000, 20000, '2000'),('2024-9-12 10:00:03', 3, 300, 3000, 30000, 30000, '3000'),('2024-9-12 10:00:04', 4, 400, 4000, 40000, 40000, '4000'),('2024-9-12 10:00:05', 5, 500, 5000, 50000, 50000, '5000'),('2024-9-12 10:00:06', 6, 600, 6000, 60000, 60000, '6000'),('2024-9-12 10:00:07', 7, 700, 7000, 70000, 70000, '7000'),('2024-9-12 10:00:00', 8, 800, 8000, 80000, 80000, '8000'),('2024-9-12 10:00:02', 9, 900, 9000, 90000, 90000, '9000'),('2024-9-12 10:00:03', 10, 1000, 10000, 100000, 100000, '10000');


-- t1 only one ptag
-- distinct
explain select distinct ptag1 from only_tag.table1;
explain select distinct ptag1, t1 from only_tag.table1;
explain select distinct ptag1, t1, t2 from only_tag.table1;
explain select distinct ptag1, t1, t2, t3 from only_tag.table1;

-- group by contain ptag
explain select ptag1 from only_tag.table1 group by ptag1;
explain select ptag1, t1 from only_tag.table1 group by ptag1, t1;
explain select ptag1, t1, t2 from only_tag.table1 group by ptag1, t1, t2;
explain select ptag1, t1, t2, t3 from only_tag.table1 group by ptag1, t1, t2, t3;

explain select ptag1, sum(t1) from only_tag.table1 group by ptag1;
explain select sum(t1) from only_tag.table1 group by ptag1;
explain select ptag1, t1, sum(t2) from only_tag.table1 group by ptag1, t1;
explain select ptag1, t1, t2, sum(t3) from only_tag.table1 group by ptag1, t1, t2;
explain select ptag1, sum(t1), sum(t2), sum(t3) from only_tag.table1 group by ptag1, t1, t2, t3;

-- ptag filter
explain select ptag1 from only_tag.table1 where ptag1 = 100 group by ptag1;
explain select ptag1, t1 from only_tag.table1 where ptag1 = 100 group by ptag1, t1;
explain select ptag1, t1, t2 from only_tag.table1 where ptag1 = 100 group by ptag1, t1, t2;
explain select ptag1, t1, t2, t3 from only_tag.table1 where ptag1 = 100 group by ptag1, t1, t2, t3;

-- tag filter

explain select ptag1 from only_tag.table1 where t1 > 1000 group by ptag1;
explain select ptag1, t1 from only_tag.table1 where t1 > 1000 group by ptag1, t1;
explain select ptag1, t1, t2 from only_tag.table1 where t1 > 1000 group by ptag1, t1, t2;
explain select ptag1, t1, t2, t3 from only_tag.table1 where t1 > 1000 group by ptag1, t1, t2, t3;

explain select ptag1 from only_tag.table1 where ptag1 = 100 and t1 > 1000 group by ptag1;
explain select ptag1, t1 from only_tag.table1 where ptag1 = 100 and t1 > 1000 group by ptag1, t1;
explain select ptag1, t1, t2 from only_tag.table1 where ptag1 = 100 and t1 > 1000 group by ptag1, t1, t2;
explain select ptag1, t1, t2, t3 from only_tag.table1 where ptag1 = 100 and t1 > 1000 group by ptag1, t1, t2, t3;

-- exists project
explain select ptag1, sum(t1+t2) from only_tag.table1 group by ptag1;
explain select sum(t1+t2) from only_tag.table1 group by ptag1;
explain select ptag1, sum(t1+t2) from only_tag.table1 group by ptag1, t1;
explain select ptag1, sum(t1+t2) from only_tag.table1 group by ptag1, t1, t2;
explain select ptag1, sum(t1+t2+t3) from only_tag.table1 group by ptag1, t1, t2, t3;

-- having
explain select sum(t1+t2) from only_tag.table1 group by ptag1 having sum(t1) > 3000;
explain select sum(t1+t2) from only_tag.table1 group by ptag1 having sum(t1) > 3000;
explain select sum(t1+t2) from only_tag.table1 group by ptag1, t1 having sum(t1) > 3000;
explain select sum(t1+t2) from only_tag.table1 group by ptag1, t1, t2 having sum(t1) > 3000;
explain select sum(t1+t2+t3) from only_tag.table1 group by ptag1, t1, t2, t3 having sum(t1) > 3000;

-- exists order by 
explain select ptag1, sum(t1+t2) as sm from only_tag.table1 where ptag1 = 100 and t1 > 1000 group by ptag1 order by sm;
explain select ptag1, sum(t1+t2) as sm from only_tag.table1 where ptag1 = 100 and t1 > 1000 group by ptag1, t1 order by sm;
explain select ptag1, sum(t1+t2) as sm from only_tag.table1 where ptag1 = 100 and t1 > 1000 group by ptag1, t1, t2 order by sm;
explain select ptag1, sum(t1+t2) as sm from only_tag.table1 where ptag1 = 100 and t1 > 1000 group by ptag1, t1, t2, t3 order by sm;


-- exists limit 
explain select ptag1, sum(t1+t2) as sm from only_tag.table1 where t1 > 1000 group by ptag1 order by sm limit 2 offset 1;
explain select ptag1, sum(t1+t2) as sm from only_tag.table1 where t1 > 1000 group by ptag1, t1 order by sm limit 2 offset 1;
explain select ptag1, sum(t1+t2) as sm from only_tag.table1 where t1 > 1000 group by ptag1, t1, t2 order by sm limit 2 offset 1;
explain select ptag1, sum(t1+t2) as sm from only_tag.table1 where t1 > 1000 group by ptag1, t1, t2, t3 order by sm limit 2 offset 1;


-- t2  ptag
-- distinct
explain select distinct ptag1 from only_tag.table2;
explain select distinct ptag1, t1 from only_tag.table2;
explain select distinct ptag1, t1, t2 from only_tag.table2;
explain select distinct ptag1, t1, t2, t3 from only_tag.table2;

-- group by contain ptag
explain select ptag1 from only_tag.table2 group by ptag1;
explain select ptag1, t1 from only_tag.table2 group by ptag1, t1;
explain select ptag1, t1, t2 from only_tag.table2 group by ptag1, t1, t2;
explain select ptag1, t1, t2, t3 from only_tag.table2 group by ptag1, t1, t2, t3;

explain select ptag1, sum(t1) from only_tag.table2 group by ptag1;
explain select ptag1, t1, sum(t2) from only_tag.table2 group by ptag1, t1;
explain select ptag1, t1, t2, sum(t3) from only_tag.table2 group by ptag1, t1, t2;
explain select ptag1, sum(t1), sum(t2), sum(t3) from only_tag.table2 group by ptag1, t1, t2, t3;

-- ptag filter
explain select ptag1 from only_tag.table2 where ptag1 = 100 group by ptag1;
explain select ptag1, t1 from only_tag.table2 where ptag1 = 100 group by ptag1, t1;
explain select ptag1, t1, t2 from only_tag.table2 where ptag1 = 100 group by ptag1, t1, t2;
explain select ptag1, t1, t2, t3 from only_tag.table2 where ptag1 = 100 group by ptag1, t1, t2, t3;

-- tag filter
explain select ptag1 from only_tag.table2 where ptag1 = 100 and t1 > 1000 group by ptag1;
explain select ptag1, t1 from only_tag.table2 where ptag1 = 100 and t1 > 1000 group by ptag1, t1;
explain select ptag1, t1, t2 from only_tag.table2 where ptag1 = 100 and t1 > 1000 group by ptag1, t1, t2;
explain select ptag1, t1, t2, t3 from only_tag.table2 where ptag1 = 100 and t1 > 1000 group by ptag1, t1, t2, t3;

-- exists project
explain select ptag1, sum(t1+t2) from only_tag.table2 group by ptag1;
explain select ptag1, sum(t1+t2) from only_tag.table2 group by ptag1, t1;
explain select ptag1, sum(t1+t2) from only_tag.table2 group by ptag1, t1, t2;
explain select ptag1, sum(t1+t2+t3) from only_tag.table2 group by ptag1, t1, t2, t3;

-- exists having
explain select ptag1, sum(t1+t2) from only_tag.table2 group by ptag1 having sum(t1) > 3000;
explain select sum(t1+t2) from only_tag.table2 group by ptag1 having sum(t1) > 3000;
explain select ptag1, sum(t1+t2) from only_tag.table2 group by ptag1, t1 having sum(t1) > 3000;
explain select ptag1, sum(t1+t2) from only_tag.table2 group by ptag1, t1, t2 having sum(t1) > 3000;
explain select ptag1, sum(t1+t2+t3) from only_tag.table2 group by ptag1, t1, t2, t3 having sum(t1) > 3000;

-- exists order by 
explain select ptag1, sum(t1+t2) as sm from only_tag.table2 where ptag1 = 100 and t1 > 1000 group by ptag1 order by sm;
explain select ptag1, sum(t1+t2) as sm from only_tag.table2 where ptag1 = 100 and t1 > 1000 group by ptag1, t1 order by sm;
explain select ptag1, sum(t1+t2) as sm from only_tag.table2 where ptag1 = 100 and t1 > 1000 group by ptag1, t1, t2 order by sm;
explain select ptag1, sum(t1+t2) as sm from only_tag.table2 where ptag1 = 100 and t1 > 1000 group by ptag1, t1, t2, t3 order by sm;

-- exists limit 
explain select ptag1, sum(t1+t2) as sm from only_tag.table2 where t1 > 1000 group by ptag1 order by sm limit 2 offset 1;
explain select ptag1, sum(t1+t2) as sm from only_tag.table2 where t1 > 1000 group by ptag1, t1 order by sm limit 2 offset 1;
explain select ptag1, sum(t1+t2) as sm from only_tag.table2 where t1 > 1000 group by ptag1, t1, t2 order by sm limit 2 offset 1;
explain select ptag1, sum(t1+t2) as sm from only_tag.table2 where t1 > 1000 group by ptag1, t1, t2, t3 order by sm limit 2 offset 1;



-- t1 only one ptag result
-- distinct
select distinct ptag1 from only_tag.table1 ORDER by ptag1;
select distinct ptag1, t1 from only_tag.table1 ORDER by ptag1;
select distinct ptag1, t1, t2 from only_tag.table1 ORDER by ptag1;
select distinct ptag1, t1, t2, t3 from only_tag.table1 ORDER by ptag1;

-- group by contain ptag
select ptag1 from only_tag.table1 group by ptag1 ORDER by ptag1;
select ptag1, t1 from only_tag.table1 group by ptag1, t1 ORDER by ptag1;
select ptag1, t1, t2 from only_tag.table1 group by ptag1, t1, t2 ORDER by ptag1;
select ptag1, t1, t2, t3 from only_tag.table1 group by ptag1, t1, t2, t3 ORDER by ptag1;

select ptag1, sum(t1) from only_tag.table1 group by ptag1 ORDER by ptag1;
select ptag1, t1, sum(t2) from only_tag.table1 group by ptag1, t1 ORDER by ptag1;
select ptag1, t1, t2, sum(t3) from only_tag.table1 group by ptag1, t1, t2 ORDER by ptag1;
select ptag1, sum(t1), sum(t2), sum(t3) from only_tag.table1 group by ptag1, t1, t2, t3 ORDER by ptag1;

-- ptag filter
select ptag1 from only_tag.table1 where ptag1 = 100 group by ptag1 ORDER by ptag1;
select ptag1, t1 from only_tag.table1 where ptag1 = 100 group by ptag1, t1 ORDER by ptag1;
select ptag1, t1, t2 from only_tag.table1 where ptag1 = 100 group by ptag1, t1, t2 ORDER by ptag1;
select ptag1, t1, t2, t3 from only_tag.table1 where ptag1 = 100 group by ptag1, t1, t2, t3 ORDER by ptag1;

-- tag filter

select ptag1 from only_tag.table1 where t1 > 1000 group by ptag1 ORDER by ptag1;
select ptag1, t1 from only_tag.table1 where t1 > 1000 group by ptag1, t1 ORDER by ptag1;
select ptag1, t1, t2 from only_tag.table1 where t1 > 1000 group by ptag1, t1, t2 ORDER by ptag1;
select ptag1, t1, t2, t3 from only_tag.table1 where t1 > 1000 group by ptag1, t1, t2, t3 ORDER by ptag1;

select ptag1 from only_tag.table1 where ptag1 = 200 and t1 > 1000 group by ptag1 ORDER by ptag1;
select ptag1, t1 from only_tag.table1 where ptag1 = 200 and t1 > 1000 group by ptag1, t1 ORDER by ptag1;
select ptag1, t1, t2 from only_tag.table1 where ptag1 = 200 and t1 > 1000 group by ptag1, t1, t2 ORDER by ptag1;
select ptag1, t1, t2, t3 from only_tag.table1 where ptag1 = 200 and t1 > 1000 group by ptag1, t1, t2, t3 ORDER by ptag1;

-- exists project
select ptag1, sum(t1+t2) from only_tag.table1 group by ptag1 ORDER by ptag1;
select ptag1, sum(t1+t2) from only_tag.table1 group by ptag1, t1 ORDER by ptag1;
select ptag1, sum(t1+t2) from only_tag.table1 group by ptag1, t1, t2 ORDER by ptag1;
select ptag1, sum(t1+t2+t3) from only_tag.table1 group by ptag1, t1, t2, t3 ORDER by ptag1;

-- having
select sum(t1+t2) as s1 from only_tag.table1 group by ptag1 having sum(t1) > 3000 order by s1;
select sum(t1+t2) as s1 from only_tag.table1 group by ptag1 having sum(t1) > 3000 order by s1;
select sum(t1+t2) as s1 from only_tag.table1 group by ptag1, t1 having sum(t1) > 3000 order by s1;
select sum(t1+t2) as s1 from only_tag.table1 group by ptag1, t1, t2 having sum(t1) > 3000 order by s1;
select sum(t1+t2+t3) as s1 from only_tag.table1 group by ptag1, t1, t2, t3 having sum(t1) > 3000 order by s1;

-- exists order by 
select ptag1, sum(t1+t2) as sm from only_tag.table1 where ptag1 = 200 and t1 > 1000 group by ptag1 order by sm;
select ptag1, sum(t1+t2) as sm from only_tag.table1 where ptag1 = 200 and t1 > 1000 group by ptag1, t1 order by sm;
select ptag1, sum(t1+t2) as sm from only_tag.table1 where ptag1 = 200 and t1 > 1000 group by ptag1, t1, t2 order by sm;
select ptag1, sum(t1+t2) as sm from only_tag.table1 where ptag1 = 200 and t1 > 1000 group by ptag1, t1, t2, t3 order by sm;


-- exists limit 
select ptag1, sum(t1+t2) as sm from only_tag.table1 where t1 > 1000 group by ptag1 order by sm limit 2 offset 1;
select ptag1, sum(t1+t2) as sm from only_tag.table1 where t1 > 1000 group by ptag1, t1 order by sm limit 2 offset 1;
select ptag1, sum(t1+t2) as sm from only_tag.table1 where t1 > 1000 group by ptag1, t1, t2 order by sm limit 2 offset 1;
select ptag1, sum(t1+t2) as sm from only_tag.table1 where t1 > 1000 group by ptag1, t1, t2, t3 order by sm limit 2 offset 1;


-- t2  ptag
-- distinct
select distinct ptag1 from only_tag.table2 ORDER by ptag1;
select distinct ptag1, t1 from only_tag.table2 ORDER by ptag1;
select distinct ptag1, t1, t2 from only_tag.table2 ORDER by ptag1;
select distinct ptag1, t1, t2, t3 from only_tag.table2 ORDER by ptag1;

-- group by contain ptag
select ptag1 from only_tag.table2 group by ptag1 ORDER by ptag1;
select ptag1, t1 from only_tag.table2 group by ptag1, t1 ORDER by ptag1;
select ptag1, t1, t2 from only_tag.table2 group by ptag1, t1, t2 ORDER by ptag1;
select ptag1, t1, t2, t3 from only_tag.table2 group by ptag1, t1, t2, t3 ORDER by ptag1;

select ptag1, sum(t1) from only_tag.table2 group by ptag1 ORDER by ptag1;
select ptag1, t1, sum(t2) from only_tag.table2 group by ptag1, t1 ORDER by ptag1;
select ptag1, t1, t2, sum(t3) from only_tag.table2 group by ptag1, t1, t2 ORDER by ptag1;
select ptag1, sum(t1), sum(t2), sum(t3) from only_tag.table2 group by ptag1, t1, t2, t3 ORDER by ptag1;

-- ptag filter
select ptag1 from only_tag.table2 where ptag1 = 100 group by ptag1 ORDER by ptag1;
select ptag1, t1 from only_tag.table2 where ptag1 = 100 group by ptag1, t1 ORDER by ptag1;
select ptag1, t1, t2 from only_tag.table2 where ptag1 = 100 group by ptag1, t1, t2 ORDER by ptag1;
select ptag1, t1, t2, t3 from only_tag.table2 where ptag1 = 100 group by ptag1, t1, t2, t3 ORDER by ptag1;

-- tag filter
select ptag1 from only_tag.table2 where ptag1 = 200 and t1 > 1000 group by ptag1 ORDER by ptag1;
select ptag1, t1 from only_tag.table2 where ptag1 = 200 and t1 > 1000 group by ptag1, t1 ORDER by ptag1;
select ptag1, t1, t2 from only_tag.table2 where ptag1 = 200 and t1 > 1000 group by ptag1, t1, t2 ORDER by ptag1;
select ptag1, t1, t2, t3 from only_tag.table2 where ptag1 = 200 and t1 > 1000 group by ptag1, t1, t2, t3 ORDER by ptag1;

-- exists project
select ptag1, sum(t1+t2) from only_tag.table2 group by ptag1 ORDER by ptag1;
select ptag1, sum(t1+t2) from only_tag.table2 group by ptag1, t1 ORDER by ptag1;
select ptag1, sum(t1+t2) from only_tag.table2 group by ptag1, t1, t2 ORDER by ptag1;
select ptag1, sum(t1+t2+t3) from only_tag.table2 group by ptag1, t1, t2, t3 ORDER by ptag1;

-- exists having
select sum(t1+t2) as s1 from only_tag.table2 group by ptag1 having sum(t1) > 3000 order by s1;
select sum(t1+t2) as s1 from only_tag.table2 group by ptag1 having sum(t1) > 3000 order by s1;
select sum(t1+t2) as s1 from only_tag.table2 group by ptag1, t1 having sum(t1) > 3000 order by s1;
select sum(t1+t2) as s1 from only_tag.table2 group by ptag1, t1, t2 having sum(t1) > 3000 order by s1;
select sum(t1+t2+t3) as s1 from only_tag.table2 group by ptag1, t1, t2, t3 having sum(t1) > 3000 order by s1;

-- exists order by 
select ptag1, sum(t1+t2) as sm from only_tag.table2 where ptag1 = 200 and t1 > 1000 group by ptag1 order by sm;
select ptag1, sum(t1+t2) as sm from only_tag.table2 where ptag1 = 200 and t1 > 1000 group by ptag1, t1 order by sm;
select ptag1, sum(t1+t2) as sm from only_tag.table2 where ptag1 = 200 and t1 > 1000 group by ptag1, t1, t2 order by sm;
select ptag1, sum(t1+t2) as sm from only_tag.table2 where ptag1 = 200 and t1 > 1000 group by ptag1, t1, t2, t3 order by sm;

-- exists limit 
select ptag1, sum(t1+t2) as sm from only_tag.table2 where t1 > 1000 group by ptag1 order by sm limit 2 offset 1;
select ptag1, sum(t1+t2) as sm from only_tag.table2 where t1 > 1000 group by ptag1, t1 order by sm limit 2 offset 1;
select ptag1, sum(t1+t2) as sm from only_tag.table2 where t1 > 1000 group by ptag1, t1, t2 order by sm limit 2 offset 1;
select ptag1, sum(t1+t2) as sm from only_tag.table2 where t1 > 1000 group by ptag1, t1, t2, t3 order by sm limit 2 offset 1;


-- can not push down filter t1+t2 > 3000
explain select avg(t1), sum(t2), count(t3) from only_tag.table3 where t1+t2 > 3000 and t3 < 80000 group by ptag1;
select avg(t1), sum(t2), count(t3) from only_tag.table3 where t1+t2 > 3000 and t3 < 80000 group by ptag1 order by avg(t1);


use defaultdb;
drop database only_tag cascade;