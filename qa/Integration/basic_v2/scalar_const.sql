set cluster setting sql.query_cache.enabled=true;

drop database if EXISTS test;
drop database if EXISTS test_ts;

-- create
create database test;
use test;

create table test1(col1 smallint primary key, col2 int, col3 bigint, col4 float, col5 bool, col6 varchar);
insert into test1 values(1000,1000000,100000000000000000,100000000000000000.101,true, 'test_c1'), (2000,2000000,200000000000000000,200000000000000000.202,true, 'test_c2');

create ts database test_ts;
use test_ts;

create table ts_table
(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar)
attributes (attr1 smallint not null, attr2 int not null, attr3 bigint, attr4 float, attr5 bool, attr6 varchar)
primary attributes (attr1, attr2);


create table ts_table2
(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar)
attributes (attr1 smallint not null, attr2 int not null, attr3 bigint, attr4 float, attr5 bool, attr6 varchar)
primary attributes (attr1, attr2);

insert into ts_table values('2023-05-31 10:00:00', 1000,1000000,100000000000000000,100000000000000000.101,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts'), ('2023-05-31 11:00:00', 2000,2000000,200000000000000000,200000000000000000.202,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts');
insert into ts_table2 values('2023-05-31 10:00:00', 1000,1000000,100000000000000000,100000000000000000.101,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts'), ('2023-05-31 11:00:00', 2000,2000000,200000000000000000,200000000000000000.202,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts');

-- simple function
explain select extract('second', time) from ts_table;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from ts_table;
explain select extract('second', '2024-01-01 00:00:00'::timestamp-1h) from ts_table;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table group by e1;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table order by e1 desc limit 1 offset 1;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table order by attr2 desc limit 1 offset 1;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table where e3 < 300000000000000000;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table where e3 < 300000000000000000;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table where attr3 < 300000000000000000;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table where attr2 < 2000000;

-- project has function
--- simple function with UNION
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table union select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp+1h) from test_ts.ts_table union select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1 < 2000 order by e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1-1000 < 3000 order by e2 desc limit 1 offset 1;

--- simple function with uncorrelated subquery
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) order by e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp-1s) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) order by e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 group by e1 having e1 < 3000) order by e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 order by e3 desc limit 1 offset 1) order by e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) group by e2 having e2 < 3000000 order by e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) order by e3 desc limit 1 offset 1;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) order by e2;

--- simple function with correlated subquery
explain select extract('second', '2024-01-01 00:00:00'::timestamp) = ( select e4 from test_ts.ts_table2 limit 1 ) from test_ts.ts_table;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) in ( select e4 from test_ts.ts_table2 where e2 > 1000000 group by e4) from test_ts.ts_table;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) in ( select e4 from test_ts.ts_table2 order by e3 desc limit 10 offset 1) from test_ts.ts_table;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) in ( select e4 from test_ts.ts_table2 ) from test_ts.ts_table order by e3 desc limit 10 offset 1;

--- simple function with JOIN
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp+1d) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1 left join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp-1d) from test_ts.ts_table as t1 left join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1 left join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1 left join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1 right join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp+1d) from test_ts.ts_table as t1 right join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1 right join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1 right join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1 full join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp-1d) from test_ts.ts_table as t1 full join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1 full join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1 full join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

--- simple function with multiple tables
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table, test_ts.ts_table2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp+1s) from test_ts.ts_table, test_ts.ts_table2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e1;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 group by t1.e1, t2.e1 having t1.e1 > 1000 or t2.e1 < 3000 order by t1.e1;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 order by t1.e1 desc limit 10 offset 1;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 order by t1.e1 desc limit 10 offset 1;

-- filters has function
--- simple function
explain select extract('second', time) from ts_table where time > date_trunc('hour', time-1h);
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from ts_table where time < date_trunc('hour', '2024-01-01 00:00:00'::timestamp-1h);
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from ts_table where time < date_trunc('hour', '2024-01-01 00:00:00'::timestamp-1h) group by e1;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from ts_table where time < date_trunc('hour', '2024-01-01 00:00:00'::timestamp-1h) order by e1 desc limit 1;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from ts_table where time < date_trunc('hour', '2024-01-01 00:00:00'::timestamp-1h) order by attr1 desc limit 1;

--- simple function with uncorrelated subquery
explain select e1 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where time < date_trunc('hour', '2024-01-01 00:00:00'::timestamp-1h)) order by e2;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where time < date_trunc('hour', '2024-01-01 00:00:00'::timestamp-1h)) order by e2;

--- simple function with correlated subquery
explain select time = ( select time from test_ts.ts_table2 where time < date_trunc('hour', '2024-01-01 00:00:00'::timestamp-1h) limit 1 ) from test_ts.ts_table;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) = ( select e4 from test_ts.ts_table2 where time < date_trunc('hour', '2024-01-01 00:00:00'::timestamp-1h) limit 1 ) from test_ts.ts_table;

--- simple function with multiple tables
explain select t1.time from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.time < date_trunc('hour', '2024-01-01 00:00:00'::timestamp-1h) order by t1.e1;
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.time < date_trunc('hour', '2024-01-01 00:00:00'::timestamp-1h) order by t1.e1;

-- args has function
explain select extract('year', localtimestamp()) from ts_table;
explain select extract('year', localtimestamp()) from test_ts.ts_table group by e1 having e1 < 3000;
explain select extract('year', localtimestamp()) from test_ts.ts_table order by e1 desc limit 1 offset 1;
explain select extract('year', localtimestamp()) from test_ts.ts_table order by attr2 desc limit 1 offset 1;
explain select extract('year', localtimestamp()) from test_ts.ts_table where time < date_trunc('hour', '2024-01-01 00:00:00'::timestamp-1h);
explain select extract('year', localtimestamp()) from test_ts.ts_table where e3 < 300000000000000000;
explain select extract('year', localtimestamp()) from test_ts.ts_table where e3 < 300000000000000000;
explain select extract('year', localtimestamp()) from test_ts.ts_table where attr3 < 300000000000000000;
explain select extract('year', localtimestamp()) from test_ts.ts_table where attr2 < 2000000;

-- args has function with UNION
explain select extract('year', localtimestamp()) from test_ts.ts_table union select extract('year', localtimestamp()) from test_ts.ts_table2;
explain select extract('year', localtimestamp()) from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1 < 2000 order by e2;
explain select extract('year', localtimestamp()) from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1-1000 < 3000 order by e2 desc limit 1 offset 1;

-- args has function with uncorrelated subquery
explain select extract('year', localtimestamp()) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) order by e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 group by e1 having e1 < 3000) order by e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 order by e3 desc limit 1 offset 1) order by e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) group by e2 having e2 < 3000000 order by e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) order by e3 desc limit 1 offset 1;
explain select extract('year', localtimestamp()) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) order by e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e3 = (select e3 from test_ts.ts_table where e6='test_ts1' limit 1)) group by e2 having e2 < 3000000 order by e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e3 = (select e3 from test_ts.ts_table where e6='test_ts1' limit 1)) order by e3 desc limit 1 offset 1;

-- args has function with correlated subquery
explain select extract('year', localtimestamp()) = ( select e4 from test_ts.ts_table2 limit 1 ) from test_ts.ts_table;
explain select extract('year', localtimestamp()) in ( select e4 from test_ts.ts_table2 where e2 > 1000000 group by e4) from test_ts.ts_table;
explain select extract('year', localtimestamp()) in ( select e4 from test_ts.ts_table2 order by e3 desc limit 10 offset 1) from test_ts.ts_table;
explain select extract('year', localtimestamp()) in ( select e4 from test_ts.ts_table2 ) from test_ts.ts_table order by e3 desc limit 10 offset 1;
explain select extract('year', localtimestamp()) in ( select e4 from test_ts.ts_table2 ), case e2 when 1000000 then 10 when 2000000 then 20 end from test_ts.ts_table order by e3 desc limit 1 offset 1;

-- args has function with JOIN
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

explain select extract('year', localtimestamp()) from test_ts.ts_table as t1 left join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1 left join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1 left join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

explain select extract('year', localtimestamp()) from test_ts.ts_table as t1 right join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1 right join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1 right join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

explain select extract('year', localtimestamp()) from test_ts.ts_table as t1 full join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1 full join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e2;
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1 full join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

-- args has function with multiple tables
explain select extract('year', localtimestamp()) from test_ts.ts_table, test_ts.ts_table2;
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e1;
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 group by t1.e1, t2.e1 having t1.e1 > 1000 or t2.e1 < 3000 order by t1.e1;
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 order by t1.e1 desc limit 10 offset 1;
explain select extract('year', localtimestamp()) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 order by t1.e1 desc limit 10 offset 1;


-- aggregate function
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from ts_table;
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table where e3 < 300000000000000000;
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table where e3 < 300000000000000000;
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table where attr3 < 300000000000000000;
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table where attr2 < 2000000;

-- aggregate function with UNION
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table union select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table2;

-- aggregate function with uncorrelated subquery
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) group by e2 having e2 < 3000000 order by e2;
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e3 = (select e3 from test_ts.ts_table where e6='test_ts1' limit 1)) group by e2 having e2 < 3000000 order by e2;

-- aggregate function with correlated subquery
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) = ( select e4 from test_ts.ts_table2 limit 1 ) from test_ts.ts_table;
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) in ( select e4 from test_ts.ts_table2 order by e3 desc limit 10 offset 1) from test_ts.ts_table;

-- aggregate function with JOIN
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table as t1 left join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table as t1 right join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table as t1 full join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

-- aggregate function with multiple tables
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table, test_ts.ts_table2;
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 group by t1.e1, t2.e1 having t1.e1 > 1000 or t2.e1 < 3000 order by t1.e1;

-- cross-mode 
explain select extract('second', '2024-01-01 00:00:00'::timestamp) from test.test1 as a join test_ts.ts_table as b on a.col1=b.e1 where a.col1=1000 group by e1 order by e1;
explain select extract('year', localtimestamp()) from test.test1 as a join test_ts.ts_table as b on a.col1=b.e1 where a.col1=1000 group by e1 order by e1;
explain select max(extract('second', '2024-01-01 00:00:00'::timestamp)) from test.test1 as a join test_ts.ts_table as b on a.col1=b.e1 where a.col1=1000 group by e1 order by e1;

-- ZDP-40972
CREATE ts DATABASE test_select_join;
CREATE TABLE test_select_join.t1(
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
    e18 CHAR(200),e19 VARBYTES,
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

INSERT INTO test_select_join.t1 VALUES(0,1,0,0,0,0,0,true,0,'','','','','','','','','','','','','','','',0,0,0,0,0,false,'','','','','','','','','','');

SELECT CAST(999999999999999999999 AS INT8) FROM test_select_join.t1 ORDER BY id;

use defaultdb;
drop database test cascade;
drop database test_ts cascade;
DROP DATABASE test_select_join cascade;
