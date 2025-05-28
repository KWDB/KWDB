set cluster setting sql.all_push_down.enabled=false;

-- create
create database test;
use test;

create table test1(col1 smallint, col2 int, col3 bigint, col4 float, col5 bool, col6 varchar);
insert into test1 values(1000,1000000,100000000000000000,100000000000000000.101,true, 'test_c1'), (2000,2000000,200000000000000000,200000000000000000.202,true, 'test_c2');

create ts database test_ts;
use test_ts;

create table ts_table
(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar)
attributes (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float, attr5 bool, attr6 varchar)
primary attributes (attr1);
insert into ts_table values('2023-05-31 10:00:00', 1000,1000000,100000000000000000,100000000000000000.101,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts'), ('2023-05-31 11:00:00', 2000,2000000,200000000000000000,200000000000000000.202,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts');

create table ts_table2
(time timestamp not null, e1 smallint, e2 int, e3 bigint, e4 float, e5 bool, e6 varchar)
attributes (attr1 smallint not null, attr2 int, attr3 bigint, attr4 float, attr5 bool, attr6 varchar)
primary attributes (attr1);
insert into ts_table2 values('2023-05-31 10:00:00', 1000,1000000,100000000000000000,100000000000000000.101,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts'), ('2023-05-31 11:00:00', 2000,2000000,200000000000000000,200000000000000000.202,true, 'test_ts1', 1000, 1000000, 1000000000, 100.11, false, 'test_attr_ts');

-- ZDP-27161
select 1699496698000::timestamp from ts_table;

-- simple select
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

-- select with expr
select e1 from test_ts.ts_table where e1 between 1000 and 2000 group by e1 having e1 < 3000;
select e1 from test_ts.ts_table where e2 not in (2000000, 3000000) group by e1 having e1 < 3000;
select e1 from test_ts.ts_table where e6 like '%test' group by e1 having e1 in (1000, 3000);
select e1, e4 from test_ts.ts_table where e1 < 2000 and e2 < 1000000 group by e1, e4 having e1 < 3000 and e4 > 1000.001;
select e1, e4 from test_ts.ts_table where e1 < 2000 or e2 > 1000000 group by e1, e4 having e1 < 3000 or e4 > 1000.001;
select e1, e4 from test_ts.ts_table where e2+e1 < 20000000 group by e1, e4 having e1+e4 > 200000000;
select e1, e4 from test_ts.ts_table where e2-e1 < 20000000 group by e1, e4 having e1-e4 > 200000000;
select e1, e4 from test_ts.ts_table where e2*e1 > 20000000 group by e1, e4 having e1*e4 > 200000000;
select e1, e3 from test_ts.ts_table where e2&e1 = 1 group by e1, e3 having e1&e3 = 0;
select e1, e3 from test_ts.ts_table where e2|e1 = 1 group by e1, e3 having e1|e3 = 0;
select e1, e3 from test_ts.ts_table where e2#e1 = 1 group by e1, e3 having e1#e3 = 0;
select e1, e3 from test_ts.ts_table where e2%e1 = 1 group by e1, e3 having e1%e3 = 0;
select e1, e3 from test_ts.ts_table where e2||e1 = 1 group by e1, e3 having e1||e3 = 0;
select e1 from test_ts.ts_table where e2<<1 = 1 group by e1 having e1>>1 = 2;
select sin(e4), length(e6) from test_ts.ts_table where e1 < 3000 and substr(e6, 1, 3)='tes' order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table where cast(e1 as float) < 3000 and substring(e6, 1, 3)='tes' order by e4, e6;
select ceil(e4), left(e6, 2) from test_ts.ts_table where cast(e1 as string) < '3000' and right(e6, 3)='ts2' order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table where cast(e1 as int) < 3000 or upper(e6) = 'TEST_TS1' order by e4, e6;
select now(), time_bucket(time, '5s'), power(e4, 2), lpad(e6, 1) from test_ts.ts_table where cast(e1 as bigint) < 3000 and rpad(e6, 1)='1' order by e4, e6;
select date_trunc('second', time), round(e4), ltrim(e6) from test_ts.ts_table where cast(e1 as float) < 3000.0 and rtrim(e6) = 'test_ts2' order by e4, e6;

select e1 from test_ts.ts_table where e1 between 1000 and 2000 order by e1 desc limit 1 offset 1;
select e1 from test_ts.ts_table where e2 not in (2000000, 3000000) order by e1 desc limit 1 offset 1;
select e1 from test_ts.ts_table where e6 like '%test' order by e1 desc limit 1 offset 1;
select e1 from test_ts.ts_table where e6 not like '%test' order by e1 desc limit 1 offset 1;
select e1, e4 from test_ts.ts_table where e2+e1 < 20000000 order by e1 desc limit 1 offset 1;
select e1, e4 from test_ts.ts_table where e2-e1 < 20000000 order by e1 desc limit 1 offset 1;
select e1, e4 from test_ts.ts_table where e2*e1 > 20000000 order by e1 desc limit 1 offset 1;
select e1, e4 from test_ts.ts_table where e2/e1 < 20000000 order by e1 desc limit 1 offset 1;
select e1, e4 from test_ts.ts_table where e2&e1 = 1 order by e1 desc limit 1 offset 1;
select e1, e3 from test_ts.ts_table where e2&e1 = 1 order by e1 desc limit 1 offset 1;
select e1, e3 from test_ts.ts_table where e2|e1 = 1 order by e1 desc limit 1 offset 1;
select e1, e3 from test_ts.ts_table where e2#e1 = 1 order by e1 desc limit 1 offset 1;
select e1, e3 from test_ts.ts_table where e2%e1 = 1 order by e1 desc limit 1 offset 1;
select e1 from test_ts.ts_table where e2<<1 = 1 order by e1 desc limit 1 offset 1;
select sin(e4), length(e6) from test_ts.ts_table where e1 < 3000 and substr(e6, 1, 3)='tes' order by e1 desc limit 1 offset 1;
select cos(e4), concat(e6, 'test') from test_ts.ts_table where cast(e1 as float) < 3000 and substring(e6, 1, 3)='tes' order by e1 desc limit 1 offset 1;
select ceil(e4), left(e6, 2) from test_ts.ts_table where cast(e1 as string) < '3000' and right(e6, 3)='ts2' order by e1 desc limit 1 offset 1;
select floor(e4), lower(e6) from test_ts.ts_table where cast(e1 as int) < 3000 or upper(e6) = 'TEST_TS1' order by e1 desc limit 1 offset 1;
select now(), time_bucket(time, '5s'), power(e4, 2), lpad(e6, 1) from test_ts.ts_table where cast(e1 as bigint) < 3000 and rpad(e6, 1)='1' order by e1 desc limit 1 offset 1;
select date_trunc('second', time), round(e4), ltrim(e6) from test_ts.ts_table where cast(e1 as float) < 3000.0 and rtrim(e6) = 'test_ts2' order by e1 desc limit 1 offset 1;
select avg(e1) from test_ts.ts_table group by e1 having e1 < 3000 order by e1 desc limit 1 offset 1;
select min(e1) from test_ts.ts_table group by e1 having e1 < 3000 order by e1 desc limit 1 offset 1;
                       
-- timestamp column convert to other types
select time::timestamp from test_ts.ts_table;
select time::smallint from test_ts.ts_table;
select time::int from test_ts.ts_table;
select time::bigint from test_ts.ts_table;
select time::float from test_ts.ts_table;
select time::bool from test_ts.ts_table;
select time::char from test_ts.ts_table;
select time::char(10) from test_ts.ts_table;
select time::nchar from test_ts.ts_table;
select time::nchar(255) from test_ts.ts_table;
select time::varchar from test_ts.ts_table;
select time::varchar(254) from test_ts.ts_table;
select time::varchar(4095) from test_ts.ts_table;
select time::nvarchar from test_ts.ts_table;
select time::nvarchar(63) from test_ts.ts_table;
select time::nvarchar(4095) from test_ts.ts_table;

-- smallint column convert to other types
select e1::timestamp from test_ts.ts_table;
select e1::smallint from test_ts.ts_table;
select e1::int from test_ts.ts_table;
select e1::bigint from test_ts.ts_table;
select e1::float from test_ts.ts_table;
select e1::bool from test_ts.ts_table;
select e1::char from test_ts.ts_table;
select e1::char(10) from test_ts.ts_table;
select e1::nchar from test_ts.ts_table;
select e1::nchar(255) from test_ts.ts_table;
select e1::varchar from test_ts.ts_table;
select e1::varchar(254) from test_ts.ts_table;
select e1::varchar(4095) from test_ts.ts_table;
select e1::nvarchar from test_ts.ts_table;
select e1::nvarchar(63) from test_ts.ts_table;
select e1::nvarchar(4095) from test_ts.ts_table;

-- int column convert to other types
select e2::timestamp from test_ts.ts_table;
select e2::smallint from test_ts.ts_table;
select e2::int from test_ts.ts_table;
select e2::bigint from test_ts.ts_table;
select e2::float from test_ts.ts_table;
select e2::bool from test_ts.ts_table;
select e2::char from test_ts.ts_table;
select e2::char(10) from test_ts.ts_table;
select e2::nchar from test_ts.ts_table;
select e2::nchar(255) from test_ts.ts_table;
select e2::varchar from test_ts.ts_table;
select e2::varchar(254) from test_ts.ts_table;
select e2::varchar(4095) from test_ts.ts_table;
select e2::nvarchar from test_ts.ts_table;
select e2::nvarchar(63) from test_ts.ts_table;
select e2::nvarchar(4095) from test_ts.ts_table;

-- bigint column convert to other types
select e3::timestamp from test_ts.ts_table;
select e3::smallint from test_ts.ts_table;
select e3::int from test_ts.ts_table;
select e3::bigint from test_ts.ts_table;
select e3::float from test_ts.ts_table;
select e3::bool from test_ts.ts_table;
select e3::char from test_ts.ts_table;
select e3::char(10) from test_ts.ts_table;
select e3::nchar from test_ts.ts_table;
select e3::nchar(255) from test_ts.ts_table;
select e3::varchar from test_ts.ts_table;
select e3::varchar(254) from test_ts.ts_table;
select e3::varchar(4095) from test_ts.ts_table;
select e3::nvarchar from test_ts.ts_table;
select e3::nvarchar(63) from test_ts.ts_table;
select e3::nvarchar(4095) from test_ts.ts_table;

-- float column convert to other types
select e4::timestamp from test_ts.ts_table;
select e4::smallint from test_ts.ts_table;
select e4::int from test_ts.ts_table;
select e4::bigint from test_ts.ts_table;
select e4::float from test_ts.ts_table;
select e4::bool from test_ts.ts_table;
select e4::char from test_ts.ts_table;
select e4::char(10) from test_ts.ts_table;
select e4::nchar from test_ts.ts_table;
select e4::nchar(255) from test_ts.ts_table;
select e4::varchar from test_ts.ts_table;
select e4::varchar(254) from test_ts.ts_table;
select e4::varchar(4095) from test_ts.ts_table;
select e4::nvarchar from test_ts.ts_table;
select e4::nvarchar(63) from test_ts.ts_table;
select e4::nvarchar(4095) from test_ts.ts_table;

-- bool column convert to other types
select e5::timestamp from test_ts.ts_table;
select e5::smallint from test_ts.ts_table;
select e5::int from test_ts.ts_table;
select e5::bigint from test_ts.ts_table;
select e5::float from test_ts.ts_table;
select e5::bool from test_ts.ts_table;
select e5::char from test_ts.ts_table;
select e5::char(10) from test_ts.ts_table;
select e5::nchar from test_ts.ts_table;
select e5::nchar(255) from test_ts.ts_table;
select e5::varchar from test_ts.ts_table;
select e5::varchar(254) from test_ts.ts_table;
select e5::varchar(4095) from test_ts.ts_table;
select e5::nvarchar from test_ts.ts_table;
select e5::nvarchar(63) from test_ts.ts_table;
select e5::nvarchar(4095) from test_ts.ts_table;

-- varchar column convert to other types
select e6::timestamp from test_ts.ts_table;
select e6::smallint from test_ts.ts_table;
select e6::int from test_ts.ts_table;
select e6::bigint from test_ts.ts_table;
select e6::float from test_ts.ts_table;
select e6::bool from test_ts.ts_table;
select e6::char from test_ts.ts_table;
select e6::char(10) from test_ts.ts_table;
select e6::nchar from test_ts.ts_table;
select e6::nchar(255) from test_ts.ts_table;
select e6::varchar from test_ts.ts_table;
select e6::varchar(254) from test_ts.ts_table;
select e6::varchar(4095) from test_ts.ts_table;
select e6::nvarchar from test_ts.ts_table;
select e6::nvarchar(63) from test_ts.ts_table;
select e6::nvarchar(4095) from test_ts.ts_table;

-- window functions are not supported in tstable and stable
SELECT row_number() OVER (), * FROM (SELECT DISTINCT attr6, sum(e1) OVER (PARTITION BY attr6) AS "attr" FROM ts_table ORDER BY "attr" DESC);

with with_table as (select * from test_ts.ts_table) select * from with_table where e1=1000;


-- union and subquery: time series table and relational table
-- UNION
select e1 from test_ts.ts_table union select col1 from test.test1 order by e1;
select e1 from test_ts.ts_table union all select col1 from test.test1 order by e1;
select e1 from test_ts.ts_table intersect select col1 from test.test1 order by e1;

select * from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1 < 2000 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t group by t.e2 having t.e2 < 3000000 order by e2;
select * from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e3 < 3000000000 order by e1 desc limit 1 offset 1;
select e1, e2, case e2 when 1000 then '1000' when 2000 then '2000' end from (select e1,e2,e3,e6 from test_ts.ts_table union (select col1,col2,col3,col6 from test.test1 union select e1,e2,e3,e6 from test_ts.ts_table)) as t where t.e6 = 'test_ts1' order by e2;

-- UNION with groupby
select e2 from (select e1,e2,e3,e6 from test_ts.ts_table union (select col1,col2,col3,col6 from test.test1 union select e1,e2,e3,e6 from test_ts.ts_table)) as t where t.e1 in (2000, 3000) and t.e6 like '%tes' group by t.e2, t.e6 having t.e2 < 3000000 or t.e6 like '%tes' order by e2;
select e2 from (select e1,e2,e3,e6 from test_ts.ts_table union (select col1,col2,col3,col6 from test.test1 union select e1,e2,e3,e6 from test_ts.ts_table)) as t where t.e1 not in (2000, 3000) and t.e6 not like '%tes' group by t.e2, t.e6 having t.e2 < 3000000 or t.e6 not like '%tes' order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1+1000 < 3000 group by t.e2 having t.e2+1000000 < 3000000 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1-1000 < 3000 group by t.e2 having t.e2-1000000 < 3000000 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1*10 = 1 group by t.e2 having t.e2*1000000 = 1 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1/10 = 1 group by t.e2 having t.e2/1000000 = 1 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1&10 = 1 group by t.e2 having t.e2&10 = 1 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1|10 = 1 group by t.e2 having t.e2|10 = 1 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1#10 = 1 group by t.e2 having t.e2#10 = 10 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1%10 = 1 group by t.e2 having t.e2%10 = 1 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1<<1 < 1000 group by t.e2 having t.e2>>1 < 3000000 order by e2;
select sin(e4), length(e6) from (select e4,e6 from test_ts.ts_table union (select col4,col6 from test.test1 union select e4,e6 from test_ts.ts_table)) as t where substr(e6, 1, 3)='tes' group by t.e4, t.e6 having t.e4 < 3000000 or t.e6 like '%tes' order by e4, e6;
select cos(e4), concat(e6, 'test') from (select e4,e6 from test_ts.ts_table union (select col4, col6 from test.test1 union select e4, e6 from test_ts.ts_table)) as t where cast(e4 as float) < 3000 and substring(e6, 1, 3)='tes' group by t.e4, t.e6 having t.e4 < 3000000 or t.e6 like '%tes' order by e4, e6;
select ceil(e4), left(e6, 1) from (select e4,e6 from test_ts.ts_table union (select col4,col6 from test.test1 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as string) < '3000' and right(e6, 1)='tes' group by t.e4, t.e6 having t.e4 < 3000000 or t.e6 like '%tes' order by e4, e6;
select floor(e4), lower(e6) from (select e4,e6 from test_ts.ts_table union (select col4,col6 from test.test1 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as int) < 3000 and upper(e6)='TEST_TS1' group by t.e4, t.e6 having t.e4 < 3000000 or t.e6 like '%tes' order by e4, e6;
select power(e4, 2), lpad(e6, 1) from (select e4,e6 from test_ts.ts_table union (select col4,col6 from test.test1 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as bigint) < 3000 and rpad(e6, 1)='tes' group by t.e4, t.e6 having t.e4 < 3000000 or t.e6 like '%tes' order by e4, e6;
select round(e4), ltrim(e6, 'test') from (select e4,e6 from test_ts.ts_table union (select col4,col6 from test.test1 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as bigint) < 3000 and rtrim(e6)='tes' group by t.e4, t.e6 having t.e4 < 3000000 or t.e6 like '%tes' order by e4, e6;

-- UNION with orderby
select e2 from (select e1,e2,e3,e6 from test_ts.ts_table union (select col1,col2,col3,col6 from test.test1 union select e1,e2,e3,e6 from test_ts.ts_table)) as t where t.e1 in (2000, 3000) and t.e6 like '%tes' order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3,e6 from test_ts.ts_table union (select col1,col2,col3,col6 from test.test1 union select e1,e2,e3,e6 from test_ts.ts_table)) as t where t.e1 not in (2000, 3000) and t.e6 not like '%tes' order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1+1000 < 3000 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1-1000 < 3000 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1*10 = 1 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1/10 = 1 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1&10 = 1 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1|10 = 1 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1#10 = 1 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1%10 = 1 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select col1,col2,col3 from test.test1 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1<<1 < 1000 order by e2 desc limit 1 offset 1;
select cos(e4), concat(e6, 'test') from (select e4,e6 from test_ts.ts_table union (select col4, col6 from test.test1 union select e4, e6 from test_ts.ts_table)) as t where cast(e4 as float) < 3000 and substring(e6, 1, 3)='tes' order by e4 desc limit 1 offset 1;
select ceil(e4), left(e6, 1) from (select e4,e6 from test_ts.ts_table union (select col4,col6 from test.test1 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as string) < '3000' and right(e6, 1)='tes' order by e4 desc limit 1 offset 1;
select floor(e4), lower(e6) from (select e4,e6 from test_ts.ts_table union (select col4,col6 from test.test1 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as int) < 3000 and upper(e6)='TEST_TS1' order by e4 desc limit 1 offset 1;
select power(e4, 2), lpad(e6, 1) from (select e4,e6 from test_ts.ts_table union (select col4,col6 from test.test1 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as bigint) < 3000 and rpad(e6, 1)='tes' order by e4 desc limit 1 offset 1;
select round(e4), ltrim(e6) from (select e4,e6 from test_ts.ts_table union (select col4,col6 from test.test1 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as bigint) < 3000 and rtrim(e6)='tes' order by e4 desc limit 1 offset 1;

-- uncorrelated subquery
select * from test_ts.ts_table where e1 in (select col1 from test.test1) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 group by col1 having col1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 order by col3 desc limit 1 offset 1) order by e2;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) group by e2 having e2 < 3000000 order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1) order by e3 desc limit 1 offset 1;
select e1, e2, case e2 when 1000 then '1000' when 2000 then '2000' end from test_ts.ts_table where e1 in (select col1 from test.test1) order by e2;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1 where col3 = (select e3 from test_ts.ts_table where e6='test_ts1' limit 1)) group by e2 having e2 < 3000000 order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col3 = (select e3 from test_ts.ts_table where e6='test_ts1' limit 1)) order by e3 desc limit 1 offset 1;

-- subquery with groupby
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1 between 1000 and 2000 group by col1 having col1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col6 like '%tes' group by col1 having col1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col6 not like '%tes' group by col1 having col1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1+10 > 1000 group by col1 having col1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1-10 > 1000 group by col1 having col1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1*10 > 1000 group by col1 having col1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1/10 > 1000 group by col1 having col1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1&10 = 1 group by col1 having col1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1|10 = 1 group by col1 having col1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1#10 = 1 group by col1 having col1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1%10 = 1 group by col1 having col1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1<<1 = 1 group by col1 having col1 < 3000) order by e2;
select ceil(e4), left(e6, 1) from test_ts.ts_table where e1 in (select col1 from test.test1 where right(col6, 1)='tes' group by col1 having col1 < 3000);
select floor(e4), lower(e6) from test_ts.ts_table where e1 in (select col1 from test.test1 where upper(col6)='TEST_TS1' group by col1 having col1 < 3000);
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table where e1 in (select col1 from test.test1 where rpad(col6, 1)='tes' group by col1 having col1 < 3000);
select round(e4), ltrim(e6) from test_ts.ts_table where e1 in (select col1 from test.test1 where rtrim(col6)='test_ts1' group by col1 having col1 < 3000);

-- subquery with orderby
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1 between 1000 and 2000 order by col3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col6 like '%tes' order by col3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col6 not like '%tes' order by col3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1+10 > 1000 order by col3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1-10 > 1000 order by col3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1*10 > 1000 order by col3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1/10 > 1000 order by col3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1&10 = 1 order by col3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1|10 = 1 order by col3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1#10 = 1 order by col3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1%10 = 1 order by col3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select col1 from test.test1 where col1<<1 = 1 order by col3 desc limit 1 offset 1) order by e2;
select sin(e4), length(e6) from test_ts.ts_table where e1 in (select col1 from test.test1 where substr(col6, 1, 3)='tes' order by col3 desc limit 1 offset 1) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table where e1 in (select col1 from test.test1 where substring(col6, 1, 3)='tes' order by col3 desc limit 1 offset 1) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table where e1 in (select col1 from test.test1 where right(col6, 1)='tes' order by col3 desc limit 1 offset 1) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table where e1 in (select col1 from test.test1 where upper(col6)='TEST_TS1' order by col3 desc limit 1 offset 1) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table where e1 in (select col1 from test.test1 where rpad(col6, 1)='tes' order by col3 desc limit 1 offset 1) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table where e1 in (select col1 from test.test1 where rtrim(col6)='test_ts1' order by col3 desc limit 1 offset 1) order by e4, e6;


-- select with groupby
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1 between 1000 and 2000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1-10 > 1000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1*10 > 1000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1/10 > 1000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1&1 = 1 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1|1 = 1 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1#1 = 1 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1%1 = 1 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1<<1 = 1 group by e2 having e2 < 3000000 order by e2;
select sin(e4), length(e6) from test_ts.ts_table where e1 in (select col1 from test.test1) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1';
select cos(e4), concat(e6, 'test') from test_ts.ts_table where e1 in (select col1 from test.test1) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1';
select ceil(e4), left(e6, 1) from test_ts.ts_table where e1 in (select col1 from test.test1) and right(e6, 1)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1';
select floor(e4), lower(e6) from test_ts.ts_table where e1 in (select col1 from test.test1) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1';
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table where e1 in (select col1 from test.test1) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1';
select round(e4), ltrim(e6) from test_ts.ts_table where e1 in (select col1 from test.test1) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1';


-- select with orderby
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1 between 1000 and 2000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1-10 > 1000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1*10 > 1000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1/10 > 1000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1&1 = 1 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1|1 = 1 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1#1 = 1 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1%1 = 1 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select col1 from test.test1) and e1<<1 = 1 order by e2 desc limit 1 offset 1;
select sin(e4), length(e6) from test_ts.ts_table where e1 in (select col1 from test.test1) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select cos(e4), concat(e6, 'test') from test_ts.ts_table where e1 in (select col1 from test.test1) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select ceil(e4), left(e6, 1) from test_ts.ts_table where e1 in (select col1 from test.test1) and right(e6, 1)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select floor(e4), lower(e6) from test_ts.ts_table where e1 in (select col1 from test.test1) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table where e1 in (select col1 from test.test1) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select round(e4), ltrim(e6) from test_ts.ts_table where e1 in (select col1 from test.test1) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select avg(e4) from test_ts.ts_table where e1 in (select col1 from test.test1) and substr(e6, 1, 3)='tes' group by e4 having e4 < 3000000 order by e4 desc limit 1 offset 1;
select max(e4) from test_ts.ts_table where e1 in (select col1 from test.test1) and substr(e6, 1, 3)='tes' group by e4 having e4 < 3000000 order by e4 desc limit 1 offset 1;
select sum(e4) from test_ts.ts_table where e1 in (select col1 from test.test1) and substr(e6, 1, 3)='tes' group by e4 having e4 < 3000000 order by e4 desc limit 1 offset 1;

-- subquery with groupby
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 group by e1 having e1 < 3000) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1+10 > 1000 group by e1 having e1 < 3000) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1-10 > 1000 group by e1 having e1 < 3000) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1*10 > 1000 group by e1 having e1 < 3000) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1/10 > 1000 group by e1 having e1 < 3000) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1&10 = 1 group by e1 having e1 < 3000) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1|10 = 1 group by e1 having e1 < 3000) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1#10 = 1 group by e1 having e1 < 3000) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1%10 = 1 group by e1 having e1 < 3000) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1<<1 = 1 group by e1 having e1 < 3000) order by col2;
select sin(col4), length(col6) from test.test1 where col1 in (select e1 from test_ts.ts_table where substr(e6, 1, 3)='tes' group by e1 having e1 < 3000) order by col4, col6;
select cos(col4), concat(col6, 'test') from test.test1 where col1 in (select e1 from test_ts.ts_table where substring(e6, 1, 3)='tes' group by e1 having e1 < 3000) order by col4, col6;
select ceil(col4), left(col6, 1) from test.test1 where col1 in (select e1 from test_ts.ts_table where right(e6, 1)='tes' group by e1 having e1 < 3000) order by col4, col6;
select floor(col4), lower(col6) from test.test1 where col1 in (select e1 from test_ts.ts_table where upper(e6)='TEST_TS1' group by e1 having e1 < 3000) order by col4, col6;
select power(col4, 2), lpad(col6, 1) from test.test1 where col1 in (select e1 from test_ts.ts_table where rpad(e6, 1)='tes' group by e1 having e1 < 3000) order by col4, col6;
select round(col4), ltrim(col6) from test.test1 where col1 in (select e1 from test_ts.ts_table where rtrim(e6)='test_ts1' group by e1 having e1 < 3000) order by col4, col6;

-- subquery with orderby
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1 > 1000 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1+10 > 1000 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1-10 > 1000 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1*10 > 1000 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1/10 > 1000 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1&10 = 1 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1|10 = 1 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1#10 = 1 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1%10 = 1 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1||10 = false order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 where col1 in (select e1 from test_ts.ts_table where e1<<1 = 1 order by e3 desc limit 1 offset 1) order by col2;
select sin(col4), length(col6) from test.test1 where col1 in (select e1 from test_ts.ts_table where substr(e6, 1, 3)='tes' order by e3 desc limit 1 offset 1) order by col4, col6;
select cos(col4), concat(col6, 'test') from test.test1 where col1 in (select e1 from test_ts.ts_table where substring(e6, 1, 3)='tes' order by e3 desc limit 1 offset 1) order by col4, col6;
select ceil(col4), left(col6, 1) from test.test1 where col1 in (select e1 from test_ts.ts_table where right(e6, 1)='tes' order by e3 desc limit 1 offset 1) order by col4, col6;
select floor(col4), lower(col6) from test.test1 where col1 in (select e1 from test_ts.ts_table where upper(e6)='TEST_TS1' order by e3 desc limit 1 offset 1) order by col4, col6;
select power(col4, 2), lpad(col6, 1) from test.test1 where col1 in (select e1 from test_ts.ts_table where rpad(e6, 1)='tes' order by e3 desc limit 1 offset 1) order by col4, col6;
select round(col4), ltrim(col6) from test.test1 where col1 in (select e1 from test_ts.ts_table where rtrim(e6)='test_ts1' order by e3 desc limit 1 offset 1) order by col4, col6;


-- select with groupby
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1 between 1000 and 2000 group by col2 having col2 < 3000000 order by col2;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1+10 > 1000 group by col2 having col2 < 3000000 order by col2;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1-10 > 1000 group by col2 having col2 < 3000000 order by col2;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1*10 > 1000 group by col2 having col2 < 3000000 order by col2;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1/10 > 1000 group by col2 having col2 < 3000000 order by col2;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1&1 = 1 group by col2 having col2 < 3000000 order by col2;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1|1 = 1 group by col2 having col2 < 3000000 order by col2;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1#1 = 1 group by col2 having col2 < 3000000 order by col2;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1%1 = 1 group by col2 having col2 < 3000000 order by col2;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1<<1 = 1 group by col2 having col2 < 3000000 order by col2;

-- select with orderby
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1 between 1000 and 2000 order by col2 desc limit 1 offset 1;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1+10 > 1000 order by col2 desc limit 1 offset 1;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1-10 > 1000 order by col2 desc limit 1 offset 1;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1*10 > 1000 order by col2 desc limit 1 offset 1;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1/10 > 1000 order by col2 desc limit 1 offset 1;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1&1 = 1 order by col2 desc limit 1 offset 1;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1|1 = 1 order by col2 desc limit 1 offset 1;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1#1 = 1 order by col2 desc limit 1 offset 1;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1%1 = 1 order by col2 desc limit 1 offset 1;
select col2 from test.test1 where col1 in (select e1 from test_ts.ts_table) and col1<<1 = 1 order by col2 desc limit 1 offset 1;

select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) order by col2;
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000 group by e1 having e1 < 2000) order by col2;
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000 order by e3 desc limit 1 offset 1);
select col2 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) group by col2 having col2 < 3000000;
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) order by col3 desc limit 1 offset 1;
select *, case col2 when 1000 then '1000' when 2000 then '2000' end from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) order by col3 desc limit 1 offset 1;

-- subquery with groupby
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 group by e1 having e1 < 2000) order by col2;
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1+10<1000 and t1.col2-1000 > 2000000 group by e1 having e1 < 2000) order by col2;
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1*10>1000 and t1.col2/1000 < 2000 group by e1 having e1 < 2000) order by col2;
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1*1 = 1 and t1.col2|1 = 1 group by e1 having e1 < 2000) order by col2;
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1#1 = 1 and t1.col2%1 = 0 group by e1 having e1 < 2000) order by col2;
select sin(col4), length(col6) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where substr(e6, 1, 3)='tes' and t1.col2 > 2000000 group by e1 having e1 < 2000) order by col4, col6;
select cos(col4), concat(col6, 'test') from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where substring(e6, 1, 3)='tes' and t1.col2 > 2000000 group by e1 having e1 < 2000) order by col4, col6;
select cos(col4), concat(col6, 'test') from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where substring(e6, 1, 3)='tes' and t1.col2 > 2000000 group by e1 having e1 < 2000) order by col4, col6;
select ceil(col4), left(col6, 1) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where right(e6, 1)='tes' and t1.col2 > 2000000 group by e1 having e1 < 2000) order by col4, col6;
select floor(col4), lower(col6) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where upper(e6)='TEST_TS1' and upper(t1.col6) like '%test' group by e1 having e1 < 2000) order by col4, col6;
select power(col4, 2), lpad(col6, 1) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where rpad(e6, 1)='tes' and upper(t1.col6) like '%test' group by e1 having e1 < 2000) order by col4, col6;
select round(col4), ltrim(col6) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where rtrim(e6)='test_ts1' and upper(t1.col6) like '%test' group by e1 having e1 < 2000) order by col4, col6;

-- subquery with orderby
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1+10<1000 and t1.col2-1000 > 2000000 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1*10>1000 and t1.col2/1000 < 2000 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1&1 = 1 and t1.col2|1 = 1 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1#1 = 1 and t1.col2%1 = 0 order by e3 desc limit 1 offset 1) order by col2;
select * from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1||1 = true and t1.col2<<1 = 1 order by e3 desc limit 1 offset 1) order by col2;
select sin(col4), length(col6) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where substr(e6, 1, 3)='tes' and t1.col2 > 2000000 order by e3 desc limit 1 offset 1) order by col4, col6;
select cos(col4), concat(col6, 'test') from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where substring(e6, 1, 3)='tes' and t1.col2 > 2000000 order by e3 desc limit 1 offset 1) order by col4, col6;
select cos(col4), concat(col6, 'test') from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where substring(e6, 1, 3)='tes' and t1.col2 > 2000000 order by e3 desc limit 1 offset 1) order by col4, col6;
select ceil(col4), left(col6, 1) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where right(e6, 1)='tes' and t1.col2 > 2000000 order by e3 desc limit 1 offset 1) order by col4, col6;
select floor(col4), lower(col6) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where upper(e6)='TEST_TS1' and upper(t1.col6) like '%test' order by e3 desc limit 1 offset 1) order by col4, col6;
select power(col4, 2), lpad(col6, 1) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where rpad(e6, 1)='tes' and upper(t1.col6) like '%test' order by e3 desc limit 1 offset 1) order by col4, col6;
select round(col4), ltrim(col6) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where rtrim(e6)='test_ts1' and upper(t1.col6) like '%test' order by e3 desc limit 1 offset 1) order by col4, col6;

-- select with groupby
select col4 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1 between 1000 and 2000 group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4;
select col4 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1+10 > 2000 group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4;
select col4 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1-10 > 2000 group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4;
select col4 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1*10 > 2000 group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4;
select col4 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1/10 > 2000 group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4;
select col4 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1&1 = 1 and col2|1=1 group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4;
select col4 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1#1 = 1 and col2%1=1 group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4;
select col4  from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1||1 = false and col2<<1=1 group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4;
select sin(col4), length(col6) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and substr(col6, 1, 3)='tes' group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4, col6;
select cos(col4), concat(col6, 'test') from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and substring(col6, 1, 3)='tes' group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4, col6;
select ceil(col4), left(col6, 1) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and upper(col6)='TEST_TS1' group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4, col6;
select floor(col4), lower(col6) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and rpad(col6, 1)='tes' group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4, col6;
select power(col4, 2), lpad(col6, 1) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and substr(col6, 1, 3)='tes' group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4, col6;
select round(col4), ltrim(col6) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and rtrim(col6)='test_ts1' group by col4, col6 having col4 > 2000000.101 and col6 like '%tes' order by col4, col6;

-- select with orderby
select col4, col6 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1 between 1000 and 2000 order by col3 desc limit 1 offset 1;
select col4, col6 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1+10 > 2000 order by col3 desc limit 1 offset 1;
select col4, col6 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1-10 > 2000 order by col3 desc limit 1 offset 1;
select col4, col6 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1*10 > 2000 order by col3 desc limit 1 offset 1;
select col4, col6 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1/10 > 2000 order by col3 desc limit 1 offset 1;
select col4, col6 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1&1 = 1 and col2|1=1 order by col3 desc limit 1 offset 1;
select col4, col6 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1#1 = 1 and col2%1=1 order by col3 desc limit 1 offset 1;
select col4, col6 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1||1 = false and col2<<1=1 order by col3 desc limit 1 offset 1;
select col4, col6 from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and col1 order by col3 desc limit 1 offset 1;
select sin(col4), length(col6) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and substr(col6, 1, 3)='tes' order by col3 desc limit 1 offset 1;
select cos(col4), concat(col6, 'test') from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and substring(col6, 1, 3)='tes' order by col3 desc limit 1 offset 1;
select ceil(col4), left(col6, 1) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and upper(col6)='TEST_TS1' order by col3 desc limit 1 offset 1;
select floor(col4), lower(col6) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and rpad(col6, 1)='tes' order by col3 desc limit 1 offset 1;
select power(col4, 2), lpad(col6, 1) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and substr(col6, 1, 3)='tes' order by col3 desc limit 1 offset 1;
select round(col4), ltrim(col6) from test.test1 as t1 where col1 in (select e1 from test_ts.ts_table where e1<1000 and t1.col2 > 2000000) and rtrim(col6)='test_ts1' order by col3 desc limit 1 offset 1;

-- join
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e2 > 1000000 group by e1 having e1 < 2000) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 order by e3 desc limit 1 offset 1) order by col2; 
select col2 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) group by col2 having col2 < 3000000 order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) order by col3 desc limit 1 offset 1; 

-- subquery with groupby
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e2 between 1000000 and 2000000 group by e1 having e1 < 2000) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e2+1000 > 1000000 group by e1 having e1 < 2000) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e2-1000 > 1000000 group by e1 having e1 < 2000) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e2*1000 > 1000000 group by e1 having e1 < 2000) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e2/1000 > 1000000 group by e1 having e1 < 2000) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e1|1=1 and e2&1=1 group by e1 having e1 < 2000) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e1#1=1 and e2%1=1 group by e1 having e1 < 2000) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e1||1=false and e2<<1=1 group by e1 having e1 < 2000) order by col2; 
select sin(col4), length(col6) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where substr(e6, 1, 3)='tes' group by e1 having e1 < 2000); 
select cos(col4), concat(col6, 'test') from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where substring(e6, 1, 3)='tes' group by e1 having e1 < 2000); 
select ceil(col4), left(col6, 1) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where right(e6, 1)='tes' group by e1 having e1 < 2000); 
select floor(col4), lower(col6) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where upper(e6)='TEST_TS1' group by e1 having e1 < 2000); 
select power(col4, 2), lpad(col6, 1) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where rpad(e6, 1)='tes' group by e1 having e1 < 2000); 
select round(col4), ltrim(col6) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where rtrim(e6)='test_ts1' group by e1 having e1 < 2000); 

-- subquery with orderby
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e2 between 1000000 and 2000000 order by e3 desc limit 1 offset 1) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e2+1000 > 1000000 order by e3 desc limit 1 offset 1) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e2-1000 > 1000000 order by e3 desc limit 1 offset 1) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e2*1000 > 1000000 order by e3 desc limit 1 offset 1) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e2/1000 > 1000000 order by e3 desc limit 1 offset 1) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e1|1=1 and e2&1=1 order by e3 desc limit 1 offset 1) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e1#1=1 and e2%1=1 order by e3 desc limit 1 offset 1) order by col2; 
select * from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where e1||1=false and e2<<1=1 order by e3 desc limit 1 offset 1) order by col2; 
select sin(col4), length(col6) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where substr(e6, 1, 3)='tes' order by e3 desc limit 1 offset 1); 
select cos(col4), concat(col6, 'test') from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where substring(e6, 1, 3)='tes' order by e3 desc limit 1 offset 1); 
select ceil(col4), left(col6, 1) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where right(e6, 1)='tes' order by e3 desc limit 1 offset 1); 
select floor(col4), lower(col6) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where upper(e6)='TEST_TS1' order by e3 desc limit 1 offset 1); 
select power(col4, 2), lpad(col6, 1) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where rpad(e6, 1)='tes' order by e3 desc limit 1 offset 1); 
select round(col4), ltrim(col6) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3 where rtrim(e6)='test_ts1' order by e3 desc limit 1 offset 1); 

-- select with groupby
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2 between 1000000 and 2000000 group by col4, col6 having col4 > 3000000 and col6 like '%te' order by col4, col6; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2+100 > 2000000 group by col4, col6 having col4 > 3000000 and col6 like '%te' order by col4, col6; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2-100 > 2000000 group by col4, col6 having col4 > 3000000 and col6 like '%te' order by col4, col6; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2*100 > 2000000 group by col4, col6 having col4 > 3000000 and col6 like '%te' order by col4, col6; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2/100 > 2000000 group by col4, col6 having col4 > 3000000 and col6 like '%te' order by col4, col6; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2&1=1 and col3|1=1 group by col4, col6 having col4 > 3000000 and col6 like '%te' order by col4, col6; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2#1=1 and col3%1=1 group by col4, col6 having col4 > 3000000 and col6 like '%te' order by col4, col6; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2||1=true and col3<<1=1 group by col4, col6 having col4 > 3000000 and col6 like '%te' order by col4, col6; 
select sin(col4), length(col6) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and substr(col6, 1, 3)='tes' group by col4, col6 having col4 > 3000000 and col6 like '%te'; 
select cos(col4), concat(col6, 'test') from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and substring(col6, 1, 3)='tes' group by col4, col6 having col4 > 3000000 and col6 like '%te'; 
select ceil(col4), left(col6, 1) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and right(col6, 1)='tes' group by col4, col6 having col4 > 3000000 and col6 like '%te'; 
select floor(col4), lower(col6) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and upper(col6)='TEST_TS1' group by col4, col6 having col4 > 3000000 and col6 like '%te'; 
select power(col4, 2), lpad(col6, 1) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and rpad(col6, 1)='tes' group by col4, col6 having col4 > 3000000 and col6 like '%te'; 
select round(col4), ltrim(col6) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and rtrim(col6)='test_ts1' group by col4, col6 having col4 > 3000000 and col6 like '%te'; 

-- select with orderby
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2 between 1000000 and 2000000 order by col4 desc limit 1 offset 1; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2+100 > 2000000 order by col4 desc limit 1 offset 1; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2-100 > 2000000 order by col4 desc limit 1 offset 1; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2*100 > 2000000 order by col4 desc limit 1 offset 1; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2/100 > 2000000 order by col4 desc limit 1 offset 1; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2&1=1 and col3|1=1 order by col4 desc limit 1 offset 1; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2#1=1 and col3%1=1 order by col4 desc limit 1 offset 1; 
select col4, col6 from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and col2||1=true and col3<<1=1 order by col4 desc limit 1 offset 1; 
select sin(col4), length(col6) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and substr(col6, 1, 3)='tes' order by col4 desc limit 1 offset 1; 
select cos(col4), concat(col6, 'test') from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and substring(col6, 1, 3)='tes' order by col4 desc limit 1 offset 1; 
select ceil(col4), left(col6, 1) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and right(col6, 1)='tes' order by col4 desc limit 1 offset 1; 
select floor(col4), lower(col6) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and upper(col6)='TEST_TS1' order by col4 desc limit 1 offset 1; 
select power(col4, 2), lpad(col6, 1) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and rpad(col6, 1)='tes' order by col4 desc limit 1 offset 1; 
select round(col4), ltrim(col6) from test.test1 where col1 in (select m.e1 from test_ts.ts_table as m join test.test1 as n on m.e3=n.col3) and rtrim(col6)='test_ts1' order by col4 desc limit 1 offset 1; 

select e1 = ( select col1 from test.test1 limit 1 ) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where col2 > 1000000 group by col1 having col1 < 4000) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 order by col3 desc limit 10 offset 1) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 ) from test_ts.ts_table where e2 > 1000000 group by e1 having e1 < 2000 order by e1;
select e1 in ( select col1 from test.test1 ) from test_ts.ts_table order by e3 desc limit 10 offset 1;
select e1 in ( select col1 from test.test1 ), case e2 when 1000000 then 10 when 2000000 then 20 end from test_ts.ts_table order by e3 desc limit 1 offset 1;

select e1 in ( select col1 from test.test1 where col2 between 1000000 and 2000000 group by col1 having col1 < 4000) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where col1+100 > 2000 and col2-1000 > 1000000 group by col1 having col1 < 4000) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where col1*100 > 2000 and col2/1000 > 1000000 group by col1 having col1 < 4000) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where col1&1=1 and col1|1=1 group by col1 having col1 < 4000) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where col1#1=1 and col1%1=1 group by col1 having col1 < 4000) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where col1||1=false and col1<<1=1 group by col1 having col1 < 4000) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where sin(col4)=1 and length(col6)=1 group by col1 having col1 < 4000) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where cos(col4)=1 and concat(col6, 'test')='test' group by col1 having col1 < 4000) from test_ts.ts_table order by e1;
select e1 in ( select max(col1) from test.test1 where ceil(col4)=1 and left(col6, 1)='t' group by col1 having col1 < 4000) from test_ts.ts_table order by e1;
select e1 in ( select min(col1) from test.test1 where ceil(col4)=1 and left(col6, 1)='t' group by col1 having col1 < 4000) from test_ts.ts_table order by e1;

select e1 in ( select col1 from test.test1 where col2 between 1000000 and 2000000 order by col3 desc limit 10 offset 1) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where col1+100 > 2000 and col2-1000 > 1000000 order by col3 desc limit 10 offset 1) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where col1*100 > 2000 and col2/1000 > 1000000 order by col3 desc limit 10 offset 1) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where col1&1=1 and col1|1=1 order by col3 desc limit 10 offset 1) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where col1#1=1 and col1%1=1 order by col3 desc limit 10 offset 1) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where col1||1=false and col1<<1=1 order by col3 desc limit 10 offset 1) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where sin(col4)=1 and length(col6)=1 order by col3 desc limit 10 offset 1) from test_ts.ts_table order by e1;
select e1 in ( select col1 from test.test1 where cos(col4)=1 and concat(col6, 'test')='test' order by col3 desc limit 10 offset 1) from test_ts.ts_table order by e1;
select e1 in ( select max(col3) from test.test1 where ceil(col4)=1 and left(col6, 1)='t' group by col3 order by col3 desc limit 10 offset 1) from test_ts.ts_table order by e1;
select e1 in ( select min(col3) from test.test1 where ceil(col4)=1 and left(col6, 1)='t' group by col3 order by col3 desc limit 10 offset 1) from test_ts.ts_table order by e1;

select col1 = ( select e1 from test_ts.ts_table  order by e1 limit 1) from test.test1;
select col1 = ( select e1 from test_ts.ts_table  order by e1 limit 1) from test.test1 where col3 in (select e3 from test_ts.ts_table where e6='test_ts2');
select col1 in ( select e1 from test_ts.ts_table where e2 > 1000000 group by e1 having e1 < 2000 order by e1 ) from test.test1;
select col1 in ( select e1 from test_ts.ts_table order by e3 desc limit 10 offset 1 ) from test.test1;
select col1 in ( select e1 from test_ts.ts_table order by e1 ) from test.test1 where col2 > 1000000 group by col1 having col1 < 2000;
select col1 in ( select e1 from test_ts.ts_table order by e1 ) from test.test1 order by col3 desc limit 10 offset 1;
select col1 in ( select e1 from test_ts.ts_table order by e1 ), case col2 when 1000000 then 10 when 2000000 then 20 end from test.test1;

select col1 in ( select e1 from test_ts.ts_table where e2 between 1000000 and 2000000 group by e1 having e1 < 4000 order by e1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where e1+100 > 2000 and e2-1000 > 1000000 group by e1 having e1 < 4000 order by e1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where e1*100 > 2000 and e2/1000 > 1000000 group by e1 having e1 < 4000 order by e1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where e1&1=1 and e2|1=1 group by e1 having e1 < 4000 order by e1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where e1#1=1 and e2%1=1 group by e1 having e1 < 4000 order by e1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where e1||1=false and e2<<1=1 group by e1 having e1 < 4000 order by e1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where sin(e4)=1 and length(e6)=1 group by e1 having e1 < 4000 order by e1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where cos(e4)=1 and concat(e6, 'test')='test' group by e1 having e1 < 4000 order by e1) from test.test1;
select col1 in ( select max(e1) from test_ts.ts_table where ceil(e4)=1 and left(e6, 1)='t' group by e1 having e1 < 4000 order by e1) from test.test1;
select col1 in ( select min(e1) from test_ts.ts_table where ceil(e4)=1 and left(e6, 1)='t' group by e1 having e1 < 4000 order by e1) from test.test1;

select col1 in ( select e1 from test_ts.ts_table where e2 between 1000000 and 2000000 order by e1 desc limit 10 offset 1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where e1+100 > 2000 and e2-1000 > 1000000 order by e1 desc limit 10 offset 1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where e1*100 > 2000 and e2/1000 > 1000000 order by e1 desc limit 10 offset 1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where e1&1=1 and e2|1=1 order by e1 desc limit 10 offset 1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where e1#1=1 and e2%1=1 order by e1 desc limit 10 offset 1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where e1||1=false and e2<<1=1 order by e1 desc limit 10 offset 1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where sin(e4)=1 and length(e6)=1 order by e1 desc limit 10 offset 1) from test.test1;
select col1 in ( select e1 from test_ts.ts_table where cos(e4)=1 and concat(e6, 'test')='test' order by e1 desc limit 10 offset 1) from test.test1;
select col1 in ( select max(e1) from test_ts.ts_table where ceil(e4)=1 and left(e6, 1)='t' group by e1 order by e1 desc limit 10 offset 1) from test.test1;
select col1 in ( select min(e1) from test_ts.ts_table where ceil(e4)=1 and left(e6, 1)='t' group by e1 order by e1 desc limit 10 offset 1) from test.test1;

-- correlated subquery
select col1, col2, col3 from test.test1 as t1 where exists (select e1 from test_ts.ts_table as t2 where t1.col1=t2.e1 and t1.col2 > t2.e2) order by col1;
select * from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 group by col1 having col1 < 2000) order by e1;
select * from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 order by col3 desc limit 10 offset 1) order by e1;
select e1 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) group by e1 having e1 < 2000 order by e1;
select * from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) order by e3 desc limit 10 offset 1;
select *, case e2 when 1000 then '1000' when 2000 then '2000' end from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) order by e1;

-- subquery with groupby
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and t1.e1 between 1000 and 2000 group by col1 having col1 < 2000) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and t1.e1+100 > 1000 and t2.col1-100 < 2000 group by col1 having col1 < 2000) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and t1.e1*100 > 1000 and t2.col1/100 < 2000 group by col1 having col1 < 2000) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and t1.e1&1=1 and t2.col1|1=1 group by col1 having col1 < 2000) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and t1.e1#1=1 and t2.col1%1=1 group by col1 having col1 < 2000) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and t1.e1||1=false and t2.col1<<1=1 group by col1 having col1 < 2000) order by e4, e6;
select sin(e4), length(e6) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and substr(e6, 1, 3)='tes' group by col1 having col1 < 2000) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and substring(e6, 1, 3)='tes' group by col1 having col1 < 2000) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and right(e6, 1)='tes' group by col1 having col1 < 2000) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and upper(e6)='TEST_TS1' group by col1 having col1 < 2000) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and rpad(e6, 1)='tes' group by col1 having col1 < 2000) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and rtrim(e6)='test_ts1' group by col1 having col1 < 2000) order by e4, e6;


-- subquery with orderby
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and t1.e1 between 1000 and 2000 order by col3 desc limit 10 offset 1) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and t1.e1+100 > 1000 and t2.col1-100 < 2000 order by col3 desc limit 10 offset 1) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and t1.e1*100 > 1000 and t2.col1/100 < 2000 order by col3 desc limit 10 offset 1) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and t1.e1&1=1 and t2.col1|1=1 order by col3 desc limit 10 offset 1) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and t1.e1#1=1 and t2.col1%1=1 order by col3 desc limit 10 offset 1) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and t1.e1||1=false and t2.col1<<1=1 order by col3 desc limit 10 offset 1) order by e4, e6;
select sin(e4), length(e6) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and substr(e6, 1, 3)='tes' order by col3 desc limit 10 offset 1) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and substring(e6, 1, 3)='tes' order by col3 desc limit 10 offset 1) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and right(e6, 1)='tes' order by col3 desc limit 10 offset 1) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and upper(e6)='TEST_TS1' order by col3 desc limit 10 offset 1) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and rpad(e6, 1)='tes' order by col3 desc limit 10 offset 1) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 and rtrim(e6)='test_ts1' order by col3 desc limit 10 offset 1) order by e4, e6;


-- select with groupby
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and e1+100 > 1000 and e2-100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and e1 between 1000 and 2000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and e1*100 > 100000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and e1&1=1 and e2|1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and e1#1=1 and e2%1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and e1||1=true and e2<<1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select sin(e4), length(e6) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and right(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;


 
-- select with orderby
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and e1+100 > 1000 and e2-100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and e1 between 1000 and 2000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and e1*100 > 100000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and e1&1=1 and e2|1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and e1#1=1 and e2%1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and e1||1=true and e2<<1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select sin(e4), length(e6) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select ceil(e4), left(e6, 1) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and right(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select floor(e4), lower(e6) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select round(e4), ltrim(e6) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select avg(e4) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) group by e4 having e4 < 2000000.01 order by e4 desc limit 10 offset 1;
select max(e4) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) group by e4 having e4 < 2000000.01 order by e4 desc limit 10 offset 1;
select sum(e4) from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1) group by e4 having e4 < 2000000.01 order by e4 desc limit 10 offset 1;

select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) order by e1;
select e1 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) group by e1 having e1 < 2000  order by e1;
select e1 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) order by e3 desc limit 10 offset 1;
select *, case e2 when 1000 then '1000' when 2000 then '2000' end from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) order by e1;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e1;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e1;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000 group by e1 having e1 < 2000) as n where m.e1=n.e1) order by e1;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000 order by col3 desc limit 10 offset 1) as n where m.e1=n.e1) order by e4, e6;

select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 and e1 between 1000 and 2000 group by e1 having e1 < 2000) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 and e1+100 > 1000 and e2-100<2000000 group by e1 having e1 < 2000) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 and e1*100 > 1000 and e2/100<2000000 group by e1 having e1 < 2000) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 and e1&1=1 and e2|1=1 group by e1 having e1 < 2000) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 and e1#1=1 and e2%1=1 group by e1 having e1 < 2000) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 and e1||1=true and e2<<1=1 group by e1 having e1 < 2000) order by e4, e6;
select sin(e4), length(e6) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e4, e6;


select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 and e1 between 1000 and 2000 order by e3 desc limit 10 offset 1) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 and e1+100 > 1000 and e2-100<2000000 order by e3 desc limit 10 offset 1) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 and e1*100 > 1000 and e2/100<2000000 order by e3 desc limit 10 offset 1) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 and e1&1=1 and e2|1=1 order by e3 desc limit 10 offset 1) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 and e1#1=1 and e2%1=1 order by e3 desc limit 10 offset 1) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 and e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1) order by e4, e6;
select sin(e4), length(e6) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e4, e6;


select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1+100 > 1000 and e2-100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1 between 1000 and 2000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1*100 > 1000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1*100 > 1000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1&1=1 and e2|1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1#1=1 and e2%1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1||1=true and e2<<1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select sin(e4), length(e6) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select cos(e4), concat(e6, 'test') from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select ceil(e4), left(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and right(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select floor(e4), lower(e6) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select round(e4), ltrim(e6) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 


select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1+100 > 1000 and e2-100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1 between 1000 and 2000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te'; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1*100 > 1000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1*100 > 1000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1&1=1 and e2|1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1#1=1 and e2%1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and e1||1=true and e2<<1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select sin(e4), length(e6) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select cos(e4), concat(e6, 'test') from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select ceil(e4), left(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and right(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select floor(e4), lower(e6) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select round(e4), ltrim(e6) from test_ts.ts_table as m where e1 in (select e1 from (select col1 as e1 from test.test1 where m.e1 < 10000) as n where m.e1=n.e1) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 

select e1 in (select col1 from test.test1 as t1 where t1.col1=t2.e1) from test_ts.ts_table as t2 where e3 < 2000000000 group by e1 having e1 < 2000;
select e1 in (select col1 from test.test1 as t1 where t1.col1=t2.e1) from test_ts.ts_table as t2 where t2.e4 in (select col4 from test.test1) group by e1 having e1 < 2000;

select col1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.col1) from test.test1 as t2 where col2 > 2000000 group by col1 having col1 > 1000;

select e1 in (select col1 from test.test1 as t1 where t1.col1=t2.e1), e2 from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select col1 from test.test1 as t1 where t1.col1=t2.e1), sin(e4), length(e6) from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select col1 from test.test1 as t1 where t1.col1=t2.e1), cos(e4), concat(e6, 'test') from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select col1 from test.test1 as t1 where t1.col1=t2.e1), ceil(e4), left(e6, 1) from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select col1 from test.test1 as t1 where t1.col1=t2.e1), floor(e4), lower(e6) from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select col1 from test.test1 as t1 where t1.col1=t2.e1), power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select col1 from test.test1 as t1 where t1.col1=t2.e1), round(e4), ltrim(e6) from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;

select e1 from (select e1 from test_ts.ts_table);
select e1 from (select * from test_ts.ts_table) where e3 < 2000000000 group by e1 having e1 < 2000;
select e1, e2, e5 from (select * from test_ts.ts_table) order by e3 desc limit 10 offset 1;
select e1, case e2 when 1000 then '1000' when 2000 then '2000' end from (select * from test_ts.ts_table);


select e4, 46 from (select * from test_ts.ts_table) where e1+100 > 2000 and e2-100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes';
select e4, 46 from (select * from test_ts.ts_table) where e1*100 > 2000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes';
select e4, 46 from (select * from test_ts.ts_table) where e1&1=1 and e2|1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes';
select e4, 46 from (select * from test_ts.ts_table) where e1#1=1 and e2%1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes';
select e4, 46 from (select * from test_ts.ts_table) where e1||1=1 and e2<<1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes';
select sin(e4), length(e6) from (select * from test_ts.ts_table) where substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4, e6;
select cos(e4), concat(e6, 'test') from (select * from test_ts.ts_table) where substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4, e6;
select ceil(e4), left(e6, 1) from (select * from test_ts.ts_table) where right(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4, e6;
select floor(e4), lower(e6) from (select * from test_ts.ts_table) where upper(e6)='TEST_TS1' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4, e6;
select power(e4, 2), lpad(e6, 1) from (select * from test_ts.ts_table) where rpad(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4, e6;
select round(e4), ltrim(e6) from (select * from test_ts.ts_table) where rtrim(e6)='test_ts1' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4, e6;


select e4, 46 from (select * from test_ts.ts_table) where e1+100 > 2000 and e2-100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select e4, 46 from (select * from test_ts.ts_table) where e1*100 > 2000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select e4, 46 from (select * from test_ts.ts_table) where e1&1=1 and e2|1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select e4, 46 from (select * from test_ts.ts_table) where e1#1=1 and e2%1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select e4, 46 from (select * from test_ts.ts_table) where e1||1=1 and e2<<1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select sin(e4), length(e6) from (select * from test_ts.ts_table) where substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select cos(e4), concat(e6, 'test') from (select * from test_ts.ts_table) where substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select ceil(e4), left(e6, 1) from (select * from test_ts.ts_table) where right(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select floor(e4), lower(e6) from (select * from test_ts.ts_table) where upper(e6)='TEST_TS1' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select power(e4, 2), lpad(e6, 1) from (select * from test_ts.ts_table) where rpad(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select round(e4), ltrim(e6) from (select * from test_ts.ts_table) where rtrim(e6)='test_ts1' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select avg(e4) from (select * from test_ts.ts_table) group by e4 having e4 < 2000000.01 order by e4 desc limit 10 offset 1;
select max(e4) from (select * from test_ts.ts_table) group by e4 having e4 < 2000000.01 order by e4 desc limit 10 offset 1;
select sum(e4) from (select * from test_ts.ts_table) group by e4 having e4 < 2000000.01 order by e4 desc limit 10 offset 1;


-- JOIN
select e2 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 order by e2;
select e2 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where t1.e2 < 3000000 and t2.col4 > 1000.101 order by e2;
select e2 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 group by e2 having e2 > 1000000 order by e2;
select e2 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 order by e3 desc limit 10 offset 1;

select e2 from test_ts.ts_table as t1 left join test.test1 as t2 on t1.e1=t2.col1 order by e2;
select e2 from test_ts.ts_table as t1 left join test.test1 as t2 on t1.e1=t2.col1 where t1.e2 < 3000000 and t2.col4 > 1000.101 order by e2;
select e2 from test_ts.ts_table as t1 left join test.test1 as t2 on t1.e1=t2.col1 group by e2 having e2 > 1000000 order by e2;
select e2 from test_ts.ts_table as t1 left join test.test1 as t2 on t1.e1=t2.col1 order by e3 desc limit 10 offset 1;

select e2 from test_ts.ts_table as t1 right join test.test1 as t2 on t1.e1=t2.col1 order by e2;
select e2 from test_ts.ts_table as t1 right join test.test1 as t2 on t1.e1=t2.col1 where t1.e2 < 3000000 and t2.col4 > 1000.101 order by e2;
select e2 from test_ts.ts_table as t1 right join test.test1 as t2 on t1.e1=t2.col1 group by e2 having e2 > 1000000 order by e2;
select e2 from test_ts.ts_table as t1 right join test.test1 as t2 on t1.e1=t2.col1 order by e3 desc limit 10 offset 1;

select e2 from test_ts.ts_table as t1 full join test.test1 as t2 on t1.e1=t2.col1 order by e2;
select e2 from test_ts.ts_table as t1 full join test.test1 as t2 on t1.e1=t2.col1 where t1.e2 < 3000000 and t2.col4 > 1000.101 order by e2;
select e2 from test_ts.ts_table as t1 full join test.test1 as t2 on t1.e1=t2.col1 group by e2 having e2 > 1000000 order by e2;
select e2 from test_ts.ts_table as t1 full join test.test1 as t2 on t1.e1=t2.col1 order by e3 desc limit 10 offset 1;

select e4, e6 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where e1+100 > 2000 and e2-1000 < 2000000 group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where e1*100 > 2000 and e2/1000 < 2000000 group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where e1&1=1 and e2|1=1 group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where e1#1=1 and e2%1=1 group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where e1||1=true and e2<<1=1 group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4, e6;
select sin(e4), length(e6) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where  substr(e6, 1, 3)='tes' group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where substring(e6, 1, 3)='tes' group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where right(e6, 1)='tes' group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where upper(e6)='TEST_TS1' group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where e4 group by rpad(e6, 1)='tes', e6 having e4 > 1000000.01 and e6 like '%te' order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where rtrim(e6)='test_ts1' group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4, e6;


select e4, e6 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where e1+100 > 2000 and e2-1000 < 2000000 group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where e1*100 > 2000 and e2/1000 < 2000000 group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where e1&1=1 and e2|1=1 group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where e1#1=1 and e2%1=1 group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where e1||1=true and e2<<1=1 group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select sin(e4), length(e6) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where  substr(e6, 1, 3)='tes' group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where substring(e6, 1, 3)='tes' group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select ceil(e4), left(e6, 1) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where right(e6, 1)='tes' group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select floor(e4), lower(e6) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where upper(e6)='TEST_TS1' group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where e4 group by rpad(e6, 1)='tes', e6 having e4 > 1000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select round(e4), ltrim(e6) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 where rtrim(e6)='test_ts1' group by e4, e6 having e4 > 1000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select count(e6) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 group by e6 having e6 like '%te' order by e6 desc limit 10 offset 1;
select min(e6) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 group by e6 having e6 like '%te' order by e6 desc limit 10 offset 1;
select count(e6) from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 group by e6 having e6 like '%te' order by e6 desc limit 10 offset 1;

-- from multiple tables
select e1 from test_ts.ts_table, test.test1 order by e1;
select e1 from test_ts.ts_table as t1, test.test1 as t2 where t1.e2 < 3000000 and t2.col4 > 1000.101 order by e1;
select e1 from test_ts.ts_table as t1, test.test1 as t2 group by t1.e1, t2.col1 having t1.e1 > 1000 or t2.col1 < 3000 order by e1;
select e1 from test_ts.ts_table as t1, test.test1 as t2 order by t1.e3 desc limit 10 offset 1;
select e1 from test_ts.ts_table as t1, test.test1 as t2 order by t2.col3 desc limit 10 offset 1;

select e1 from test_ts.ts_table as t1, test.test1 as t2 where t1.e1=t2.col1 and e1+100 > 2000 and col1-100<2000 order by e1;
select e1 from test_ts.ts_table as t1, test.test1 as t2 where t1.e1=t2.col1 and e1*100 > 2000 and col1/100<2000 order by e1;
select e1 from test_ts.ts_table as t1, test.test1 as t2 where t1.e1=t2.col1 and e1&1=1 and col1|1=1 order by e1;
select e1 from test_ts.ts_table as t1, test.test1 as t2 where t1.e1=t2.col1 and e1#1=1 and col1%1=1 order by e1;
select e1 from test_ts.ts_table as t1, test.test1 as t2 where t1.e1=t2.col1 and e1||1=true and col1<<1=1 order by e1;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as t1, test.test1 as t2 where t1.e1=t2.col1 and substring(e6, 1, 3)='tes' order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table as t1, test.test1 as t2 where t1.e1=t2.col1 and right(e6, 1)='tes';
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t1, test.test1 as t2 where t1.e1=t2.col1 and rpad(e6, 1)='tes';

-- union and subquery: double time series table
-- UNION
select e1 from test_ts.ts_table union select e1 from test_ts.ts_table2 order by e1;
select e1 from test_ts.ts_table union all select e1 from test_ts.ts_table2 order by e1;
select e1 from test_ts.ts_table intersect select e1 from test_ts.ts_table2 order by e1;

select * from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1 < 2000 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t group by t.e2 having t.e2 < 3000000 order by e2;
select * from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e3 < 3000000000 order by e1 desc limit 1 offset 1;
select e1, e2, case e2 when 1000 then '1000' when 2000 then '2000' end from (select e1,e2,e3,e6 from test_ts.ts_table union (select e1,e2,e3,e6 from test_ts.ts_table2 union select e1,e2,e3,e6 from test_ts.ts_table)) as t where t.e6 = 'test_ts1' order by e2;

--UNION with groupby
select e2, e6 from (select e1,e2,e3,e6 from test_ts.ts_table union (select e1,e2,e3,e6 from test_ts.ts_table2 union select e1,e2,e3,e6 from test_ts.ts_table)) as t where t.e1 in (2000, 3000) and t.e6 like '%tes' group by t.e2, t.e6 having t.e2 < 3000000 or t.e6 like '%tes' order by e2;
select e2, e6 from (select e1,e2,e3,e6 from test_ts.ts_table union (select e1,e2,e3,e6 from test_ts.ts_table2 union select e1,e2,e3,e6 from test_ts.ts_table)) as t where t.e1 not in (2000, 3000) and t.e6 not like '%tes' group by t.e2, t.e6 having t.e2 < 3000000 or t.e6 not like '%tes' order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1+1000 < 3000 group by t.e2 having t.e2+1000000 < 3000000 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1-1000 < 3000 group by t.e2 having t.e2-1000000 < 3000000 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1*10 = 1 group by t.e2 having t.e2*1000000 = 1 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1/10 = 1 group by t.e2 having t.e2/1000000 = 1 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1&10 = 1 group by t.e2 having t.e2&10 = 1 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1|10 = 1 group by t.e2 having t.e2|10 = 1 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1#10 = 1 group by t.e2 having t.e2#10 = 10 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1%10 = 1 group by t.e2 having t.e2%10 = 1 order by e2;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1<<1 < 1000 group by t.e2 having t.e2>>1 < 3000000 order by e2;
select sin(e4), length(e6) from (select e4,e6 from test_ts.ts_table union (select e4,e6 from test_ts.ts_table2 union select e4,e6 from test_ts.ts_table)) as t where substr(e6, 1, 3)='tes' group by t.e4, t.e6 having t.e4 < 3000000 or t.e6 like '%tes' order by e4, e6;
select cos(e4), concat(e6, 'test') from (select e4,e6 from test_ts.ts_table union (select e4, e6 from test_ts.ts_table2 union select e4, e6 from test_ts.ts_table)) as t where cast(e4 as float) < 3000 and substring(e6, 1, 3)='tes' group by t.e4, t.e6 having t.e4 < 3000000 or t.e6 like '%tes' order by e4, e6;
select ceil(e4), left(e6, 1) from (select e4,e6 from test_ts.ts_table union (select e4,e6 from test_ts.ts_table2 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as string) < '3000' and right(e6, 1)='tes' group by t.e4, t.e6 having t.e4 < 3000000 or t.e6 like '%tes' order by e4, e6;
select floor(e4), lower(e6) from (select e4,e6 from test_ts.ts_table union (select e4,e6 from test_ts.ts_table2 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as int) < 3000 and upper(e6)='TEST_TS1' group by t.e4, t.e6 having t.e4 < 3000000 or t.e6 like '%tes' order by e4, e6;
select power(e4, 2), lpad(e6, 1) from (select e4,e6 from test_ts.ts_table union (select e4,e6 from test_ts.ts_table2 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as bigint) < 3000 and rpad(e6, 1)='tes' group by t.e4, t.e6 having t.e4 < 3000000 or t.e6 like '%tes' order by e4, e6;
select round(e4), ltrim(e6, 'test') from (select e4,e6 from test_ts.ts_table union (select e4,e6 from test_ts.ts_table2 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as bigint) < 3000 and rtrim(e6)='tes' group by t.e4, t.e6 having t.e4 < 3000000 or t.e6 like '%tes' order by e4, e6;

-- UNION with orderby
select e2, e6 from (select e1,e2,e3,e6 from test_ts.ts_table union (select e1,e2,e3,e6 from test_ts.ts_table2 union select e1,e2,e3,e6 from test_ts.ts_table)) as t where t.e1 in (2000, 3000) and t.e6 like '%tes' order by e2 desc limit 1 offset 1;
select e2, e6 from (select e1,e2,e3,e6 from test_ts.ts_table union (select e1,e2,e3,e6 from test_ts.ts_table2 union select e1,e2,e3,e6 from test_ts.ts_table)) as t where t.e1 not in (2000, 3000) and t.e6 not like '%tes' order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1+1000 < 3000 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1-1000 < 3000 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1*10 = 1 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1/10 = 1 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1&10 = 1 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1|10 = 1 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1#10 = 1 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1%10 = 1 order by e2 desc limit 1 offset 1;
select e2 from (select e1,e2,e3 from test_ts.ts_table union (select e1,e2,e3 from test_ts.ts_table2 union select e1,e2,e3 from test_ts.ts_table)) as t where t.e1<<1 < 1000 order by e2 desc limit 1 offset 1;
select sin(e4), length(e6) from (select e4,e6 from test_ts.ts_table union (select e4,e6 from test_ts.ts_table2 union select e4,e6 from test_ts.ts_table)) as t where substr(e6, 1, 3)='tes' order by e4 desc limit 1 offset 1;
select cos(e4), concat(e6, 'test') from (select e4,e6 from test_ts.ts_table union (select e4, e6 from test_ts.ts_table2 union select e4, e6 from test_ts.ts_table)) as t where cast(e4 as float) < 3000 and substring(e6, 1, 3)='tes' order by e4 desc limit 1 offset 1;
select ceil(e4), left(e6, 1) from (select e4,e6 from test_ts.ts_table union (select e4,e6 from test_ts.ts_table2 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as string) < '3000' and right(e6, 1)='tes' order by e4 desc limit 1 offset 1;
select floor(e4), lower(e6) from (select e4,e6 from test_ts.ts_table union (select e4,e6 from test_ts.ts_table2 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as int) < 3000 and upper(e6)='TEST_TS1' order by e4 desc limit 1 offset 1;
select power(e4, 2), lpad(e6, 1) from (select e4,e6 from test_ts.ts_table union (select e4,e6 from test_ts.ts_table2 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as bigint) < 3000 and rpad(e6, 1)='tes' order by e4 desc limit 1 offset 1;
select round(e4), ltrim(e6) from (select e4,e6 from test_ts.ts_table union (select e4,e6 from test_ts.ts_table2 union select e4,e6 from test_ts.ts_table)) as t where cast(e4 as bigint) < 3000 and rtrim(e6)='tes' order by e4 desc limit 1 offset 1;


-- uncorrelated subquery
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 order by e3 desc limit 1 offset 1) order by e2;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) group by e2 having e2 < 3000000 order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) order by e3 desc limit 1 offset 1;
select e1, e2, case e2 when 1000 then '1000' when 2000 then '2000' end from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) order by e2;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e3 = (select e3 from test_ts.ts_table where e6='test_ts1' limit 1)) group by e2 having e2 < 3000000 order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e3 = (select e3 from test_ts.ts_table where e6='test_ts1' limit 1)) order by e3 desc limit 1 offset 1;

-- subquery with groupby
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1 between 1000 and 2000 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e6 like '%tes' group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e6 not like '%tes' group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1+10 > 1000 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1-10 > 1000 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1*10 > 1000 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1/10 > 1000 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1&10 = 1 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1|10 = 1 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1#10 = 1 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1%10 = 1 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1<<1 = 1 group by e1 having e1 < 3000) order by e2;
select sin(e4), length(e6) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where substr(e6, 1, 3)='tes' group by e1 having e1 < 3000) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where substring(e6, 1, 3)='tes' group by e1 having e1 < 3000) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where right(e6, 1)='tes' group by e1 having e1 < 3000) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where upper(e6)='TEST_TS1' group by e1 having e1 < 3000) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where rpad(e6, 1)='tes' group by e1 having e1 < 3000) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where rtrim(e6)='test_ts1' group by e1 having e1 < 3000) order by e4, e6;

-- subquery with orderby
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1 between 1000 and 2000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e6 like '%tes' order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e6 not like '%tes' order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1+10 > 1000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1-10 > 1000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1*10 > 1000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1/10 > 1000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1&10 = 1 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1|10 = 1 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1#10 = 1 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1%10 = 1 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where e1<<1 = 1 order by e3 desc limit 1 offset 1) order by e2;
select sin(e4), length(e6) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where substr(e6, 1, 3)='tes' order by e3 desc limit 1 offset 1) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where substring(e6, 1, 3)='tes' order by e3 desc limit 1 offset 1) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where right(e6, 1)='tes' order by e3 desc limit 1 offset 1) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where upper(e6)='TEST_TS1' order by e3 desc limit 1 offset 1) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where rpad(e6, 1)='tes' order by e3 desc limit 1 offset 1) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2 where rtrim(e6)='test_ts1' order by e3 desc limit 1 offset 1) order by e4, e6;

-- select with groupby
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1 between 1000 and 2000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1-10 > 1000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1*10 > 1000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1/10 > 1000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1&1 = 1 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1|1 = 1 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1#1 = 1 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1%1 = 1 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1<<1 = 1 group by e2 having e2 < 3000000 order by e2;
select sin(e4), length(e6) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and right(e6, 1)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4, e6;

-- select with orderby
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1 between 1000 and 2000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1-10 > 1000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1*10 > 1000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1/10 > 1000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1&1 = 1 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1|1 = 1 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1#1 = 1 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1%1 = 1 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and e1<<1 = 1 order by e2 desc limit 1 offset 1;
select sin(e4), length(e6) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select cos(e4), concat(e6, 'test') from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select ceil(e4), left(e6, 1) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and right(e6, 1)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select floor(e4), lower(e6) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select round(e4), ltrim(e6) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select avg(e4) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and substr(e6, 1, 3)='tes' group by e4 having e4 < 3000000 order by e4 desc limit 1 offset 1;
select max(e4) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and substr(e6, 1, 3)='tes' group by e4 having e4 < 3000000 order by e4 desc limit 1 offset 1;
select sum(e4) from test_ts.ts_table where e1 in (select e1 from test_ts.ts_table2) and substr(e6, 1, 3)='tes' group by e4 having e4 < 3000000 order by e4 desc limit 1 offset 1;


select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table order by e3 desc limit 1 offset 1) order by e2;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) group by e2 having e2 < 3000000 order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) order by e3 desc limit 1 offset 1;
select *, case e2 when 1000 then '1000' when 2000 then '2000' end from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e2 = (select e2 from test_ts.ts_table2 where e6='test_c1' limit 1)) order by e2;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e2 = (select e2 from test_ts.ts_table2 where e6='test_c1' limit 1)) group by e2 having e2 < 3000000 order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e2 = (select e2 from test_ts.ts_table2 where e6='test_c1' limit 1)) order by e3 desc limit 1 offset 1; 

-- subquery with groupby
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1+10 > 1000 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1-10 > 1000 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1*10 > 1000 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1/10 > 1000 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1&10 = 1 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1|10 = 1 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1#10 = 1 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1%10 = 1 group by e1 having e1 < 3000) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1<<1 = 1 group by e1 having e1 < 3000) order by e2;
select sin(e4), length(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where substr(e6, 1, 3)='tes' group by e1 having e1 < 3000) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where substring(e6, 1, 3)='tes' group by e1 having e1 < 3000) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where right(e6, 1)='tes' group by e1 having e1 < 3000) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where upper(e6)='TEST_TS1' group by e1 having e1 < 3000) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where rpad(e6, 1)='tes' group by e1 having e1 < 3000) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where rtrim(e6)='test_ts1' group by e1 having e1 < 3000) order by e4, e6;


-- subquery with orderby
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1 > 1000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1+10 > 1000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1-10 > 1000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1*10 > 1000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1/10 > 1000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1&10 = 1 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1|10 = 1 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1#10 = 1 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1%10 = 1 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1||10 = false order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where e1<<1 = 1 order by e3 desc limit 1 offset 1) order by e2;
select sin(e4), length(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where substr(e6, 1, 3)='tes' order by e3 desc limit 1 offset 1) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where substring(e6, 1, 3)='tes' order by e3 desc limit 1 offset 1) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where right(e6, 1)='tes' order by e3 desc limit 1 offset 1) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where upper(e6)='TEST_TS1' order by e3 desc limit 1 offset 1) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where rpad(e6, 1)='tes' order by e3 desc limit 1 offset 1) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table where rtrim(e6)='test_ts1' order by e3 desc limit 1 offset 1) order by e4, e6;


-- select with groupby
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1 between 1000 and 2000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1+10 > 1000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1-10 > 1000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1*10 > 1000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1/10 > 1000 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1&1 = 1 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1|1 = 1 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1#1 = 1 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1%1 = 1 group by e2 having e2 < 3000000 order by e2;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1<<1 = 1 group by e2 having e2 < 3000000 order by e2;
select sin(e4), length(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and right(e6, 1)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4, e6;


-- select with orderby
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1 between 1000 and 2000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1+10 > 1000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1-10 > 1000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1*10 > 1000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1/10 > 1000 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1&1 = 1 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1|1 = 1 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1#1 = 1 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1%1 = 1 order by e2 desc limit 1 offset 1;
select e2 from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and e1<<1 = 1 order by e2 desc limit 1 offset 1;
select sin(e4), length(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select ceil(e4), left(e6, 1) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and right(e6, 1)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select floor(e4), lower(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select round(e4), ltrim(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 3000000 and e6 ='test_ts1' order by e4 desc limit 1 offset 1;
select count(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and substr(e6, 1, 3)='tes' group by e6 having e6 ='test_ts1' order by e6 desc limit 1 offset 1;
select min(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and substr(e6, 1, 3)='tes' group by e6 having e6 ='test_ts1' order by e6 desc limit 1 offset 1;
select count(e6) from test_ts.ts_table2 where e1 in (select e1 from test_ts.ts_table) and substr(e6, 1, 3)='tes' group by e6 having e6 ='test_ts1' order by e6 desc limit 1 offset 1;



select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) order by e2;
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000 group by e1 having e1 < 2000) order by e2;
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000 order by e3 desc limit 1 offset 1) order by e2;
select e2 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) group by e2 having e2 < 3000000 order by e2;
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) order by e3 desc limit 1 offset 1;
select *, case e2 when 1000 then '1000' when 2000 then '2000' end from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) order by e3 desc limit 1 offset 1;

-- subquery with groupby
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 group by e1 having e1 < 2000) order by e2;
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1+10<1000 and t1.e2-1000 > 2000000 group by e1 having e1 < 2000) order by e2;
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1*10>1000 and t1.e2/1000 < 2000 group by e1 having e1 < 2000) order by e2;
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1*1 = 1 and t1.e2|1 = 1 group by e1 having e1 < 2000) order by e2;
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1#1 = 1 and t1.e2%1 = 0 group by e1 having e1 < 2000) order by e2;
select sin(e4), length(e6) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where substr(e6, 1, 3)='tes' and t1.e2 > 2000000 group by e1 having e1 < 2000) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where substring(e6, 1, 3)='tes' and t1.e2 > 2000000 group by e1 having e1 < 2000) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where substring(e6, 1, 3)='tes' and t1.e2 > 2000000 group by e1 having e1 < 2000) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where right(e6, 1)='tes' and t1.e2 > 2000000 group by e1 having e1 < 2000) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where upper(e6)='TEST_TS1' and upper(t1.e6) like '%test' group by e1 having e1 < 2000) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where rpad(e6, 1)='tes' and upper(t1.e6) like '%test' group by e1 having e1 < 2000) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where rtrim(e6)='test_ts1' and upper(t1.e6) like '%test' group by e1 having e1 < 2000) order by e4, e6;


-- subquery with orderby
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1 between 1000 and 2000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1+10<1000 and t1.e2-1000 > 2000000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1*10>1000 and t1.e2/1000 < 2000 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1&1 = 1 and t1.e2|1 = 1 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1#1 = 1 and t1.e2%1 = 0 order by e3 desc limit 1 offset 1) order by e2;
select * from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1||1 = true and t1.e2<<1 = 1 order by e3 desc limit 1 offset 1) order by e2;
select sin(e4), length(e6) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where substr(e6, 1, 3)='tes' and t1.e2 > 2000000 order by e3 desc limit 1 offset 1) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where substring(e6, 1, 3)='tes' and t1.e2 > 2000000 order by e3 desc limit 1 offset 1) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where substring(e6, 1, 3)='tes' and t1.e2 > 2000000 order by e3 desc limit 1 offset 1) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where right(e6, 1)='tes' and t1.e2 > 2000000 order by e3 desc limit 1 offset 1) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where upper(e6)='TEST_TS1' and upper(t1.e6) like '%test' order by e3 desc limit 1 offset 1) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where rpad(e6, 1)='tes' and upper(t1.e6) like '%test' order by e3 desc limit 1 offset 1) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where rtrim(e6)='test_ts1' and upper(t1.e6) like '%test' order by e3 desc limit 1 offset 1) order by e4, e6;


-- select with groupby
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1 between 1000 and 2000 group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1+10 > 2000 group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1-10 > 2000 group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1*10 > 2000 group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1/10 > 2000 group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1&1 = 1 and e2|1=1 group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1#1 = 1 and e2%1=1 group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1||1 = false and e2<<1=1 group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select sin(e4), length(e6) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and upper(e6)='TEST_TS1' group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and rpad(e6, 1)='tes' group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and rtrim(e6)='test_ts1' group by e4, e6 having e4 > 2000000.101 and e6 like '%tes' order by e4, e6;


-- select with orderby
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1 between 1000 and 2000 order by e3 desc limit 1 offset 1;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1+10 > 2000 order by e3 desc limit 1 offset 1;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1-10 > 2000 order by e3 desc limit 1 offset 1;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1*10 > 2000 order by e3 desc limit 1 offset 1;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1/10 > 2000 order by e3 desc limit 1 offset 1;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1&1 = 1 and e2|1=1 order by e3 desc limit 1 offset 1;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1#1 = 1 and e2%1=1 order by e3 desc limit 1 offset 1;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1||1 = false and e2<<1=1 order by e3 desc limit 1 offset 1;
select e4, e6 from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and e1 order by e3 desc limit 1 offset 1;
select sin(e4), length(e6) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and substr(e6, 1, 3)='tes' order by e3 desc limit 1 offset 1;
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and substring(e6, 1, 3)='tes' order by e3 desc limit 1 offset 1;
select ceil(e4), left(e6, 1) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and upper(e6)='TEST_TS1' order by e3 desc limit 1 offset 1;
select floor(e4), lower(e6) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and rpad(e6, 1)='tes' order by e3 desc limit 1 offset 1;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and substr(e6, 1, 3)='tes' order by e3 desc limit 1 offset 1;
select round(e4), ltrim(e6) from test_ts.ts_table2 as t1 where e1 in (select e1 from test_ts.ts_table where e1<1000 and t1.e2 > 2000000) and rtrim(e6)='test_ts1' order by e3 desc limit 1 offset 1;

-- join
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) order by e2; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e2 > 1000000 group by m.e1 having m.e1 < 2000) order by e2; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 order by m.e3 desc limit 1 offset 1) order by e2; 
select e2 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) group by e2 having e2 < 3000000 order by e2; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) order by e3 desc limit 1 offset 1; 
select *, case e2 when 1000 then '1000' when 2000 then '2000' end from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) order by e2; 

-- subquery with groupby
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e2 between 1000000 and 2000000 group by m.e1 having m.e1 < 2000) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e2+1000 > 1000000 group by m.e1 having m.e1 < 2000) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e2-1000 > 1000000 group by m.e1 having m.e1 < 2000) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e2*1000 > 1000000 group by m.e1 having m.e1 < 2000) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e2/1000 > 1000000 group by m.e1 having m.e1 < 2000) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e1|1=1 and m.e2&1=1 group by m.e1 having m.e1 < 2000) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e1#1=1 and m.e2%1=1 group by m.e1 having m.e1 < 2000) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e1||1=false and m.e2<<1=1 group by m.e1 having m.e1 < 2000) order by e4, e6; 
select sin(e4), length(e6) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where substr(m.e6, 1, 3)='tes' group by m.e1 having m.e1 < 2000) order by e4, e6; 
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where substring(m.e6, 1, 3)='tes' group by m.e1 having m.e1 < 2000) order by e4, e6; 
select ceil(e4), left(e6, 1) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where right(m.e6, 1)='tes' group by m.e1 having m.e1 < 2000) order by e4, e6; 
select floor(e4), lower(e6) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where upper(m.e6)='TEST_TS1' group by m.e1 having m.e1 < 2000) order by e4, e6; 
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where rpad(m.e6, 1)='tes' group by m.e1 having m.e1 < 2000) order by e4, e6; 
select round(e4), ltrim(e6) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where rtrim(m.e6)='test_ts1' group by m.e1 having m.e1 < 2000) order by e4, e6; 


-- subquery with orderby
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e2 between 1000000 and 2000000 order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e2+1000 > 1000000 order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e2-1000 > 1000000 order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e2*1000 > 1000000 order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e2/1000 > 1000000 order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e1|1=1 and m.e2&1=1 order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e1#1=1 and m.e2%1=1 order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select * from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where m.e1||1=false and m.e2<<1=1 order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select sin(e4), length(e6) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where substr(m.e6, 1, 3)='tes' order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where substring(m.e6, 1, 3)='tes' order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select ceil(e4), left(e6, 1) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where right(m.e6, 1)='tes' order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select floor(e4), lower(e6) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where upper(m.e6)='TEST_TS1' order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where rpad(m.e6, 1)='tes' order by m.e3 desc limit 1 offset 1) order by e4, e6; 
select round(e4), ltrim(e6) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3 where rtrim(m.e6)='test_ts1' order by m.e3 desc limit 1 offset 1) order by e4, e6; 


-- select with groupby
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2 between 1000000 and 2000000 group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2+100 > 2000000 group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2-100 > 2000000 group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2*100 > 2000000 group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2/100 > 2000000 group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2&1=1 and e3|1=1 group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2#1=1 and e3%1=1 group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2||1=true and e3<<1=1 group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select sin(e4), length(e6) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select ceil(e4), left(e6, 1) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and right(e6, 1)='tes' group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select floor(e4), lower(e6) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and upper(e6)='TEST_TS1' group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and rpad(e6, 1)='tes' group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 
select round(e4), ltrim(e6) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and rtrim(e6)='test_ts1' group by e4, e6 having e4 > 3000000 and e6 like '%te' order by e4, e6; 


-- select with orderby
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2 between 1000000 and 2000000 order by e4 desc limit 1 offset 1; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2+100 > 2000000 order by e4 desc limit 1 offset 1; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2-100 > 2000000 order by e4 desc limit 1 offset 1; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2*100 > 2000000 order by e4 desc limit 1 offset 1; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2/100 > 2000000 order by e4 desc limit 1 offset 1; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2&1=1 and e3|1=1 order by e4 desc limit 1 offset 1; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2#1=1 and e3%1=1 order by e4 desc limit 1 offset 1; 
select e4, e6 from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and e2||1=true and e3<<1=1 order by e4 desc limit 1 offset 1; 
select sin(e4), length(e6) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and substr(e6, 1, 3)='tes' order by e4 desc limit 1 offset 1; 
select cos(e4), concat(e6, 'test') from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and substring(e6, 1, 3)='tes' order by e4 desc limit 1 offset 1; 
select ceil(e4), left(e6, 1) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and right(e6, 1)='tes' order by e4 desc limit 1 offset 1; 
select floor(e4), lower(e6) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and upper(e6)='TEST_TS1' order by e4 desc limit 1 offset 1; 
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and rpad(e6, 1)='tes' order by e4 desc limit 1 offset 1; 
select round(e4), ltrim(e6) from test_ts.ts_table2 where e1 in (select m.e1 from test_ts.ts_table as m join test_ts.ts_table2 as n on m.e3=n.e3) and rtrim(e6)='test_ts1' order by e4 desc limit 1 offset 1; 

select e1 = ( select e1 from test_ts.ts_table2 limit 1 ) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where e2 > 1000000 group by e1 having e1 < 4000) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 order by e3 desc limit 10 offset 1) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 ) from test_ts.ts_table where e2 > 1000000 group by e1 having e1 < 2000;
select e1 in ( select e1 from test_ts.ts_table2 ) from test_ts.ts_table order by e3 desc limit 10 offset 1;
select e1 in ( select e1 from test_ts.ts_table2 ), case e2 when 1000000 then 10 when 2000000 then 20 end from test_ts.ts_table order by e3 desc limit 1 offset 1;

select e1 in ( select e1 from test_ts.ts_table2 where e2 between 1000000 and 2000000 group by e1 having e1 < 4000) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where e1+100 > 2000 and e2-1000 > 1000000 group by e1 having e1 < 4000) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where e1*100 > 2000 and e2/1000 > 1000000 group by e1 having e1 < 4000) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where e1&1=1 and e1|1=1 group by e1 having e1 < 4000) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where e1#1=1 and e1%1=1 group by e1 having e1 < 4000) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where e1||1=false and e1<<1=1 group by e1 having e1 < 4000) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where sin(e4)=1 and length(e6)=1 group by e1 having e1 < 4000) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where cos(e4)=1 and concat(e6, 'test')='test' group by e1 having e1 < 4000) from test_ts.ts_table;
select e1 in ( select max(e1) from test_ts.ts_table2 where ceil(e4)=1 and left(e6, 1)='t' group by e1 having e1 < 4000) from test_ts.ts_table;
select e1 in ( select min(e1) from test_ts.ts_table2 where ceil(e4)=1 and left(e6, 1)='t' group by e1 having e1 < 4000) from test_ts.ts_table;

select e1 in ( select e1 from test_ts.ts_table2 where e2 between 1000000 and 2000000 order by e3 desc limit 10 offset 1) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where e1+100 > 2000 and e2-1000 > 1000000 order by e3 desc limit 10 offset 1) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where e1*100 > 2000 and e2/1000 > 1000000 order by e3 desc limit 10 offset 1) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where e1&1=1 and e1|1=1 order by e3 desc limit 10 offset 1) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where e1#1=1 and e1%1=1 order by e3 desc limit 10 offset 1) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where e1||1=false and e1<<1=1 order by e3 desc limit 10 offset 1) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where sin(e4)=1 and length(e6)=1 order by e3 desc limit 10 offset 1) from test_ts.ts_table;
select e1 in ( select e1 from test_ts.ts_table2 where cos(e4)=1 and concat(e6, 'test')='test' order by e3 desc limit 10 offset 1) from test_ts.ts_table;
select e1 in ( select max(e3) from test_ts.ts_table2 where ceil(e4)=1 and left(e6, 1)='t' group by e3 order by e3 desc limit 10 offset 1) from test_ts.ts_table;
select e1 in ( select min(e3) from test_ts.ts_table2 where ceil(e4)=1 and left(e6, 1)='t' group by e3 order by e3 desc limit 10 offset 1) from test_ts.ts_table;

select e1 = ( select e1 from test_ts.ts_table limit 1) from test_ts.ts_table2;
--select e1 = ( select e1 from test_ts.ts_table limit 1) from test_ts.ts_table2 where e3 in (select e3 from test_ts.ts_table where e6='test_ts2');
select e1 in ( select e1 from test_ts.ts_table where e2 > 1000000 group by e1 having e1 < 2000 ) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table order by e3 desc limit 10 offset 1 ) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table ) from test_ts.ts_table2 where e2 > 1000000 group by e1 having e1 < 2000;
select e1 in ( select e1 from test_ts.ts_table ) from test_ts.ts_table2 order by e3 desc limit 10 offset 1;
select e1 in ( select e1 from test_ts.ts_table ), case e2 when 1000000 then 10 when 2000000 then 20 end from test_ts.ts_table2;

select e1 in ( select e1 from test_ts.ts_table where e2 between 1000000 and 2000000 group by e1 having e1 < 4000) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where e1+100 > 2000 and e2-1000 > 1000000 group by e1 having e1 < 4000) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where e1*100 > 2000 and e2/1000 > 1000000 group by e1 having e1 < 4000) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where e1&1=1 and e2|1=1 group by e1 having e1 < 4000) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where e1#1=1 and e2%1=1 group by e1 having e1 < 4000) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where e1||1=false and e2<<1=1 group by e1 having e1 < 4000) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where sin(e4)=1 and length(e6)=1 group by e1 having e1 < 4000) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where cos(e4)=1 and concat(e6, 'test')='test' group by e1 having e1 < 4000) from test_ts.ts_table2;
select e1 in ( select max(e1) from test_ts.ts_table where ceil(e4)=1 and left(e6, 1)='t' group by e1 having e1 < 4000) from test_ts.ts_table2;
select e1 in ( select min(e1) from test_ts.ts_table where ceil(e4)=1 and left(e6, 1)='t' group by e1 having e1 < 4000) from test_ts.ts_table2;

select e1 in ( select e1 from test_ts.ts_table where e2 between 1000000 and 2000000 order by e1 desc limit 10 offset 1) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where e1+100 > 2000 and e2-1000 > 1000000 order by e1 desc limit 10 offset 1) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where e1*100 > 2000 and e2/1000 > 1000000 order by e1 desc limit 10 offset 1) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where e1&1=1 and e2|1=1 order by e1 desc limit 10 offset 1) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where e1#1=1 and e2%1=1 order by e1 desc limit 10 offset 1) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where e1||1=false and e2<<1=1 order by e1 desc limit 10 offset 1) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where sin(e4)=1 and length(e6)=1 order by e1 desc limit 10 offset 1) from test_ts.ts_table2;
select e1 in ( select e1 from test_ts.ts_table where cos(e4)=1 and concat(e6, 'test')='test' order by e1 desc limit 10 offset 1) from test_ts.ts_table2;
select e1 in ( select max(e1) from test_ts.ts_table where ceil(e4)=1 and left(e6, 1)='t' group by e1 order by e1 desc limit 10 offset 1) from test_ts.ts_table2;
select e1 in ( select min(e1) from test_ts.ts_table where ceil(e4)=1 and left(e6, 1)='t' group by e1 order by e1 desc limit 10 offset 1) from test_ts.ts_table2;

-- correlated subquery
select e1, e2, e3 from test_ts.ts_table2 as t1 where exists (select e1 from test_ts.ts_table as t2 where t1.e1=t2.e1 and t1.e2 > t2.e2) order by e1;
select * from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 group by e1 having e1 < 2000) order by e1;
select * from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 order by e3 desc limit 10 offset 1) order by e1;
select e1 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) group by e1 having e1 < 2000 order by e1;
select * from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) order by e3 desc limit 10 offset 1;
select *, case e2 when 1000 then '1000' when 2000 then '2000' end from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) order by e1;

-- subquery with groupby
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1 between 1000 and 2000 group by e1 having e1 < 2000) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1+100 > 1000 and t2.e1-100 < 2000 group by e1 having e1 < 2000) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1*100 > 1000 and t2.e1/100 < 2000 group by e1 having e1 < 2000) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1&1=1 and t2.e1|1=1 group by e1 having e1 < 2000) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1#1=1 and t2.e1%1=1 group by e1 having e1 < 2000) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1||1=false and t2.e1<<1=1 group by e1 having e1 < 2000) order by e4, e6;
select sin(e4), length(e6) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and substr(e6, 1, 3)='tes' group by e1 having e1 < 2000) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and substring(e6, 1, 3)='tes' group by e1 having e1 < 2000) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and right(e6, 1)='tes' group by e1 having e1 < 2000) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and upper(e6)='TEST_TS1' group by e1 having e1 < 2000) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and rpad(e6, 1)='tes' group by e1 having e1 < 2000) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and rtrim(e6)='test_ts1' group by e1 having e1 < 2000) order by e4, e6;


-- subquery with orderby
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1 between 1000 and 2000 order by e3 desc limit 10 offset 1) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1+100 > 1000 and t2.e1-100 < 2000 order by e3 desc limit 10 offset 1) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1*100 > 1000 and t2.e1/100 < 2000 order by e3 desc limit 10 offset 1) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1&1=1 and t2.e1|1=1 order by e3 desc limit 10 offset 1) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1#1=1 and t2.e1%1=1 order by e3 desc limit 10 offset 1) order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1||1=false and t2.e1<<1=1 order by e3 desc limit 10 offset 1) order by e4, e6;
select sin(e4), length(e6) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and substr(e6, 1, 3)='tes' order by e3 desc limit 10 offset 1) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and substring(e6, 1, 3)='tes' order by e3 desc limit 10 offset 1) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and right(e6, 1)='tes' order by e3 desc limit 10 offset 1) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and upper(e6)='TEST_TS1' order by e3 desc limit 10 offset 1) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and rpad(e6, 1)='tes' order by e3 desc limit 10 offset 1) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1 and rtrim(e6)='test_ts1' order by e3 desc limit 10 offset 1) order by e4, e6;


-- select with groupby
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and e1+100 > 1000 and e2-100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and e1 between 1000 and 2000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and e1*100 > 100000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and e1&1=1 and e2|1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and e1#1=1 and e2%1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and e1||1=true and e2<<1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select sin(e4), length(e6) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and right(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6;

 
-- select with orderby
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and e1+100 > 1000 and e2-100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and e1 between 1000 and 2000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and e1*100 > 100000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and e1&1=1 and e2|1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and e1#1=1 and e2%1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select e4, e6 from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and e1||1=true and e2<<1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select sin(e4), length(e6) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select ceil(e4), left(e6, 1) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and right(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select floor(e4), lower(e6) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select round(e4), ltrim(e6) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1;
select avg(e4) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) group by e4 having e4 < 2000000.01 order by e4 desc limit 10 offset 1;
select max(e4) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) group by e4 having e4 < 2000000.01 order by e4 desc limit 10 offset 1;
select sum(e4) from test_ts.ts_table as t1 where exists (select e1 from test_ts.ts_table2 as t2 where t1.e1=t2.e1) group by e4 having e4 < 2000000.01 order by e4 desc limit 10 offset 1;


select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) order by e1;
select e1 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) group by e1 having e1 < 2000 order by e1;
select e1 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) order by e3 desc limit 10 offset 1;
select *, case e2 when 1000 then '1000' when 2000 then '2000' end from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) order by e1;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e1;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e1;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000 group by e1 having e1 < 2000) as n where m.e1=n.e1) order by e1;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000 order by e3 desc limit 10 offset 1) as n where m.e1=n.e1) order by e1;

select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 and e1 between 1000 and 2000 group by e1 having e1 < 2000) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 and e1+100 > 1000 and e2-100<2000000 group by e1 having e1 < 2000) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 and e1*100 > 1000 and e2/100<2000000 group by e1 having e1 < 2000) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 and e1&1=1 and e2|1=1 group by e1 having e1 < 2000) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 and e1#1=1 and e2%1=1 group by e1 having e1 < 2000) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 and e1||1=true and e2<<1=1 group by e1 having e1 < 2000) order by e4, e6;
select sin(e4), length(e6) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 group by e1 having e1 < 2000) order by e4, e6;

select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 and e1 between 1000 and 2000 order by e3 desc limit 10 offset 1) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 and e1+100 > 1000 and e2-100<2000000 order by e3 desc limit 10 offset 1) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 and e1*100 > 1000 and e2/100<2000000 order by e3 desc limit 10 offset 1) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 and e1&1=1 and e2|1=1 order by e3 desc limit 10 offset 1) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 and e1#1=1 and e2%1=1 order by e3 desc limit 10 offset 1) order by e4, e6;
select * from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 and e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1) order by e4, e6;
select sin(e4), length(e6) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e4, e6;
select cos(e4), concat(e6, 'test') from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e4, e6;
select ceil(e4), left(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e4, e6;
select floor(e4), lower(e6) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e4, e6;
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e4, e6;
select round(e4), ltrim(e6) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1 order by e3 desc limit 10 offset 1) order by e4, e6;

select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1+100 > 1000 and e2-100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1 between 1000 and 2000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1*100 > 1000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1*100 > 1000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1&1=1 and e2|1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1#1=1 and e2%1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1||1=true and e2<<1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select sin(e4), length(e6) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select cos(e4), concat(e6, 'test') from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select ceil(e4), left(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and right(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select floor(e4), lower(e6) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 
select round(e4), ltrim(e6) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4, e6; 

select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1+100 > 1000 and e2-100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1 between 1000 and 2000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te'; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1*100 > 1000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1*100 > 1000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1&1=1 and e2|1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1#1=1 and e2%1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select e4, e6 from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and e1||1=true and e2<<1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select sin(e4), length(e6) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select cos(e4), concat(e6, 'test') from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select ceil(e4), left(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and right(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select floor(e4), lower(e6) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and upper(e6)='TEST_TS1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select power(e4, 2), lpad(e6, 1) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and rpad(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 
select round(e4), ltrim(e6) from test_ts.ts_table as m where e1 in (select e1 from (select e1 as e1 from test_ts.ts_table2 where m.e1 < 10000) as n where m.e1=n.e1) and rtrim(e6)='test_ts1' group by e4, e6 having e4 < 2000000.01 and e6 like '%te' order by e4 desc limit 10 offset 1; 


select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1) from test_ts.ts_table as t2 where e3 < 2000000000 group by e1 having e1 < 2000 order by e1;
select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table as t2 order by e3 desc limit 10 offset 1;

select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1) from test_ts.ts_table2 as t2 where e2 > 2000000 group by e1 having e1 > 1000 order by e1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1) from test_ts.ts_table2 as t2 order by e3 desc limit 10 offset 1;

select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table as t2 where e1 between 1000 and 2000 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table as t2 where e1+100 > 1000 and e2-100 < 2000000 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table as t2 where e1*100 > 1000 and e2/100 < 2000000 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table as t2 where e1&1=1 and e2|1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table as t2 where e1#1=1 and e2%1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), sin(e4), length(e6) from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), cos(e4), concat(e6, 'test') from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), ceil(e4), left(e6, 1) from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), floor(e4), lower(e6) from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), power(e4, 2), lpad(e6, 1) from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table2 as t1 where t1.e1=t2.e1), round(e4), ltrim(e6) from test_ts.ts_table as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select max(e1) from test_ts.ts_table2 as t1 where t1.e1=t2.e1)from test_ts.ts_table as t2 order by e3 desc limit 10 offset 1;
select e1 in (select min(e1) from test_ts.ts_table2 as t1 where t1.e1=t2.e1)from test_ts.ts_table as t2 order by e3 desc limit 10 offset 1;

select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1) from test_ts.ts_table2 as t2 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table2 as t2 where e1 between 1000 and 2000 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table2 as t2 where e1+100 > 1000 and e2-100 < 2000000 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table2 as t2 where e1*100 > 1000 and e2/100 < 2000000 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table2 as t2 where e1&1=1 and e2|1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table2 as t2 where e1#1=1 and e2%1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1), e2 from test_ts.ts_table2 as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1), sin(e4), length(e6) from test_ts.ts_table2 as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1), cos(e4), concat(e6, 'test') from test_ts.ts_table2 as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1), ceil(e4), left(e6, 1) from test_ts.ts_table2 as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1), floor(e4), lower(e6) from test_ts.ts_table2 as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1), power(e4, 2), lpad(e6, 1) from test_ts.ts_table2 as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select e1 from test_ts.ts_table as t1 where t1.e1=t2.e1), round(e4), ltrim(e6) from test_ts.ts_table2 as t2 where e1||1=true and e2<<1=1 order by e3 desc limit 10 offset 1;
select e1 in (select max(e1) from test_ts.ts_table as t1 where t1.e1=t2.e1)from test_ts.ts_table2 as t2 order by e3 desc limit 10 offset 1;
select e1 in (select min(e1) from test_ts.ts_table as t1 where t1.e1=t2.e1)from test_ts.ts_table2 as t2 order by e3 desc limit 10 offset 1;

select e1 from (select e1 from test_ts.ts_table);
select e1 from (select * from test_ts.ts_table) where e3 < 2000000000 group by e1 having e1 < 2000;
select e1, e2, e5 from (select * from test_ts.ts_table) order by e3 desc limit 10 offset 1;
select e1, case e2 when 1000 then '1000' when 2000 then '2000' end from (select * from test_ts.ts_table);


select e4, 46 from (select * from test_ts.ts_table) where e1+100 > 2000 and e2-100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes';
select e4, 46 from (select * from test_ts.ts_table) where e1*100 > 2000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes';
select e4, 46 from (select * from test_ts.ts_table) where e1&1=1 and e2|1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes';
select e4, 46 from (select * from test_ts.ts_table) where e1#1=1 and e2%1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes';
select e4, 46 from (select * from test_ts.ts_table) where e1||1=1 and e2<<1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes';
select sin(e4), length(e6) from (select * from test_ts.ts_table) where substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4, e6;
select cos(e4), concat(e6, 'test') from (select * from test_ts.ts_table) where substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4, e6;
select ceil(e4), left(e6, 1) from (select * from test_ts.ts_table) where right(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4, e6;
select floor(e4), lower(e6) from (select * from test_ts.ts_table) where upper(e6)='TEST_TS1' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4, e6;
select power(e4, 2), lpad(e6, 1) from (select * from test_ts.ts_table) where rpad(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4, e6;
select round(e4), ltrim(e6) from (select * from test_ts.ts_table) where rtrim(e6)='test_ts1' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4, e6;

select e4, 46 from (select * from test_ts.ts_table) where e1+100 > 2000 and e2-100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select e4, 46 from (select * from test_ts.ts_table) where e1*100 > 2000 and e2/100 < 2000000 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select e4, 46 from (select * from test_ts.ts_table) where e1&1=1 and e2|1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select e4, 46 from (select * from test_ts.ts_table) where e1#1=1 and e2%1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select e4, 46 from (select * from test_ts.ts_table) where e1||1=1 and e2<<1=1 group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select sin(e4), length(e6) from (select * from test_ts.ts_table) where substring(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select cos(e4), concat(e6, 'test') from (select * from test_ts.ts_table) where substr(e6, 1, 3)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select ceil(e4), left(e6, 1) from (select * from test_ts.ts_table) where right(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select floor(e4), lower(e6) from (select * from test_ts.ts_table) where upper(e6)='TEST_TS1' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select power(e4, 2), lpad(e6, 1) from (select * from test_ts.ts_table) where rpad(e6, 1)='tes' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select round(e4), ltrim(e6) from (select * from test_ts.ts_table) where rtrim(e6)='test_ts1' group by e4, e6 having e4 < 2000000.01 and e6 like '%tes' order by e4 desc limit 10 offset 1;
select avg(e4) from (select * from test_ts.ts_table) group by e4 having e4 < 2000000.01 order by e4 desc limit 10 offset 1;
select max(e4) from (select * from test_ts.ts_table) group by e4 having e4 < 2000000.01 order by e4 desc limit 10 offset 1;
select sum(e4) from (select * from test_ts.ts_table) group by e4 having e4 < 2000000.01 order by e4 desc limit 10 offset 1;

-- JOIN
select t1.e2 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
select t1.e2 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e2;
select t1.e2 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

select t1.e2 from test_ts.ts_table as t1 left join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
select t1.e2 from test_ts.ts_table as t1 left join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e2;
select t1.e2 from test_ts.ts_table as t1 left join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

select t1.e2 from test_ts.ts_table as t1 right join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
select t1.e2 from test_ts.ts_table as t1 right join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e2;
select t1.e2 from test_ts.ts_table as t1 right join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;

select t1.e2 from test_ts.ts_table as t1 full join test_ts.ts_table2 as t2 on t1.e1=t2.e1 order by t1.e2;
select t1.e2 from test_ts.ts_table as t1 full join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e2;
select t1.e2 from test_ts.ts_table as t1 full join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e2 having t1.e2 > 1000000 order by t1.e2;


select t1.e4, t1.e6 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e1+100 > 2000 and t1.e2-1000 < 2000000 group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4, t1.e6;
select t1.e4, t1.e6 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e1*100 > 2000 and t1.e2/1000 < 2000000 group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4, t1.e6;
select t1.e4, t1.e6 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e1&1=1 and t1.e2|1=1 group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4, t1.e6;
select t1.e4, t1.e6 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e1#1=1 and t1.e2%1=1 group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4, t1.e6;
select t1.e4, t1.e6 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e1||1=true and t1.e2<<1=1 group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4, t1.e6;
select sin(t1.e4), length(t1.e6) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where  substr(t1.e6, 1, 3)='tes' group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4, t1.e6;
select cos(t1.e4), concat(t1.e6, 'test') from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where substring(t1.e6, 1, 3)='tes' group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4, t1.e6;
select ceil(t1.e4), left(t1.e6, 1) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where right(t1.e6, 1)='tes' group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4, t1.e6;
select floor(t1.e4), lower(t1.e6) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where upper(t1.e6)='TEST_TS1' group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4, t1.e6;
select power(t1.e4, 2), lpad(t1.e6, 1) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e4 group by rpad(t1.e6, 1)='tes', t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4, t1.e6;
select round(t1.e4), ltrim(t1.e6) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where rtrim(t1.e6)='test_ts1' group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4, t1.e6;


select t1.e4, t1.e6 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e1+100 > 2000 and t1.e2-1000 < 2000000 group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4 desc limit 10 offset 1;
select t1.e4, t1.e6 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e1*100 > 2000 and t1.e2/1000 < 2000000 group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4 desc limit 10 offset 1;
select t1.e4, t1.e6 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e1&1=1 and t1.e2|1=1 group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4 desc limit 10 offset 1;
select t1.e4, t1.e6 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e1#1=1 and t1.e2%1=1 group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4 desc limit 10 offset 1;
select t1.e4, t1.e6 from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e1||1=true and t1.e2<<1=1 group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4 desc limit 10 offset 1;
select sin(t1.e4), length(t1.e6) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where  substr(t1.e6, 1, 3)='tes' group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4 desc limit 10 offset 1;
select cos(t1.e4), concat(t1.e6, 'test') from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where substring(t1.e6, 1, 3)='tes' group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4 desc limit 10 offset 1;
select ceil(t1.e4), left(t1.e6, 1) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where right(t1.e6, 1)='tes' group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4 desc limit 10 offset 1;
select floor(t1.e4), lower(t1.e6) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where upper(t1.e6)='TEST_TS1' group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4 desc limit 10 offset 1;
select power(t1.e4, 2), lpad(t1.e6, 1) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where t1.e4 group by rpad(t1.e6, 1)='tes', t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4 desc limit 10 offset 1;
select round(t1.e4), ltrim(t1.e6) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 where rtrim(t1.e6)='test_ts1' group by t1.e4, t1.e6 having t1.e4 > 1000000.01 and t1.e6 like '%te' order by t1.e4 desc limit 10 offset 1;
select count(t1.e6) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e6 having t1.e6 like '%te' order by t1.e6 desc limit 10 offset 1;
select min(t1.e6) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e6 having t1.e6 like '%te' order by t1.e6 desc limit 10 offset 1;
select count(t1.e6) from test_ts.ts_table as t1 join test_ts.ts_table2 as t2 on t1.e1=t2.e1 group by t1.e6 having t1.e6 like '%te' order by t1.e6 desc limit 10 offset 1;

-- from multiple tables
select t1.e1 from test_ts.ts_table, test_ts.ts_table2 order by t1.e1;
select t1.e1 from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e2 < 3000000 and t2.e4 > 1000.101 order by t1.e1;
select t1.e1 from test_ts.ts_table as t1, test_ts.ts_table2 as t2 group by t1.e1, t2.e1 having t1.e1 > 1000 or t2.e1 < 3000 order by t1.e1;
select t1.e1 from test_ts.ts_table as t1, test_ts.ts_table2 as t2 order by t1.e1 desc limit 10 offset 1;
select t1.e1 from test_ts.ts_table as t1, test_ts.ts_table2 as t2 order by t1.e1 desc limit 10 offset 1;

select t1.e4, t1.e6 from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1+100 > 2000 and t1.e1-100<2000 order by t1.e4, t1.e6;
select t1.e4, t1.e6 from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1*100 > 2000 and t1.e1/100<2000 order by t1.e4, t1.e6;
select t1.e4, t1.e6 from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1&1=1 and t1.e1|1=1 order by t1.e4, t1.e6;
select t1.e4, t1.e6 from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1#1=1 and t1.e1%1=1 order by t1.e4, t1.e6;
select t1.e4, t1.e6 from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e1=t2.e1 and t1.e1||1=true and t1.e1<<1=1 order by t1.e4, t1.e6;
select sin(t1.e4), length(t1.e6) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e1=t2.e1 and substr(t1.e6, 1, 3)='tes' order by t1.e4, t1.e6;
select cos(t1.e4), concat(t1.e6, 'test') from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e1=t2.e1 and substring(t1.e6, 1, 3)='tes' order by t1.e4, t1.e6;
select ceil(t1.e4), left(t1.e6, 1) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e1=t2.e1 and right(t1.e6, 1)='tes' order by t1.e4, t1.e6;
select floor(t1.e4), lower(t1.e6) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e1=t2.e1 and upper(t1.e6)='TEST_TS1' order by t1.e4, t1.e6;
select power(t1.e4, 2), lpad(t1.e6, 1) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e1=t2.e1 and rpad(t1.e6, 1)='tes' order by t1.e4, t1.e6;
select round(t1.e4), ltrim(t1.e6) from test_ts.ts_table as t1, test_ts.ts_table2 as t2 where t1.e1=t2.e1 and rtrim(t1.e6)='test_ts1' order by t1.e4, t1.e6;

-- ZDP-45235
set cluster setting sql.all_push_down.enabled=true;
explain select count(1) from test_ts.ts_table;
explain select count(e1+e2) from test_ts.ts_table;

-- ZDP-40021
select count(localtime()) from test_ts.ts_table;

-- ZDP-44520
use test_ts;
create table ts_table3(time timestamp not null, e1 smallint) attributes (attr1 smallint not null) primary attributes (attr1);
insert into ts_table3 values('2025-01-01 10:00:00', 1, 1), ('2025-01-01 11:00:00', 2, 2),('2025-01-01 12:00:00', 3, 3);
select count(EXISTS (select t1.e1 from ts_table as t1, lateral (select t1.e2, t2.e1 from ts_table2 as t2) as t3)) from ts_table3;

-- ZDP-33026
CREATE TS DATABASE tsmtest;

CREATE TABLE tsmtest.d1(time timestamptz not null, s0 DOUBLE PRECISION ,  s1 DOUBLE PRECISION , s2 DOUBLE PRECISION , s3 DOUBLE PRECISION, s4 DOUBLE PRECISION , s5 DOUBLE PRECISION , s6 DOUBLE PRECISION , s7 DOUBLE PRECISION , s8  DOUBLE PRECISION , s9 DOUBLE PRECISION , s10 DOUBLE PRECISION ,s11 DOUBLE PRECISION , s12 DOUBLE PRECISION , s13 DOUBLE PRECISION , s14 DOUBLE PRECISION ,   s15 DOUBLE PRECISION , s16 DOUBLE PRECISION , s17 DOUBLE PRECISION , s18 DOUBLE PRECISION , s19 DOUBLE PRECISION , s20 DOUBLE PRECISION , s21 DOUBLE PRECISION , s22 DOUBLE PRECISION , s23 DOUBLE PRECISION , s24 DOUBLE PRECISION , s25 DOUBLE PRECISION , s26 DOUBLE PRECISION , s27 DOUBLE PRECISION , s28 DOUBLE PRECISION , s29 DOUBLE PRECISION , s30 DOUBLE PRECISION , s31 DOUBLE PRECISION , s32 DOUBLE PRECISION , s33 DOUBLE PRECISION , s34 DOUBLE PRECISION ,  s35 DOUBLE PRECISION , s36 DOUBLE PRECISION , s37 DOUBLE PRECISION , s38 DOUBLE PRECISION , s39 DOUBLE PRECISION , s40 DOUBLE PRECISION , s41 DOUBLE PRECISION , s42 DOUBLE PRECISION , s43 DOUBLE PRECISION , s44 DOUBLE PRECISION , s45 DOUBLE PRECISION , s46 DOUBLE PRECISION , s47 DOUBLE PRECISION , s48 DOUBLE PRECISION , s49 DOUBLE PRECISION , s50 DOUBLE PRECISION , s51 DOUBLE PRECISION , s52 DOUBLE PRECISION , s53 DOUBLE PRECISION , s54 DOUBLE PRECISION ,  s55 DOUBLE PRECISION , s56 DOUBLE PRECISION , s57 DOUBLE PRECISION , s58 DOUBLE PRECISION , s59 DOUBLE PRECISION , s60 DOUBLE PRECISION , s61 DOUBLE          PRECISION , s62 DOUBLE PRECISION , s63 DOUBLE PRECISION , s64 DOUBLE PRECISION , s65 DOUBLE PRECISION , s66 DOUBLE PRECISION , s67 DOUBLE PRECISION , s68 DOUBLE PRECISION , s69 DOUBLE PRECISION , s70 DOUBLE PRECISION , s71 DOUBLE PRECISION , s72 DOUBLE PRECISION , s73 DOUBLE PRECISION , s74 DOUBLE PRECISION ,  s75 DOUBLE PRECISION , s76 DOUBLE PRECISION , s77 DOUBLE PRECISION , s78 DOUBLE PRECISION , s79 DOUBLE PRECISION , s80 DOUBLE PRECISION , s81 DOUBLE PRECISION , s82 DOUBLE PRECISION , s83 DOUBLE PRECISION , s84 DOUBLE PRECISION , s85 DOUBLE PRECISION , s86 DOUBLE PRECISION , s87 DOUBLE PRECISION , s88 DOUBLE PRECISION , s89 DOUBLE PRECISION , s90 DOUBLE PRECISION , s91 DOUBLE PRECISION , s92 DOUBLE PRECISION , s93 DOUBLE PRECISION , s94 DOUBLE PRECISION ,  s95 DOUBLE PRECISION , s96 DOUBLE PRECISION , s97 DOUBLE PRECISION , s98 DOUBLE PRECISION , s99 DOUBLE PRECISION )tags(id_station char(64) not null) primary tags(id_station);

WITH data1 AS (
SELECT "time", s65 AS s_1
FROM tsmtest.d1
WHERE (
("time" > (TIMESTAMP '2019-04-07T07:08:23' - interval '1 day'))
AND ("time" < TIMESTAMP '2019-04-07T07:08:23')
)
AND (id_station = '<stitsmtest.d1>')
),
data2 AS (
SELECT "time", s44 AS s_2
FROM tsmtest.d1
WHERE (
("time" > (TIMESTAMP '2019-04-07T07:08:23' - interval '1 day'))
AND ("time" < TIMESTAMP '2019-04-07T07:08:23')
)
AND (id_station IN ('st9'))
)
SELECT data1.time, s_1, s_2, (s_1 + s_2) / 2 AS avg
FROM data1, data2
WHERE data1.time = data2.time;

-- ZDP-33510
CREATE TABLE t_cnc (
	k_timestamp TIMESTAMPTZ NOT NULL,
	cnc_sn VARCHAR(200) NULL,
	cnc_sw_mver VARCHAR(30) NULL,
	cnc_sw_sver VARCHAR(30) NULL,
	cnc_tol_mem VARCHAR(10) NULL,
	cnc_use_mem VARCHAR(10) NULL,
	cnc_unuse_mem VARCHAR(10) NULL,
	cnc_status VARCHAR(2) NULL,
	path_quantity VARCHAR(30) NULL,
	axis_quantity VARCHAR(30) NULL,
	axis_path VARCHAR(100) NULL,
	axis_type VARCHAR(100) NULL,
	axis_unit VARCHAR(100) NULL,
	axis_num VARCHAR(100) NULL,
	axis_name VARCHAR(100) NULL,
	sp_name VARCHAR(100) NULL,
	abs_pos VARCHAR(200) NULL,
	rel_pos VARCHAR(200) NULL,
	mach_pos VARCHAR(200) NULL,
	dist_pos VARCHAR(200) NULL,
	sp_override FLOAT8 NULL,
	sp_set_speed VARCHAR(30) NULL,
	sp_act_speed VARCHAR(30) NULL,
	sp_load VARCHAR(300) NULL,
	feed_set_speed VARCHAR(30) NULL,
	feed_act_speed VARCHAR(30) NULL,
	feed_override VARCHAR(30) NULL,
	servo_load VARCHAR(300) NULL,
	parts_count VARCHAR(30) NULL,
	cnc_cycletime VARCHAR(30) NULL,
	cnc_alivetime VARCHAR(30) NULL,
	cnc_cuttime VARCHAR(30) NULL,
	cnc_runtime VARCHAR(30) NULL,
	mprog_name VARCHAR(500) NULL,
	mprog_num VARCHAR(30) NULL,
	sprog_name VARCHAR(500) NULL,
	sprog_num VARCHAR(30) NULL,
	prog_seq_num VARCHAR(30) NULL,
	prog_seq_content VARCHAR(1000) NULL,
	alarm_count VARCHAR(10) NULL,
	alarm_type VARCHAR(100) NULL,
	alarm_code VARCHAR(100) NULL,
	alarm_content VARCHAR(2000) NULL,
	alarm_time VARCHAR(200) NULL,
	cur_tool_num VARCHAR(20) NULL,
	cur_tool_len_num VARCHAR(20) NULL,
	cur_tool_len VARCHAR(20) NULL,
	cur_tool_len_val VARCHAR(20) NULL,
	cur_tool_x_len VARCHAR(20) NULL,
	cur_tool_x_len_val VARCHAR(20) NULL,
	cur_tool_y_len VARCHAR(20) NULL,
	cur_tool_y_len_val VARCHAR(20) NULL,
	cur_tool_z_len VARCHAR(20) NULL,
	cur_tool_z_len_val VARCHAR(20) NULL,
	cur_tool_rad_num VARCHAR(20) NULL,
	cur_tool_rad VARCHAR(20) NULL,
	cur_tool_rad_val VARCHAR(20) NULL,
	device_state INT4 NULL,
	value1 VARCHAR(10) NULL,
	value2 VARCHAR(10) NULL,
	value3 VARCHAR(10) NULL,
	value4 VARCHAR(10) NULL,
	value5 VARCHAR(10) NULL
) TAGS (
	machine_code VARCHAR(64) NOT NULL,
	op_group VARCHAR(64) NOT NULL,
	brand VARCHAR(64) NOT NULL,
	number_of_molds INT4 ) PRIMARY TAGS(machine_code, op_group);

CREATE TABLE t_electmeter (
	k_timestamp TIMESTAMPTZ NOT NULL,
	elect_name VARCHAR(63) NOT NULL,
	vol_a FLOAT8 NOT NULL,
	cur_a FLOAT8 NOT NULL,
	powerf_a FLOAT8 NULL,
	allenergy_a INT4 NOT NULL,
	pallenergy_a INT4 NOT NULL,
	rallenergy_a INT4 NOT NULL,
	allrenergy1_a INT4 NOT NULL,
	allrenergy2_a INT4 NOT NULL,
	powera_a FLOAT8 NOT NULL,
	powerr_a FLOAT8 NOT NULL,
	powerl_a FLOAT8 NOT NULL,
	vol_b FLOAT8 NOT NULL,
	cur_b FLOAT8 NOT NULL,
	powerf_b FLOAT8 NOT NULL,
	allenergy_b INT4 NOT NULL,
	pallenergy_b INT4 NOT NULL,
	rallenergy_b INT4 NOT NULL,
	allrenergy1_b INT4 NOT NULL,
	allrenergy2_b INT4 NOT NULL,
	powera_b FLOAT8 NOT NULL,
	powerr_b FLOAT8 NOT NULL,
	powerl_b FLOAT8 NOT NULL,
	vol_c FLOAT8 NOT NULL,
	cur_c FLOAT8 NOT NULL,
	powerf_c FLOAT8 NOT NULL,
	allenergy_c INT4 NOT NULL,
	pallenergy_c INT4 NOT NULL,
	rallenergy_c INT4 NOT NULL,
	allrenergy1_c INT4 NOT NULL,
	allrenergy2_c INT4 NOT NULL,
	powera_c FLOAT8 NOT NULL,
	powerr_c FLOAT8 NOT NULL,
	powerl_c FLOAT8 NOT NULL,
	vol_ab FLOAT8 NULL,
	vol_bc FLOAT8 NULL,
	vol_ca FLOAT8 NULL,
	infre FLOAT8 NOT NULL,
	powerf FLOAT8 NOT NULL,
	allpower FLOAT8 NOT NULL,
	pallpower FLOAT8 NOT NULL,
	rallpower FLOAT8 NOT NULL,
	powerr FLOAT8 NOT NULL,
	powerl FLOAT8 NOT NULL,
	allrenergy1 FLOAT8 NOT NULL,
	allrenergy2 FLOAT8 NOT NULL
) TAGS (
	machine_code VARCHAR(64) NOT NULL,
	op_group VARCHAR(64) NOT NULL,
	location VARCHAR(64) NOT NULL,
	cnc_number INT4 ) PRIMARY TAGS(machine_code);
	
	
select subq_0.c3 as c0 from (select ref_5.cnc_number as c3 from public.t_electmeter as ref_5 where case when ref_5.location is NULL then cast(null as "timestamp") else cast(null as "timestamp") end > pg_catalog.now()) as subq_0 limit 10;

WITH 
jennifer_0 AS (select  
    cast(coalesce(subq_0.c10,
      subq_0.c14) as "varchar") as c0, 
    subq_0.c4 as c1, 
    subq_0.c12 as c2
  from 
    (select  
          ref_0.cnc_sn as c0, 
          ref_0.cnc_use_mem as c1, 
          ref_0.cnc_sn as c2, 
          ref_0.prog_seq_num as c3, 
          ref_0.mprog_num as c4, 
          ref_0.cnc_unuse_mem as c5, 
          ref_0.parts_count as c6, 
          ref_0.op_group as c7, 
          ref_0.axis_path as c8, 
          ref_0.k_timestamp as c9, 
          ref_0.alarm_content as c10, 
          33 as c11, 
          8 as c12, 
          ref_0.cnc_status as c13, 
          ref_0.sp_name as c14, 
          ref_0.device_state as c15, 
          2 as c16, 
          ref_0.cur_tool_z_len_val as c17, 
          ref_0.rel_pos as c18
        from 
          public.t_cnc as ref_0
        where cast(null as "timetz") IS DISTINCT FROM case when true then cast(null as "timetz") else cast(null as "timetz") end
            ) as subq_0
  where ((subq_0.c9 = pg_catalog.transaction_timestamp()) 
      and (cast(nullif(pg_catalog.kwdb_internal.completed_migrations(),
          pg_catalog.kwdb_internal.completed_migrations()) as _text) > cast(null as _text))) 
    and (cast(coalesce(pg_catalog.uuid_v4(),
        cast(null as bytea)) as bytea) IS NOT DISTINCT FROM case when EXISTS (
          select  
              subq_1.c0 as c0, 
              subq_1.c0 as c1, 
              subq_0.c10 as c2, 
              97 as c3, 
              subq_0.c0 as c4, 
              subq_0.c17 as c5, 
              subq_1.c5 as c6
            from 
              (select  
                    ref_1.rallenergy_b as c0, 
                    ref_1.allrenergy2 as c1, 
                    subq_0.c2 as c2, 
                    ref_1.powerl_b as c3, 
                    ref_1.pallenergy_b as c4, 
                    subq_0.c3 as c5, 
                    subq_0.c8 as c6, 
                    ref_1.machine_code as c7
                  from 
                    public.t_electmeter as ref_1
                  where false
                  limit 177) as subq_1
            where (cast(null as "numeric") > cast(null as int8)) 
              and (cast(null as oid) IS DISTINCT FROM cast(null as oid))) then case when false then case when cast(null as date) = cast(null as date) then cast(null as bytea) else cast(null as bytea) end
             else case when cast(null as date) = cast(null as date) then cast(null as bytea) else cast(null as bytea) end
             end
           else case when false then case when cast(null as date) = cast(null as date) then cast(null as bytea) else cast(null as bytea) end
             else case when cast(null as date) = cast(null as date) then cast(null as bytea) else cast(null as bytea) end
             end
           end
        ))
select  
    subq_3.c1 as c0, 
    subq_3.c2 as c1, 
    subq_3.c1 as c2, 
    subq_3.c3 as c3, 
    subq_3.c3 as c4
  from 
    (select  
          subq_2.c0 as c0, 
          subq_2.c0 as c1, 
          subq_2.c0 as c2, 
          subq_2.c0 as c3
        from 
          (select  
                ref_2.axis_unit as c0
              from 
                public.t_cnc as ref_2
              where ref_2.machine_code is NULL) as subq_2
        where true
        limit 101) as subq_3
  where case when cast(coalesce(case when cast(null as text) ILIKE cast(null as text) then cast(null as text) else cast(null as text) end
            ,
          cast(null as text)) as text) SIMILAR TO cast(null as text) then case when false then cast(nullif(cast(null as "timestamptz"),
          case when cast(null as float8) IS NOT DISTINCT FROM cast(null as float8) then cast(null as "timestamptz") else cast(null as "timestamptz") end
            ) as "timestamptz") else cast(nullif(cast(null as "timestamptz"),
          case when cast(null as float8) IS NOT DISTINCT FROM cast(null as float8) then cast(null as "timestamptz") else cast(null as "timestamptz") end
            ) as "timestamptz") end
         else case when false then cast(nullif(cast(null as "timestamptz"),
          case when cast(null as float8) IS NOT DISTINCT FROM cast(null as float8) then cast(null as "timestamptz") else cast(null as "timestamptz") end
            ) as "timestamptz") else cast(nullif(cast(null as "timestamptz"),
          case when cast(null as float8) IS NOT DISTINCT FROM cast(null as float8) then cast(null as "timestamptz") else cast(null as "timestamptz") end
            ) as "timestamptz") end
         end
       IS NOT DISTINCT FROM pg_catalog.timezone(
      cast(case when EXISTS (
          select  
              subq_4.c0 as c0, 
              subq_3.c0 as c1, 
              subq_3.c1 as c2
            from 
              (select  
                    subq_3.c3 as c0
                  from 
                    jennifer_0 as ref_3
                  where cast(null as _timestamp) != cast(null as _timestamp)
                  limit 75) as subq_4
            where (cast(null as _time) IS DISTINCT FROM cast(null as _time)) 
              or (false)
            limit 76) then cast(nullif(cast(null as "timestamptz"),
          pg_catalog.localtimestamp()) as "timestamptz") else cast(nullif(cast(null as "timestamptz"),
          pg_catalog.localtimestamp()) as "timestamptz") end
         as "timestamptz"),
      cast(case when cast(null as _inet) > cast(coalesce(cast(null as _inet),
            cast(null as _inet)) as _inet) then pg_catalog.substr(
          cast(cast(coalesce(cast(null as text),
            cast(null as text)) as text) as text),
          cast(pg_catalog.sign(
            cast(cast(null as int8) as int8)) as int8)) else pg_catalog.substr(
          cast(cast(coalesce(cast(null as text),
            cast(null as text)) as text) as text),
          cast(pg_catalog.sign(
            cast(cast(null as int8) as int8)) as int8)) end
         as text))
;


use defaultdb;
drop database tsmtest cascade;

-- ZDP-42134
create ts database last_db;
use last_db;
create table t(k_timestamp timestamp not null, x float) tags(a int not null) primary tags(a);
insert into t values('2023-07-29 03:11:59.688', 1.0, 1);

select cast(x as decimal(18, 6)) from t;

use defaultdb;
drop database last_db cascade;

-- limit offset bug
create ts database test_limit_offset;
use test_limit_offset;
CREATE TABLE stbl_raw (
    ts TIMESTAMPTZ NOT NULL,
    data VARCHAR(16374) NULL,
    type VARCHAR(10) NULL,
    parse VARCHAR(10) NULL
) TAGS (
    device VARCHAR(64) NOT NULL,
	iot_hub_name NCHAR(64) NOT NULL ) PRIMARY TAGS(device, iot_hub_name);

EXPLAIN SELECT ts as timestamp,* FROM stbl_raw order by ts desc LIMIT 20 offset 980;
EXPLAIN SELECT ts as timestamp,* FROM stbl_raw order by ts desc LIMIT 20 offset 981;
EXPLAIN SELECT ts as timestamp,* FROM stbl_raw order by ts desc LIMIT 1000;
EXPLAIN SELECT ts as timestamp,* FROM stbl_raw order by ts desc LIMIT 1001;

show max_push_limit_number;
set max_push_limit_number = 1100;

EXPLAIN SELECT ts as timestamp,* FROM stbl_raw order by ts desc LIMIT 1100;
EXPLAIN SELECT ts as timestamp,* FROM stbl_raw order by ts desc LIMIT 1101;
EXPLAIN SELECT ts as timestamp,* FROM stbl_raw order by ts desc LIMIT 20 offset 1080;
EXPLAIN SELECT ts as timestamp,* FROM stbl_raw order by ts desc LIMIT 20 offset 1081;

reset max_push_limit_number;

use defaultdb;
drop database test_limit_offset cascade;

-- clean data
use defaultdb;
drop database test_ts cascade;
drop database test cascade;
set cluster setting sql.all_push_down.enabled=default;
