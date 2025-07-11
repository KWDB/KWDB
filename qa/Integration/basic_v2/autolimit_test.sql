set cluster setting sql.query_cache.enabled=true;

drop database if EXISTS test;
drop database if EXISTS test_ts;

create database test;
use test;

create table test1(col1 smallint primary key, col2 int, col3 bigint, col4 float, col5 bool, col6 varchar);
insert into test1 values(1000,1000000,100000000000000000,100000000000000000.101,true, 'test_c1'), (2000,2000000,200000000000000000,200000000000000000.202,true, 'test_c2');

-- create
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

insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+1, attr2, attr3, attr4, attr5, attr6 from ts_table;
insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+2, attr2, attr3, attr4, attr5, attr6 from ts_table;
insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+3, attr2, attr3, attr4, attr5, attr6 from ts_table;
insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+4, attr2, attr3, attr4, attr5, attr6 from ts_table;
insert into ts_table(time, e1, e2, e3, e4, e5, e6, attr1, attr2, attr3, attr4, attr5, attr6) select time, e1, e2, e3, e4, e5, e6, attr1+5, attr2, attr3, attr4, attr5, attr6 from ts_table;

select count(1) from ts_table;

set cluster setting sql.auto_limit.quantity=10;

explain select * from test_ts.ts_table;
explain with with_table as (select * from test_ts.ts_table) select * from with_table where e1=1000;

explain select e1 from test_ts.ts_table union select col1 from test.test1 order by e1;
explain (select e1 from test_ts.ts_table limit 1) union select col1 from test.test1 order by e1;
explain select e1 from test_ts.ts_table union (select col1 from test.test1 limit 1) order by e1;
explain select * from test_ts.ts_table where e1 in (select col1 from test.test1 limit 1) order by e2;
explain select e1 = ( select col1 from test.test1 limit 1 ) from test_ts.ts_table order by e1;
explain select * from test_ts.ts_table as t1 where exists (select col1 from test.test1 as t2 where t1.e1=t2.col1 group by col1 having col1 < 2000 limit 1) order by e1;
explain select e2 from test_ts.ts_table as t1 join test.test1 as t2 on t1.e1=t2.col1 order by e2;

prepare p1 as select attr1 from test_ts.ts_table group by attr1 order by attr1;
execute p1;
prepare p2 as select attr1 from test_ts.ts_table group by attr1 order by attr1 limit $1;
execute p2(11);

-- ZDP-41401
set cluster setting sql.query_cache.enabled=true;
set cluster setting sql.auto_limit.quantity=default;
select time from test_ts.ts_table order by time;
set cluster setting sql.auto_limit.quantity=10;
select time from test_ts.ts_table order by time;

-- ZDP-41661
set cluster setting sql.auto_limit.quantity=20;
select time from test_ts.ts_table order by time;

use defaultdb;
set cluster setting sql.query_cache.enabled=default;
set cluster setting sql.auto_limit.quantity=default;
drop database test_ts cascade;
drop database test cascade;
