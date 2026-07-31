create database test;
use test;

create table test1(col1 smallint primary key, col2 int, col3 bigint, col4 float, col5 varchar);
insert into test1 values(1,100,1000,1000.101, '111'), (2,200,2000,2000.202, '222');

-- test concat
select col1 || 1 from test1;
select col1 || 'test_const' from test1;
select col5 || 1 from test1;

select col1 || col2 from test1;
select col1 || col3 from test1;
select col1 || col4 from test1;
select col1 || col5 from test1;
select col2 || col3 from test1;
select col2 || col4 from test1;
select col2 || col5 from test1;
select col3 || col4 from test1;
select col3 || col5 from test1;
select col4 || col5 from test1;

create table test2(col1 bigint, col2 float, col3 varchar);
insert into test2 select col4, col4, col5 from test1;
insert into test2 select col1, 'test', col5 from test1;

insert into test2 select col5, col3, col4 from test1;
insert into test2 select col5, col5, col4 from test1;

use defaultdb;
drop database test cascade;
