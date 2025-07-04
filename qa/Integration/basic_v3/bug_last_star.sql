drop database if exists last_star cascade;
create ts database last_star;
use last_star;
create table t1 (ts timestamp not null, e1 int, e2 varchar, e3 bool) tags (tag1 int not null) primary tags (tag1);
insert into t1 values ('2024-04-01 00:00:00.000', 1, 'abc', true, 10);
select last(*) from t1;
select last(*) from t1 where e1 is not null;
explain select last(*) from t1 where e1 is not null;
alter table t1 drop column e2;

alter table t1 add column e2 varchar;
select last(*) from t1;
select last(*) from t1 where e1 is not null;

select last(t1.*) from t1;
select last(t1.*) from t1 where e1 is not null;
explain select last(t1.*) from t1 where e1 is not null;

drop database if exists last_star cascade;
