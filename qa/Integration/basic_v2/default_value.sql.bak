create ts database default_val_db;
use default_val_db;
create table t(ts timestamptz not null, a int, b int not null default 1, c varchar default 'aaa') tags(ptag int not null, tag1 int default 1) primary tags(ptag);
create table t(ts timestamptz not null, a int, b int not null default 1, c varchar default 'aaa') tags(ptag int not null) primary tags(ptag);
show create t;

insert into t values (1681111110000,1,2,'bbb',1);
select * from t order by ptag;

insert into t(ts,a,ptag) values (1681111110001,2,2);
select * from t order by ptag;

insert into t(ts,a,ptag) values (1681111110002,3,3),(1681111110003,4,4);
select * from t order by ptag;

alter table t add column d timestamp default 1681111110000;
insert into t (ts,a,ptag) values (1681111110004,5,5);
select * from t order by ptag;

alter table t alter column d set default 1681111110001;
insert into t (ts,a,ptag) values (1681111110005,6,6);
select * from t order by ptag;

alter table t alter column d drop default;
insert into t (ts,a,ptag) values (1681111110006,7,7);
select * from t order by ptag;
show create t;

alter table t alter column b set default 'abc';
insert into t (ts,a,b,ptag) values (1681111110006,7,null,7);

create table t1(ts timestamp not null default current_timestamp(), a int) tags(ptag int not null) primary tags(ptag);
create table t1(ts timestamp not null default now(), a int default abs(-2)) tags(ptag int not null) primary tags(ptag);
create table t1(ts timestamp not null default now(), a int) tags(ptag int not null) primary tags(ptag);
show create t1;
alter table t1 alter column ts set default current_timestamp();
alter table t1 alter column ts drop default;
alter table t1 add column b timestamp default current_timestamp();
alter table t1 alter column a set default abs(-2);
show create t1;

drop table t;
drop table t1;
use defaultdb;
drop database default_val_db;