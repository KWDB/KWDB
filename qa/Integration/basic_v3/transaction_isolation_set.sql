show transaction isolation level;

begin;
show transaction isolation level;
set transaction_isolation = 'read committed';
show transaction_isolation;
show transaction isolation level;
set session transaction isolation level serializable;
show transaction isolation level;
set session transaction isolation level read committed;
show transaction isolation level;
set transaction_isolation = 'repeatable read';
show transaction_isolation;
show transaction isolation level;
set session transaction isolation level serializable;
show transaction isolation level;
set session transaction isolation level repeatable read;
show transaction isolation level;
commit;

show transaction isolation level;
show transaction_isolation;

begin transaction isolation level read committed;
show transaction isolation level;
set transaction_isolation = 'serializable';
show transaction isolation level;
commit;

show transaction_isolation;
show transaction isolation level;

begin transaction isolation level repeatable read;
show transaction isolation level;
set transaction_isolation = 'serializable';
show transaction isolation level;
commit;

show transaction_isolation;
show transaction isolation level;

show default_transaction_isolation;
set default_transaction_isolation = 'read committed';
show default_transaction_isolation;
set default_transaction_isolation = 'repeatable read';
show default_transaction_isolation;
show transaction isolation level;

begin;
show transaction isolation level;
set default_transaction_isolation = 'serializable';
show transaction isolation level;
set default_transaction_isolation = 'repeatable read';
show transaction isolation level;
set transaction_isolation = 'repeatable read';
show transaction isolation level;
set transaction_isolation = 'serializable';
show transaction isolation level;
commit;
set default_transaction_isolation = 'serializable';

show transaction isolation level;
show transaction_isolation;
show default_transaction_isolation;
show cluster setting sql.txn.cluster_transaction_isolation;
set cluster setting sql.txn.cluster_transaction_isolation = 'read committed';
show transaction_isolation;
set cluster setting sql.txn.cluster_transaction_isolation = 'repeatable read';
show transaction_isolation;
show default_transaction_isolation;
show cluster setting sql.txn.cluster_transaction_isolation;
set cluster setting sql.txn.cluster_transaction_isolation = 'serializable';

create table iso_t1(a int primary key, b int);
insert into iso_t1 values(1,100),(2,200),(3,300),(4,400),(5,500);

begin;
show transaction isolation level;
select * from iso_t1 order by a;
savepoint s1;
insert into iso_t1 values(6,600);
select * from iso_t1 order by a;
rollback to savepoint s1;
select * from iso_t1 order by a;

insert into iso_t1 values(6,600);
select * from iso_t1 order by a;
savepoint s2;
delete  from iso_t1 where a = 6;
select * from iso_t1 order by a;
rollback to savepoint s2;
select * from iso_t1 order by a;

update iso_t1 set b = 1200 where a = 1;
select * from iso_t1 order by a;
savepoint s3;
update iso_t1 set b = 2200 where a = 1;
select * from iso_t1 order by a;
rollback to savepoint s3;
select * from iso_t1 order by a;
savepoint s4;


rollback to savepoint s4;
select * from iso_t1 order by a;
rollback to savepoint s3;
select * from iso_t1 order by a;
rollback to savepoint s2;
select * from iso_t1 order by a;
rollback to savepoint s1;
select * from iso_t1 order by a;
commit;

select * from iso_t1 order by a;
drop table iso_t1;

create ts database iso_d1;
create table iso_d1.t1(ts timestamp not null,a int) tags(b int not null) primary tags(b);
insert into iso_d1.t1 values(0,0,0);

begin;
select * from iso_d1.t1 order by a;
insert into iso_d1.t1 values(1,1,1);
select * from iso_d1.t1 order by a;
commit;

begin;
create table iso_d1.t2(ts timestamp not null,a int) tags(b int not null) primary tags(b);
commit;

begin;
drop table iso_d1.t1;
commit;

begin;
alter table iso_d1.t1 rename tag b to c;
commit;

begin;
alter table iso_d1.t1 drop tag b;
commit;

begin;
select * from iso_d1.t1 order by a;
savepoint s1;
insert into iso_d1.t1 values(1,1,1);
select * from iso_d1.t1 order by a;
rollback to savepoint s1;
select * from iso_d1.t1 order by a;
commit;

drop database iso_d1 cascade;

--- bug-43943
create table iso_t2(a int);

begin;
show transaction isolation level;
set transaction isolation level read committed;
select * from iso_t2;
set transaction isolation level read committed;
set transaction isolation level serializable;
commit;

begin;
show transaction isolation level;
set transaction isolation level serializable;
select * from iso_t2;
set transaction isolation level serializable;
set transaction isolation level read committed;
commit;

drop table iso_t2;