drop database if exists test_db cascade;
create ts database test_db;

create ts database if not exists test_db;

create ts database test_db;

create table if not exists test_db.t1(ts timestamp not null, a int) tags(b int not null) primary tags(b);

create table test_db.t1(ts timestamp not null, a int) tags(b int not null) primary tags(b);

create table if not exists test_db.t1(ts timestamp not null, a int) tags(b int not null) primary tags(b);

drop table test_db.t1;

drop database test_db;

