create ts database tag_index_d1;
use tag_index_d1;

create table t1 (ts timestamp not null, a int) tags(b int not null, c int, d int, g varchar, h varchar(20), i varbytes, j varbytes(20)) primary tags(b);
create index i1 on t1(b);
create index i2 on t1(b, c);
create index i3 on t1(c);
create index i4 on t1(c, d);
create index i5 on t1(d);
create index i6 on t1(ts);
create index i7 on t1(a);
create index i8 on t1(b);
create index i11 on t1(g);
create index i12 on t1(h);
create index i13 on t1(i);
create index i14 on t1(j);

comment on index t1@i3 is 't1.i3.index';

show index from t1;
show index from t1 with comment;

alter table t1 add tag e int;
create index i9 on t1(e);
create index i10 on t1(c, d, e);

show index from t1;
show index from t1 with comment;

alter table t1 drop tag e;
alter table t1 drop tag d;
alter table t1 drop tag c;


alter table t1 alter tag e set data type int8;

alter table t1 rename tag c to f;

drop index t1@i3,t1@i4,t1@i5,t1@i9,t1@i10;

drop index t1@i3;
drop index t1@i4;
drop index t1@i5;
drop index t1@i9;
drop index t1@i10;

alter table t1 alter tag e set data type int8;
alter table t1 drop tag e;
alter table t1 drop tag d;

create index i1 on t1(f);
drop index t1@i1;

drop table t1;
drop table t1 cascade;

drop database tag_index_d1 cascade;

