set cluster setting sql.stats.tag_automatic_collection.enabled = false;
create ts database test;
use test;
Create table t1(k_timestamp timestamp not null,c1 int2,c2 int4,c3 float4,c4 float8,c5 char,c6 varchar(10),c7 int8,c8 nchar(10),c9 nvarchar(10),c10 varbytes,c11 timestamptz,c12 bool) tags (size int not null) primary tags (size) ;
set timezone = 8;
Insert into t1 values ('2024-1-1 1:00:00',1,100,0.1,0.1,'a','aa',1000,'e','f','g','2024-1-1 1:00:00',true,2);
Insert into t1 values ('2024-1-1 1:01:00',2,200,0.2,0.1,'a','aaa',1000,'e','f','g','2024-1-1 1:00:00',true,2);
Insert into t1 values ('2024-1-1 2:00:00',3,200,0.3,0.1,'a','aaa',2000,'ee','ff','gg','2024-1-1 1:00:01',true,6);
Insert into t1 values ('2024-1-1 3:00:00',4,500,0.4,0.2,'b','bb',2000,'ee','ff','gg','2024-1-1 1:00:01',false,4);
Insert into t1 values ('2024-1-1 4:00:00',5,500,0.5,0.2,'b','bb',3000,'eee','ff','gg','2024-1-1 1:00:02',false,5);
Insert into t1 values ('2024-1-1 5:00:00',6,6,0.6,0.2,'b','bbb',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,6);
Insert into t1 values ('2024-1-1 6:00:00',7,7,0.7,0.3,'c','cc',3000,'eee','fff','ggg','2024-1-1 1:00:02',true,7);
Insert into t1 values ('2024-1-1 7:00:00',8,8,0.8,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',false,8);
Insert into t1 values ('2024-1-1 8:00:00',9,9,0.9,0.3,'c','cc',5000,'eeee','fff','ggg','2024-1-1 1:00:03',true,9);
Insert into t1 values ('2024-1-1 9:00:00',10,10,1.0,0.3,'c','ccc',6000,'eeee','fffff','ggg','2024-1-1 1:00:05',false,10);
Insert into t1 values ('2024-1-1 10:00:00',null,null,null,null,null,null,6000,'eeee','fffff','ggggg','2024-1-1 1:00:06',true,10);
Insert into t1 values ('2024-1-1 11:00:00',null,null,null,null,null,null,10000,'eeee','fffff','ggggg','2024-1-1 1:00:06',false,10);
select * from t1 order by k_timestamp;

explain select * from test.t1 where _osn > 17600000000 and _osn < 17700000000;
explain select * from test.t1 where _osn > 17600000000 and _osn < 17700000000 and k_timestamp > '2000-1-1';
explain select * from test.t1 where _osn > 17600000000 and _osn < 17700000000 and c1 > 10 and size = 1;

explain select _osn,_op,_event,k_timestamp,c1,c2,c3,c4,c5,c6,c6,c8,c9,c10,c11,c12,size from test.t1 where size >= 1 and _osn >= 176000000000 and _osn < 177000000000 ;
explain select _osn,_op,_event,k_timestamp,c1,c2,c3,c4,c5,c6,c6,c8,c9,c10,c11,c12,size from test.t1 where size >= 1 and _osn >= 176000000000 and _osn < 177000000000 order by _osn;
explain select * from test.t1 where size >= 1 and _osn >= 176000000000 and _osn < 177000000000 order by _osn;
explain select count(*) from test.t1 where _osn >= 17600000000;

select max(_osn),_osn from test.t1;
select max(_op),k_timestamp,c1 from test.t1;
select min(_event),k_timestamp,c1 from test.t1;

explain select count(_osn) from test.t1;
select count(_osn) from test.t1;
explain select count(_osn),max(_osn),min(_osn),first(_op),last(_event) from test.t1;
select count(_osn),first(_op),last(_event) from test.t1;

create table t11(ts timestamp not null ,a int) tags( b int2 not null,t int) primary tags(b);
insert into t11 values('2024-07-03 00:00:00', 1, 1,0);
insert into t11 values('2024-07-03 00:00:00', 1, 2,0);
alter table t11 add column c int;
delete from t11 where b = 1;
insert into t11 values('2024-07-03 00:00:00', 1, 1, 1,0);
alter table t11 drop column a;
delete from t11 where b = 1;
update t11 set t=1 where b=2;
select _op,_event,* from t11  order by _osn;
select _op,_event,* from t11 where _op = '\x01' order by ts  limit 1;
select _op from t11 where _osn>0 order by _osn;

explain select * from test.t1 where size >= 1 and _osn >= 176000000000 and _osn < 177000000000 order by _osn;
explain select * from test.t1 where size >= 1 and _osn > 176000000000 and _osn <= 177000000000 order by _osn;

Create table t2(k_timestamp timestamp not null,_osn int2,c2 int4,c3 float4,c4 float8,c5 char,c6 varchar(10),c7 int8,c8 nchar(10),c9 nvarchar(10),c10 varbytes,c11 timestamptz,c12 bool) tags (size int not null) primary tags (size) ;
Create table t3(k_timestamp timestamp not null,c1 int2,c2 int4,c3 float4,c4 float8,c5 char,c6 varchar(10),c7 int8,c8 nchar(10),c9 nvarchar(10),c10 varbytes,c11 timestamptz,c12 bool) tags (_op int not null) primary tags (_op) ;
Create table t5(_event timestamp not null,c1 int2,c2 int4,c3 float4,c4 float8,c5 char,c6 varchar(10),c7 int8,c8 nchar(10),c9 nvarchar(10),c10 varbytes,c11 timestamptz,c12 bool) tags (size int not null) primary tags (size) ;

set timezone = 0;
use default;
drop database test cascade;
set cluster setting sql.stats.tag_automatic_collection.enabled = true;