--test_case001
create ts database ts_db;
use ts_db;
create table ts_db.t1(ts timestamp not null,a int, b int) tags(tag1 int not null,
tag2 int) primary tags(tag1);
insert into ts_db.t1 values('2022-01-01 00:00:01+00:00',11,11,33,44);
insert into ts_db.t1 values('2022-01-02 00:00:01+00:00',22,22,33,44);
insert into ts_db.t1 values('2022-01-03 00:00:01+00:00',33,33,33,44);
insert into ts_db.t1 values('2022-01-04 00:00:01+00:00',44,44,33,44);
insert into ts_db.t1 values('2022-01-05 00:00:01+00:00',55,55,44,44);
insert into ts_db.t1 values('2022-01-06 00:00:01+00:00',66,44,44,44);
insert into ts_db.t1 values('2022-01-07 00:00:01+00:00',77,44,55,44);
insert into ts_db.t1 values('2022-01-08 00:00:01+00:00',88,22,66,66);
insert into ts_db.t1 values('2022-01-09 00:00:01+00:00',99,33,66,77);

create table ts_db.t2(ts timestamp not null,a int, b int) tags(tag1 int not null,
tag2 int) primary tags(tag1);
insert into ts_db.t2 values('2022-02-01 00:00:01+00:00',21,12,13,55);
insert into ts_db.t2 values('2022-02-02 00:00:01+00:00',20,22,23,55);
insert into ts_db.t2 values('2022-02-03 00:00:01+00:00',23,33,33,55);
insert into ts_db.t2 values('2022-02-04 00:00:01+00:00',24,44,33,55);
insert into ts_db.t2 values('2022-02-05 00:00:01+00:00',25,55,66,55);
insert into ts_db.t2 values('2022-02-06 00:00:01+00:00',26,33,66,55);
insert into ts_db.t2 values('2022-02-07 00:00:01+00:00',27,33,55,55);
insert into ts_db.t2 values('2022-02-08 00:00:01+00:00',28,22,66,66);
insert into ts_db.t2 values('2022-02-09 00:00:01+00:00',29,33,66,77);

EXPORT INTO CSV "nodelocal://1/tb1" FROM select ts,a,tag1 from t1;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb1");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb2" FROM select ts,a,tag1 from ts_db.t1 where a > 11;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb2");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb3" FROM select ts,a,tag1 from t1 order by b;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb3");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb21" FROM select ts,a,b from t1 order by b,tag1;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb21");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb20" FROM select ts, a, tag1 from t1 order by a;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb20");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb4" FROM select ts,sum(a),sum(tag1) from t1 group by ts;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb4");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb5" FROM select ts,sum(a),sum(tag1) from t1 group by ts having ts < '2022-01-06 00:00:01+00:00';
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb5");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb6" FROM select ts,a,tag1 from ts_db.t1 order by a offset 5;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb6");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb7" FROM select ts,a,tag1 from ts_db.t1 order by a limit 5;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb7");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb8" FROM select if (a=11, b=11, tag1=33) from t1;

EXPORT INTO CSV "nodelocal://1/tb15" FROM SELECT ts,a,tag1 FROM t1 WHERE tag1 = (SELECT MAX(b) FROM t2 WHERE t2.b = t1.b);
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb15");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb17" FROM select sum(a) = (select a from t2 limit 1) from t1;

EXPORT INTO CSV "nodelocal://1/tb18" FROM select sum(a) = (select a from t2 where t2.a=tag1) from t1;

--test_case002
--testmode 1n
EXPORT INTO CSV "nodelocal://1/tb9" FROM select t1.ts, t1.a, t1.tag1, t1.tag2, t2.a from t1 left join t2 on t1.a = t2.a;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null, tag2 int, tag3 int) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb9");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb10" FROM select t2.ts, t2.a, t2.tag1, t1.tag2, t2.a from t1 right join t2 on t1.a = t2.a;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int, tag2 int, tag3 int not null) primary tags(tag3);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb10");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb11" FROM select t2.ts, t2.tag2, t2.a from t2 full join t1 on t1.a = t2.tag2;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb11");
select * from tb2 order by tag1;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb12" FROM select ts,a,tag1 from t1 union select ts,a,tag1 from t2;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb12");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb13" FROM select ts,a,tag1 from t1 intersect select ts,b,tag1 from t1;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb13");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb14" FROM select ts,a,tag1 from t1 except select ts,b,tag1 from t2;
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb14");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb16" FROM select ts,a,tag1 from t1 where a > (select avg(a) from t1);
create table ts_db.tb2(ts timestamp not null, a int) tags(tag1 int not null) primary tags(tag1);
IMPORT INTO tb2 CSV DATA ("nodelocal://1/tb16");
select * from tb2 order by a;
drop table tb2;

EXPORT INTO CSV "nodelocal://1/tb19" FROM select avg(a) from (select tag1 as a from t1);
