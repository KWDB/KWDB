drop database if EXISTS tsdb cascade;
create ts database tsdb;
use tsdb;
create table t1(ts timestamp not null,a int, b int) tags(tag1 int not null, tag2 int) primary tags(tag1);
insert into t1 values(1705028908000,11,22,33,44);
insert into t1 values(1705028909000,22,33,33,44);

prepare p1 as select * from t1 where a>$1;
execute p1(0);

prepare p2 as select * from t1 where tag1=$1;
execute p2(33);

prepare p3 as select max(a) from t1 where a>$1 group by b order by b;
execute p3(0);

prepare p4 as select max(a) from t1 where tag1=$1 and a>$2 group by b order by b;
execute p4(33,0);

prepare p5 as select max(a)+$1 from t1 where tag1=$2 and a>$3 group by b order by b;
execute p5(1,33,0);

drop database tsdb cascade;
