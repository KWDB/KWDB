set cluster setting ts.parallel_degree=8;
drop database if exists tsdb cascade;
create ts database tsdb;
use tsdb;
create table t1(ts timestamp not null, a int) tags(b int not null) primary tags(b);
insert into t1 values('2025-08-05 08:45:26.757',1,1);
insert into t1 values('2025-08-05 08:45:26.766',2,2);
insert into t1 values('2025-08-05 08:45:26.767',3,3);
insert into t1 values('2025-08-05 08:45:27.767',4,4);
insert into t1 values('2025-08-05 08:45:28.767',5,5);
insert into t1 values('2025-08-05 08:45:29.767',6,6);
insert into t1 values('2025-08-05 08:45:30.767',60,60);
insert into t1 values('2025-08-05 08:45:31.767',600,600);
insert into t1 values('2025-08-05 08:45:32.767',6000,6000);
insert into t1 values('2025-08-05 08:45:33.767',60000,60000);
insert into t1 values('2025-08-05 08:45:34.767',100,100);
insert into t1 values('2025-08-05 08:45:35.767',1000,1000);
insert into t1 values('2025-08-05 08:45:36.767',10000,10000);
insert into t1 values('2025-08-05 08:45:37.767',100000,100000);
insert into t1 values('2025-08-05 08:45:38.767',6000,90549856);
insert into t1 values('2025-08-05 08:45:39.767',60000,89498);
insert into t1 values('2025-08-05 08:45:40.767',100,9384732);
insert into t1 values('2025-08-05 08:45:41.767',1000,234566);
insert into t1 values('2025-08-05 08:45:42.767',10000,345667643);
insert into t1 values('2025-08-05 08:45:43.767',100000,4564353);

SELECT * FROM tsdb.t1 order by ts;

select count(*) from tsdb.t1;

select last(a) from tsdb.t1 group by a order by a;

select distanct a from tsdb.t1 order by a;

use defaultdb;
drop database tsdb cascade;