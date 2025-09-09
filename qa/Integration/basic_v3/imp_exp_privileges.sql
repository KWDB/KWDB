create user u1;
create user u2;
create user u3;

--create ts database
create ts database test;
use test;
create table test.t1(k_timestamp timestamptz not null,  usage_user INT8,
                      usage_system INT8,
                      usage_idle INT8,
                      usage_nice INT8,
                      usage_iowait INT8 ,
                      usage_irq INT8 ,
                      usage_softirq INT8 ,
                      usage_steal INT8 ,
                      usage_guest INT8 ,
                      usage_guest_nice INT8
) TAGS (
     hostname VARCHAR(30) NOT NULL) PRIMARY TAGS(hostname);
insert into test.t1 values('2024-01-01 00:00:01+00:00',null,null,null,null,null,null,null,null,null,null,'host_0');
insert into test.t1 values('2024-01-02 00:00:01+00:00',null,null,null,null,null,null,null,null,null,null,'host_0');

create table test.t2(k_timestamp timestamptz not null,  usage_user INT8,
                     usage_system INT8,
                     usage_idle INT8,
                     usage_nice INT8,
                     usage_iowait INT8 ,
                     usage_irq INT8 ,
                     usage_softirq INT8 ,
                     usage_steal INT8 ,
                     usage_guest INT8 ,
                     usage_guest_nice INT8
) TAGS (
     hostname VARCHAR(30) NOT NULL) PRIMARY TAGS(hostname);
insert into test.t2 values('2024-01-01 00:00:01+00:00',null,null,null,null,null,null,null,null,null,null,'host_0');
insert into test.t2 values('2024-01-02 00:00:01+00:00',null,null,null,null,null,null,null,null,null,null,'host_0');

grant select on database test to u1;
grant delete on table test.public.t1 to u2;
grant delete on table test.public.t2 to u2;
grant insert on database test to u3;

--export database
export into csv "nodelocal://1/db" from database test with privileges;

--clean database
use defaultdb;
drop database test cascade;

--export database
import database csv data ("nodelocal://1//db") with privileges;

show grants on database test;
show grants on table test.public.t1;
show grants on table test.public.t2;

--clean database
use defaultdb;
drop database test cascade;


--create ts database
create ts database test;
use test;
create table test.t1(k_timestamp timestamptz not null,  usage_user INT8,
                     usage_system INT8,
                     usage_idle INT8,
                     usage_nice INT8,
                     usage_iowait INT8 ,
                     usage_irq INT8 ,
                     usage_softirq INT8 ,
                     usage_steal INT8 ,
                     usage_guest INT8 ,
                     usage_guest_nice INT8
) TAGS (
     hostname VARCHAR(30) NOT NULL) PRIMARY TAGS(hostname);
insert into test.t1 values('2024-01-01 00:00:01+00:00',null,null,null,null,null,null,null,null,null,null,'host_0');
insert into test.t1 values('2024-01-02 00:00:01+00:00',null,null,null,null,null,null,null,null,null,null,'host_0');
grant delete on table test.public.t1 to u2;
grant select on table test.public.t1 to u3;

export into csv "nodelocal://1/test" from table test.t1 with privileges;

drop table t1;
import into test.t1 csv data ("nodelocal://1/test") with privileges;
import table create using "nodelocal://1/test/meta.sql" csv data ("nodelocal://1/test") with privileges;
show grants on table test.public.t1;

drop database test cascade;

--clean user/role/clustersetting
drop user u1;
drop user u2;
drop user u3;