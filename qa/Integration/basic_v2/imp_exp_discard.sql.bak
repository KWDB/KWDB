--test dedup import
create ts database test;
use test;
create table test.tb1(k_timestamp timestamptz not null,  usage_user INT8,
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
insert into test.tb1 values('2024-01-01 00:00:01+00:00',null,null,null,null,null,null,null,null,null,null,'host_0');
insert into test.tb1 values('2024-01-02 00:00:01+00:00',null,null,null,null,null,null,null,null,null,null,'host_0');
select * from test.tb1 order by k_timestamp, hostname;
export into csv "nodelocal://1/tbKeep/tb1/" from table test.tb1;
export into csv "nodelocal://1/tbReject/tb1/" from table test.tb1;
export into csv "nodelocal://1/tbDiscard/tb1/" from table test.tb1;
-- test keep
SET CLUSTER SETTING ts.dedup.rule = 'keep';
import into test.tb1 csv data ('nodelocal://1/tbKeep/tb1');
select * from test.tb1 order by k_timestamp, hostname;
drop table test.tb1;

-- test discard
SET CLUSTER SETTING ts.dedup.rule = 'discard';
create table test.tb1(k_timestamp timestamptz not null,  usage_user INT8,
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
insert into test.tb1 values('2024-01-01 00:00:01+00:00',null,null,null,null,null,null,null,null,null,null,'host_0');
import into test.tb1 csv data ('nodelocal://1/tbDiscard/tb1');
select * from test.tb1 order by k_timestamp, hostname;
drop table test.tb1;
-- test reject
SET CLUSTER SETTING ts.dedup.rule = 'reject';
create table test.tb1(k_timestamp timestamptz not null,  usage_user INT8,
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
insert into test.tb1 values('2024-01-01 00:00:01+00:00',null,null,null,null,null,null,null,null,null,null,'host_0');
import into test.tb1 csv data ('nodelocal://1/tbReject/tb1');
select * from test.tb1 order by k_timestamp, hostname;
drop table test.tb1;


use defaultdb;
drop database test cascade;

SET CLUSTER SETTING ts.dedup.rule = 'override';