drop database if exists stream_db_fvt cascade;
create ts database stream_db_fvt;

drop database if exists stream_db_fvt_out cascade;
create ts database stream_db_fvt_out;

drop database if exists stream_db_fvt_out1 cascade;
create database stream_db_fvt_out1;

drop database if exists stream_db_fvt1 cascade;
create ts database stream_db_fvt1;

drop database if exists stream_db_fvt2 cascade;
create ts database stream_db_fvt2;
       
--- create stream using non-existent target relation
create stream test_stream1 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS select * from stream_db_fvt.cpu where usage_user > 50;

create table stream_db_fvt_out.cpu_stream_out
(
    w_begin              timestamptz not null,
    w_end                timestamptz not null,
    usage_user_avg       bigint      not null,
    usage_system_avg     bigint      not null,
    count                bigint      not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_stream_out2
(
    w_begin              timestamptz not null,
    w_end                timestamptz not null,
    usage_user_avg       bigint      not null,
    usage_system_avg     bigint      not null,
    count                bigint      not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

--- create stream using non-existent source relation
create stream test_stream1 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS select * from stream_db_fvt.cpu where usage_user > 50;

create table stream_db_fvt.cpu
(
    k_timestamp      timestamp not null,
    usage_user       bigint    not null,
    usage_system     bigint    not null,
    usage_idle       bigint    not null,
    usage_nice       bigint    not null,
    usage_iowait     bigint    not null,
    usage_irq        bigint    not null,
    usage_softirq    bool      not null,
    usage_softirq_f  float,
    usage_steal      bigint,
    usage_guest      bigint,
    usage_guest_nice bigint
) attributes (
    hostname char(30) not null,
    region char(30),
    datacenter char(30),
    rack char(30),
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname);


--- create stream using a wrong query
create stream test_stream1 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS select * from stream_db_fvt.cpu where usage_user > 50;

--- create stream
create stream test_stream1 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;
create stream test_stream1 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;
create stream test_stream1 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;

--- create stream
create stream test_stream2 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;

select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];

select name,target_table_name,options,query,status,create_by,error_message from [show streams];

--- create stream without the target database
create stream test_stream3 into cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;
use stream_db_fvt_out;
create stream test_stream3 into cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream3];

--- create stream without the source database
create stream test_stream4 into cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;
use stream_db_fvt;
create stream test_stream4 into cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;
create stream test_stream4 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream4];

select name,target_table_name,options,query,status,create_by,error_message from [show streams];

-- create stream with duplicated name
create stream test_stream2 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;

create stream if not exists test_stream2 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;

-- create stream with alias name
create stream test_stream5 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp) as w_begin, last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;
create stream test_stream5 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT hostname as hostname, max(usage_user) as usage_user_avg, first(k_timestamp) as w_begin, max(usage_system) as usage_system_avg, last(k_timestamp) as w_end, count(*) as count FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;
create stream test_stream5 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp) as w_begin, last_row(k_timestamp) as w_end, hostname as hostname, count(*) as count, max(usage_system) as usage_system_avg, max(usage_user) as usage_user_avg FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;

-- create stream using the same table as source and target table
create stream test_stream0 into stream_db_fvt.cpu with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;

-- missing first
create stream test_stream0 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT last_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;

-- missing last
create stream test_stream0 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), first_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;

-- error filter
create stream test_stream6 into stream_db_fvt_out.cpu_stream_out2 with options (enable='off') AS SELECT * from stream_db_fvt_out.cpu_stream_out where abs(count)=3 or max(count)>3;
create stream test_stream6 into stream_db_fvt_out.cpu_stream_out2 with options (enable='off') AS SELECT * from stream_db_fvt_out.cpu_stream_out where count/0>2;
create stream test_stream6 into stream_db_fvt_out.cpu_stream_out2 with options (enable='off') AS SELECT * from stream_db_fvt_out.cpu_stream_out a where random()>2 && (SELECT count(*) from stream_db_fvt_out.cpu_stream_out b where a.w_begin=b.w_begin) > 0;
-- error select column
create stream test_stream7 into stream_db_fvt_out.cpu_stream_out2 with options (enable='off') AS SELECT w_begin,w_end,usage_user_avg,usage_system_avg,max(count),hostname from stream_db_fvt_out.cpu_stream_out where count+usage_user_avg>3;
create stream test_stream7 into stream_db_fvt_out.cpu_stream_out2 with options (enable='off') AS SELECT w_begin,w_end,usage_user_avg,usage_system_avg,count/0,hostname from stream_db_fvt_out.cpu_stream_out;
create stream test_stream7 into stream_db_fvt_out.cpu_stream_out2 with options (enable='off') AS SELECT w_begin,w_end,abs(usage_user_avg)+usage_system_avg,usage_system_avg,random(),hostname from stream_db_fvt_out.cpu_stream_out;

-- error sub query
create stream test_stream8 into stream_db_fvt_out.cpu_stream_out2 with options (enable='off') AS SELECT * from stream_db_fvt_out.cpu_stream_out where 2>1 order by w_begin;
create stream test_stream8 into stream_db_fvt_out.cpu_stream_out2 with options (enable='off') AS SELECT * from (select * from stream_db_fvt_out.cpu_stream_out);
create stream test_stream8 into stream_db_fvt_out.cpu_stream_out2 with options (enable='off') AS SELECT * from kwdb_internal.kwdb_streams;
create stream test_stream8 into kwdb_internal.kwdb_streams with options (enable='off') AS SELECT * from stream_db_fvt_out.cpu_stream_out;
create stream test_stream8 into stream_db_fvt_out.cpu_stream_out2 with options (enable='off') AS SELECT a.* from stream_db_fvt_out.cpu_stream_out a,stream_db_fvt_out.cpu_stream_out b where a.w_begin=b.w_begin;

-- alter stream options
alter stream test_stream1 set max_delay='1m';
alter stream test_stream1 set max_delay='20m';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set max_delay='1';
alter stream test_stream1 set max_delay='0';
alter stream test_stream1 set max_delay='0s';
alter stream test_stream1 set max_delay='1s';
alter stream test_stream1 set max_delay='1d';
alter stream test_stream1 set max_delay='1aaa';

alter stream test_stream1 set sync_time='30s';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set sync_time='1';
alter stream test_stream1 set sync_time='0s';
alter stream test_stream1 set sync_time='1s';
alter stream test_stream1 set sync_time='1d';
alter stream test_stream1 set sync_time='1aaa';

alter stream test_stream1 set process_history='on';
alter stream test_stream1 set process_history='On';
alter stream test_stream1 set process_history='oN';
alter stream test_stream1 set process_history='ON';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set process_history='OFF';
alter stream test_stream1 set process_history='OfF';
alter stream test_stream1 set process_history='oFF';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set process_history='yes';
alter stream test_stream1 set process_history='no';


alter stream test_stream1 set ignore_expired='off';
alter stream test_stream1 set ignore_expired='oFF';
alter stream test_stream1 set ignore_expired='OFF';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set ignore_expired='on';
alter stream test_stream1 set ignore_expired='On';
alter stream test_stream1 set ignore_expired='oN';
alter stream test_stream1 set ignore_expired='ON';
alter stream test_stream1 set ignore_expired='yes';
alter stream test_stream1 set ignore_expired='No';

alter stream test_stream1 set ignore_update='off';
alter stream test_stream1 set ignore_update='oFF';
alter stream test_stream1 set ignore_update='OFF';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set ignore_update='ON';
alter stream test_stream1 set ignore_update='On';
alter stream test_stream1 set ignore_update='oN';
alter stream test_stream1 set ignore_update='YES';
alter stream test_stream1 set ignore_update='NO';

alter stream test_stream1 set max_retries='10';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set max_retries='0';
alter stream test_stream1 set max_retries='-1';

alter stream test_stream1 set checkpoint_interval='5s';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set checkpoint_interval='0s';
alter stream test_stream1 set checkpoint_interval='1day';
alter stream test_stream1 set checkpoint_interval='25aaa';

alter stream test_stream1 set heartbeat_interval='2s';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set heartbeat_interval='2ms';
alter stream test_stream1 set heartbeat_interval='2min';
alter stream test_stream1 set heartbeat_interval='2aaa';

alter stream test_stream1 set recalculate_delay_rounds='20';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set recalculate_delay_rounds='200';
alter stream test_stream1 set recalculate_delay_rounds='-1';
alter stream test_stream1 set recalculate_delay_rounds='0';

alter stream test_stream1 set buffer_size='1024Mib';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set buffer_size='1Mib';
alter stream test_stream1 set buffer_size='0';
alter stream test_stream1 set buffer_size='1Gib';
alter stream test_stream1 set buffer_size='aaa';

alter stream test_stream1 set enable='aaa';
alter stream test_stream1 set enable='Yes';
alter stream test_stream1 set enable='On';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];

alter table stream_db_fvt_out.cpu_stream_out drop column w_begin;
alter table stream_db_fvt_out.cpu_stream_out drop column count;
alter table stream_db_fvt_out.cpu_stream_out add column count_a int;

alter table stream_db_fvt.cpu drop column k_timestamp;
alter table stream_db_fvt.cpu drop column usage_user;
alter table stream_db_fvt.cpu drop column usage_guest;
alter table stream_db_fvt.cpu add column usage_user_a int;

alter stream test_stream1 set enable='OFF';
alter table stream_db_fvt_out.cpu_stream_out add column count_a int;
alter table stream_db_fvt.cpu add column usage_user_b int;
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];

-- drop table
drop table stream_db_fvt_out.cpu_stream_out;
drop table stream_db_fvt.cpu;

drop stream test_stream1;

drop table stream_db_fvt_out.cpu_stream_out;
drop table stream_db_fvt.cpu;

drop stream test_stream2;
drop stream test_stream3;
drop stream test_stream4;
drop stream test_stream5;

drop table stream_db_fvt_out.cpu_stream_out;
drop table stream_db_fvt_out.cpu_stream_out2;
drop table stream_db_fvt.cpu;
select name,target_table_name,options,query,status,create_by,error_message from [show streams];

create table stream_db_fvt_out.cpu_stream_out
(
    w_begin              timestamptz not null,
    w_end                timestamptz not null,
    usage_user_avg       bigint      not null,
    usage_system_avg     bigint      not null,
    count                bigint      not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_stream_out2
(
    w_begin              timestamptz not null,
    w_end                timestamptz not null,
    usage_user_avg       bigint      not null,
    usage_system_avg     bigint      not null,
    count                bigint      not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_stream_out3
(
    k_timestamp          timestamptz not null,
    usage_user_system    bigint      not null,
    usage_idel_nice      bigint      not null,
    usage_iowait_irq     bigint      not null,
    count                bigint      not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out1.cpu_stream_out
(
    k_timestamp          timestamp not null,
    usage_user_system    decimal    ,
    usage_idel_nice      decimal    ,
    usage_iowait_irq     decimal    ,
    hostname             char(30)    not null
);

create table stream_db_fvt_out1.cpu_stream_out1
(
    k_timestamp          timestamptz(3) not null,
    usage_user_system    decimal    ,
    usage_idel_nice      decimal    ,
    usage_iowait_irq     decimal    ,
    hostname             char(30)    not null
);

create table stream_db_fvt_out1.cpu_stream_out2
(
    k_timestamp          timestamp(6) not null,
    usage_user_system    decimal    ,
    usage_idel_nice      decimal    ,
    usage_iowait_irq     decimal    ,
    hostname             char(30)    not null
);

create table stream_db_fvt.cpu
(
    k_timestamp      timestamp not null,
    usage_user       bigint    not null,
    usage_system     bigint    not null,
    usage_idle       bigint    not null,
    usage_nice       bigint    not null,
    usage_iowait     bigint    not null,
    usage_irq        bigint    not null,
    usage_softirq    bool      not null,
    usage_softirq_f  float,
    usage_steal      bigint,
    usage_guest      bigint,
    usage_guest_nice bigint
) attributes (
    hostname char(30) not null,
    region char(30),
    datacenter char(30),
    rack char(30),
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname);

-- window functions
create stream test_stream1 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;
create stream test_stream2 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY COUNT_WINDOW(5), hostname;
create stream test_stream3 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY SESSION_WINDOW(k_timestamp, '5m'), hostname;
create stream test_stream4 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY EVENT_WINDOW(usage_user > 50, usage_system < 20), hostname;
create stream test_stream5 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY STATE_WINDOW(usage_idle), hostname;

-- time_bucket function
create stream test_stream6 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- non-agg query using ts target table
CREATE STREAM test_stream7 INTO stream_db_fvt_out.cpu_stream_out3 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='1m',SYNC_TIME='30s',HEARTBEAT_INTERVAL='1000ms',BUFFER_SIZE='1024kib') AS SELECT k_timestamp AS k_timestamp, hostname AS hostname, usage_user+usage_system AS usage_user_system, usage_idle*usage_nice AS usage_idel_nice, usage_iowait/usage_irq AS usage_iowait_irq FROM stream_db_fvt.cpu;

-- non-agg query using relation target table
CREATE STREAM test_stream8 INTO stream_db_fvt_out1.cpu_stream_out WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='1m',SYNC_TIME='30s',HEARTBEAT_INTERVAL='1000ms',BUFFER_SIZE='1024kib') AS SELECT k_timestamp AS k_timestamp, hostname AS hostname, usage_user+usage_system AS usage_user_system, usage_idle*usage_nice AS usage_idel_nice, usage_iowait/usage_irq AS usage_iowait_irq FROM stream_db_fvt.cpu;
CREATE STREAM test_stream8 INTO stream_db_fvt_out1.cpu_stream_out1 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='1m',SYNC_TIME='30s',HEARTBEAT_INTERVAL='1000ms',BUFFER_SIZE='1024kib') AS SELECT k_timestamp AS k_timestamp, hostname AS hostname, usage_user+usage_system AS usage_user_system, usage_idle*usage_nice AS usage_idel_nice, usage_iowait/usage_irq AS usage_iowait_irq FROM stream_db_fvt.cpu;

-- drop table cascade
create table stream_db_fvt_out.cpu_stream_out10
(
    w_begin              timestamptz not null,
    w_end                timestamptz not null,
    usage_user_avg       bigint      not null,
    usage_system_avg     bigint      not null,
    count                bigint      not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_stream_out11
(
    w_begin              timestamptz not null,
    w_end                timestamptz not null,
    usage_user_avg       bigint      not null,
    usage_system_avg     bigint      not null,
    count                bigint      not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out1.cpu_stream_out10
(
    w_begin              timestamptz(3) not null,
    w_end                timestamptz(3) not null,
    usage_user_avg       decimal        not null,
    usage_system_avg     decimal        not null,
    count                decimal        not null,
    hostname             char(30)    not null
);

create table stream_db_fvt_out1.cpu_stream_out11
(
    w_begin              timestamptz(3) not null,
    w_end                timestamptz(3) not null,
    usage_user_avg       decimal        not null,
    usage_system_avg     decimal        not null,
    count                decimal        not null,
    hostname             char(30)    not null
);

create table stream_db_fvt1.cpu10
(
    k_timestamp      timestamp(3) not null,
    usage_user       bigint    not null,
    usage_system     bigint    not null,
    usage_idle       bigint    not null,
    usage_nice       bigint    not null,
    usage_iowait     bigint    not null,
    usage_irq        bigint    not null,
    usage_softirq    bool      not null,
    usage_softirq_f  float,
    usage_steal      bigint,
    usage_guest      bigint,
    usage_guest_nice bigint
) attributes (
    hostname char(30) not null,
    region char(30),
    datacenter char(30),
    rack char(30),
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname);

create stream test_stream10 into stream_db_fvt_out.cpu_stream_out10 with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt1.cpu10 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream11 into stream_db_fvt_out.cpu_stream_out11 with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt1.cpu10 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream12 into stream_db_fvt_out1.cpu_stream_out10 with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt1.cpu10 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream13 into stream_db_fvt_out1.cpu_stream_out11 with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt1.cpu10 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

drop table stream_db_fvt_out.cpu_stream_out10;
drop table stream_db_fvt_out.cpu_stream_out10 cascade;

drop table stream_db_fvt_out1.cpu_stream_out10;
drop table stream_db_fvt_out1.cpu_stream_out10 cascade;

drop table stream_db_fvt1.cpu10;
drop table stream_db_fvt1.cpu10 cascade;

-- result checking
show stream test_stream10;
show stream test_stream11;
show stream test_stream12;
show stream test_stream13;
use stream_db_fvt_out;
show tables;
use stream_db_fvt_out1;
show tables;
use stream_db_fvt1;
show tables;

-- data type compatible checking
create table stream_db_fvt_out.cpu_stream_out20
(
    w_begin          timestamptz not null,
    w_end            timestamptz not null,
    k_timestamp      timestamp not null,
    usage_user       bigint    not null,
    usage_system     bigint    not null,
    usage_idle       bigint    not null,
    usage_nice       bigint    not null,
    usage_iowait     bigint    not null,
    usage_irq        bigint    not null,
    usage_softirq    bool      not null,
    usage_softirq_f  float,
    usage_steal      bigint,
    usage_guest      bigint,
    usage_guest_nice bigint,
    timestamp1       timestamp,
    timestamp2       timestamp(3),
    timestamp3       timestamp(6),
    timestamp4       timestamp(9),
    timestamp5       timestamptz,
    timestamp6       timestamptz(3),
    timestamp7       timestamptz(6),
    timestamp8       timestamptz(9),
    int1             int,
    int2             int2,
    int3             int4,
    int4             int8,
    float1           float,
    float2           float4,
    float3           float8,
    bool1            bool,
    s1               CHAR(1023),
    s2               NCHAR(255),
    s3               VARCHAR(4096),
    s4               CHAR,
    s5               CHAR(255),
    s6               NCHAR,
    s7               NVARCHAR(4096),
    s8               VARCHAR(1023),
    s9               NVARCHAR(200),
    s10              NCHAR(255),
    s11              CHAR(200),
    s12              VARBYTES,
    s13              VARBYTES(60),
    s14              VARCHAR,
    s15              NVARCHAR
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out1.cpu_stream_out21
(
    w_begin          timestamptz(3) not null,
    w_end            timestamptz(3) not null,
    k_timestamp      timestamp not null,
    usage_user       bigint    not null,
    usage_system     bigint    not null,
    usage_idle       bigint    not null,
    usage_nice       bigint    not null,
    usage_iowait     bigint    not null,
    usage_irq        bigint    not null,
    usage_softirq    bool      not null,
    usage_softirq_f  float,
    usage_steal      bigint,
    usage_guest      bigint,
    usage_guest_nice bigint,
    timestamp1       timestamp,
    timestamp2       timestamp(3),
    timestamp3       timestamp(6),
    timestamp5       timestamptz,
    timestamp6       timestamptz(3),
    timestamp7       timestamptz(6),
    int1             int,
    int2             int2,
    int3             int4,
    int4             int8,
    float1           float,
    float2           float4,
    float3           float8,
    decimal1         decimal,
    bool1            bool,
    s1               CHAR(1023),
    s2               NCHAR(255),
    s3               VARCHAR(4096),
    s4               CHAR,
    s5               CHAR(255),
    s6               NCHAR,
    s7               NVARCHAR(4096),
    s8               VARCHAR(1023),
    s9               NVARCHAR(200),
    s10              NCHAR(255),
    s11              CHAR(200),
    s12              VARBYTES,
    s14              VARCHAR,
    s15              NVARCHAR,
    hostname         char(30) not null
);

create table stream_db_fvt1.cpu20
(
    k_timestamp      timestamp not null,
    usage_user       bigint    not null,
    usage_system     bigint    not null,
    usage_idle       bigint    not null,
    usage_nice       bigint    not null,
    usage_iowait     bigint    not null,
    usage_irq        bigint    not null,
    usage_softirq    bool      not null,
    usage_softirq_f  float,
    usage_steal      bigint,
    usage_guest      bigint,
    usage_guest_nice bigint,
    timestamp1       timestamp,
    timestamp2       timestamp(3),
    timestamp3       timestamp(6),
    timestamp4       timestamp(9),
    timestamp5       timestamptz,
    timestamp6       timestamptz(3),
    timestamp7       timestamptz(6),
    timestamp8       timestamptz(9),
    int1             int,
    int2             int2,
    int3             int4,
    int4             int8,
    float1           float,
    float2           float4,
    float3           float8,
    bool1            bool,
    s1               CHAR(1023),
    s2               NCHAR(255),
    s3               VARCHAR(4096),
    s4               CHAR,
    s5               CHAR(255),
    s6               NCHAR,
    s7               NVARCHAR(4096),
    s8               VARCHAR(1023),
    s9               NVARCHAR(200),
    s10              NCHAR(255),
    s11              CHAR(200),
    s12              VARBYTES,
    s13              VARBYTES(60),
    s14              VARCHAR,
    s15              NVARCHAR
) attributes (
    hostname char(30) not null,
    region char(30),
    datacenter char(30),
    rack char(30),
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname);

-- data type compatible checking for ts table
-- timestamp to timestamp
create stream test_stream200 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp1) as timestamp1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp to timestamp(3)
create stream test_stream201 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp1) as timestamp2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp to timestamp(6)
create stream test_stream202 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp1) as timestamp3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp to timestamp(9)
create stream test_stream203 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp1) as timestamp4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamp(3) to timestamp
create stream test_stream204 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp2) as timestamp1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(3) to timestamp(3)
create stream test_stream205 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp2) as timestamp2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(3) to timestamp(6)
create stream test_stream206 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp2) as timestamp3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(3) to timestamp(9)
create stream test_stream207 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp2) as timestamp4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamp(6) to timestamp
create stream test_stream208 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp3) as timestamp1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(6) to timestamp(3)
create stream test_stream209 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp3) as timestamp2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(6) to timestamp(6)
create stream test_stream210 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp3) as timestamp3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(6) to timestamp(9)
create stream test_stream211 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp3) as timestamp4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamp(9) to timestamp
create stream test_stream212 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp4) as timestamp1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(9) to timestamp(3)
create stream test_stream213 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp4) as timestamp2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(9) to timestamp(6)
create stream test_stream214 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp4) as timestamp3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(9) to timestamp(9)
create stream test_stream215 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp4) as timestamp4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamp to timestamptz
create stream test_stream216 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp1) as timestamp5, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamptz to timestamp
create stream test_stream217 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp5) as timestamp1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamptz to timestamptz
create stream test_stream220 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp5) as timestamp5, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz to timestamptz(3)
create stream test_stream221 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp5) as timestamp6, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz to timestamptz(6)
create stream test_stream222 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp5) as timestamp7, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz to timestamptz(9)
create stream test_stream223 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp5) as timestamp8, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamptz(3) to timestamptz
create stream test_stream224 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp6) as timestamp5, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(3) to timestamptz(3)
create stream test_stream225 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp6) as timestamp6, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(3) to timestamptz(6)
create stream test_stream226 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp6) as timestamp7, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(3) to timestamptz(9)
create stream test_stream227 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp6) as timestamp8, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamptz(6) to timestamptz
create stream test_stream228 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp7) as timestamp5, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(6) to timestamptz(3)
create stream test_stream229 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp7) as timestamp6, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(6) to timestamptz(6)
create stream test_stream230 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp7) as timestamp7, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(6) to timestamptz(9)
create stream test_stream231 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp7) as timestamp8, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamptz(9) to timestamptz
create stream test_stream232 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp8) as timestamp5, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(9) to timestamptz(3)
create stream test_stream233 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp8) as timestamp6, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(9) to timestamptz(6)
create stream test_stream234 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp8) as timestamp7, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(9) to timestamptz(9)
create stream test_stream235 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp8) as timestamp8, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- int to int
create stream test_stream240 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int1) as int1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int to int2
create stream test_stream241 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int1) as int2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int to int4
create stream test_stream242 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int1) as int3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int to int8
create stream test_stream243 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int1) as int4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- int2 to int
create stream test_stream244 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int2) as int1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int2 to int2
create stream test_stream245 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int2) as int2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int2 to int4
create stream test_stream246 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int2) as int3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int2 to int8
create stream test_stream247 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int2) as int4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- int4 to int
create stream test_stream248 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int3) as int1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int4 to int2
create stream test_stream249 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int3) as int2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int4 to int4
create stream test_stream250 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int3) as int3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int4 to int8
create stream test_stream251 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int3) as int4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- int8 to int
create stream test_stream252 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int4) as int1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int8 to int2
create stream test_stream253 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int4) as int2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int8 to int4
create stream test_stream254 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int4) as int3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int8 to int8
create stream test_stream255 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int4) as int4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;


-- float to float
create stream test_stream260 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float1) as float1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float to float4
create stream test_stream261 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float1) as float2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float to float8
create stream test_stream262 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float1) as float3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- float4 to float
create stream test_stream263 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float2) as float1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float4 to float4
create stream test_stream264 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float2) as float2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float4 to float8
create stream test_stream265 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float2) as float3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- float8 to float
create stream test_stream266 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float3) as float1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float8 to float44
create stream test_stream267 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float3) as float2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float8 to float8
create stream test_stream268 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float3) as float3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- string
create stream test_stream270 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream271 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream272 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream273 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream274 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s5, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream275 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s6, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream276 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s7, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream277 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s8, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream278 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s9, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream279 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s10, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream280 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s11, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream281 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s12, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream282 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s13, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream283 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s14, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream284 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s15, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- bool
create stream test_stream290 into stream_db_fvt_out.cpu_stream_out20 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(bool1) as bool1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- cleanup
drop table stream_db_fvt_out.cpu_stream_out20 cascade;

-- data type compatible checking for relation table

-- timestamp to timestamp
create stream test_stream200 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp1) as timestamp1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp to timestamp(3)
create stream test_stream201 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp1) as timestamp2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp to timestamp(6)
create stream test_stream202 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp1) as timestamp3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp to timestamp(9)
create stream test_stream203 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp1) as timestamp4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamp(3) to timestamp
create stream test_stream204 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp2) as timestamp1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(3) to timestamp(3)
create stream test_stream205 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp2) as timestamp2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(3) to timestamp(6)
create stream test_stream206 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp2) as timestamp3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(3) to timestamp(9)
create stream test_stream207 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp2) as timestamp4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamp(6) to timestamp
create stream test_stream208 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp3) as timestamp1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(6) to timestamp(3)
create stream test_stream209 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp3) as timestamp2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(6) to timestamp(6)
create stream test_stream210 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp3) as timestamp3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(6) to timestamp(9)
create stream test_stream211 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp3) as timestamp4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamp(9) to timestamp
create stream test_stream212 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp4) as timestamp1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(9) to timestamp(3)
create stream test_stream213 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp4) as timestamp2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(9) to timestamp(6)
create stream test_stream214 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp4) as timestamp3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamp(9) to timestamp(9)
create stream test_stream215 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp4) as timestamp4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamp to timestamptz
create stream test_stream216 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp1) as timestamp5, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamptz to timestamp
create stream test_stream217 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp5) as timestamp1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamptz to timestamptz
create stream test_stream220 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp5) as timestamp5, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz to timestamptz(3)
create stream test_stream221 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp5) as timestamp6, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz to timestamptz(6)
create stream test_stream222 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp5) as timestamp7, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz to timestamptz(9)
create stream test_stream223 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp5) as timestamp8, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamptz(3) to timestamptz
create stream test_stream224 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp6) as timestamp5, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(3) to timestamptz(3)
create stream test_stream225 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp6) as timestamp6, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(3) to timestamptz(6)
create stream test_stream226 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp6) as timestamp7, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(3) to timestamptz(9)
create stream test_stream227 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp6) as timestamp8, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamptz(6) to timestamptz
create stream test_stream228 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp7) as timestamp5, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(6) to timestamptz(3)
create stream test_stream229 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp7) as timestamp6, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(6) to timestamptz(6)
create stream test_stream230 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp7) as timestamp7, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(6) to timestamptz(9)
create stream test_stream231 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp7) as timestamp8, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- timestamptz(9) to timestamptz
create stream test_stream232 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp8) as timestamp5, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(9) to timestamptz(3)
create stream test_stream233 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp8) as timestamp6, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(9) to timestamptz(6)
create stream test_stream234 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp8) as timestamp7, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- timestamptz(9) to timestamptz(9)
create stream test_stream235 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, max(timestamp8) as timestamp8, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- int to int
create stream test_stream240 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int1) as int1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int to int2
create stream test_stream241 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int1) as int2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int to int4
create stream test_stream242 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int1) as int3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int to int8
create stream test_stream243 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int1) as int4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int to decimal
create stream test_stream243 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int1) as decimal1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- int2 to int
create stream test_stream244 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int2) as int1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int2 to int2
create stream test_stream245 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int2) as int2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int2 to int4
create stream test_stream246 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int2) as int3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int2 to int8
create stream test_stream247 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int2) as int4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int2 to decimal
create stream test_stream247 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int2) as decimal1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- int4 to int
create stream test_stream248 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int3) as int1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int4 to int2
create stream test_stream249 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int3) as int2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int4 to int4
create stream test_stream250 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int3) as int3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int4 to int8
create stream test_stream251 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int3) as int4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int4 to decimal
create stream test_stream251 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int3) as decimal1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- int8 to int
create stream test_stream252 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int4) as int1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int8 to int2
create stream test_stream253 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int4) as int2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int8 to int4
create stream test_stream254 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int4) as int3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int8 to int8
create stream test_stream255 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int4) as int4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- int8 to decimal
create stream test_stream255 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(int4) as decimal1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;


-- float to float
create stream test_stream260 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float1) as float1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float to float4
create stream test_stream261 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float1) as float2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float to float8
create stream test_stream262 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float1) as float3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float to decimal
create stream test_stream262 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float1) as decimal1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- float4 to float
create stream test_stream263 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float2) as float1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float4 to float4
create stream test_stream264 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float2) as float2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float4 to float8
create stream test_stream265 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float2) as float3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float4 to decimal
create stream test_stream265 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float2) as decimal1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- float8 to float
create stream test_stream266 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float3) as float1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float8 to float44
create stream test_stream267 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float3) as float2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float8 to float8
create stream test_stream268 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float3) as float3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- float8 to decimal
create stream test_stream268 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(float3) as decimal1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- string
create stream test_stream270 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream271 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s2, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream272 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s3, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream273 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s4, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream274 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s5, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream275 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s6, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream276 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s7, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream277 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s8, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream278 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s9, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream279 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s10, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream280 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s11, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream281 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s12, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream282 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s13, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream283 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s14, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
create stream test_stream284 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(s1) as s15, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- bool
create stream test_stream290 into stream_db_fvt_out1.cpu_stream_out21 with options (enable='off') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, last(bool1) as bool1, hostname as hostname FROM stream_db_fvt1.cpu20 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;

-- cleanup
drop table stream_db_fvt_out1.cpu_stream_out21 cascade;

-- normal stream query
create table stream_db_fvt.cpu_normal
(
    k_timestamp      timestamp(6) not null,
    usage_user       bigint    not null,
    usage_system     bigint    not null,
    usage_idle       bigint    not null,
    usage_nice       bigint    not null,
    usage_iowait     bigint    not null,
    usage_irq        bigint    not null,
    usage_softirq    bigint    not null,
    usage_steal      bigint,
    usage_guest      bigint,
    usage_guest_nice bigint
) attributes (
    hostname char(30) not null,
    region char(30),
    datacenter char(30),
    rack char(30),
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_avg_normal
(
    k_timestamp          timestamp(6) not null,
    usage_user_system    bigint       not null,
    usage_idel_nice      bigint       not null,
    usage_iowait_irq     float        not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out1.cpu_avg_normal
(
    k_timestamp          timestamptz not null,
    usage_user_system    decimal,
    usage_idel_nice      decimal,
    usage_iowait_irq     decimal,
    hostname             char(30)    not null,
    PRIMARY KEY (k_timestamp ASC, hostname ASC)
);


INSERT INTO stream_db_fvt.cpu_normal values ('2023-05-31 10:00:00.123456789', 58, 8, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', 'beijing', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_normal values ('2023-05-31 10:00:10.123456789', 58, 33, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_normal values ('2023-05-31 10:00:20.123456789', 58, 44, 24, 61, 22, 63, 6, 44, 80, 38, 'host_1', 'shanghai', '', '', '', '', '', '', '', '');


CREATE STREAM cpu_stream_normal_1 INTO stream_db_fvt_out.cpu_avg_normal WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT k_timestamp AS k_timestamp, hostname AS hostname, usage_user+usage_system AS usage_user_system, usage_idle*usage_nice AS usage_idel_nice, usage_iowait/usage_irq AS usage_iowait_irq FROM stream_db_fvt.cpu_normal where usage_system>11;
CREATE STREAM cpu_stream_normal_2 INTO stream_db_fvt_out1.cpu_avg_normal WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT k_timestamp AS k_timestamp, hostname AS hostname, usage_user+usage_system AS usage_user_system, usage_idle*usage_nice AS usage_idel_nice, usage_iowait/usage_irq AS usage_iowait_irq FROM stream_db_fvt.cpu_normal ;
CREATE STREAM cpu_stream_normal_3 INTO stream_db_fvt_out1.cpu_avg_normal WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT k_timestamp AS k_timestamp, hostname AS hostname, usage_user+usage_system AS usage_user_system, usage_idle*usage_nice AS usage_idel_nice, usage_iowait/usage_irq AS usage_iowait_irq FROM stream_db_fvt.cpu_normal where usage_system>11;
CREATE STREAM cpu_stream_normal_4 INTO stream_db_fvt_out1.cpu_avg_normal WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='1500ms',heartbeat_interval='2s') AS SELECT k_timestamp AS k_timestamp, hostname AS hostname, usage_user+usage_system AS usage_user_system, usage_idle*usage_nice AS usage_idel_nice, usage_iowait/usage_irq AS usage_iowait_irq FROM stream_db_fvt.cpu_normal;

select pg_sleep(2);

INSERT INTO stream_db_fvt.cpu_normal values ('2023-05-31 10:00:30.123456789', 58, 10, 24, 61, 22, 63, 6, 44, 80, 38, 'host_1', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_normal values ('2023-05-31 10:00:40.123456789', 58, 55, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_normal values ('2023-05-31 10:00:50.123456789', 58, 66, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_normal values ('2023-05-31 10:01:00.123456789', 58, 12, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_normal values ('2023-05-31 10:01:10.123456789', 58, 77, 25, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_normal values ('2023-05-31 10:01:20.123456789', 58, 88, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_normal values ('2023-05-31 10:10:00.123456789', 58, 3, 25, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');

select pg_sleep(5);

select * from stream_db_fvt_out.cpu_avg_normal order by k_timestamp,hostname;
select * from stream_db_fvt_out1.cpu_avg_normal order by k_timestamp,hostname;

-- cleanup
drop table stream_db_fvt.cpu_normal cascade;
drop table stream_db_fvt_out.cpu_avg_normal cascade;
drop table stream_db_fvt_out1.cpu_avg_normal cascade;

-- agg stream query
create table stream_db_fvt.cpu_agg
(
    k_timestamp      timestamp(6) not null,
    usage_user       bigint    not null,
    usage_system     bigint    not null,
    usage_idle       bigint    not null,
    usage_nice       bigint    not null,
    usage_iowait     bigint    not null,
    usage_irq        bigint    not null,
    usage_softirq    bigint    not null,
    usage_steal      bigint,
    usage_guest      bigint,
    usage_guest_nice bigint
) attributes (
    hostname char(30) not null,
    region char(30),
    datacenter char(30),
    rack char(30),
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_avg_agg1
(
    w_begin              timestamp(6) not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       bigint    not null,
    usage_system_avg     bigint    not null,
    count                bigint    not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_avg_agg2
(
    w_begin              timestamp(6) not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       bigint    not null,
    usage_system_avg     bigint    not null,
    count                bigint    not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_avg_agg3
(
    w_begin              timestamp(6) not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       bigint    not null,
    usage_system_avg     bigint    not null,
    count                bigint    not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_avg_agg4
(
    w_begin              timestamp(6) not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       bigint    not null,
    usage_system_avg     bigint    not null,
    count                bigint    not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_avg_agg5
(
    w_begin              timestamp(6) not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       bigint    not null,
    usage_system_avg     bigint    not null,
    count                bigint    not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_avg_agg6
(
    w_begin              timestamp(6) not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       bigint    not null,
    usage_system_avg     bigint    not null,
    count                bigint    not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_avg_agg7
(
    w_begin              timestamp(6) not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       bigint    not null,
    usage_system_avg     bigint    not null,
    count                bigint    not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_avg_agg8
(
    w_begin              timestamptz(6) not null,
    w_end                timestamptz(6) not null,
    w_begin1             timestamptz(6) not null,
    w_end1               timestamptz(6) not null,
    usage_user_avg       bigint    not null,
    usage_system_avg     bigint    not null,
    count                bigint    not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out.cpu_avg_agg9
(
    w_begin              timestamp(6) not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       bigint    not null,
    usage_system_avg     bigint    not null,
    count                bigint    not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);

create table stream_db_fvt_out1.cpu_avg_agg1
(
    w_begin              timestamptz(6)   not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       decimal     not null,
    usage_system_avg     decimal     not null,
    count                decimal     not null,
    hostname             char(30)    not null,
    PRIMARY KEY (w_begin ASC, hostname ASC)
);

create table stream_db_fvt_out1.cpu_avg_agg2
(
    w_begin              timestamptz(6)   not null,
    w_end                timestamptz(6)   not null,
    usage_user_avg       decimal     not null,
    usage_system_avg     decimal     not null,
    count                decimal     not null,
    hostname             char(30)    not null
);

create table stream_db_fvt_out1.cpu_avg_agg3
(
    w_begin              timestamptz(6)   not null,
    w_end                timestamptz(6)   not null,
    usage_user_avg       decimal     not null,
    usage_system_avg     decimal     not null,
    count                decimal     not null,
    hostname             char(30)    not null,
    PRIMARY KEY (w_begin ASC, hostname ASC)
);

create table stream_db_fvt_out1.cpu_avg_agg4
(
    w_begin              timestamptz(6)   not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       decimal     not null,
    usage_system_avg     decimal     not null,
    count                decimal     not null,
    hostname             char(30)    not null,
    PRIMARY KEY (w_begin ASC, hostname ASC)
);

create table stream_db_fvt_out1.cpu_avg_agg5
(
    w_begin              timestamptz(6)   not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       decimal     not null,
    usage_system_avg     decimal     not null,
    count                decimal     not null,
    hostname             char(30)    not null,
    PRIMARY KEY (w_begin ASC, hostname ASC)
);

create table stream_db_fvt_out1.cpu_avg_agg6
(
    w_begin              timestamptz(6)   not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       decimal     not null,
    usage_system_avg     decimal     not null,
    count                decimal     not null,
    hostname             char(30)    not null,
    PRIMARY KEY (w_begin ASC, hostname ASC)
);

create table stream_db_fvt_out1.cpu_avg_agg7
(
    w_begin              timestamptz(6)   not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       decimal     not null,
    usage_system_avg     decimal     not null,
    count                decimal     not null,
    hostname             char(30)    not null,
    PRIMARY KEY (w_begin ASC, hostname ASC)
);

set cluster setting ts.stream.max_active_number=20;

INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:00:00.123456789', 58, 30, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', 'beijing', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:00:10.123456789', 58, 33, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:00:10.123456789', 58, 0, 24, 61, 22, 63, 6, 44, 80, 38, 'host_1', 'shanghai', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:00:20.123456789', 58, 44, 24, 61, 22, 63, 6, 44, 80, 38, 'host_1', 'shanghai', '', '', '', '', '', '', '', '');

-- ts table
-- 1. TIME_BUCKET
CREATE STREAM cpu_stream_agg_ts_1 INTO stream_db_fvt_out.cpu_avg_agg1 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- 2. TIME_WINDOW
-- for TIME_WINDOW case, use first_row/last_row instead of first/last to ensure the stream recalculation is working correctly.
CREATE STREAM cpu_stream_agg_ts_2 INTO stream_db_fvt_out.cpu_avg_agg2 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first_row(k_timestamp) as w_begin, last_row(k_timestamp) as w_end, avg(usage_user) as usage_user_avg, avg(usage_system) as usage_system_avg, count(*) as count, hostname as hostname FROM stream_db_fvt.cpu_agg WHERE usage_system+usage_system>0 GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;
-- 3. COUNT_WINDOW
CREATE STREAM cpu_stream_agg_ts_3 INTO stream_db_fvt_out.cpu_avg_agg3 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='10s',SYNC_TIME='5s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, avg(usage_system) as usage_system_avg, hostname as hostname, count(*) as count, avg(usage_user) as usage_user_avg FROM stream_db_fvt.cpu_agg WHERE abs(usage_system)>0 GROUP BY COUNT_WINDOW(5), hostname;
-- 4. SESSION_WINDOW
CREATE STREAM cpu_stream_agg_ts_4 INTO stream_db_fvt_out.cpu_avg_agg4 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>-1 GROUP BY SESSION_WINDOW(k_timestamp, '5m'), hostname;
-- 5. EVENT_WINDOW
CREATE STREAM cpu_stream_agg_ts_5 INTO stream_db_fvt_out.cpu_avg_agg5 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system not in (0,100) GROUP BY EVENT_WINDOW(usage_user > 50, usage_system < 20), hostname;
-- 6. STATE_WINDOW
CREATE STREAM cpu_stream_agg_ts_6 INTO stream_db_fvt_out.cpu_avg_agg6 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='10s',SYNC_TIME='5s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY STATE_WINDOW(usage_idle), hostname;
-- 7. cannot create stream without any agg function (group by hostname only)
CREATE STREAM cpu_stream_agg_ts_7 INTO stream_db_fvt_out.cpu_avg_agg7 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY hostname;
-- 8. TIME_WINDOW slide
-- for TIME_WINDOW case, use first/last to ensure the stream recalculation is working correctly.
CREATE STREAM cpu_stream_agg_ts_8 INTO stream_db_fvt_out.cpu_avg_agg8 WITH OPTIONS(recalculate_delay_rounds='30',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end,first_row(k_timestamp) as w_begin1, last_row(k_timestamp) as w_end1, avg(usage_user) as usage_user_avg, avg(usage_system) as usage_system_avg, count(*) as count, hostname as hostname FROM stream_db_fvt.cpu_agg WHERE usage_system+usage_system>0 GROUP BY TIME_WINDOW(k_timestamp, '1m','30s'), hostname;
-- 9. COUNT_WINDOW slide
CREATE STREAM cpu_stream_agg_ts_9 INTO stream_db_fvt_out.cpu_avg_agg9 WITH OPTIONS(recalculate_delay_rounds='30',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='10s',SYNC_TIME='5s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, avg(usage_system) as usage_system_avg, hostname as hostname, count(*) as count, avg(usage_user) as usage_user_avg FROM stream_db_fvt.cpu_agg WHERE abs(usage_system)>0 GROUP BY COUNT_WINDOW(5,3), hostname;

-- relation table
-- 1. TIME_BUCKET
CREATE STREAM cpu_stream_agg_rel_1 INTO stream_db_fvt_out1.cpu_avg_agg1 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- 2. TIME_WINDOW
CREATE STREAM cpu_stream_agg_rel_2 INTO stream_db_fvt_out1.cpu_avg_agg2 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first_row(k_timestamp) as w_begin, last_row(k_timestamp) as w_end, avg(usage_user) as usage_user_avg, avg(usage_system) as usage_system_avg, count(*) as count, hostname as hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY TIME_WINDOW(k_timestamp, '1m'), hostname;
-- 3. COUNT_WINDOW
CREATE STREAM cpu_stream_agg_rel_3 INTO stream_db_fvt_out1.cpu_avg_agg3 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, avg(usage_system) as usage_system_avg, hostname as hostname, count(*) as count, avg(usage_user) as usage_user_avg FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY COUNT_WINDOW(5), hostname;
-- 4. SESSION_WINDOW
CREATE STREAM cpu_stream_agg_rel_4 INTO stream_db_fvt_out1.cpu_avg_agg4 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY SESSION_WINDOW(k_timestamp, '5m'), hostname;
-- 5. EVENT_WINDOW
CREATE STREAM cpu_stream_agg_rel_5 INTO stream_db_fvt_out1.cpu_avg_agg5 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY EVENT_WINDOW(usage_user > 50, usage_system < 20), hostname;
-- 6. STATE_WINDOW
CREATE STREAM cpu_stream_agg_rel_6 INTO stream_db_fvt_out1.cpu_avg_agg6 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY STATE_WINDOW(usage_idle), hostname;
-- 7. cannot create stream without any agg function (group by hostname only)
CREATE STREAM cpu_stream_agg_rel_7 INTO stream_db_fvt_out1.cpu_avg_agg7 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY hostname;

select pg_sleep(2);

INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:00:30.123456789', 58, 10, 24, 61, 22, 63, 6, 44, 80, 38, 'host_1', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:00:40.123456789', 58, 55, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:00:50.123456789', 58, 66, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:01:00.123456789', 58, 12, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:01:05.123456789', 58, 0, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:01:10.123456789', 58, 77, 25, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:01:20.123456789', 58, 88, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:10:00.123456789', 58, 3, 25, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');

INSERT INTO stream_db_fvt.cpu_agg
values ('2023-05-31 10:20:00', 58, 3, 25, 61, 22, 63, 6, 44, 80, 38, 'host_1', '', '', '', '', '', '', '', '', '');

INSERT INTO stream_db_fvt.cpu_agg
values ('2023-05-31 10:30:00', 58, 3, 25, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');

-- expired records
INSERT INTO stream_db_fvt.cpu_agg
values ('2023-05-31 10:00:26', 58, 44, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg
values ('2023-05-31 10:00:26', 58, 44, 24, 61, 22, 63, 6, 44, 80, 38, 'host_1', '', '', '', '', '', '', '', '', '');

select pg_sleep(25);

select * from stream_db_fvt_out.cpu_avg_agg1 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg2 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg3 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg4 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg5 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg6 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg7 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg8 order by hostname, w_begin;
select * from stream_db_fvt_out.cpu_avg_agg9 order by hostname, w_begin;

select * from stream_db_fvt_out1.cpu_avg_agg1 order by w_begin, hostname;
select * from stream_db_fvt_out1.cpu_avg_agg2 order by w_begin, hostname;
select * from stream_db_fvt_out1.cpu_avg_agg3 order by w_begin, hostname;
select * from stream_db_fvt_out1.cpu_avg_agg4 order by w_begin, hostname;
select * from stream_db_fvt_out1.cpu_avg_agg5 order by w_begin, hostname;
select * from stream_db_fvt_out1.cpu_avg_agg6 order by w_begin, hostname;
select * from stream_db_fvt_out1.cpu_avg_agg7 order by w_begin, hostname;

-- cleanup
drop database if exists stream_db_fvt cascade;
create ts database stream_db_fvt;

drop database if exists stream_db_fvt_out cascade;
create ts database stream_db_fvt_out;

drop database if exists stream_db_fvt_out1 cascade;
create database stream_db_fvt_out1;

show streams;