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


create stream test_stream1 into stream_db_fvt_out.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM stream_db_fvt.cpu GROUP BY hostname, TIME_WINDOW(k_timestamp, '1m');


-- alter stream options
alter stream test_stream1 set low_latency='off';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set low_latency='on';
alter stream test_stream1 set low_latency='On';
alter stream test_stream1 set low_latency='oN';
alter stream test_stream1 set low_latency='ON';
alter stream test_stream1 set low_latency='yes';
alter stream test_stream1 set low_latency='No';

alter stream test_stream1 set sync_time='30s';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];
alter stream test_stream1 set sync_time='1';
alter stream test_stream1 set sync_time='-1s';
alter stream test_stream1 set sync_time='1s';
alter stream test_stream1 set sync_time='1d';
alter stream test_stream1 set sync_time='1aaa';
select name,target_table_name,options,query,status,create_by,error_message from [show stream test_stream1];

-- drop table
drop table stream_db_fvt_out.cpu_stream_out;
drop table stream_db_fvt.cpu;

drop stream test_stream1;

drop table stream_db_fvt_out.cpu_stream_out;
drop table stream_db_fvt.cpu;

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


CREATE STREAM cpu_stream_normal_1 INTO stream_db_fvt_out.cpu_avg_normal WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT k_timestamp AS k_timestamp, hostname AS hostname, usage_user+usage_system AS usage_user_system, usage_idle*usage_nice AS usage_idel_nice, usage_iowait/usage_irq AS usage_iowait_irq FROM stream_db_fvt.cpu_normal where usage_system>11;
CREATE STREAM cpu_stream_normal_2 INTO stream_db_fvt_out1.cpu_avg_normal WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT k_timestamp AS k_timestamp, hostname AS hostname, usage_user+usage_system AS usage_user_system, usage_idle*usage_nice AS usage_idel_nice, usage_iowait/usage_irq AS usage_iowait_irq FROM stream_db_fvt.cpu_normal ;
CREATE STREAM cpu_stream_normal_3 INTO stream_db_fvt_out1.cpu_avg_normal WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT k_timestamp AS k_timestamp, hostname AS hostname, usage_user+usage_system AS usage_user_system, usage_idle*usage_nice AS usage_idel_nice, usage_iowait/usage_irq AS usage_iowait_irq FROM stream_db_fvt.cpu_normal where usage_system>11;
CREATE STREAM cpu_stream_normal_4 INTO stream_db_fvt_out1.cpu_avg_normal WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='1500ms',heartbeat_interval='2s') AS SELECT k_timestamp AS k_timestamp, hostname AS hostname, usage_user+usage_system AS usage_user_system, usage_idle*usage_nice AS usage_idel_nice, usage_iowait/usage_irq AS usage_iowait_irq FROM stream_db_fvt.cpu_normal;

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
CREATE STREAM cpu_stream_agg_ts_1 INTO stream_db_fvt_out.cpu_avg_agg1 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='10s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='5s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- 2. TIME_WINDOW
-- for TIME_WINDOW case, use first_row/last_row instead of first/last to ensure the stream recalculation is working correctly.
CREATE STREAM cpu_stream_agg_ts_2 INTO stream_db_fvt_out.cpu_avg_agg2 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='10s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='5s',heartbeat_interval='1s') AS SELECT first_row(k_timestamp) as w_begin, last_row(k_timestamp) as w_end, avg(usage_user) as usage_user_avg, avg(usage_system) as usage_system_avg, count(*) as count, hostname as hostname FROM stream_db_fvt.cpu_agg WHERE usage_system+usage_system>0 GROUP BY hostname, TIME_WINDOW(k_timestamp, '1m');
-- 3. COUNT_WINDOW
CREATE STREAM cpu_stream_agg_ts_3 INTO stream_db_fvt_out.cpu_avg_agg3 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='10s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='5s',heartbeat_interval='1s') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, avg(usage_system) as usage_system_avg, hostname as hostname, count(*) as count, avg(usage_user) as usage_user_avg FROM stream_db_fvt.cpu_agg WHERE abs(usage_system)>0 GROUP BY hostname, COUNT_WINDOW(5);
-- 4. SESSION_WINDOW
CREATE STREAM cpu_stream_agg_ts_4 INTO stream_db_fvt_out.cpu_avg_agg4 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='10s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='5s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>-1 GROUP BY hostname, SESSION_WINDOW(k_timestamp, '5m');
-- 5. EVENT_WINDOW
CREATE STREAM cpu_stream_agg_ts_5 INTO stream_db_fvt_out.cpu_avg_agg5 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='10s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='5s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system not in (0,100) GROUP BY hostname, EVENT_WINDOW(usage_user > 50, usage_system < 20);
-- 6. STATE_WINDOW
CREATE STREAM cpu_stream_agg_ts_6 INTO stream_db_fvt_out.cpu_avg_agg6 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='10s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='5s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY hostname, STATE_WINDOW(usage_idle);
-- 7. cannot create stream without any agg function (group by hostname only)
CREATE STREAM cpu_stream_agg_ts_7 INTO stream_db_fvt_out.cpu_avg_agg7 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='10s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='5s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY hostname;
-- 8. TIME_WINDOW slide
-- for TIME_WINDOW case, use first/last to ensure the stream recalculation is working correctly.
CREATE STREAM cpu_stream_agg_ts_8 INTO stream_db_fvt_out.cpu_avg_agg8 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='30',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='10s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='5s',heartbeat_interval='1s') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end,first_row(k_timestamp) as w_begin1, last_row(k_timestamp) as w_end1, avg(usage_user) as usage_user_avg, avg(usage_system) as usage_system_avg, count(*) as count, hostname as hostname FROM stream_db_fvt.cpu_agg WHERE usage_system+usage_system>0 GROUP BY hostname, TIME_WINDOW(k_timestamp, '1m','30s');
-- 9. COUNT_WINDOW slide
CREATE STREAM cpu_stream_agg_ts_9 INTO stream_db_fvt_out.cpu_avg_agg9 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='30',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='on', MAX_DELAY='10s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='5s',heartbeat_interval='1s') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, avg(usage_system) as usage_system_avg, hostname as hostname, count(*) as count, avg(usage_user) as usage_user_avg FROM stream_db_fvt.cpu_agg WHERE abs(usage_system)>0 GROUP BY hostname, COUNT_WINDOW(5,3);

-- relation table
-- 1. TIME_BUCKET
CREATE STREAM cpu_stream_agg_rel_1 INTO stream_db_fvt_out1.cpu_avg_agg1 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY TIME_BUCKET(k_timestamp, '1m'), hostname;
-- 2. TIME_WINDOW
CREATE STREAM cpu_stream_agg_rel_2 INTO stream_db_fvt_out1.cpu_avg_agg2 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first_row(k_timestamp) as w_begin, last_row(k_timestamp) as w_end, avg(usage_user) as usage_user_avg, avg(usage_system) as usage_system_avg, count(*) as count, hostname as hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY hostname, TIME_WINDOW(k_timestamp, '1m');
-- 3. COUNT_WINDOW
CREATE STREAM cpu_stream_agg_rel_3 INTO stream_db_fvt_out1.cpu_avg_agg3 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, avg(usage_system) as usage_system_avg, hostname as hostname, count(*) as count, avg(usage_user) as usage_user_avg FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY hostname, COUNT_WINDOW(5);
-- 4. SESSION_WINDOW
CREATE STREAM cpu_stream_agg_rel_4 INTO stream_db_fvt_out1.cpu_avg_agg4 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY hostname, SESSION_WINDOW(k_timestamp, '5m');
-- 5. EVENT_WINDOW
CREATE STREAM cpu_stream_agg_rel_5 INTO stream_db_fvt_out1.cpu_avg_agg5 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY hostname, EVENT_WINDOW(usage_user > 50, usage_system < 20);
-- 6. STATE_WINDOW
CREATE STREAM cpu_stream_agg_rel_6 INTO stream_db_fvt_out1.cpu_avg_agg6 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY hostname, STATE_WINDOW(usage_idle);
-- 7. cannot create stream without any agg function (group by hostname only)
CREATE STREAM cpu_stream_agg_rel_7 INTO stream_db_fvt_out1.cpu_avg_agg7 WITH OPTIONS(low_latency='on',recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='0',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM stream_db_fvt.cpu_agg WHERE usage_system>0 GROUP BY hostname;

select pg_sleep(1);

INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:00:30.123456789', 58, 10, 24, 61, 22, 63, 6, 44, 80, 38, 'host_1', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:00:40.123456789', 58, 55, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:00:50.123456789', 58, 66, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:01:00.123456789', 58, 12, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:01:05.123456789', 58, 0, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:01:10.123456789', 58, 77, 25, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:01:20.123456789', 58, 88, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg values ('2023-05-31 10:10:00.123456789', 58, 3, 25, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
select pg_sleep(0.5);
INSERT INTO stream_db_fvt.cpu_agg
values ('2023-05-31 10:20:00', 58, 3, 25, 61, 22, 63, 6, 44, 80, 38, 'host_1', '', '', '', '', '', '', '', '', '');

INSERT INTO stream_db_fvt.cpu_agg
values ('2023-05-31 10:30:00', 58, 3, 25, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');

-- expired records
INSERT INTO stream_db_fvt.cpu_agg
values ('2023-05-31 10:00:26', 58, 44, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');
INSERT INTO stream_db_fvt.cpu_agg
values ('2023-05-31 10:00:26', 58, 44, 24, 61, 22, 63, 6, 44, 80, 38, 'host_1', '', '', '', '', '', '', '', '', '');
select pg_sleep(0.5);
-- Immediate result
select * from stream_db_fvt_out.cpu_avg_agg1 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg2 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg3 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg4 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg5 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg6 order by w_begin, hostname;
select * from stream_db_fvt_out.cpu_avg_agg7 order by w_begin, hostname;
select pg_sleep(1);
select * from stream_db_fvt_out.cpu_avg_agg8 order by hostname, w_begin;
select * from stream_db_fvt_out.cpu_avg_agg9 order by hostname, w_begin;

select * from stream_db_fvt_out1.cpu_avg_agg1 order by w_begin, hostname;
select * from stream_db_fvt_out1.cpu_avg_agg2 order by w_begin, hostname;
select * from stream_db_fvt_out1.cpu_avg_agg3 order by w_begin, hostname;
select * from stream_db_fvt_out1.cpu_avg_agg4 order by w_begin, hostname;
select * from stream_db_fvt_out1.cpu_avg_agg5 order by w_begin, hostname;
select * from stream_db_fvt_out1.cpu_avg_agg6 order by w_begin, hostname;
select * from stream_db_fvt_out1.cpu_avg_agg7 order by w_begin, hostname;

-- Final result
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