drop database if exists pipe_db_fvt cascade;
create ts database pipe_db_fvt;

drop database if exists pipe_db_fvt1 cascade;
create ts database pipe_db_fvt1;

drop database if exists pipe_db_fvt2 cascade;
create ts database pipe_db_fvt2;

create table defaultdb.mock_sink_to_db (topic string not null,kind string,db_name string,tb_name string,key int8,data string);

-- create pipe on non-existent relation
create pipe test_pipe1 for table pipe_db_fvt.cpu(k_timestamp, usage_user,hostname) where usage_user > 50 with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1');

create table pipe_db_fvt.cpu
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

create pipe test_pipe1 for table pipe_db_fvt.cpu(k_timestamp, usage_user,hostname)
       where (usage_user > 50 or abs(usage_user) < 99 and usage_steal is not null or usage_softirq = true)
             and (hostname = 'host_1' or hostname = 'host_2')
             and (k_timestamp >= '2023-06-01 10:10:00' and k_timestamp < '2023-06-01 20:20:00')
       with options (enable='off',buffer_size='10',ignore_history='off',sink='kafka://localhost:9095?topic_name=test_topic1');

create pipe test_pipe1_1 for table pipe_db_fvt.cpu(k_timestamp, usage_user,hostname)
       where usage_user > 50 or abs(usage_user) < 99 and usage_steal is not null and usage_softirq = true
             and hostname = 'host_1' or hostname = 'host_2' or os = 'Ubuntu'
       with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1_1');

create pipe test_pipe1_2 for table pipe_db_fvt.cpu(k_timestamp, usage_user,hostname)
       where usage_user > 50 or abs(usage_user) < 99 or usage_steal is not null or usage_softirq = true
             and hostname = 'host_1' or hostname = 'host_2' and os = 'Ubuntu'
       with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1_2');

create pipe test_pipe1_3 for table pipe_db_fvt.cpu(k_timestamp, usage_user,hostname)
       where (usage_user > 50 or abs(usage_user) < 99 and usage_steal is not null or usage_softirq = true)
             and (hostname = 'host_1' or hostname = 'host_2')
       with options (sink='kafka://localhost:9095?topic_name=test_topic1_3');

create pipe test_pipe1_4 for table pipe_db_fvt.cpu(k_timestamp, usage_user,hostname)
       where (usage_user > 50 or abs(usage_user) < 99 and usage_steal is not null or usage_softirq = true)
             and (hostname = 'host_1' or hostname = 'host_2')
       with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1_4', message_format='postgresql');

create pipe test_pipe1_5 for table pipe_db_fvt.cpu(k_timestamp, usage_user,hostname)
       where (usage_user > 50 or abs(usage_user) < 99 and usage_steal is not null or usage_softirq = true)
             and (hostname = 'host_1' or hostname = 'host_2')
             between k_timestamp >= '2023-06-01 10:10:00' and k_timestamp < '2023-06-01 20:20:00'
       with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1_5');

create pipe test_pipe1_6 for table pipe_db_fvt.cpu(k_timestamp, usage_user,hostname)
       where time_bucket(k_timestamp,'6h') < '2023-06-01 20:20:00' and abs(usage_user) < 99
       with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1_6');

create pipe test_pipe1_7 for table pipe_db_fvt.cpu(k_timestamp1,usage_user,hostname)
       with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1_err');

create pipe test_pipe1_8 for table pipe_db_fvt.cpu(k_timestamp,usage_user,hostname)
       where usage_user1 < 99
       with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1_err');

create pipe test_pipe1_9 for table pipe_db_fvt.cpu(k_timestamp,usage_user,hostname)
       where hostname SIMILAR TO 'host_1' and k_timestamp>'-1001-01-01 00:00:00'
       with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1_err');

select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipe test_pipe1];

--- enable/disable pipe
alter pipe test_pipe1 set options(enable='On');
alter pipe test_pipe1 set options(enable='oN');
alter pipe test_pipe1 set options(enable='OfF');
alter pipe test_pipe1 set options(enable='aaa');

--- alter sink
alter pipe test_pipe1 set options(sink='kafka://localhost:9095?topic_name=test_topic_alter1');
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipe test_pipe1];

alter pipe test_pipe1 set options(sink='kafka://localhost:9095?topic_name=test_topic_alter2', enable='ON');

--- alter message format
alter pipe test_pipe1 set options(message_format='json');
alter pipe test_pipe1 set options(message_format='jSON');
alter pipe test_pipe1 set options(message_format='zzz');

--- alter buffer size
alter pipe test_pipe1 set options(buffer_size='');
alter pipe test_pipe1 set options(buffer_size='-1');
alter pipe test_pipe1 set options(buffer_size='a');
alter pipe test_pipe1 set options(buffer_size=2);
alter pipe test_pipe1 set options(buffer_size='0');
alter pipe test_pipe1 set options(buffer_size='1');

--- alter ignore history
alter pipe test_pipe1 set options(ignore_history='');
alter pipe test_pipe1 set options(ignore_history='a');
alter pipe test_pipe1 set options(ignore_history='on');

create pipe test_pipe2 for table pipe_db_fvt.cpu(*) where usage_user != 50 with options (enable = 'off',sink='kafka://localhost:9095?topic_name=test_topic2');
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipe test_pipe2];

create pipe test_pipe3 for table pipe_db_fvt.cpu(*) where abs(usage_user) > 50 and usage_softirq = true with options (enable = 'off',sink = 'kafka://localhost:9095?topic_name=test_topic3');
select name,
       table_name,
       filter,
       options,
       low_watermark
from [show pipe test_pipe3];

create pipe test_pipe4 for table pipe_db_fvt.cpu(*) where abs(usage_user + usage_system) + abs(usage_system + 1) = 50 with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic4');
select name,
       table_name,
       filter,
       options,
       low_watermark
from [show pipe test_pipe4];

create pipe test_pipe5 for table pipe_db_fvt.cpu(*) where usage_user is null with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic5');
select name,
       table_name,
       filter,
       options,
       low_watermark
from [show pipe test_pipe5];

create pipe test_pipe6 for table pipe_db_fvt.cpu(*) where usage_user is not null with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic6');
select name,
       table_name,
       filter,
       options,
       low_watermark
from [show pipe test_pipe6];

create pipe test_pipe6_1 for table pipe_db_fvt.cpu(*) with enable='off',sink='kafka://localhost:9095?topic_name=Test_TopiC6_1';
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark
from [show pipe test_pipe6_1];

create pipe test_pipe6_2 for table pipe_db_fvt.cpu(*) with eNabLe='off',sInK='kafka://localhost:9095?topic_name=test_topic6_2';

--- create pipe with duplicate name
create pipe test_pipe1 for table pipe_db_fvt.cpu(k_timestamp, usage_user ,hostname) where usage_user > 50 with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1');

--- create pipe with non-existent column
create pipe test_pipe7 for table pipe_db_fvt.cpu(usage_user1) where usage_user is not null with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1');
create pipe test_pipe8 for table pipe_db_fvt.cpu(*) where usage_user1 != 50 with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1');
create pipe test_pipe9 for table pipe_db_fvt.cpu(*) where abs(usage_user1) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic1';

--- create pipe with special condition
create pipe test_pipe9_1 for table pipe_db_fvt.cpu(*) where 1=2 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic9_1';
create pipe test_pipe9_2 for table pipe_db_fvt.cpu(*) where 1=1 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic9_2';

--- create pipe with supported functions
create pipe test_pipe9_3 for table pipe_db_fvt.cpu(*) where round(usage_softirq_f) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic9_3';
create pipe test_pipe9_4 for table pipe_db_fvt.cpu(*) where mod(usage_user,2) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic9_4';
create pipe test_pipe9_5 for table pipe_db_fvt.cpu(*) where ceiling(usage_user) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic9_5';
create pipe test_pipe9_6 for table pipe_db_fvt.cpu(*) where floor(usage_user) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic9_6';
--- create pipe with supported functions and wrong input parameter type
create pipe test_pipe9_7 for table pipe_db_fvt.cpu(*) where round(usage_user) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic9_3';
create pipe test_pipe9_8 for table pipe_db_fvt.cpu(*) where mod(usage_user) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic9_8';

--- create pipe with unsupported functions
create pipe test_pipe9_11 for table pipe_db_fvt.cpu(*) where max(usage_user) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic1';
create pipe test_pipe9_11 for table pipe_db_fvt.cpu(*) where min(usage_user) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic1';
create pipe test_pipe9_11 for table pipe_db_fvt.cpu(*) where sum(usage_user) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic1';
create pipe test_pipe9_11 for table pipe_db_fvt.cpu(*) where avg(usage_user) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic1';
create pipe test_pipe9_11 for table pipe_db_fvt.cpu(*) where first(usage_user) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic1';
create pipe test_pipe9_11 for table pipe_db_fvt.cpu(*) where last(usage_user) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic1';
create pipe test_pipe9_11 for table pipe_db_fvt.cpu(*) where count(usage_user) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic1';
create pipe test_pipe9_11 for table pipe_db_fvt.cpu(*) where count(distinct usage_user) >= 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic1';
create pipe test_pipe9_11 for table pipe_db_fvt.cpu(*) where cast(usage_user as bool) = true with enable='off',sink='kafka://localhost:9095?topic_name=test_topic1';
create pipe test_pipe9_11 for table pipe_db_fvt.cpu(*) where now()>timestamp'1970-1-1 00:00:00' with enable='off',sink='kafka://localhost:9095?topic_name=test_topic1';
create pipe test_pipe9_11 for table pipe_db_fvt.cpu(*) where now()>k_timestamp with enable='off',sink='kafka://localhost:9095?topic_name=test_topic1';

-- create pipe with wrong parameters
create pipe test_pipe10 for table pipe_db_fvt.cpu(*) where abs(usage_user1) <= 50 with options (enable1='off',sink='kafka://localhost:9095?topic_name=test_topic1');
create pipe test_pipe10 for table pipe_db_fvt.cpu(*) where abs(usage_user) <= 50 with options (enable1='off',sink='kafka://localhost:9095?topic_name=test_topic1');
create pipe test_pipe11 for table pipe_db_fvt.cpu(*) where abs(usage_user1) != 50 with options (enable='off',sink1='kafka://localhost:9095?topic_name=test_topic1');
create pipe test_pipe11 for table pipe_db_fvt.cpu(*) where abs(usage_user) != 50 with options (enable='off',sink1='kafka://localhost:9095?topic_name=test_topic1');
create pipe test_pipe12 for table pipe_db_fvt.cpu(*) where abs(usage_user) + abs(usage_system) = 50 with Enable='off';
create pipe test_pipe13 for table pipe_db_fvt.cpu(*) where abs(usage_user) + abs(usage_system) = 50 with Sink='kafka://localhost:9095?topic_name=test_topic1';
create pipe test_pipe14 for table pipe_db_fvt.cpu(*) where abs(usage_user) + abs(usage_system) = 50 with Message_Format='json';

-- create pipe with non-existent function
create pipe test_pipe15 for table pipe_db_fvt.cpu(*) where abccc(usage_user) + bcaaa(usage_system) = 50 with enable='off',sink='kafka://localhost:9095?topic_name=test_topic15';

-- verification
select name from [show pipes];
--- drop non-existent pipe
DROP PIPE IF EXISTS test_pipe15;

-- alter target table and filters
alter pipe test_pipe1 set table pipe_db_fvt.cpu(usage_user,usage_system,hostname,k_timestamp) where usage_system >= 20;
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipe test_pipe1];

alter pipe test_pipe1 set table pipe_db_fvt.cpu(*) where abs(usage_system) < 50;
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipe test_pipe1];

alter pipe test_pipe1 set table pipe_db_fvt.cpu(*) where abs(usage_idle) + abs(usage_system) <= 50;
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipe test_pipe1];

alter pipe test_pipe1 set table pipe_db_fvt.cpu(*) where usage_system is null;
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipe test_pipe1];

alter pipe test_pipe1 set table pipe_db_fvt.cpu(*) where usage_system is not null;
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipe test_pipe1];

alter pipe test_pipe1 set table pipe_db_fvt.cpu(usage_user);
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipe test_pipe1];

alter pipe test_pipe1 set table pipe_db_fvt.cpu(*);
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipes]
where name ='test_pipe1';

--- alter with wrong table or columns
alter pipe test_pipe1 set table pipe_db_fvt.cpu(usage_user1) where usage_user is not null;
alter pipe test_pipe1 set table pipe_db_fvt.cpu(*) where usage_user1 > 50;
alter pipe test_pipe1 set table pipe_db_fvt.cpu(*) where abs(usage_user1) > 50;
alter pipe test_pipe1 set table pipe_db_fvt.cpu(*) where abs(usage_user1) > 50 and usage_system <= -10;

--- alter with case-insensitive on/off option.
alter pipe test_pipe1 set options (enable='ofF',sink='kafka://localhost:9095?topic_name=Test_topic1');
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipes]
where name ='test_pipe1';

alter pipe test_pipe1 set enable='oFf',sink='kafka://localhost:9095?topic_name=tesT_Topic1';

alter pipe test_pipe1 set options (enable='oN',sink='kafka://localhost:9095?topic_name=tesT_topic1');
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipes]
where name ='test_pipe1';
alter pipe test_pipe1 set enable='ON',sink='kafka://localhost:9095?topic_name=test_Topic1';

--- alter with wrong kafka connection
alter pipe test_pipe1 set options (enable='off',sink='kafka1://localhost:9095?topic_name=test_topic1');
alter pipe test_pipe1 set enable='off',sink='aaa://localhost:9095?topic_name=test_topic1';
-- need fix
-- alter pipe test_pipe1 set options (enable='off',sink='kafka://:9092?topic_name=test_topic1');
alter pipe test_pipe1 set enable='off',sink='kafka://localhost:9095?topic_name=';
alter pipe test_pipe1 set options (enable='off',sink='kafka://localhost:9095?topic_name');
alter pipe test_pipe1 set enable1='off',sink='kafka://localhost:9095?topic_name=test_topic1';
alter pipe test_pipe1 set options (enable='off',sink1='kafka://localhost:9095?topic_name=test_topic1');

-- verification
select name from [show pipes];

drop pipe test_pipe2;
--- drop non-existent pipe
drop pipe test_pipe2;
drop pipe IF EXISTS test_pipe2;

-- verification
select name from [show pipes];

create table pipe_db_fvt.cpu1
(
    k_timestamp      timestamp not null,
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

create pipe test_pipe21 for table pipe_db_fvt.cpu1(*) where usage_user > 50 with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1');
create pipe test_pipe22 for table pipe_db_fvt.cpu1(*) where usage_user > 50 with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1');

drop table pipe_db_fvt.cpu1;

drop pipe test_pipe21;
drop pipe test_pipe22;

drop table pipe_db_fvt.cpu1;

-- verification
select name from [show pipes];

create table pipe_db_fvt.cpu2
(
    k_timestamp      timestamp not null,
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
    region char(30) not null,
    datacenter char(30) not null,
    rack char(30),
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname,region,datacenter);

create pipe test_pipe21 for table pipe_db_fvt.cpu2(*)
       where ((hostname = 'host_1' OR hostname = 'host_2')) AND (region = 'beijing' OR region = 'shanghai')
             AND (((usage_user > 50 OR usage_user < 90) AND (abs(usage_system) > 30 OR abs(usage_system) <80)))
       with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1');

create pipe test_pipe22 for table pipe_db_fvt.cpu2(*)
       where hostname = 'host1' OR region = 'beijing' AND usage_user > 50
       with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1');

--- add/drop/rename metrics column
alter table pipe_db_fvt.cpu2 drop column usage_guest;
alter table pipe_db_fvt.cpu2 add column usage_guest_a int;
alter table pipe_db_fvt.cpu2 rename column usage_irq to usage_irq1;

--- add/drop/rename tag column
alter table pipe_db_fvt.cpu2 add tag a1 timestamp;
alter table pipe_db_fvt.cpu2 drop tag service_environment;
alter table pipe_db_fvt.cpu2 rename tag team to team1;

--- alter column type
alter table pipe_db_fvt.cpu2 alter column usage_steal type int;
alter table pipe_db_fvt.cpu2 alter tag os type char(100);

drop pipe test_pipe21;
drop pipe test_pipe22;

--- add/drop/rename metrics column
alter table pipe_db_fvt.cpu2 drop column usage_guest;
alter table pipe_db_fvt.cpu2 add column usage_guest_b int;
alter table pipe_db_fvt.cpu2 rename column usage_irq to usage_irq1;

--- add/drop/rename tag column
alter table pipe_db_fvt.cpu2 add tag a1 int;
alter table pipe_db_fvt.cpu2 drop tag service_environment;
alter table pipe_db_fvt.cpu2 rename tag team to team1;

--- alter column type
alter table pipe_db_fvt.cpu2 alter column usage_steal type int;
alter table pipe_db_fvt.cpu2 alter tag os type char(100);

--- alter pipe using a different ts table, the low_watermark should be reset to the invalid-water mark.
update system.kwdb_pipes set low_water_mark=1000 where name='test_pipe1';
alter pipe test_pipe1 set table pipe_db_fvt.cpu2(*) where abs(usage_idle) + abs(usage_system) <= 50;
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipes]
where name ='test_pipe1';

-- drop table [cascade]
create pipe test_pipe21 for table pipe_db_fvt.cpu2(*) where usage_user > 50 with options (enable='off',sink='kafka://localhost:9095?topic_name=test_topic1');
select name,
       table_name,
       column_names,
       filter,
       options,
       low_watermark,
       status,
       create_by,
       start_time,
       end_time,
       error_message
from [show pipe test_pipe21];

drop table pipe_db_fvt.cpu2;
drop table pipe_db_fvt.cpu2 cascade;

-- verification
select name,table_name from [show pipes] where name ='test_pipe21';

-- drop database [cascade]
create table pipe_db_fvt1.cpu3
(
    k_timestamp      timestamp not null,
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

create table pipe_db_fvt1.kafka_error
(
    k_timestamp      timestamp not null,
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

insert into pipe_db_fvt1.kafka_error values('2000-01-01 00:00:00',58,2,24,61,22,63,6,44,80,38,'host_1','eu-central-1','eu-central-1a','6','Ubuntu15.10','x86','SF','19','1','host_0');


create pipe test_pipe31 for table pipe_db_fvt1.cpu3(*) where usage_user > 50 with options (
       enable='off',sink='mock://localhost:9095?topic_name=test_topic31');
create pipe test_pipe32 for table pipe_db_fvt1.cpu3(*) where usage_user > 50 with options (
       enable='on',buffer_size='0',sink='mock://root@127.0.102.145:26257?topic_name=test_topic32&mock_sink_to_db=defaultdb.mock_sink_to_db');
create pipe test_pipe33 for table pipe_db_fvt1.cpu3(*) where usage_user > 50 and os = 'Ubuntu15.10' with options (
       enable='on',buffer_size='0',sink='mock://root@127.0.102.145:26257?topic_name=test_topic33&mock_sink_to_db=defaultdb.mock_sink_to_db');
-- mock kafka send realtime error
create pipe test_pipe34 for table pipe_db_fvt1.kafka_error with options (
       enable='on',buffer_size='0',sink='mock://root@127.0.102.145:26257?topic_name=test_topic34&mock_sink_to_db=defaultdb.mock_sink_to_db');
-- mock kafka send history error
create pipe test_pipe35 for table pipe_db_fvt1.kafka_error with options (enable='on',buffer_size='0',ignore_history='off',
       sink='mock://root@127.0.102.145:26257?topic_name=test_topic35&mock_sink_to_db=defaultdb.mock_sink_to_db');
select pg_sleep(4);
insert into pipe_db_fvt1.cpu3 values('2000-01-01 00:00:00',58,2,24,61,22,63,6,44,80,38,'host_1','eu-central-1','eu-central-1a','6','Ubuntu15.10','x86','SF','19','1','host_0');
insert into pipe_db_fvt1.cpu3 (k_timestamp, usage_user, usage_system, usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice,hostname) values
('2000-01-02 00:00:00',58,2,24,61,22,63,6,44,80,38,'host_0'),('2000-01-03 00:00:00',1,1,1,1,1,1,1,1,1,1,'host_1'),('2000-01-04 00:00:00',55,1,1,1,1,1,1,1,1,1,'host_2'),('2000-01-05 00:00:00',3,3,3,3,3,3,3,3,3,3,'host_2');
insert into pipe_db_fvt1.kafka_error values('2000-01-01 00:01:00',58,2,24,61,22,63,6,44,80,38,'host_1','eu-central-1','eu-central-1a','6','Ubuntu15.10','x86','SF','19','1','host_0');
select pg_sleep(2);
drop database pipe_db_fvt1;
drop pipe test_pipe31;
drop pipe test_pipe32;
drop pipe test_pipe33;
select status,error_message from [show pipes] where name in ('test_pipe34','test_pipe35');
drop pipe test_pipe34;
drop pipe test_pipe35;
drop database pipe_db_fvt1 cascade;

select topic,kind,db_name,tb_name,data from defaultdb.mock_sink_to_db
where topic in ('test_topic32','test_topic33','test_topic34','test_topic35') order by topic,key;

-- verification
select name from [show pipes];

--public cluster settings
select * from [show cluster settings] where variable like 'ts.pipe%' or variable like 'ts.cdc%';
set cluster setting ts.cdc.max_active_number=20;
set cluster setting ts.cdc.max_active_number=-1;
set cluster setting ts.pipe.sink_max_retries=12;
set cluster setting ts.pipe.sink_max_retries=-1;

-- internal cluster settings;
show cluster setting ts.pipe.heartbeat.interval;
set cluster setting ts.pipe.heartbeat.interval="1s";
set cluster setting ts.pipe.heartbeat.interval=0;
select * from [show cluster settings] where variable like 'ts.pipe%' or variable like 'ts.cdc%';

drop pipe test_pipe1_1;
drop pipe test_pipe1_2;
drop pipe test_pipe1_9;
drop pipe test_pipe1;
drop pipe test_pipe1_1;
drop pipe test_pipe1_2;
drop pipe test_pipe1_9;
drop pipe test_pipe3;
drop pipe test_pipe4;
drop pipe test_pipe5;
drop pipe test_pipe6;
drop pipe test_pipe6_1;
drop pipe test_pipe6_2;
drop pipe test_pipe9_1;
drop pipe test_pipe9_2;
drop pipe test_pipe9_3;
drop pipe test_pipe9_4;
drop pipe test_pipe9_5;
drop pipe test_pipe9_6;
drop pipe test_pipe21;

select name,table_name,status from [show pipes];
-- cleanup
drop database if exists pipe_db_fvt cascade;
drop database if exists pipe_db_fvt1 cascade;
drop database if exists pipe_db_fvt2 cascade;
drop table defaultdb.mock_sink_to_db cascade;
select pg_sleep(10);