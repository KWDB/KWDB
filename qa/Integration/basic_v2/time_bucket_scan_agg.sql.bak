drop database if exists test cascade;
create ts database test;

create table test.cpu
(
    k_timestamp      timestamp not null,
    usage_user       bigint,
    usage_system     bigint,
    usage_idle       int,
    usage_nice       int,
    usage_iowait     smallint,
    usage_irq        smallint,
    usage_softirq    float,
    usage_steal      float,
    usage_guest      real,
    usage_guest_nice real
) attributes (
    hostname char(30) not null,
    region varchar(8) not null,
    datacenter int not null,
    rack int not null,
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname, region, datacenter, rack);

INSERT INTO test.cpu
values ('2023-05-31 10:00:00', -2147483648, -2147483648, -2147483648, -2147483648, -32768, -32768, 54.122111, 54.122111, 3.141593, 3.141593, 'host_0', 'aaaaaaaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', -2147483648, -2147483648, -2147483648, -2147483648, -32768, -32768, 54.122111, 54.122111, 3.141593, 3.141593, 'host_0', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 84, 2147483647, -2147483648, 2147483647, 29, 20, -54.122111, -54.122111, 53, 74, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 29, 48, 5, 63, -17, 52, 60, null, 93, 1, 'host_2', 'a', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 8, 21, -89, 78, 30, 81, 33, 24, 24, 82, 'host_3', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 2, 26, 64, 6, 38, 20, -71, 19, 40, 54, 'host_4', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', -76, -40, -63, -7, -81, -20, -29, -55, null, -15, 'host_5', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 44, 70, 20, -67, 65, 11, 7, 92, 0, 31, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 92, 35, 99, 9, 31, -1, 2, 24, 96, 69, 'host_7', 'aaa', -888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 90, 0, 81, 28, 25, -44, 8, -89, 11, 76, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:01', 2147483647, -2147483648, 2147483647, -2147483648, 32767, -32768, -100.1, 54.122111, -3.141593, null, 'host_0', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', null, 2147483647, 2147483647, 2147483647, 32767, 32767, 54.122111, -54.1221111, 3.141593, -3.141593, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 29, 48, 5, 63, 17, 52, 60, 49, 93, 1, 'host_3', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 8, 21, 89, 78, 30, 81, 33, 24, 24, 82, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 2, 26, 64, 6, 38, 20, 71, null, 40, 54, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 76, 40, 63, 7, 81, 20, 29, -55, 20, 15, 'host_6', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_7', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 92, 35,-99, 9, 31, 1, 2, 24, 96, 69, 'host_8', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 21, 77, 90, 83, 41, 84, 26, 60, null, 36, 'host_9', 'aaa', -888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 90, 0, -81, 28, 25, 44, 8, 89, 11, 76, 'host_2', 'a', -888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:02', 58, -2, 24, 61, 22, 63, 6, 44, null, 38, 'host_2', 'aaa', 2147483647, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_3', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 29, 48, -5, 63, -17, 52, 60, 49, 93, 1, 'host_4', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 8, 21, 89, -78, 30, 81, 33, 24, 24, 82, 'host_5', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 2, 26, -64, 6, -38, 20, 71, null, 40, 54, 'host_6', 'aaa', 2147483647, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_7', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_8', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 92, 35, 99, -9, 31, 1, -2, 24, 96, 69, 'host_9', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 21, 77, -90, 83, 41, 84, 26, 60, 43, null, 'host_0', 'aaaaaaaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 90, 0, 81, 28, 25, 44, 8, -89, 11, 76, 'host_1', 'aaa', 2147483647, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:04', null, 2, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 29, 48, 5, -63, 17, -52, 60, 49, 93, 1, 'host_5', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 8, 21, 89, 78, 30, -81, 33, 24, 24, 82, 'host_6', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 2, 26, -64, 6, 38, -20, 71, null, 40, 54, 'host_7', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_8', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_9', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 92, 35, 99, 9, 31, -1, 2, 24, 96, 69, 'host_0', 'aaaaaaaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_1', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 90, 0, 81, 28, 25, 44, 8, -89, 11, 76, 'host_2', 'aaa', -2147483648, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:03', 58, null, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 29, 48, 5, 63, 17, 52, 60, 49, 93, 1, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 8, 21, 89, 78, 30, 81, 33, 24, 24, 82, 'host_6', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 2, 26, 64, 6, 38, -20, 71, null, 40, 54, 'host_7', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 44, 70, 20, 67, 65, 11, -7, 92, 0, 31, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 92, 35, 99, 9, 31, 1, 2, 24, 96, 69, 'host_0', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 90, 0, 81, 28, 25, 44, 8, 89, 11, 76, 'host_2', 'aaa', 888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_3', 'aaaaaaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_7', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_0', 'aaa', 0, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_2', 'aaa', 888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_3', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_5', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_7', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_8', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_0', 'aaa', -888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_2', 'aaa', 888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:10', 58, null, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 29, 48, 5, -63, 17, 52, 60, 49, 93, 1, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 8, 21, 89, -78, 30, 81, 33, 24, 24, 82, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 2, 26, 64, 6, 38, -20, 71, null, -40, 54, 'host_7', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 76, 40, -63, 7, 81, -20, 29, 55, 20, 15, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 44, 70, 20, 67, 65, 11, 7, -92, 0, 31, 'host_9', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 92, 35, 99, 9, -31, 1, 2, 24, 96, 69, 'host_0', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 21, 77, 90, 83, 41, -84, 26, 60, 43, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 90, 0, 81, 28, 25, -44, 8, 89, 11, 76, 'host_2', 'aaa', -888888, 6666, '', '', '', '', '', '');

SELECT hostname, max(usage_user) as max_cur, min(usage_user) as min_cur, avg(usage_user) as avg_cur
FROM test.cpu
where 1 = 1
  and hostname = 'host_4'
  and k_timestamp > '2023-01-01 00:00:00'::timestamp and k_timestamp < '2024-01-01 00:00:00':: timestamp
group by hostname
order by hostname asc;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       lastts(usage_guest),
       sum(usage_steal),
       firstts(usage_nice),
       max(usage_user),
       max(usage_irq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       first_row(usage_idle),
       lastts(usage_nice),
       lastts(usage_irq),
       lastts(usage_guest_nice),
       max(usage_idle),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_1'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       sum(usage_guest),
       count(usage_idle),
       avg(usage_system),
       max(usage_steal),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaa'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       last_row(usage_iowait),
       avg(usage_irq),
       max(usage_system),
       firstts(usage_irq),
       lastts(usage_softirq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaaaaaaa'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       first_row(usage_irq),
       lastts(usage_iowait),
       sum(usage_iowait),
       hostname,
       first_row(usage_system),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND region = 'aaa'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b, first (usage_nice), min (usage_system), min (usage_softirq), last_row(usage_guest_nice), hostname, lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
  AND region = 'aaaaaaaa'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       avg(usage_steal),
       avg(usage_guest_nice),
       last_row(usage_guest),
       min(usage_idle),
       hostname,
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts LIMIT 10;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       sum(usage_nice),
       sum(usage_idle),
       min(usage_irq),
       sum(usage_system),
       last_row(usage_user),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_1'
  AND region = 'aaa'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts
OFFSET 2;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       min(usage_iowait),
       last_row(usage_irq),
       count(usage_nice),
       last_row(usage_idle),
       last_row(usage_system),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaa'
  AND datacenter = 888888
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       first_row(usage_iowait),
       last_row(usage_steal),
       min(usage_user),
       last_row(usage_nice),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaa'
  AND datacenter = 888888
  AND rack = 8888.88
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       sum(usage_guest_nice),
       sum(usage_irq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       min(usage_nice),
       firstts(usage_user),
       min(usage_guest), first (usage_idle), first (usage_softirq),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       count(usage_iowait), last (usage_guest_nice), sum (usage_softirq), first_row(usage_user),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts
    LIMIT 1
OFFSET 1;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       firstts(usage_iowait), last (usage_user), min (usage_steal),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       first_row(usage_guest),
       firstts(usage_system),
       lastts(usage_system),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname, first (usage_steal), count (usage_system), firstts(usage_steal), lastts(usage_steal), first_row(usage_steal),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname, first (usage_guest_nice), last (usage_guest), first (usage_iowait), firstts(usage_guest),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       lastts(usage_user),
       firstts(usage_guest_nice), last (usage_softirq), last_row(usage_softirq),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       firstts(usage_idle),
       first_row(usage_guest_nice),
       avg(usage_user),
       count(usage_softirq),
       count(usage_steal),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       max(usage_nice), first (usage_system), avg (usage_idle), last (usage_irq), first (usage_irq),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       max(usage_guest_nice),
       lastts(usage_idle),
       max(usage_guest),
       avg(usage_nice), first (usage_guest),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       min(usage_guest_nice),
       max(usage_softirq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname, last (usage_steal), count (usage_user), avg (usage_iowait), count (usage_guest),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       count(usage_guest_nice),
       firstts(usage_softirq), last (usage_nice), sum (usage_user), max (usage_iowait),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT count(usage_irq),
       time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       avg(usage_softirq),
       first_row(usage_softirq),
       first_row(usage_nice), last (usage_iowait),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;


-- explain plan start

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b, hostname, lastts(usage_guest), sum(usage_steal), firstts(usage_nice), max(usage_user), max(usage_irq), lastts(usage_user) as last_ts
FROM test.cpu WHERE k_timestamp >= '2023-01-01 00:00:00' AND k_timestamp < '2024-01-01 00:00:00' AND hostname = 'host_0' GROUP BY hostname, region, datacenter, rack, k_timestamp_b ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b, hostname, first_row(usage_idle), lastts(usage_nice), lastts(usage_irq), lastts(usage_guest_nice), max(usage_idle), lastts(usage_user) as last_ts FROM test.cpu WHERE k_timestamp >= '2023-01-01 00:00:00' AND k_timestamp < '2024-01-01 00:00:00' AND hostname = 'host_1' GROUP BY hostname, region, datacenter, rack, k_timestamp_b ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b, hostname, sum(usage_guest), count(usage_idle), avg(usage_system), max(usage_steal), lastts(usage_user) as last_ts FROM test.cpu WHERE k_timestamp >= '2023-01-01 00:00:00' AND k_timestamp < '2024-01-01 00:00:00' AND hostname = 'host_0' AND region = 'aaa' GROUP BY hostname, region, datacenter, rack, k_timestamp_b ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b, hostname, last_row(usage_iowait), avg(usage_irq), max(usage_system), firstts(usage_irq), lastts(usage_softirq), lastts(usage_user) as last_ts FROM test.cpu WHERE k_timestamp >= '2023-01-01 00:00:00' AND k_timestamp < '2024-01-01 00:00:00' AND hostname = 'host_0' AND region = 'aaaaaaaa' GROUP BY hostname, region, datacenter, rack, k_timestamp_b ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,first_row(usage_irq),lastts(usage_iowait),sum(usage_iowait),hostname,first_row(usage_system),lastts(usage_user) as last_ts FROM test.cpu WHERE k_timestamp >= '2023-01-01 00:00:00' AND k_timestamp < '2024-01-01 00:00:00' AND region = 'aaa' GROUP BY hostname, region, datacenter, rack, k_timestamp_b ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b, first (usage_nice), min (usage_system), min (usage_softirq), last_row(usage_guest_nice), hostname, lastts(usage_user) as last_ts FROM test.cpu WHERE k_timestamp >= '2023-01-01 00:00:00' AND k_timestamp < '2024-01-01 00:00:00' AND region = 'aaaaaaaa' GROUP BY hostname, region, datacenter, rack, k_timestamp_b ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b, avg(usage_steal), avg(usage_guest_nice), last_row(usage_guest), min(usage_idle), hostname, lastts(usage_user) as last_ts FROM test.cpu WHERE k_timestamp >= '2023-01-01 00:00:00' AND k_timestamp < '2024-01-01 00:00:00' GROUP BY hostname, region, datacenter, rack, k_timestamp_b ORDER BY hostname, k_timestamp_b, last_ts LIMIT 10;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       sum(usage_nice),
       sum(usage_idle),
       min(usage_irq),
       sum(usage_system),
       last_row(usage_user),
       lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_1'
  AND region = 'aaa'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts
OFFSET 2;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       min(usage_iowait),
       last_row(usage_irq),
       count(usage_nice),
       last_row(usage_idle),
       last_row(usage_system),
       lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaa'
  AND datacenter = 888888
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       first_row(usage_iowait),
       last_row(usage_steal),
       min(usage_user),
       last_row(usage_nice),
       lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaa'
  AND datacenter = 888888
  AND rack = 8888.88
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       sum(usage_guest_nice),
       sum(usage_irq),
       lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       min(usage_nice),
       firstts(usage_user),
       min(usage_guest), first (usage_idle), first (usage_softirq),
    lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       count(usage_iowait), last (usage_guest_nice), sum (usage_softirq), first_row(usage_user),
    lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts
    LIMIT 1
OFFSET 1;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       firstts(usage_iowait), last (usage_user), min (usage_steal),
    lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       first_row(usage_guest),
       firstts(usage_system),
       lastts(usage_system),
       lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname, first (usage_steal), count (usage_system), firstts(usage_steal), lastts(usage_steal), first_row(usage_steal),
    lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname, first (usage_guest_nice), last (usage_guest), first (usage_iowait), firstts(usage_guest),
    lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       lastts(usage_user),
       firstts(usage_guest_nice), last (usage_softirq), last_row(usage_softirq),
    lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       firstts(usage_idle),
       first_row(usage_guest_nice),
       avg(usage_user),
       count(usage_softirq),
       count(usage_steal),
       lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       max(usage_nice), first (usage_system), avg (usage_idle), last (usage_irq), first (usage_irq),
    lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       max(usage_guest_nice),
       lastts(usage_idle),
       max(usage_guest),
       avg(usage_nice), first (usage_guest),
    lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, datacenter, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       min(usage_guest_nice),
       max(usage_softirq),lastts(usage_user) as last_ts FROM test.cpu WHERE k_timestamp >= '2023-01-01 00:00:00' AND k_timestamp < '2024-01-01 00:00:00' GROUP BY hostname, region, datacenter, rack, k_timestamp_b ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b, hostname, last (usage_steal), count (usage_user), avg (usage_iowait), count (usage_guest), lastts(usage_user) as last_ts FROM test.cpu WHERE k_timestamp >= '2023-01-01 00:00:00' AND k_timestamp < '2024-01-01 00:00:00' GROUP BY hostname, region, datacenter, rack, k_timestamp_b ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b, hostname, count(usage_guest_nice), firstts(usage_softirq), last (usage_nice), sum (usage_user), max (usage_iowait), lastts(usage_user) as last_ts FROM test.cpu WHERE k_timestamp >= '2023-01-01 00:00:00' AND k_timestamp < '2024-01-01 00:00:00' GROUP BY hostname, region, datacenter, rack, k_timestamp_b ORDER BY hostname, k_timestamp_b, last_ts;

explain SELECT count(usage_irq),time_bucket(k_timestamp, '2s') as k_timestamp_b,hostname,avg(usage_softirq),first_row(usage_softirq),first_row(usage_nice), last (usage_iowait), lastts(usage_user) as last_ts FROM test.cpu WHERE k_timestamp >= '2023-01-01 00:00:00' AND k_timestamp < '2024-01-01 00:00:00' GROUP BY hostname, region, datacenter, rack, k_timestamp_b ORDER BY hostname, k_timestamp_b, last_ts;

-- explain plan end

drop database test cascade;

create ts database test;

create table test.cpu
(
    k_timestamp      timestamp not null,
    usage_user       bigint,
    usage_system     bigint,
    usage_idle       int,
    usage_nice       int,
    usage_iowait     smallint,
    usage_irq        smallint,
    usage_softirq    float,
    usage_steal      float,
    usage_guest      real,
    usage_guest_nice real
) attributes (
    hostname char(30) not null,
    region varchar(8),
    datacenter int,
    rack int not null,
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname);

INSERT INTO test.cpu
values ('2023-05-31 10:00:00', -2147483648, -2147483648, -2147483648, -2147483648, -32768, -32768, 54.122111, 54.122111, 3.141593, 3.141593, 'host_0', null, 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', -2147483648, -2147483648, -2147483648, -2147483648, -32768, -32768, 54.122111, 54.122111, 3.141593, 3.141593, 'host_0', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 84, 2147483647, -2147483648, 2147483647, 29, 20, -54.122111, -54.122111, 53, 74, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 29, 48, 5, 63, -17, 52, 60, null, 93, 1, 'host_2', 'a', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 8, 21, -89, 78, 30, 81, 33, 24, 24, 82, 'host_3', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 2, 26, 64, 6, 38, 20, -71, 19, 40, 54, 'host_4', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', -76, -40, -63, -7, -81, -20, -29, -55, null, -15, 'host_5', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 44, 70, 20, -67, 65, 11, 7, 92, 0, 31, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 92, 35, 99, 9, 31, -1, 2, 24, 96, 69, 'host_7', 'aaa', -888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 90, 0, 81, 28, 25, -44, 8, -89, 11, 76, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:01', 2147483647, -2147483648, 2147483647, -2147483648, 32767, -32768, -100.1, 54.122111, -3.141593, null, 'host_0', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', null, 2147483647, 2147483647, 2147483647, 32767, 32767, 54.122111, -54.1221111, 3.141593, -3.141593, 'host_1', null, 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 29, 48, 5, 63, 17, 52, 60, 49, 93, 1, 'host_3', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 8, 21, 89, 78, 30, 81, 33, 24, 24, 82, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 2, 26, 64, 6, 38, 20, 71, null, 40, 54, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 76, 40, 63, 7, 81, 20, 29, -55, 20, 15, 'host_6', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_7', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 92, 35,-99, 9, 31, 1, 2, 24, 96, 69, 'host_8', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 21, 77, 90, 83, 41, 84, 26, 60, null, 36, 'host_9', 'aaa', -888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 90, 0, -81, 28, 25, 44, 8, 89, 11, 76, 'host_2', 'a', -888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:02', 58, -2, 24, 61, 22, 63, 6, 44, null, 38, 'host_2', 'aaa', 2147483647, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_3', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 29, 48, -5, 63, -17, 52, 60, 49, 93, 1, 'host_4', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 8, 21, 89, -78, 30, 81, 33, 24, 24, 82, 'host_5', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 2, 26, -64, 6, -38, 20, 71, null, 40, 54, 'host_6', 'aaa', 2147483647, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_7', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_8', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 92, 35, 99, -9, 31, 1, -2, 24, 96, 69, 'host_9', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 21, 77, -90, 83, 41, 84, 26, 60, 43, null, 'host_0', 'aaaaaaaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 90, 0, 81, 28, 25, 44, 8, -89, 11, 76, 'host_1', 'aaa', 2147483647, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:04', null, 2, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 29, 48, 5, -63, 17, -52, 60, 49, 93, 1, 'host_5', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 8, 21, 89, 78, 30, -81, 33, 24, 24, 82, 'host_6', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 2, 26, -64, 6, 38, -20, 71, null, 40, 54, 'host_7', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_8', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_9', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 92, 35, 99, 9, 31, -1, 2, 24, 96, 69, 'host_0', 'aaaaaaaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_1', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 90, 0, 81, 28, 25, 44, 8, -89, 11, 76, 'host_2', 'aaa', -2147483648, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:03', 58, null, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 29, 48, 5, 63, 17, 52, 60, 49, 93, 1, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 8, 21, 89, 78, 30, 81, 33, 24, 24, 82, 'host_6', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 2, 26, 64, 6, 38, -20, 71, null, 40, 54, 'host_7', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 44, 70, 20, 67, 65, 11, -7, 92, 0, 31, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 92, 35, 99, 9, 31, 1, 2, 24, 96, 69, 'host_0', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 90, 0, 81, 28, 25, 44, 8, 89, 11, 76, 'host_2', 'aaa', 888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_3', 'aaaaaaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_7', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_8', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_9', '', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_0', '', 0, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', null, null, null, null, null, null, null, null, null, null, 'host_2', 'aaa', 888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_3', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_5', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_7', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_8', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_0', 'aaa', -888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', null, null, null, null, null, null, null, null, null, null, 'host_2', 'aaa', 888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:10', 58, null, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 29, 48, 5, -63, 17, 52, 60, 49, 93, 1, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 8, 21, 89, -78, 30, 81, 33, 24, 24, 82, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 2, 26, 64, 6, 38, -20, 71, null, -40, 54, 'host_7', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 76, 40, -63, 7, 81, -20, 29, 55, 20, 15, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 44, 70, 20, 67, 65, 11, 7, -92, 0, 31, 'host_9', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 92, 35, 99, 9, -31, 1, 2, 24, 96, 69, 'host_0', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 21, 77, 90, 83, 41, -84, 26, 60, 43, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 90, 0, 81, 28, 25, -44, 8, 89, 11, 76, 'host_2', 'aaa', -888888, 6666, '', '', '', '', '', '');

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       lastts(usage_guest),
       sum(usage_steal),
       firstts(usage_nice),
       max(usage_user),
       max(usage_irq),
       lastts(usage_user) as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       first_row(usage_idle),
       lastts(usage_nice),
       lastts(usage_irq),
       lastts(usage_guest_nice),
       max(usage_idle),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_1'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       sum(usage_guest),
       count(usage_idle),
       avg(usage_system),
       max(usage_steal),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaa'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       last_row(usage_iowait),
       avg(usage_irq),
       max(usage_system),
       firstts(usage_irq),
       lastts(usage_softirq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaaaaaaa'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       first_row(usage_irq),
       lastts(usage_iowait),
       sum(usage_iowait),
       hostname,
       first_row(usage_system),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND region = 'aaa'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
    first (usage_nice), min (usage_system), min (usage_softirq), last_row(usage_guest_nice), hostname,
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
  AND region = 'aaaaaaaa'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       avg(usage_steal),
       avg(usage_guest_nice),
       last_row(usage_guest),
       min(usage_idle),
       hostname,
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts LIMIT 10;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       sum(usage_nice),
       sum(usage_idle),
       min(usage_irq),
       sum(usage_system),
       last_row(usage_user),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_1'
  AND region = 'aaa'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts
OFFSET 2;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       min(usage_iowait),
       last_row(usage_irq),
       count(usage_nice),
       last_row(usage_idle),
       last_row(usage_system),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaa'
  AND datacenter = 888888
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       first_row(usage_iowait),
       last_row(usage_steal),
       min(usage_user),
       last_row(usage_nice),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaa'
  AND datacenter = 888888
  AND rack = 8888.88
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       sum(usage_guest_nice),
       sum(usage_irq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       min(usage_nice),
       firstts(usage_user),
       min(usage_guest), first (usage_idle), first (usage_softirq),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
    last (usage_system), avg (usage_guest), last (usage_idle), first (usage_user),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       count(usage_iowait), last (usage_guest_nice), sum (usage_softirq), first_row(usage_user),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts
    LIMIT 1
OFFSET 1;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       firstts(usage_iowait), last (usage_user), min (usage_steal),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       first_row(usage_guest),
       firstts(usage_system),
       lastts(usage_system),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
    first (usage_steal), count (usage_system), firstts(usage_steal), lastts(usage_steal), first_row(usage_steal),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
    first (usage_guest_nice), last (usage_guest), first (usage_iowait), firstts(usage_guest),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       lastts(usage_user),
       firstts(usage_guest_nice), last (usage_softirq), last_row(usage_softirq),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       firstts(usage_idle),
       first_row(usage_guest_nice),
       avg(usage_user),
       count(usage_softirq),
       count(usage_steal),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       max(usage_nice), first (usage_system), avg (usage_idle), last (usage_irq), first (usage_irq),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       max(usage_guest_nice),
       lastts(usage_idle),
       max(usage_guest),
       avg(usage_nice), first (usage_guest),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       min(usage_guest_nice),
       max(usage_softirq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT count(usage_irq),
       time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       avg(usage_softirq),
       first_row(usage_softirq),
       first_row(usage_nice), last (usage_iowait),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

drop database test cascade;

drop database if exists test cascade;
create ts database test;

create table test.cpu
(
    k_timestamp      timestamp not null,
    usage_user       bigint not null,
    usage_system     bigint,
    usage_idle       int,
    usage_nice       int,
    usage_iowait     smallint,
    usage_irq        smallint,
    usage_softirq    float,
    usage_steal      float,
    usage_guest      real,
    usage_guest_nice real
) attributes (
    hostname char(30) not null,
    region varchar(8) not null,
    datacenter int,
    rack int not null,
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname, region, rack);

INSERT INTO test.cpu
values ('2023-05-31 10:00:00', 2147483647, -2147483648, -2147483648, -2147483648, -32768, -32768, 54.122111, 54.122111, 3.141593, 3.141593, 'host_0', 'aaaaaaaa', null, 6666, '', null, '', null, '', ''),
       ('2023-05-31 10:00:00', -2147483648, -2147483648, -2147483648, -2147483648, -32768, -32768, 54.122111, 54.122111, 3.141593, 3.141593, 'host_0', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 84, 2147483647, -2147483648, 2147483647, 29, 20, -54.122111, -54.122111, 53, 74, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 29, 48, 5, 63, -17, 52, 60, null, 93, 1, 'host_2', 'a', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 8, 21, -89, 78, 30, 81, 33, 24, 24, 82, 'host_3', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 2, 26, 64, 6, 38, 20, -71, 19, 40, 54, 'host_4', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', -76, -40, -63, -7, -81, -20, -29, -55, null, -15, 'host_5', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 44, 70, 20, -67, 65, 11, 7, 92, 0, 31, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 92, 35, 99, 9, 31, -1, 2, 24, 96, 69, 'host_7', 'aaa', -888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 90, 0, 81, 28, 25, -44, 8, -89, 11, 76, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:01', 2147483647, -2147483648, 2147483647, -2147483648, 32767, -32768, -100.1, 54.122111, -3.141593, null, 'host_0', 'aaa', -2147483648, 6666, '', '', null, '', '', ''),
       ('2023-05-31 10:00:01', 2147483647, 2147483647, 2147483647, 2147483647, 32767, 32767, 54.122111, -54.1221111, 3.141593, -3.141593, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 29, 48, 5, 63, 17, 52, 60, 49, 93, 1, 'host_3', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 8, 21, 89, 78, 30, 81, 33, 24, 24, 82, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 2, 26, 64, 6, 38, 20, 71, null, 40, 54, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 76, 40, 63, 7, 81, 20, 29, -55, 20, 15, 'host_6', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_7', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 92, 35,-99, 9, 31, 1, 2, 24, 96, 69, 'host_8', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 21, 77, 90, 83, 41, 84, 26, 60, null, 36, 'host_9', 'aaa', -888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 90, 0, -81, 28, 25, 44, 8, 89, 11, 76, 'host_2', 'a', -888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:02', 58, -2, 24, 61, 22, 63, 6, 44, null, 38, 'host_2', 'aaa', null, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_3', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 29, 48, -5, 63, -17, 52, 60, 49, 93, 1, 'host_4', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 8, 21, 89, -78, 30, 81, 33, 24, 24, 82, 'host_5', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 2, 26, -64, 6, -38, 20, 71, null, 40, 54, 'host_6', 'aaa', null, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_7', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_8', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 92, 35, 99, -9, 31, 1, -2, 24, 96, 69, 'host_9', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 21, 77, -90, 83, 41, 84, 26, 60, 43, null, 'host_0', 'aaaaaaaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 90, 0, 81, 28, 25, 44, 8, -89, 11, 76, 'host_1', 'aaa', 2147483647, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:04', 2147483647, 2, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 29, 48, 5, -63, 17, -52, 60, 49, 93, 1, 'host_5', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 8, 21, 89, 78, 30, -81, 33, 24, 24, 82, 'host_6', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 2, 26, -64, 6, 38, -20, 71, null, 40, 54, 'host_7', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_8', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_9', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 92, 35, 99, 9, 31, -1, 2, 24, 96, 69, 'host_0', 'aaaaaaaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_1', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 90, 0, 81, 28, 25, 44, 8, -89, 11, 76, 'host_2', 'aaa', -2147483648, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:03', 58, null, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 29, 48, 5, 63, 17, 52, 60, 49, 93, 1, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 8, 21, 89, 78, 30, 81, 33, 24, 24, 82, 'host_6', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 2, 26, 64, 6, 38, -20, 71, null, 40, 54, 'host_7', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 44, 70, 20, 67, 65, 11, -7, 92, 0, 31, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 92, 35, 99, 9, 31, 1, 2, 24, 96, 69, 'host_0', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 90, 0, 81, 28, 25, 44, 8, 89, 11, 76, 'host_2', 'aaa', 888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_3', 'aaaaaaa', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_4', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_7', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_0', 'aaa', 0, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_2', 'aaa', null, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_3', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_5', 'aaa', null, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_7', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_8', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_9', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_0', 'aaa', -888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_2', 'aaa', null, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:10', 58, null, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 29, 48, 5, -63, 17, 52, 60, 49, 93, 1, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 8, 21, 89, -78, 30, 81, 33, 24, 24, 82, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 2, 26, 64, 6, 38, -20, 71, null, -40, 54, 'host_7', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 76, 40, -63, 7, 81, -20, 29, 55, 20, 15, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 44, 70, 20, 67, 65, 11, 7, -92, 0, 31, 'host_9', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 92, 35, 99, 9, -31, 1, 2, 24, 96, 69, 'host_0', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 21, 77, 90, 83, 41, -84, 26, 60, 43, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:10', 90, 0, 81, 28, 25, -44, 8, 89, 11, 76, 'host_2', 'aaa', -888888, 6666, '', '', '', '', '', '');

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       lastts(usage_guest),
       sum(usage_steal),
       firstts(usage_nice),
       max(usage_user),
       max(usage_irq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       first_row(usage_idle),
       lastts(usage_nice),
       lastts(usage_irq),
       lastts(usage_guest_nice),
       max(usage_idle),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_1'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       sum(usage_guest),
       count(usage_idle),
       avg(usage_system),
       max(usage_steal),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaa'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       last_row(usage_iowait),
       avg(usage_irq),
       max(usage_system),
       firstts(usage_irq),
       lastts(usage_softirq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaaaaaaa'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       first_row(usage_irq),
       lastts(usage_iowait),
       sum(usage_iowait),
       hostname,
       first_row(usage_system),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND region = 'aaa'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b, first (usage_nice), min (usage_system), min (usage_softirq), last_row(usage_guest_nice), hostname,
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
  AND region = 'aaaaaaaa'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       avg(usage_steal),
       avg(usage_guest_nice),
       last_row(usage_guest),
       min(usage_idle),
       hostname,
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts LIMIT 10;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       sum(usage_nice),
       sum(usage_idle),
       min(usage_irq),
       sum(usage_system),
       last_row(usage_user),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_1'
  AND region = 'aaa'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts
OFFSET 2;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       min(usage_iowait),
       last_row(usage_irq),
       count(usage_nice),
       last_row(usage_idle),
       last_row(usage_system),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaa'
  AND datacenter = 888888
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       first_row(usage_iowait),
       last_row(usage_steal),
       min(usage_user),
       last_row(usage_nice),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  AND hostname = 'host_0'
  AND region = 'aaa'
  AND datacenter = 888888
  AND rack = 8888.88
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       sum(usage_guest_nice),
       sum(usage_irq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       min(usage_nice),
       firstts(usage_user),
       min(usage_guest), first (usage_idle), first (usage_softirq),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
    last (usage_system), avg (usage_guest), last (usage_idle), first (usage_user),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       count(usage_iowait), last (usage_guest_nice), sum (usage_softirq), first_row(usage_user),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts
    LIMIT 1
OFFSET 1;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       firstts(usage_iowait), last (usage_user), min (usage_steal),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       first_row(usage_guest),
       firstts(usage_system),
       lastts(usage_system),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
    first (usage_steal), count (usage_system), firstts(usage_steal), lastts(usage_steal), first_row(usage_steal),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
    first (usage_guest_nice), last (usage_guest), first (usage_iowait), firstts(usage_guest),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       lastts(usage_user),
       firstts(usage_guest_nice), last (usage_softirq), last_row(usage_softirq),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       firstts(usage_idle),
       first_row(usage_guest_nice),
       avg(usage_user),
       count(usage_softirq),
       count(usage_steal),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       max(usage_nice), first (usage_system), avg (usage_idle), last (usage_irq), first (usage_irq),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       max(usage_guest_nice),
       lastts(usage_idle),
       max(usage_guest),
       avg(usage_nice), first (usage_guest),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       min(usage_guest_nice),
       max(usage_softirq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

SELECT count(usage_irq),
       time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       avg(usage_softirq),
       first_row(usage_softirq),
       first_row(usage_nice), last (usage_iowait),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, k_timestamp_b, last_ts;

drop database test cascade;

drop database test_last cascade;

-- ZDP-3670
create ts database test_last;
create table test_last.tb (k_timestamp timestamp not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float,e6 float8,e7 bool,e8 char,e9 char(1023),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(255),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(1023),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (att1 int2 not null,att2 int,att3 int8,att4 bool,att5 float4,att6 float8,att7 varchar,att8 varchar(64)) primary tags (att1);
create table test_last.stb (k_timestamp timestamp not null,e1 timestamp,e2 int2,e3 int,e4 int8,e5 float,e6 float8,e7 bool,e8 char,e9 char(1023),e10 nchar,e11 nchar(255),e12 varchar,e13 varchar(255),e14 varchar(4096),e15 nvarchar,e16 nvarchar(255),e17 nvarchar(4096),e18 varbytes,e19 varbytes(1023),e20 varbytes,e21 varbytes(254),e22 varbytes(4096)) tags (name varchar(10) not null, att1 int2,att2 int,att3 int8,att4 bool,att5 float4,att6 float8,att7 varchar,att8 varchar(3000),att9 char(1000),att10 varbytes(10),att11 varbytes(100)) primary tags (name);
insert into test_last.tb values('2023-05-15 09:10:11.123',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
                                '','','','','','','','','','','','','','','', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-05-15 09:10:14.678',111111110011,400,40000,400000,4000000.404,400000000.4040444,false,
                                'c','char1','n','nchar255定长类型^&@#nchar255定长类型^&@#nchar255定长类型^&@#',
                                'varchar_11','varchar255测试cdf~#varchar255测试cdf~#','varcharlengthis4096_0字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
                                'nvarchar-0','nvarchar255()&^%{}','nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}',
                                'b','bytes1023','varbytes_1',b'\xaa\xbb\xcc',b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-05-15 09:10:12.345', 111111110022, 200, 20000, 200000, 2000000.202, 200000000.2020222, true,
                                'h', 'char2', 'c', 'nchar255_1定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
                                'varchar_22', 'varchar255中文测试abc!<>&^%$varchar255中文测试abc!<>&^%$', 'varcharlengthis4096字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
                                'nvarchar-1', 'nvarchar255nvarchar255()&^%{}','nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}',
                                'y', b'bytes', 'varbytes_2', b'\xdd\xee\xff', b'\xee\xee\xee\xff\xff\xff\xee\xee\xee\xff\xff\xff', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-07-10 20:18:43.145', 111111110044, 300, 30000, 300000, 3000000.303, 300000000.3030333, false,
                                'r', 'char3', 'a', 'nchar255_0定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
                                'varchar_44', 'varchar255_中文测试abc!<>&^%$varchar255_中文测试abc!<>&^%$', 'varcharlengthof4096通用查询test%%&!+varcharlengthof4096通用查询test%%&!+',
                                'nvarchar-2', 'nvarchar255()&^%{}', 'nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}',
                                'e', b'byte12', 'varbytes_3', b'\xbb\xcc\xff', 'varbytes4096$%^varbytes4096$%^varbytes4096$%^', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.tb values('2023-07-10 20:18:41.456', 111111110033, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
                                'a', 'char4', 'h', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
                                'varchar_33', 'varchar255_123456中文测试abc!<>&^%$varchar255_123456中文测试abc!<>&^%$', 'varcharlengthof4096字符类型test%%&!+varcharlengthof4096字符类型test%%&!+',
                                'nvarchar-3', 'nvarchar255_+!@#nvarchar255_+!@#', 'nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}',
                                't', b'bytes', 'varbytes_2', b'\xaa\xdd\xee', b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xee\xee\xee', 1, 2, 3, true, 1.1, 1.2, '', '');
insert into test_last.stb values('2023-05-15 09:10:11.123',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
                                 '','','','','','','','','','','','','','','','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2023-05-15 09:10:14.678', 111111110022, 200, 20000, 200000, 2000000.202, 200000000.2020222, true,
                                 'h', 'char2', 'c', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
                                 'varchar_22', 'varchar255中文测试abc!<>&^%$varchar255中文测试abc!<>&^%$', 'varcharlengthis4096字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
                                 'nvarchar-1', 'nvarchar255nvarchar255()&^%{}','nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}',
                                 'y', b'bytes', 'varbytes_2', b'\xdd\xee\xff', b'\xee\xee\xee\xff\xff\xff\xee\xee\xee\xff\xff\xff','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2023-05-15 09:10:12.345',111111110011,100,10000,100000,1000000.101,100000000.1010111,false,
                                 'c','char1','n','nchar255定长类型^&@#nchar255定长类型^&@#nchar255定长类型^&@#',
                                 'varchar_11','varchar255测试cdf~#varchar255测试cdf~#','varcharlengthis4096字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
                                 'nvarchar-0','nvarchar255()&^%{}','nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}',
                                 'b','bytes1023','varbytes_1',b'\xaa\xbb\xcc',b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2023-07-10 20:18:43.446', 111111110044, 300, 30000, 300000, 3000000.303, 300000000.3030333, false,
                                 'r', 'char3', 'a', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
                                 'varchar_44', 'varchar255_中文测试abc!<>&^%$varchar255_中文测试abc!<>&^%$', 'varcharlengthof4096通用查询test%%&!+varcharlengthof4096通用查询test%%&!+',
                                 'nvarchar-2', 'nvarchar255()&^%{}', 'nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}',
                                 'e', b'byte12', 'varbytes_3', b'\xbb\xcc\xff', 'varbytes4096$%^varbytes4096$%^varbytes4096$%^','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2023-07-10 20:18:43.145', 111111110033, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
                                 'a', 'char4', 'h', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
                                 'varchar_33', 'varchar255_123456中文测试abc!<>&^%$varchar255_123456中文测试abc!<>&^%$', 'varcharlengthof4096字符类型test%%&!+varcharlengthof4096字符类型test%%&!+',
                                 'nvarchar-3', 'nvarchar255_+!@#nvarchar255_+!@#', 'nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}',
                                 't', b'bytes', 'varbytes_2', b'\xaa\xdd\xee', b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd','stb_1',null,null,null,null,null,null,null,'beijing',null,'red',null);
insert into test_last.stb values('2022-06-16 10:18:30.123',111111110011,100,10000,100000,1000000.101,100000000.1010111,false,
                                 'c','char1','n','nchar255定长类型^&@#nchar255定长类型^&@#nchar255定长类型^&@#',
                                 'varchar_11','varchar255测试cdf~#varchar255测试cdf~#','varcharlengthis4096字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
                                 'nvarchar-0','nvarchar255()&^%{}','nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}nvarchar4096中英文测试0中英文测试0中英文测试0()&^%{}',
                                 'b','bytes1023','varbytes_1',b'\xaa\xbb\xcc',b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd','stb_2',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2022-06-16 10:18:35.981', 111111110022, 200, 20000, 200000, 2000000.202, 200000000.2020222, true,
                                 'h', 'char2', 'c', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
                                 'varchar_22', 'varchar255中文测试abc!<>&^%$varchar255中文测试abc!<>&^%$', 'varcharlengthis4096字符类型测试%%&!+varcharlengthis4096字符类型测试%%&!+',
                                 'nvarchar-1', 'nvarchar255nvarchar255()&^%{}','nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096_1中英文测试1中英文测试1中英文测试1()&^%{}',
                                 'y', b'bytes', 'varbytes_2', b'\xdd\xee\xff', b'\xee\xee\xee\xff\xff\xff\xee\xee\xee\xff\xff\xff','stb_2',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-02-10 12:05:51.445',111111110000,100,10000,100000,1000000.101,100000000.1010111,true,
                                 '','','','','','','','','','','','','','','','stb_3',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-02-10 12:05:52.556', 111111110033, 700, 70000, 700000, 7000000.707, 700000000.7070777, true,
                                 'a', 'char4', 'h', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
                                 'varchar_33', 'varchar255_123456中文测试abc!<>&^%$varchar255_123456中文测试abc!<>&^%$', 'varcharlengthof4096字符类型test%%&!+varcharlengthof4096字符类型test%%&!+',
                                 'nvarchar-3', 'nvarchar255_+!@#nvarchar255_+!@#', 'nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}nvarchar4096_2中英文测试2中英文测试2中英文测试2()&^%{}',
                                 't', b'bytes', 'varbytes_2', b'\xaa\xdd\xee', b'\xaa\xaa\xaa\xbb\xbb\xbb\xcc\xcc\xcc\xdd\xdd\xdd','stb_3',null,null,null,null,null,null,null,'shanghai',null,'blue',null);
insert into test_last.stb values('2023-02-10 12:05:50.166', 111111110044, 300, 30000, 300000, 3000000.303, 300000000.3030333, false,
                                 'r', 'char3', 'a', 'nchar255定长类型测试1测试2测试3()&^%{}nchar255定长类型测试1测试2测试3()&^%{}',
                                 'varchar_44', 'varchar255_中文测试abc!<>&^%$varchar255_中文测试abc!<>&^%$', 'varcharlengthof4096通用查询test%%&!+varcharlengthof4096通用查询test%%&!+',
                                 'nvarchar-2', 'nvarchar255()&^%{}', 'nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}nvarchar4096中英文测试1中英文测试1中英文测试1()&^%{}',
                                 'e', b'byte12', 'varbytes_3', b'\xbb\xcc\xff', 'varbytes4096$%^varbytes4096$%^varbytes4096$%^','stb_3',null,null,null,null,null,null,null,'shanghai',null,'blue',null);


-- tb
select time_bucket(k_timestamp, '10s') as tb, last(e20),last(e21),last(e22) from test_last.tb group by tb,att1 order by tb;
select time_bucket(k_timestamp, '10s') as tb, last_row(e20),last_row(e21),last_row(e22) from test_last.tb group by tb,att1 order by tb;
select time_bucket(k_timestamp, '15s') as tb, last_row(e8),last_row(e12) from test_last.tb where e8 in(' ','c','h') group by tb,att1 order by tb;

-- stb
select time_bucket(k_timestamp, '10s') as tb, last(e20),last(e21),last(e22) from test_last.stb group by tb order by tb;
select time_bucket(k_timestamp, '10s') as tb, last_row(e20),last_row(e21),last_row(e22) from test_last.stb group by tb order by tb;
select time_bucket(k_timestamp, '15s') as tb, last_row(e8),last_row(e9) from test_last.stb where e8 in(' ','c','h','r') group by tb order by tb;
select time_bucket(k_timestamp, '15s') as tb, last(e12),last(e15) from test_last.stb group by tb,name order by tb;
select time_bucket(k_timestamp, '15s') as tb, last_row(e12),last_row(e15) from test_last.stb group by tb,name order by tb;

drop database test_last cascade;

-- time unit and date before 1970
drop database if exists test cascade;
create ts database test;

create table test.cpu
(
    k_timestamp      timestamp not null,
    usage_user       bigint not null,
    usage_system     bigint,
    usage_idle       int,
    usage_nice       int,
    usage_iowait     smallint,
    usage_irq        smallint,
    usage_softirq    float,
    usage_steal      float,
    usage_guest      real,
    usage_guest_nice real
) attributes (
    hostname char(30) not null,
    region varchar(8) not null,
    datacenter int,
    rack int not null,
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname, region, rack);

INSERT INTO test.cpu
values ('1800-05-31 10:00:00', 2147483647, -2147483648, -2147483648, -2147483648, -32768, -32768, 54.122111, 54.122111, 3.141593, 3.141593, 'host_0', 'aaaaaaaa', null, 6666, '', null, '', null, '', ''),
       ('1800-05-31 10:00:00', -2147483648, -2147483648, -2147483648, -2147483648, -32768, -32768, 54.122111, 54.122111, 3.141593, 3.141593, 'host_0', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1800-05-31 10:00:00', 84, 2147483647, -2147483648, 2147483647, 29, 20, -54.122111, -54.122111, 53, 74, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1800-05-31 10:00:00', 29, 48, 5, 63, -17, 52, 60, null, 93, 1, 'host_2', 'a', -888888, 6666, '', '', '', '', '', ''),
       ('1800-05-31 10:00:00', 8, 21, -89, 78, 30, 81, 33, 24, 24, 82, 'host_3', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1800-05-31 10:00:00', 2, 26, 64, 6, 38, 20, -71, 19, 40, 54, 'host_4', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('1800-05-31 10:00:00', -76, -40, -63, -7, -81, -20, -29, -55, null, -15, 'host_5', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('1800-05-31 10:00:00', 44, 70, 20, -67, 65, 11, 7, 92, 0, 31, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1800-05-31 10:00:00', 92, 35, 99, 9, 31, -1, 2, 24, 96, 69, 'host_7', 'aaa', -888888, -6666, '', '', '', '', '', ''),
       ('1800-05-31 10:00:00', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1800-05-31 10:00:00', 90, 0, 81, 28, 25, -44, 8, -89, 11, 76, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('1800-06-01 11:00:01', 2147483647, -2147483648, 2147483647, -2147483648, 32767, -32768, -100.1, 54.122111, -3.141593, null, 'host_0', 'aaa', -2147483648, 6666, '', '', null, '', '', ''),
       ('1800-06-01 11:00:01', 2147483647, 2147483647, 2147483647, 2147483647, 32767, 32767, 54.122111, -54.1221111, 3.141593, -3.141593, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1800-06-01 11:00:01', 29, 48, 5, 63, 17, 52, 60, 49, 93, 1, 'host_3', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('1800-06-01 11:00:01', 8, 21, 89, 78, 30, 81, 33, 24, 24, 82, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1800-06-01 11:00:01', 2, 26, 64, 6, 38, 20, 71, null, 40, 54, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1800-06-01 11:00:01', 76, 40, 63, 7, 81, 20, 29, -55, 20, 15, 'host_6', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1800-06-01 11:00:01', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_7', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1800-06-01 11:00:01', 92, 35,-99, 9, 31, 1, 2, 24, 96, 69, 'host_8', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('1800-06-01 11:00:01', 21, 77, 90, 83, 41, 84, 26, 60, null, 36, 'host_9', 'aaa', -888888, -6666, '', '', '', '', '', ''),
       ('1800-06-31 11:00:01', 90, 0, -81, 28, 25, 44, 8, 89, 11, 76, 'host_2', 'a', -888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('1969-12-31 10:00:02', 58, -2, 24, 61, 22, 63, 6, 44, null, 38, 'host_2', 'aaa', null, -6666, '', '', '', '', '', ''),
       ('1969-12-31 10:00:02', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_3', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('1969-12-31 10:00:02', 29, 48, -5, 63, -17, 52, 60, 49, 93, 1, 'host_4', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('1969-12-31 10:00:02', 8, 21, 89, -78, 30, 81, 33, 24, 24, 82, 'host_5', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('1969-12-31 10:00:02', 2, 26, -64, 6, -38, 20, 71, null, 40, 54, 'host_6', 'aaa', null, -6666, '', '', '', '', '', ''),
       ('1969-12-31 10:00:02', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_7', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('1969-12-31 10:00:02', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_8', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('1969-12-31 10:00:02', 92, 35, 99, -9, 31, 1, -2, 24, 96, 69, 'host_9', 'aaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('1969-12-31 10:00:02', 21, 77, -90, 83, 41, 84, 26, 60, 43, null, 'host_0', 'aaaaaaaa', 2147483647, 6666, '', '', '', '', '', ''),
       ('1969-12-31 10:00:02', 90, 0, 81, 28, 25, 44, 8, -89, 11, 76, 'host_1', 'aaa', 2147483647, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('1970-01-01 15:00:04', 2147483647, 2, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-01-01 15:00:04', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-01-01 15:00:04', 29, 48, 5, -63, 17, -52, 60, 49, 93, 1, 'host_5', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-01-01 16:00:04', 8, 21, 89, 78, 30, -81, 33, 24, 24, 82, 'host_6', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-01-01 17:00:04', 2, 26, -64, 6, 38, -20, 71, null, 40, 54, 'host_7', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('1970-01-01 15:00:04', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_8', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-01-01 16:00:04', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_9', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-01-01 15:00:04', 92, 35, 99, 9, 31, -1, 2, 24, 96, 69, 'host_0', 'aaaaaaaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-01-01 15:00:04', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_1', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-01-01 15:00:04', 90, 0, 81, 28, 25, 44, 8, -89, 11, 76, 'host_2', 'aaa', -2147483648, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('1970-02-01 10:00:04', 2147483647, 2, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-01 10:00:04', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-01 10:00:04', 29, 48, 5, -63, 17, -52, 60, 49, 93, 1, 'host_5', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-01 10:00:04', 8, 21, 89, 78, 30, -81, 33, 24, 24, 82, 'host_6', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-01 10:00:04', 2, 26, -64, 6, 38, -20, 71, null, 40, 54, 'host_7', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('1970-02-01 10:00:04', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_8', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-01 10:00:04', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_9', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-01 10:00:04', 92, 35, 99, 9, 31, -1, 2, 24, 96, 69, 'host_0', 'aaaaaaaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-01 10:00:04', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_1', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-01 10:00:04', 90, 0, 81, 28, 25, 44, 8, -89, 11, 76, 'host_2', 'aaa', -2147483648, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('1970-02-02 10:00:04', 2147483647, 2, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-02 10:00:04', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-02 10:00:04', 29, 48, 5, -63, 17, -52, 60, 49, 93, 1, 'host_5', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-02 10:00:04', 8, 21, 89, 78, 30, -81, 33, 24, 24, 82, 'host_6', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-02 10:00:04', 2, 26, -64, 6, 38, -20, 71, null, 40, 54, 'host_7', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('1970-02-02 10:00:04', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_8', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-02 10:00:04', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_9', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-02 10:00:04', 92, 35, 99, 9, 31, -1, 2, 24, 96, 69, 'host_0', 'aaaaaaaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-02 10:00:04', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_1', 'aaa', -2147483648, 6666, '', '', '', '', '', ''),
       ('1970-02-02 10:00:04', 90, 0, 81, 28, 25, 44, 8, -89, 11, 76, 'host_2', 'aaa', -2147483648, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('1970-05-31 22:00:03', 58, null, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('1970-05-31 22:00:03', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1970-05-31 22:00:03', 29, 48, 5, 63, 17, 52, 60, 49, 93, 1, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1970-05-31 22:00:03', 8, 21, 89, 78, 30, 81, 33, 24, 24, 82, 'host_6', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('1970-05-31 22:00:03', 2, 26, 64, 6, 38, -20, 71, null, 40, 54, 'host_7', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1970-05-31 22:00:03', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1970-05-31 22:00:03', 44, 70, 20, 67, 65, 11, -7, 92, 0, 31, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1970-05-31 22:00:03', 92, 35, 99, 9, 31, 1, 2, 24, 96, 69, 'host_0', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1970-05-31 22:00:03', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('1970-05-31 22:00:03', 90, 0, 81, 28, 25, 44, 8, 89, 11, 76, 'host_2', 'aaa', 888888, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_3', 'aaaaaaa', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_4', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_7', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_9', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_0', 'aaa', 0, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:08', 2147483647, null, null, null, null, null, null, null, null, null, 'host_2', 'aaa', null, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_3', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_5', 'aaa', null, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_7', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_8', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_9', 'aaa', null, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_0', 'aaa', -888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:00:09', 2147483647, null, null, null, null, null, null, null, null, null, 'host_2', 'aaa', null, 6666, '', '', '', '', '', '');

INSERT INTO test.cpu
values ('2023-05-31 10:30:10', 58, null, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:30:10', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:30:10', 29, 48, 5, -63, 17, 52, 60, 49, 93, 1, 'host_5', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:30:10', 8, 21, 89, -78, 30, 81, 33, 24, 24, 82, 'host_6', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:30:10', 2, 26, 64, 6, 38, -20, 71, null, -40, 54, 'host_7', 'aaa', -888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:30:10', 76, 40, -63, 7, 81, -20, 29, 55, 20, 15, 'host_8', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:30:10', 44, 70, 20, 67, 65, 11, 7, -92, 0, 31, 'host_9', 'aaa', 888888, -6666, '', '', '', '', '', ''),
       ('2023-05-31 10:30:10', 92, 35, 99, 9, -31, 1, 2, 24, 96, 69, 'host_0', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:30:10', 21, 77, 90, 83, 41, -84, 26, 60, 43, null, 'host_1', 'aaa', 888888, 6666, '', '', '', '', '', ''),
       ('2023-05-31 10:30:10', 90, 0, 81, 28, 25, -44, 8, 89, 11, 76, 'host_2', 'aaa', -888888, 6666, '', '', '', '', '', '');


SELECT time_bucket(k_timestamp, '4YEAR') as k_timestamp_b,
       hostname,
       lastts(usage_guest),
       sum(usage_steal),
       firstts(usage_nice),
       max(usage_user),
       max(usage_irq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE hostname = 'host_0'
AND k_timestamp >= '1800-01-01 00:00:00'
AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2yrs') as k_timestamp_b,
       hostname,
       first_row(usage_idle),
       lastts(usage_nice),
       lastts(usage_irq),
       lastts(usage_guest_nice),
       max(usage_idle),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE hostname = 'host_1'
AND k_timestamp >= '1900-01-01 00:00:00'
AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '20yr') as k_timestamp_b,
       hostname,
       sum(usage_guest),
       count(usage_idle),
       avg(usage_system),
       max(usage_steal),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE hostname = 'host_0'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '100y') as k_timestamp_b,
       hostname,
       last_row(usage_iowait),
       avg(usage_irq),
       max(usage_system),
       firstts(usage_irq),
       lastts(usage_softirq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE hostname = 'host_0'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2month') as k_timestamp_b,
       first_row(usage_irq),
       lastts(usage_iowait),
       sum(usage_iowait),
       hostname,
       first_row(usage_system),
       lastts(usage_user)            as last_ts
FROM test.cpu
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '5mon') as k_timestamp_b, first (usage_nice), min (usage_system), min (usage_softirq), last_row(usage_guest_nice), hostname,
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE region = 'aaaaaaaa'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '16mons') as k_timestamp_b,
       avg(usage_steal),
       avg(usage_guest_nice),
       last_row(usage_guest),
       min(usage_idle),
       hostname,
       lastts(usage_user)            as last_ts
FROM test.cpu
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts
LIMIT 10;

SELECT time_bucket(k_timestamp, '15weeks') as k_timestamp_b,
       hostname,
       sum(usage_nice),
       sum(usage_idle),
       min(usage_irq),
       sum(usage_system),
       last_row(usage_user),
       lastts(usage_user)            as last_ts
FROM test.cpu
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts
OFFSET 2;

SELECT time_bucket(k_timestamp, '5week') as k_timestamp_b,
       hostname,
       min(usage_iowait),
       last_row(usage_irq),
       count(usage_nice),
       last_row(usage_idle),
       last_row(usage_system),
       lastts(usage_user)            as last_ts
FROM test.cpu
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '1w') as k_timestamp_b,
       hostname,
       first_row(usage_iowait),
       last_row(usage_steal),
       min(usage_user),
       last_row(usage_nice),
       lastts(usage_user)            as last_ts
FROM test.cpu
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2day') as k_timestamp_b,
       hostname,
       sum(usage_guest_nice),
       sum(usage_irq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1700-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '10d') as k_timestamp_b,
       hostname,
       min(usage_nice),
       firstts(usage_user),
       min(usage_guest), first (usage_idle), first (usage_softirq),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1100-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '12hour') as k_timestamp_b,
       hostname,
    last (usage_system), avg (usage_guest), last (usage_idle), first (usage_user),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1500-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '3hrs') as k_timestamp_b,
       hostname,
       count(usage_iowait), last (usage_guest_nice), sum (usage_softirq), first_row(usage_user),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1800-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts

    LIMIT 1
OFFSET 1;

SELECT time_bucket(k_timestamp, '2hr') as k_timestamp_b,
       hostname,
       firstts(usage_iowait), last (usage_user), min (usage_steal),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1600-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '7h') as k_timestamp_b,
       hostname,
       first_row(usage_guest),
       firstts(usage_system),
       lastts(usage_system),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1700-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2minute') as k_timestamp_b,
       hostname,
    first (usage_steal), count (usage_system), firstts(usage_steal), lastts(usage_steal), first_row(usage_steal),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1600-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '15mins') as k_timestamp_b,
       hostname,
    first (usage_guest_nice), last (usage_guest), first (usage_iowait), firstts(usage_guest),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1600-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '60min') as k_timestamp_b,
       hostname,
       lastts(usage_user),
       firstts(usage_guest_nice), last (usage_softirq), last_row(usage_softirq),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1600-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '12min') as k_timestamp_b,
       hostname,
       firstts(usage_idle),
       first_row(usage_guest_nice),
       avg(usage_user),
       count(usage_softirq),
       count(usage_steal),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1600-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2second') as k_timestamp_b,
       hostname,
       max(usage_nice), first (usage_system), avg (usage_idle), last (usage_irq), first (usage_irq),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1600-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '20secs') as k_timestamp_b,
       hostname,
       max(usage_guest_nice),
       lastts(usage_idle),
       max(usage_guest),
       avg(usage_nice), first (usage_guest),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1600-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT time_bucket(k_timestamp, '2ec') as k_timestamp_b,
       hostname,
       min(usage_guest_nice),
       max(usage_softirq),
       lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1600-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT count(usage_irq),
       time_bucket(k_timestamp, '-10s') as k_timestamp_b,
       hostname,
       avg(usage_softirq),
       first_row(usage_softirq),
       first_row(usage_nice), last (usage_iowait),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1600-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT count(usage_irq),
       time_bucket(k_timestamp, '1.5hour') as k_timestamp_b,
       hostname,
       avg(usage_softirq),
       first_row(usage_softirq),
       first_row(usage_nice), last (usage_iowait),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1600-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT count(usage_irq),
       time_bucket(k_timestamp, '2xiaoshi') as k_timestamp_b,
       hostname,
       avg(usage_softirq),
       first_row(usage_softirq),
       first_row(usage_nice), last (usage_iowait),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1600-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;

SELECT count(usage_irq),
       time_bucket(k_timestamp, '10YEARS') as k_timestamp_b,
       hostname,
       avg(usage_softirq),
       first_row(usage_softirq),
       first_row(usage_nice), last (usage_iowait),
    lastts(usage_user)            as last_ts
FROM test.cpu
WHERE k_timestamp >= '1600-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, region, rack, k_timestamp_b
ORDER BY hostname, region, rack, k_timestamp_b, last_ts;
drop database test cascade;
