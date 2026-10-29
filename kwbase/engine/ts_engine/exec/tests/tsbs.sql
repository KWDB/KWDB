drop database if exists benchmark_ut cascade;
create ts database benchmark_ut;
create table benchmark_ut.cpu
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

INSERT INTO benchmark_ut.cpu
values ('2023-05-31 10:00:00', 58, 2, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_1', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 29, 48, 5, 63, 17, 52, 60, null, 93, 1, 'host_2', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 8, 21, 89, 78, 30, 81, 33, 24, 24, 82, 'host_3', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 2, 26, 64, 6, 38, 20, 71, 19, 40, 54, 'host_4', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 76, 40, 63, 7, 81, 20, 29, 55, null, 15, 'host_5', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_6', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 92, 35, 99, 9, 31, 1, 2, 24, 96, 69, 'host_7', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_8', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:00', 90, 0, 81, 28, 25, 44, 8, 89, 11, 76, 'host_9', '', '', '', '', '', '', '', '', '');

INSERT INTO benchmark_ut.cpu
values ('2023-05-31 10:00:01', 58, 2, 24, 61, 22, 63, 6, 44, 80, null, 'host_1', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_2', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 29, 48, 5, 63, 17, 52, 60, 49, 93, 1, 'host_3', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 8, 21, 89, 78, 30, 81, 33, 24, 24, 82, 'host_4', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 2, 26, 64, 6, 38, 20, 71, null, 40, 54, 'host_5', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_6', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_7', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 92, 35, 99, 9, 31, 1, 2, 24, 96, 69, 'host_8', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 21, 77, 90, 83, 41, 84, 26, 60, null, 36, 'host_9', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:01', 90, 0, 81, 28, 25, 44, 8, 89, 11, 76, 'host_0', '', '', '', '', '', '', '', '', '');

INSERT INTO benchmark_ut.cpu
values ('2023-05-31 10:00:02', 58, 2, 24, 61, 22, 63, 6, 44, null, 38, 'host_2', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_3', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 29, 48, 5, 63, 17, 52, 60, 49, 93, 1, 'host_4', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 8, 21, 89, 78, 30, 81, 33, 24, 24, 82, 'host_5', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 2, 26, 64, 6, 38, 20, 71, null, 40, 54, 'host_6', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_7', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_8', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 92, 35, 99, 9, 31, 1, 2, 24, 96, 69, 'host_9', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_0', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:02', 90, 0, 81, 28, 25, 44, 8, 89, 11, 76, 'host_1', '', '', '', '', '', '', '', '', '');
INSERT INTO benchmark_ut.cpu
values ('2023-05-31 10:00:03', 58, 2, 24, 61, 22, 63, 6, 44, null, 38, 'host_3', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 84, 11, 53, 87, 29, 20, 54, 77, 53, 74, 'host_4', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 29, 48, 5, 63, 17, 52, 60, 49, 93, 1, 'host_5', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 8, 21, 89, 78, 30, 81, 33, 24, 24, 82, 'host_6', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 2, 26, 64, 6, 38, 20, 71, null, 40, 54, 'host_7', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 76, 40, 63, 7, 81, 20, 29, 55, 20, 15, 'host_8', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 44, 70, 20, 67, 65, 11, 7, 92, 0, 31, 'host_9', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 92, 35, 99, 9, 31, 1, 2, 24, 96, 69, 'host_0', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 21, 77, 90, 83, 41, 84, 26, 60, 43, null, 'host_1', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:03', 90, 0, 81, 28, 25, 44, 8, 89, 11, 76, 'host_2', '', '', '', '', '', '', '', '', '');

INSERT INTO benchmark_ut.cpu
values ('2023-05-31 10:00:04', -58, 2, 24, 61, 22, 63, 6, 44, null, 38, 'host_4', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 84, -11, 53, 87, 29, -20, 54, 77, 53, 74, 'host_5', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 29, -48, -5, 63, 17, 52, 60, 49, -93, 1, 'host_6', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 8, 21, -89, -78, 30, 81, 33, 24, 24, 82, 'host_7', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 2, 26, 64, -6, -38, 20, 71, null, 40, 54, 'host_8', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 76, -40, 63, 7, -81, 20, -29, 55, 20, 15, 'host_9', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 44, 70, -20, 67, 65, -11, 7, 92, 0, 31, 'host_0', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 92, 35, 99, 9, 31, 1, -2, 24, 96, 69, 'host_1', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', 21, -77, 90, 83, 41, 84, -26, 60, 43, null, 'host_2', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:04', -90, 0, 81, 28, 25, -44, 8, 89, -11, 76, 'host_3', '', '', '', '', '', '', '', '', '');

INSERT INTO benchmark_ut.cpu
values ('2023-05-31 10:00:05', -58, 2, 24, 61, 22, 63, 6, 44, null, 38, 'host_5', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:05', 84, -11, 53, 87, 29, -20, 54, 77, 53, 74, 'host_6', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:05', 29, -48, -5, 63, 17, 52, 60, 49, -93, 1, 'host_7', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:05', 8, 21, -89, -78, 30, 81, 33, 24, 24, 82, 'host_8', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:05', 2, 26, 64, -6, -38, 20, 71, null, 40, 54, 'host_9', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:05', 76, -40, 63, 7, -81, 20, -29, 55, 20, 15, 'host_0', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:05', 44, 70, -20, 67, 65, -11, 7, 92, 0, 31, 'host_1', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:05', 92, 35, 99, 9, 31, 1, -2, 24, 96, 69, 'host_2', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:05', 21, -77, 90, 83, 41, 84, -26, 60, 43, null, 'host_3', '', '', '', '', '', '', '', '', ''),
       ('2023-05-31 10:00:05', -90, 0, 81, 28, 25, -44, 8, 89, -11, 76, 'host_4', '', '', '', '', '', '', '', '', '');

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       max(usage_user)
FROM benchmark_ut.cpu
WHERE hostname = 'host_4'
  AND k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       max(usage_user)
FROM benchmark_ut.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, k_timestamp_b;

SELECT time_bucket(k_timestamp, '2s') as k_timestamp,
       hostname,
       min(usage_user),
       max(usage_user),
       count(usage_user)
FROM benchmark_ut.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, time_bucket(k_timestamp, '2s')
ORDER BY hostname, time_bucket(k_timestamp, '2s');

SELECT time_bucket(k_timestamp, '2s') as k_timestamp,
       hostname,
       last(usage_steal),
       last_row(usage_steal),
       count(usage_steal)
FROM benchmark_ut.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, time_bucket(k_timestamp, '2s')
ORDER BY hostname, time_bucket(k_timestamp, '2s');

SELECT time_bucket(k_timestamp, '2s') as k_timestamp,
       hostname,
    first(usage_steal),
    first_row(usage_steal),
    count(usage_steal)
FROM benchmark_ut.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, time_bucket(k_timestamp, '2s')
ORDER BY hostname, time_bucket(k_timestamp, '2s');


-- TSBS double-groupby-1
SELECT time_bucket(k_timestamp, '3600s') as k_timestamp, hostname, max(usage_user1)
FROM benchmark_ut.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, time_bucket(k_timestamp, '3600s')
ORDER BY hostname, time_bucket(k_timestamp, '3600s');


-- TSBS double-groupby-5
SELECT time_bucket(k_timestamp, '3600s') as k_timestamp,
       hostname,
       max(usage_user),
       min(usage_system),
       count(usage_idle),
       sum(usage_nice),
       max(usage_iowait)
FROM benchmark_ut.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, time_bucket(k_timestamp, '3600s')
ORDER BY hostname, time_bucket(k_timestamp, '3600s');

-- TSBS double-groupby-all
SELECT time_bucket(k_timestamp, '3600s') as k_timestamp,
       hostname,
       avg(usage_user),
       avg(usage_system),
       avg(usage_idle),
       avg(usage_nice),
       avg(usage_iowait),
       avg(usage_irq),
       avg(usage_softirq),
       avg(usage_steal),
       avg(usage_guest),
       avg(usage_guest_nice)
FROM benchmark_ut.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
GROUP BY hostname, time_bucket(k_timestamp, '3600s')
ORDER BY hostname, time_bucket(k_timestamp, '3600s');


SELECT time_bucket(k_timestamp, '2s') as k_timestamp,
       hostname,
       min(usage_user),
       max(usage_system),
       avg(usage_idle),
       count(usage_nice),
       stddev(usage_iowait),
       sum(usage_irq),
       min(usage_softirq),
       max(usage_steal),
       avg(usage_guest),
       last(usage_guest_nice)
FROM benchmark_ut.cpu
WHERE k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp
    < '2024-01-01 00:00:00'
GROUP BY hostname, time_bucket(k_timestamp, '2s')
ORDER BY hostname, time_bucket(k_timestamp, '2s');