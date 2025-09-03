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

-- aggPushDown = true
explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
               hostname,
               max(usage_user)
        FROM benchmark_ut.cpu
        WHERE hostname = 'host_4'
        GROUP BY hostname, k_timestamp_b
        ORDER BY hostname, k_timestamp_b;

-- not display aggPushDown
-- no time_bucket
explain SELECT
               hostname,
               max(usage_user)
        FROM benchmark_ut.cpu
        WHERE hostname = 'host_4'
        GROUP BY hostname
        ORDER BY hostname;

-- group by other column
explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
               hostname,usage_nice,
               max(usage_user)
        FROM benchmark_ut.cpu
        WHERE hostname = 'host_4'
        GROUP BY hostname, k_timestamp_b,usage_nice
        ORDER BY hostname, k_timestamp_b;

explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
               hostname,
               max(usage_user)
        FROM benchmark_ut.cpu
        WHERE hostname = 'host_4'
        GROUP BY hostname,k_timestamp
        ORDER BY hostname, k_timestamp_b;

-- filter other column
explain SELECT time_bucket(k_timestamp, '2s') as k_timestamp_b,
       hostname,
       max(usage_user)
FROM benchmark_ut.cpu
WHERE hostname = 'host_4'
  AND k_timestamp >= '2023-01-01 00:00:00'
  AND k_timestamp < '2024-01-01 00:00:00'
  and usage_nice>2000
GROUP BY hostname, k_timestamp_b
ORDER BY hostname, k_timestamp_b;

explain select first_row(usage_user),first_row(usage_user),first_row(usage_user),first_row(usage_user)
from benchmark_ut.cpu
group by usage_idle having usage_idle > 20;

explain select count(usage_user),hostname from benchmark_ut.cpu
where hostname='host_0' and k_timestamp >='2023-01-01 00:00:00' and k_timestamp<'2024-10-01 00:00:00'
group by hostname;

drop database benchmark_ut cascade;
