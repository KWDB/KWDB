DROP DATABASE if EXISTS test_limit_optimize cascade;
CREATE ts DATABASE test_limit_optimize;
use test_limit_optimize;

CREATE TABLE cpu (
     k_timestamp TIMESTAMPTZ NOT NULL,
     usage_user1 INT8 NOT NULL,
     usage_system1 INT8 NOT NULL,
     usage_idle1 INT8 NOT NULL,
     usage_nice1 INT8 NOT NULL,
     usage_iowait1 INT8 NOT NULL,
     usage_irq1 INT8 NOT NULL,
     usage_softirq1 INT8 NOT NULL,
     usage_steal1 INT8 NOT NULL,
     usage_guest1 INT8 NOT NULL,
     usage_guest_nice1 INT8 NOT NULL,
	 k_timestamp2 TIMESTAMPTZ NOT NULL
 ) TAGS (
     hostname CHAR(30) NOT NULL,
     region CHAR(30),
     datacenter CHAR(30),
     rack CHAR(30),
     os CHAR(30),
     arch CHAR(30),
     team CHAR(30),
     service CHAR(30),
     service_version CHAR(30),
     service_environment CHAR(30) ) PRIMARY TAGS(hostname);

insert into cpu values('2016-01-01 00:00:00+00:00', 1,20, 20, 20,20,20, 20,20,20,20, '2016-01-01 00:00:00+00:00','host_1', 'test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1');
insert into cpu values('2016-01-01 00:00:10+00:00', 2,20, 20, 20,20,20, 20,20,20,20, '2016-01-01 00:00:10+00:00','host_1', 'test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1');
insert into cpu values('2016-01-01 00:00:20+00:00', 3,20, 20, 20,20,20, 20,20,20,20, '2016-01-01 00:00:20+00:00','host_1', 'test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1');
insert into cpu values('2016-01-01 00:00:30+00:00', 4,20, 20, 20,20,20, 20,20,20,20, '2016-01-01 00:00:30+00:00','host_1', 'test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1');
insert into cpu values('2016-01-01 00:00:40+00:00', 5,20, 20, 20,20,20, 20,20,20,20, '2016-01-01 00:00:40+00:00','host_1', 'test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1');
insert into cpu values('2016-01-01 00:00:50+00:00', 6,20, 20, 20,20,20, 20,20,20,20, '2016-01-01 00:00:50+00:00','host_1', 'test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1');

-- plan add pushLimitToScan flag
EXPLAIN SELECT time_bucket(k_timestamp, '60s'), max(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') ORDER BY time_bucket(k_timestamp, '60s') LIMIT 5;
EXPLAIN SELECT time_bucket(k_timestamp, '60s'), max(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') LIMIT 5;

-- use limit optimize
SELECT time_bucket(k_timestamp, '60s'), max(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') ORDER BY time_bucket(k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp, '60s'), min(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') ORDER BY time_bucket(k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp, '60s'), last(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') ORDER BY time_bucket(k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp, '60s'), first(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') ORDER BY time_bucket(k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp, '60s'), count(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') ORDER BY time_bucket(k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp, '60s'), avg(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') ORDER BY time_bucket(k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp, '60s'), sum(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') ORDER BY time_bucket(k_timestamp, '60s') LIMIT 5;

-- use limit optimize, not order by
SELECT time_bucket(k_timestamp, '60s'), max(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp, '60s'), min(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp, '60s'), last(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp, '60s'), first(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp, '60s'), count(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp, '60s'), avg(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp, '60s'), sum(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s') LIMIT 5;

-- multi time_bucket
SELECT time_bucket(k_timestamp, '60s'), time_bucket(k_timestamp, '30s'), max(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s'), time_bucket(k_timestamp, '30s') ORDER BY time_bucket(k_timestamp, '60s'), time_bucket(k_timestamp, '30s');

-- other time_bucket
SELECT time_bucket(k_timestamp2, '60s'), max(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp2 < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp2, '60s') ORDER BY time_bucket(k_timestamp2, '60s') LIMIT 5;
SELECT time_bucket(k_timestamp2, '60s'), max(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp2 < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp2, '60s') LIMIT 5;

-- order by not time bucket
SELECT time_bucket(k_timestamp, '60s'), max(usage_user1) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(k_timestamp, '60s'),k_timestamp ORDER BY k_timestamp LIMIT 5;

-- not aggreation
SELECT time_bucket(k_timestamp, '60s') FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' ORDER BY time_bucket(k_timestamp, '60s') LIMIT 5;

-- not time_bucket
SELECT count(k_timestamp) FROM test_limit_optimize.cpu WHERE k_timestamp < '2016-01-01 13:31:03.646' GROUP BY k_timestamp ORDER BY k_timestamp LIMIT 5;

CREATE TABLE cpu1 (
     k_timestamp TIMESTAMPTZ NOT NULL,
     usage_user1 INT8 NOT NULL,
     usage_system1 INT8 NOT NULL,
     usage_idle1 INT8 NOT NULL,
     usage_nice1 INT8 NOT NULL,
     usage_iowait1 INT8 NOT NULL,
     usage_irq1 INT8 NOT NULL,
     usage_softirq1 INT8 NOT NULL,
     usage_steal1 INT8 NOT NULL,
     usage_guest1 INT8 NOT NULL,
     usage_guest_nice1 INT8 NOT NULL,
	 k_timestamp2 TIMESTAMPTZ NOT NULL
 ) TAGS (
     hostname CHAR(30) NOT NULL,
     region CHAR(30),
     datacenter CHAR(30),
     rack CHAR(30),
     os CHAR(30),
     arch CHAR(30),
     team CHAR(30),
     service CHAR(30),
     service_version CHAR(30),
     service_environment CHAR(30) ) PRIMARY TAGS(hostname);
	 
insert into cpu1 values('2016-01-01 00:00:00+00:00', 1,20, 20, 20,20,20, 20,20,20,20, '2016-01-01 00:00:00+00:00','host_1', 'test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1');
insert into cpu1 values('2016-01-01 00:00:10+00:00', 2,20, 20, 20,20,20, 20,20,20,20, '2016-01-01 00:00:10+00:00','host_1', 'test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1');
insert into cpu1 values('2016-01-01 00:00:20+00:00', 3,20, 20, 20,20,20, 20,20,20,20, '2016-01-01 00:00:20+00:00','host_1', 'test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1');
insert into cpu1 values('2016-01-01 00:00:30+00:00', 4,20, 20, 20,20,20, 20,20,20,20, '2016-01-01 00:00:30+00:00','host_1', 'test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1');
insert into cpu1 values('2016-01-01 00:00:40+00:00', 5,20, 20, 20,20,20, 20,20,20,20, '2016-01-01 00:00:40+00:00','host_1', 'test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1');
insert into cpu1 values('2016-01-01 00:00:50+00:00', 6,20, 20, 20,20,20, 20,20,20,20, '2016-01-01 00:00:50+00:00','host_1', 'test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1','test_1');

-- join
SELECT time_bucket(t1.k_timestamp, '60s'), max(t1.usage_user1) FROM test_limit_optimize.cpu as t1, test_limit_optimize.cpu1 as t2 WHERE t1.k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(t1.k_timestamp, '60s') ORDER BY time_bucket(t1.k_timestamp, '60s') LIMIT 5;
SELECT time_bucket(t1.k_timestamp, '60s'), max(t1.usage_user1) FROM test_limit_optimize.cpu as t1 join test_limit_optimize.cpu1 as t2 ON t1.k_timestamp=t2.k_timestamp WHERE t1.k_timestamp < '2016-01-01 13:31:03.646' GROUP BY time_bucket(t1.k_timestamp, '60s') ORDER BY time_bucket(t1.k_timestamp, '60s') LIMIT 5;

use defaultdb;
DROP DATABASE test_limit_optimize cascade;
