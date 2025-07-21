SET CLUSTER SETTING server.advanced_distributed_operations.enabled = true;
SET cluster setting ts.rows_per_block.max_limit=10;
SET cluster setting ts.blocks_per_segment.max_limit=50;
CREATE TS DATABASE tsdb;

CREATE TABLE tsdb.tab(ts TIMESTAMP NOT NULL, col1 INT, col2 CHAR(3)) TAGS (tag1 INT8 NOT NULL, tag2 FLOAT4, tag3 CHAR(3)) PRIMARY TAGS(tag1);

INSERT INTO tsdb.tab VALUES
                         ('2024-01-05 01:00:00', 1, NULL, 111, 1.11, 'one'),
                         ('2024-01-05 02:00:00', 2, NULL, 222, 1.311, 'one'),
                         ('2024-01-05 03:00:00', 3, NULL, 333, 12.11, 'one'),
                         ('2024-01-05 04:00:00', 4, NULL, 555, 1.311, 'one'),
                         ('2024-01-05 05:00:00', 5, NULL, 44, 1.311, 'one'),
                         ('2024-01-05 06:00:00', 6, NULL, 666, 1.311, 'one'),
                         ('2024-01-05 07:00:00', 7, NULL, 888, 1.311, 'one');

SELECT * FROM tsdb.tab ORDER BY col1;

SELECT DISTINCT lease_holder FROM kwdb_internal.kwdb_ts_partitions order by lease_holder;
SELECT COUNT(partition_id) FROM kwdb_internal.kwdb_ts_partitions WHERE 2 = ANY(replicas);

CREATE TS DATABASE tsdb2;
-- upgrade: c2
-- sleep: 30s
SELECT * FROM tsdb.tab ORDER BY col1;
SELECT DISTINCT lease_holder FROM kwdb_internal.kwdb_ts_partitions order by lease_holder;
SELECT COUNT(partition_id) FROM kwdb_internal.kwdb_ts_partitions WHERE 2 = ANY(replicas);

-- expect fail
CREATE TABLE tsdb2.tab(ts TIMESTAMP NOT NULL, col1 INT, col2 CHAR(3)) TAGS (tag1 INT8 NOT NULL, tag2 FLOAT4, tag3 CHAR(3)) PRIMARY TAGS(tag1);
-- ok
INSERT INTO tsdb.tab VALUES
                         ('2024-01-05 08:00:00', 8, NULL, 111, 1.11, 'one'),
                         ('2024-01-05 09:00:00', 9, NULL, 222, 1.311, 'one'),
                         ('2024-01-05 10:00:00', 10, NULL, 333, 12.11, 'one'),
                         ('2024-01-05 11:00:00', 11, NULL, 555, 1.311, 'one'),
                         ('2024-01-05 12:00:00', 12, NULL, 44, 1.311, 'one'),
                         ('2024-01-05 13:00:00', 13, NULL, 666, 1.311, 'one'),
                         ('2024-01-05 14:00:00', 14, NULL, 888, 1.311, 'one');
SELECT * FROM tsdb.tab ORDER BY col1;
SELECT DISTINCT lease_holder FROM kwdb_internal.kwdb_ts_partitions order by lease_holder;
SELECT COUNT(partition_id) FROM kwdb_internal.kwdb_ts_partitions WHERE 2 = ANY(replicas);

-- kill: c2
-- sleep: 10s
-- export KWBASE_TESTING_VERSION_TAG=1.3.0
-- upgrade-complete: c2
-- sleep: 30s
SELECT * FROM tsdb.tab ORDER BY col1;
SELECT DISTINCT lease_holder FROM kwdb_internal.kwdb_ts_partitions order by lease_holder;
SELECT COUNT(partition_id) FROM kwdb_internal.kwdb_ts_partitions WHERE 2 = ANY(replicas);

CREATE TABLE tsdb2.tab(ts TIMESTAMP NOT NULL, col1 INT, col2 CHAR(3)) TAGS (tag1 INT8 NOT NULL, tag2 FLOAT4, tag3 CHAR(3)) PRIMARY TAGS(tag1);

INSERT INTO tsdb2.tab VALUES
                         ('2024-01-05 01:00:00', 1, NULL, 111, 1.11, 'one'),
                         ('2024-01-05 02:00:00', 2, NULL, 222, 1.311, 'one'),
                         ('2024-01-05 03:00:00', 3, NULL, 333, 12.11, 'one'),
                         ('2024-01-05 04:00:00', 4, NULL, 555, 1.311, 'one'),
                         ('2024-01-05 05:00:00', 5, NULL, 44, 1.311, 'one'),
                         ('2024-01-05 05:00:00', 6, NULL, 666, 1.311, 'one'),
                         ('2024-01-05 05:00:00', 7, NULL, 888, 1.311, 'one');

SELECT * FROM tsdb2.tab ORDER BY col1;

