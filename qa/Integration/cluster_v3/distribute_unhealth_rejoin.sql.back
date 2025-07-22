SET CLUSTER SETTING server.advanced_distributed_operations.enabled = true;
SET cluster setting ts.rows_per_block.max_limit=10;
SET cluster setting ts.blocks_per_segment.max_limit=50;

CREATE TS DATABASE tsdb;
CREATE TABLE tsdb.tab(ts   TIMESTAMP NOT NULL, col1 INT, col2 CHAR(3)) TAGS (tag1 INT8 NOT NULL, tag2 FLOAT4, tag3 CHAR(3)) PRIMARY TAGS(tag1);

INSERT INTO tsdb.tab VALUES ('2024-01-05 01:00:00', 1, NULL, 111, 1.11, 'one'), ('2024-01-05 02:00:00', 2, NULL, 222, 1.311, 'one'),('2024-01-05 03:00:00', 3, NULL, 333, 12.11, 'one'),('2024-01-05 04:00:00', 4, NULL, 555, 1.311, 'one'), ('2024-01-05 05:00:00', 5, NULL, 44, 1.311, 'one'), ('2024-01-05 05:00:00', 6, NULL, 666, 1.311, 'one'),('2024-01-05 05:00:00', 7, NULL, 888, 1.311, 'one');
select * from tsdb.tab order by col1;

-- kill: c3
-- sleep: 5s
-- is-unhealthy: c3
-- sleep: 10s
-- wait-running: c1
select * from tsdb.tab order by col1;

-- restart: c3
-- sleep: 30s
-- wait-join: c3
-- sleep: 10s
-- wait-running: c1
-- sleep: 10s
-- wait-replicas: c1
select * from tsdb.tab order by col1;

CREATE TABLE tsdb.tab1(ts   TIMESTAMP NOT NULL, col1 INT, col2 CHAR(3)) TAGS (tag1 INT8 NOT NULL, tag2 FLOAT4, tag3 CHAR(3)) PRIMARY TAGS(tag1);
INSERT INTO tsdb.tab1 VALUES ('2024-01-05 01:00:00', 1, NULL, 111, 1.11, 'one'), ('2024-01-05 02:00:00', 2, NULL, 222, 1.311, 'one'),('2024-01-05 03:00:00', 3, NULL, 333, 12.11, 'one'),('2024-01-05 04:00:00', 4, NULL, 555, 1.311, 'one'), ('2024-01-05 05:00:00', 5, NULL, 44, 1.311, 'one'), ('2024-01-05 05:00:00', 6, NULL, 666, 1.311, 'one'),('2024-01-05 05:00:00', 7, NULL, 888, 1.311, 'one');

select * from tsdb.tab1 order by col1;
select * from tsdb.tab order by col1;

drop table tsdb.tab;

select * from tsdb.tab order by col1;
select * from tsdb.tab1 order by col1;

-- kill: c4
-- sleep: 30s
-- wait-running: c1
select * from tsdb.tab order by col1;
select * from tsdb.tab1 order by col1;

drop table tsdb.tab1;

CREATE TABLE tsdb.tab(ts   TIMESTAMP NOT NULL, col1 INT, col2 CHAR(3)) TAGS (tag1 INT8 NOT NULL, tag2 FLOAT4, tag3 CHAR(3)) PRIMARY TAGS(tag1);

INSERT INTO tsdb.tab VALUES ('2024-01-05 01:00:00', 1, NULL, 111, 1.11, 'one'), ('2024-01-05 02:00:00', 2, NULL, 222, 1.311, 'one'),('2024-01-05 03:00:00', 3, NULL, 333, 12.11, 'one'),('2024-01-05 04:00:00', 4, NULL, 555, 1.311, 'one'), ('2024-01-05 05:00:00', 5, NULL, 44, 1.311, 'one'), ('2024-01-05 05:00:00', 6, NULL, 666, 1.311, 'one'),('2024-01-05 05:00:00', 7, NULL, 888, 1.311, 'one');
select * from tsdb.tab order by col1;
