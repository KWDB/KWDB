SET CLUSTER SETTING ts.rows_per_block.max_limit = 10;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 50;
SET CLUSTER SETTING server.time_until_store_dead = '1min15s';
SET CLUSTER SETTING kv.allocator.ts_consider_rebalance.enabled = true;

-- init
-- sleep: 10s
SELECT COUNT(*) FROM tsdb1.ts_t3;
SELECT COUNT(*) FROM tsdb1.ts_t4;

-- join: c6
-- join: c7
-- sleep: 30s
-- wait-nonzero-replica: c6
-- wait-nonzero-replica: c7
SELECT COUNT(*) > 0 FROM kwdb_internal.ranges WHERE database_name = 'tsdb1' AND 6=ANY(replicas);
SELECT COUNT(*) > 0 FROM kwdb_internal.ranges WHERE database_name = 'tsdb1' AND 7=ANY(replicas);
SELECT COUNT(*) FROM tsdb1.ts_t3;
SELECT COUNT(*) FROM tsdb1.ts_t4;
SELECT COUNT(1) FROM tsdb1.ts_t3;
SELECT COUNT(1) FROM tsdb1.ts_t4;

-- kill: c4
-- sleep: 30s
alter range default configure zone using num_replicas = 5;
-- sleep: 90s
-- wait-zero-replica: c4
alter range default configure zone using num_replicas = 5;
SELECT COUNT(*) = 0 FROM kwdb_internal.ranges WHERE database_name = 'tsdb1' AND 4=ANY(replicas);
SELECT COUNT(*) FROM tsdb1.ts_t3;
SELECT COUNT(*) FROM tsdb1.ts_t4;
SELECT COUNT(1) FROM tsdb1.ts_t3;
SELECT COUNT(1) FROM tsdb1.ts_t4;

-- kill: c5
-- sleep: 120s
-- wait-zero-replica: c5
SELECT COUNT(*) = 0 FROM kwdb_internal.ranges WHERE database_name = 'tsdb1' AND 5=ANY(replicas);
SELECT COUNT(*) FROM tsdb1.ts_t3;
SELECT COUNT(*) FROM tsdb1.ts_t4;
SELECT COUNT(1) FROM tsdb1.ts_t3;
SELECT COUNT(1) FROM tsdb1.ts_t4;

-- sleep: 10s
-- kill: c6
-- kill: c7
