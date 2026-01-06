set max_push_limit_number = 10000000;

-- set cluster settings for 2.0 start
set cluster setting sql.distsql.temp_storage.workmem='4096Mib';
-- set cluster setting sql.max_device_count=5000;
set cluster setting sql.all_push_down.enabled=true;
set cluster setting sql.pg_encode_short_circuit.enabled = true;
set cluster setting ts.parallel_degree=8;
set cluster setting sql.stats.ts_automatic_collection.enabled=false;
set cluster setting sql.stats.tag_automatic_collection.enabled=false;
-- alter schedule scheduled_table_retention  Recurring  '0 0 1 1 ? 2099';

set cluster setting server.tsinsert_direct.enabled = false;
set cluster setting sql.stats.tag_automatic_collection.enabled = false;
set cluster setting ts.ack_before_application.enabled=true;
-- set cluster setting ts.raftlog_combine_wal.enabled=true;
set cluster setting sql.defaults.vectorize='auto';

-- set cluster settings for 2.0 end
set cluster setting ts.raft_log.sync_period='0s';

-- enable last row cache to speed up last/last_row query which will slow down insertion a little bit
set cluster setting ts.last_row_optimization.enabled=true;
-- adjust storage 3.0 configuration to achieve better performance
set cluster setting ts.rows_per_block.min_limit=1024;
set cluster setting ts.reserved_last_segment.max_limit=2;

-- Case-insensitive, support GB, MB e.g. and GiB, MiB e.g. if no unit is specified, the default is byte(B).
set cluster setting ts.mem_segment_size.max_limit='67108864';
set cluster setting ts.block.lru_cache.max_limit ='16106127360';

set cluster setting ts.compress.stage=1;