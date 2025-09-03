SET CLUSTER SETTING server.advanced_distributed_operations.enabled = true;
SET cluster setting ts.rows_per_block.max_limit=10;
SET cluster setting ts.blocks_per_segment.max_limit=50;

-- create-test: 500,rel,ts
select count(*) from kwdb_internal.ranges_no_leases where database_name = 'test_create_rel_table' ;

select count(*) from kwdb_internal.ranges_no_leases where database_name = 'test_create_rel_table' and range_type = 'DEFAULT_RANGE';

select count(*) from kwdb_internal.ranges_no_leases where database_name = 'test_create_ts_table' and range_type != 'DEFAULT_RANGE';

select count(*) from kwdb_internal.ranges_no_leases where database_name = 'test_create_ts_table' ;

select count(*) from kwdb_internal.ranges_no_leases where database_name = 'test_create_ts_table' and range_type = 'TS_RANGE';

select count(*) from kwdb_internal.ranges_no_leases where database_name = 'test_create_ts_table' and range_type != 'TS_RANGE';




