SET CLUSTER SETTING ts.rows_per_block.max_limit = 10;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 50;

-- init
select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;
select count(1) from tsdb1.ts_t3;
select count(1) from tsdb1.ts_t4;

-- kill: c4
select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;
select count(1) from tsdb1.ts_t3;
select count(1) from tsdb1.ts_t4;
-- sleep: 120s

select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;
select count(1) from tsdb1.ts_t3;
select count(1) from tsdb1.ts_t4;
-- kill: c1,c2,c3,c5
-- sleep: 10s

-- restart: c1,c2,c3,c4,c5

select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;
select count(1) from tsdb1.ts_t3;
select count(1) from tsdb1.ts_t4;

-- node: c2

select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;
select count(1) from tsdb1.ts_t3;
select count(1) from tsdb1.ts_t4;

-- join: c6

-- upgrade: c3

-- sleep: 5s

-- upgrade-complete: c3

-- node: c3
select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;
select count(1) from tsdb1.ts_t3;
select count(1) from tsdb1.ts_t4;
-- node: c6
select count(*) from tsdb1.ts_t3;
select count(*) from tsdb1.ts_t4;
select count(1) from tsdb1.ts_t3;
select count(1) from tsdb1.ts_t4;

-- sleep: 10s
-- kill: c6
