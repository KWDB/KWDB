--basic test
drop database if exists test_cluster_setting cascade;
create ts database test_cluster_setting;
use test_cluster_setting;

set sql_safe_updates = false;
-- Outlier check
SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 1;
SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 0;
SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = -1;
SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 2.03;
SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 20d;
SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 2147483647;
SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 2147483648;
SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 10000000000;
SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = '';
SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = \x12;

SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 1;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 0;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = -1;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 2.03;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 20d;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 1000000;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 1000001;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 10000000000;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = '';
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = \x12;

SET CLUSTER SETTING ts.rows_per_block.max_limit = 10;
SET CLUSTER SETTING ts.rows_per_block.max_limit = 9;
SET CLUSTER SETTING ts.rows_per_block.max_limit = -1;
SET CLUSTER SETTING ts.rows_per_block.max_limit = 2.03;
SET CLUSTER SETTING ts.rows_per_block.max_limit = 20d;
SET CLUSTER SETTING ts.rows_per_block.max_limit = 1000;
SET CLUSTER SETTING ts.rows_per_block.max_limit = 1001;
SET CLUSTER SETTING ts.rows_per_block.max_limit = 10000000000;
SET CLUSTER SETTING ts.rows_per_block.max_limit = '';
SET CLUSTER SETTING ts.rows_per_block.max_limit = \x12;

SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = 0;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = -1;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = 2.03;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = 20d;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = 2147483647;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = 2147483648;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = 10000000000;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = '';
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = \x12;

SET CLUSTER SETTING ts.compression.type = 2;
SET CLUSTER SETTING ts.compression.type = 2.33;
SET CLUSTER SETTING ts.compression.type = '';
SET CLUSTER SETTING ts.compression.type = \x12;
SET CLUSTER SETTING ts.compression.type = 'LZ4';
SET CLUSTER SETTING ts.compression.type = 'GZIP';
SET CLUSTER SETTING ts.compression.type = 'LZO';
SET CLUSTER SETTING ts.compression.type = 'XZ';
SET CLUSTER SETTING ts.compression.type = 'ZSTD';
SET CLUSTER SETTING ts.compression.type = 'LZMA';
SET CLUSTER SETTING ts.compression.type = 'lz4';
SET CLUSTER SETTING ts.compression.type = 'gzip';
SET CLUSTER SETTING ts.compression.type = 'lzo';
SET CLUSTER SETTING ts.compression.type = 'xz';
SET CLUSTER SETTING ts.compression.type = 'zstd';
SET CLUSTER SETTING ts.compression.type = 'lzma';

SET CLUSTER SETTING ts.compression.level = 2;
SET CLUSTER SETTING ts.compression.level = 2.33;
SET CLUSTER SETTING ts.compression.level = '';
SET CLUSTER SETTING ts.compression.level = \x12;
SET CLUSTER SETTING ts.compression.level = 'LOW';
SET CLUSTER SETTING ts.compression.level = 'MIDDLE';
SET CLUSTER SETTING ts.compression.level = 'HIGH';
SET CLUSTER SETTING ts.compression.level = 'low';
SET CLUSTER SETTING ts.compression.level = 'middle';
SET CLUSTER SETTING ts.compression.level = 'high';

SET CLUSTER SETTING ts.compression.type = 'gzip';
SET CLUSTER SETTING ts.compression.level = 'middle';

SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 3;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 2;
SET CLUSTER SETTING ts.rows_per_block.max_limit = 10;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = 1;

create table t1(ts timestamp not null, a int) tags(b int not null) primary tags(b);

-- subgroup_1、subgroup_2 insert
insert into t1 values(1672531201000, 111, 1);
insert into t1 values(1672531202000, 111, 1);
insert into t1 values(1672531203000, 222, 2);
insert into t1 values(1672531204000, 222, 2);
insert into t1 values(1672531205000, 333, 3);
insert into t1 values(1672531206000, 333, 3);
insert into t1 values(1672531201000, 444, 4);
insert into t1 values(1672531202000, 444, 4);
select * from t1 order by b, ts;

SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 1;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 5;
SET CLUSTER SETTING ts.rows_per_block.max_limit = 100;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = 1;

-- subgroup_2、subgroup_3、subgroup_4 insert
insert into t1 values(1673531201000, 555, 5);
insert into t1 values(1673531202000, 555, 5);
insert into t1 values(1673531203000, 666, 6);
insert into t1 values(1673531204000, 666, 6);
insert into t1 values(1672531205000, 777, 7);
insert into t1 values(1672531206000, 777, 7);
insert into t1 values(1672531201000, 888, 8);
insert into t1 values(1672531202000, 888, 8);
select * from t1 order by b, ts;

SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 2;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 3;
SET CLUSTER SETTING ts.rows_per_block.max_limit = 10;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = 0;

-- subgroup_5 insert
insert into t1 values(1675531201000, 999, 9);
insert into t1 values(1675531202000, 999, 9);
insert into t1 values(1675531203000, 999, 9);
insert into t1 values(1675531204000, 999, 9);
insert into t1 values(1675531205000, 999, 9);
insert into t1 values(1675531206000, 999, 9);
insert into t1 values(1675531207000, 999, 9);
insert into t1 values(1675531208000, 999, 9);
insert into t1 values(1675531209000, 999, 9);
insert into t1 values(1675531210000, 999, 9);
insert into t1 values(1675531211000, 999, 9);
insert into t1 values(1676531203000, 666, 10);
insert into t1 values(1676531204000, 666, 10);
select * from t1 order by b, ts;

SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 20;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 15;
SET CLUSTER SETTING ts.rows_per_block.max_limit = 200;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = 7;

select * from t1 order by b, ts;

-- alter table
SET CLUSTER SETTING ts.dedup.rule = 'merge';
alter table t1 add column c int;
select * from t1 order by b, ts;
alter table t1 drop column c;
select * from t1 order by b, ts;
alter table t1 add column c int;
select * from t1 order by b, ts;
insert into t1 values(1672531215000, 556, NULL, 3);
insert into t1 values(1672531216000, 666, 11, 1);
insert into t1 values(1672531217000, 777, NULL, 2);
insert into t1 values(1672531217000, NULL, 0, 2);
insert into t1 values(1672531218000, 888, 33, 3);
SET CLUSTER SETTING ts.dedup.rule = 'override';

select * from t1 order by b, ts;

SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 3;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 10;
SET CLUSTER SETTING ts.rows_per_block.max_limit = 40;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = 3;

select * from t1 order by b, ts;

DROP DATABASE test_cluster_setting CASCADE;

SET CLUSTER SETTING ts.entities_per_subgroup.max_limit = 500;
SET CLUSTER SETTING ts.blocks_per_segment.max_limit = 500;
SET CLUSTER SETTING ts.rows_per_block.max_limit = 100;
SET CLUSTER SETTING ts.cached_partitions_per_subgroup.max_limit = 10;
