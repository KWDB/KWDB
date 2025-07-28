SET CLUSTER SETTING ts.dedup.rule = 'merge';
SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = true;
-- kill: c1
-- kill: c2
-- kill: c3
-- kill: c4
-- kill: c5
-- restart: c1
-- restart: c2
-- restart: c3
-- restart: c4
-- restart: c5
SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = false;
-- kill: c1
-- kill: c2
-- kill: c3
-- kill: c4
-- kill: c5
-- restart: c1
-- restart: c2
-- restart: c3
-- restart: c4
-- restart: c5
SET CLUSTER SETTING ts.dedup.rule = 'keep';
SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = true;
SET CLUSTER SETTING ts.dedup.rule = 'reject';
SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = true;
SET CLUSTER SETTING ts.dedup.rule = 'discard';
SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = true;
SET CLUSTER SETTING ts.dedup.rule = 'override';
SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = true;
-- kill: c1
-- kill: c2
-- kill: c3
-- kill: c4
-- kill: c5
-- restart: c1
-- restart: c2
-- restart: c3
-- restart: c4
-- restart: c5
SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = false;
-- kill: c1
-- kill: c2
-- kill: c3
-- kill: c4
-- kill: c5
-- restart: c1
-- restart: c2
-- restart: c3
-- restart: c4
-- restart: c5
create ts database tsdb;
SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = true;
SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = false;
create table tsdb.t1(ts timestamp not null,a int, b int) tags(tag1 int not null, tag2 int) primary tags(tag1);
SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = true;
drop table tsdb.t1;
SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = true;
-- kill: c1
-- kill: c2
-- kill: c3
-- kill: c4
-- kill: c5
-- restart: c1
-- restart: c2
-- restart: c3
-- restart: c4
-- restart: c5
