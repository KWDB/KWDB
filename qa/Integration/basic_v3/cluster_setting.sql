SHOW CLUSTER SETTING ts.txn.atomicity.enabled;
SHOW CLUSTER SETTING ts.raftlog_combine_wal.enabled;
SHOW CLUSTER SETTING ts.wal.wal_level;

SET CLUSTER SETTING ts.txn.atomicity.enabled = TRUE;
SHOW CLUSTER SETTING ts.txn.atomicity.enabled;

SET CLUSTER SETTING ts.wal.wal_level = 1;
SHOW CLUSTER SETTING ts.wal.wal_level;

SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = TRUE;
SHOW CLUSTER SETTING ts.raftlog_combine_wal.enabled;

SET CLUSTER SETTING ts.wal.wal_level = 0;
SHOW CLUSTER SETTING ts.wal.wal_level;

SET CLUSTER SETTING ts.wal.wal_level = 3;
SHOW CLUSTER SETTING ts.wal.wal_level;

SET CLUSTER SETTING ts.txn.atomicity.enabled = FALSE;
SHOW CLUSTER SETTING ts.txn.atomicity.enabled;

SET CLUSTER SETTING ts.wal.wal_level = 0;
SHOW CLUSTER SETTING ts.wal.wal_level;

SET CLUSTER SETTING ts.txn.atomicity.enabled = TRUE;
SHOW CLUSTER SETTING ts.txn.atomicity.enabled;

SET CLUSTER SETTING ts.wal.wal_level = 3;
SHOW CLUSTER SETTING ts.wal.wal_level;

SET CLUSTER SETTING ts.txn.atomicity.enabled = TRUE;
SHOW CLUSTER SETTING ts.txn.atomicity.enabled;

SET CLUSTER SETTING ts.wal.wal_level = 2;
SHOW CLUSTER SETTING ts.wal.wal_level;

SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = TRUE;
SHOW CLUSTER SETTING ts.raftlog_combine_wal.enabled;

SET CLUSTER SETTING ts.txn.atomicity.enabled = TRUE;
SHOW CLUSTER SETTING ts.txn.atomicity.enabled;

SET CLUSTER SETTING ts.txn.atomicity.enabled = FALSE;
SET CLUSTER SETTING ts.raftlog_combine_wal.enabled = FALSE;
SET CLUSTER SETTING ts.wal.wal_level = 2;
