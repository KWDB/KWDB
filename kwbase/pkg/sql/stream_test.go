// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sql

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestStream(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	path := filepath.Join(baseDir, fmt.Sprintf("test_stream_server%d", 1))
	args := base.TestServerArgs{
		StoreSpecs:    []base.StoreSpec{{Path: path}},
		CatchCoreDump: true,
	}
	s, db, _ := serverutils.StartServer(t, args)

	defer s.Stopper().Stop(ctx)

	// create a test database and tables
	_, err := db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_stream_db`)
	require.NoError(t, err)
	_, err = db.Exec(`create table test_stream_db.cpu
(
    k_timestamp      timestamp not null,
    usage_user       bigint    not null,
    usage_system     bigint    not null,
    usage_idle       bigint    not null,
    usage_nice       bigint    not null,
    usage_iowait     bigint    not null,
    usage_irq        bigint    not null,
    usage_softirq    bool      not null,
    usage_softirq_f  float,
    usage_steal      bigint,
    usage_guest      bigint,
    usage_guest_nice bigint
) attributes (
    hostname char(30) not null,
    region char(30),
    datacenter char(30),
    rack char(30),
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname);`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_stream_db.cpu values ('2023-05-31 10:00:00.123456789', 58, 8, 24, 61, 22, 63, true,0, 44, 80, 38, 'host_0', 'beijing', '', '', '', '', '', '', '', '');`)
	require.NoError(t, err)

	_, err = db.Exec(`create table test_stream_db.cpu_stream_out
(
    w_begin              timestamptz not null,
    w_end                timestamptz not null,
    usage_user_avg       bigint      not null,
    usage_system_avg     bigint      not null,
    count                bigint      not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);`)
	require.NoError(t, err)

	_, err = db.Exec(`create database test_stream_db1;`)
	require.NoError(t, err)
	_, err = db.Exec(`create table test_stream_db1.cpu_stream_out
(
    k_timestamp          timestamp not null,
    usage_user_system    decimal    ,
    usage_idel_nice      decimal    ,
    usage_iowait_irq     decimal    ,
    hostname             char(30)    not null
);`)
	require.NoError(t, err)

	_, err = db.Exec(`create table test_stream_db1.cpu_stream_out1
(
    k_timestamp          timestamptz(3) not null,
    usage_user_system    decimal    ,
    usage_idel_nice      decimal    ,
    usage_iowait_irq     decimal    ,
    hostname             char(30)    not null
);`)
	require.NoError(t, err)

	_, err = db.Exec(`create stream test_stream1 into test_stream_db.cpu_stream_out with options (enable='off') AS select * from test_stream_db.cpu where usage_user > 50;`)
	require.Error(t, err, "")

	_, err = db.Exec(`create stream test_stream1 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream1 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream1 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');`)
	require.NoError(t, err)

	_, err = db.Exec(`create stream test_stream2 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');`)
	require.NoError(t, err)

	_, err = db.Exec(`create stream test_stream3 into cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream3 into cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream4 into cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM cpu GROUP BY hostname, time_window(k_timestamp, '1m');
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream4 into cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM cpu GROUP BY hostname, time_window(k_timestamp, '1m');
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream4 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM cpu GROUP BY hostname, time_window(k_timestamp, '1m');
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream2 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream if not exists test_stream2 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');
`)
	require.NoError(t, err)

	_, err = db.Exec(`create stream test_stream5 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp) as w_begin, last_row(k_timestamp) as w_end, max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream5 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT hostname as hostname, max(usage_user) as usage_user_avg, first(k_timestamp) as w_begin, max(usage_system) as usage_system_avg, last(k_timestamp) as w_end, count(*) as count FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream5 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp) as w_begin, last_row(k_timestamp) as w_end, hostname as hostname, count(*) as count, max(usage_system) as usage_system_avg, max(usage_user) as usage_user_avg FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');
`)
	require.NoError(t, err)

	_, err = db.Exec(`create stream test_stream0 into test_stream_db.cpu with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream0 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT last_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream0 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), first_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream6 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT * from test_stream_db.cpu_stream_out where abs(count)=3 or max(count)>3;
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream6 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT * from test_stream_db.cpu_stream_out where count/0>2;
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream6 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT * from test_stream_db.cpu_stream_out a where random()>2 && (SELECT count(*) from test_stream_db.cpu_stream_out b where a.w_begin=b.w_begin) > 0;
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream7 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT w_begin,w_end,usage_user_avg,usage_system_avg,max(count),hostname from test_stream_db.cpu_stream_out where count+usage_user_avg>3;
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream7 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT w_begin,w_end,usage_user_avg,usage_system_avg,count/0,hostname from test_stream_db.cpu_stream_out;
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream7 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT w_begin,w_end,abs(usage_user_avg)+usage_system_avg,usage_system_avg,random(),hostname from test_stream_db.cpu_stream_out;
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream8 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT * from test_stream_db.cpu_stream_out where 2>1 order by w_begin;
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream8 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT * from (select * from test_stream_db.cpu_stream_out);
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream8 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT * from kwdb_internal.kwdb_streams;
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream8 into kwdb_internal.kwdb_streams with options (enable='off') AS SELECT * from test_stream_db.cpu_stream_out;
`)
	require.Error(t, err)

	_, err = db.Exec(`create stream test_stream8 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT a.* from test_stream_db.cpu_stream_out a,test_stream_db.cpu_stream_out b where a.w_begin=b.w_begin;
`)
	require.Error(t, err)

	// alter
	_, err = db.Exec(`alter table test_stream_db.cpu_stream_out drop column w_begin;`)
	require.Error(t, err)

	_, err = db.Exec(`alter table test_stream_db.cpu_stream_out drop column count;`)
	require.Error(t, err)

	_, err = db.Exec(`alter table test_stream_db.cpu_stream_out add column count_a int;`)
	require.Error(t, err)

	_, err = db.Exec(`alter table test_stream_db.cpu drop column k_timestamp;`)
	require.Error(t, err)

	_, err = db.Exec(`alter table test_stream_db.cpu drop column usage_user;`)
	require.Error(t, err)

	_, err = db.Exec(`alter table test_stream_db.cpu drop column usage_guest;`)
	require.Error(t, err)

	_, err = db.Exec(`alter table test_stream_db.cpu add column usage_user_a int;`)
	require.NoError(t, err)

	_, err = db.Exec(`drop table test_stream_db.cpu_stream_out;`)
	require.Error(t, err)

	_, err = db.Exec(`drop table test_stream_db.cpu;`)
	require.Error(t, err)

	_, err = db.Exec(`drop stream test_stream1;`)
	require.NoError(t, err)

	_, err = db.Exec(`drop table test_stream_db.cpu_stream_out;`)
	require.Error(t, err)

	_, err = db.Exec(`drop table test_stream_db.cpu;`)
	require.Error(t, err)

	_, err = db.Exec(`drop stream test_stream2;`)
	require.NoError(t, err)

	_, err = db.Exec(`drop stream test_stream5;`)
	require.NoError(t, err)

	_, err = db.Exec(`drop table test_stream_db.cpu_stream_out;`)
	require.NoError(t, err)

	_, err = db.Exec(`drop table test_stream_db.cpu;`)
	require.NoError(t, err)

	_, err = db.Exec(`create table test_stream_db.cpu_stream_out
(
    w_begin              timestamptz not null,
    w_end                timestamptz not null,
    usage_user_avg       bigint      not null,
    usage_system_avg     bigint      not null,
    count                bigint      not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);`)
	require.NoError(t, err)

	_, err = db.Exec(`create table test_stream_db.cpu
	(
		k_timestamp      timestamp not null,
		usage_user       bigint    not null,
		usage_system     bigint    not null,
		usage_idle       bigint    not null,
		usage_nice       bigint    not null,
		usage_iowait     bigint    not null,
		usage_irq        bigint    not null,
		usage_softirq    bool      not null,
		usage_softirq_f  float,
		usage_steal      bigint,
		usage_guest      bigint,
		usage_guest_nice bigint
	) attributes (
		hostname char(30) not null,
		region char(30),
		datacenter char(30),
		rack char(30),
		os char(30),
		arch char(30),
		team char(30),
		service char(30),
		service_version char(30),
		service_environment char(30)
	)
	primary attributes (hostname);`)
	require.NoError(t, err)

	_, err = db.Exec(`create stream test_stream1 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first_row(k_timestamp), last_row(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_window(k_timestamp, '1m');
`)
	require.NoError(t, err)

	_, err = db.Exec(`create stream test_stream2 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, count_window(5);`)
	require.NoError(t, err)

	_, err = db.Exec(`create stream test_stream3 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, session_window(k_timestamp, '5m');`)
	require.NoError(t, err)

	_, err = db.Exec(`create stream test_stream4 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, event_window(usage_user > 50, usage_system < 20);`)
	require.NoError(t, err)

	_, err = db.Exec(`create stream test_stream5 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, state_window(usage_idle);`)
	require.NoError(t, err)

	_, err = db.Exec(`create stream test_stream6 into test_stream_db.cpu_stream_out with options (enable='off') AS SELECT first(k_timestamp), last(k_timestamp), max(usage_user), max(usage_system), count(*), hostname FROM test_stream_db.cpu GROUP BY hostname, time_bucket(k_timestamp, '1m');`)
	require.NoError(t, err)

	// on
	_, err = db.Exec(`create table test_stream_db.cpu_normal
(
    k_timestamp      timestamp(6) not null,
    usage_user       bigint    not null,
    usage_system     bigint    not null,
    usage_idle       bigint    not null,
    usage_nice       bigint    not null,
    usage_iowait     bigint    not null,
    usage_irq        bigint    not null,
    usage_softirq    bigint    not null,
    usage_steal      bigint,
    usage_guest      bigint,
    usage_guest_nice bigint
) attributes (
    hostname char(30) not null,
    region char(30),
    datacenter char(30),
    rack char(30),
    os char(30),
    arch char(30),
    team char(30),
    service char(30),
    service_version char(30),
    service_environment char(30)
    )
primary attributes (hostname);`)
	require.NoError(t, err)

	_, err = db.Exec(`create table test_stream_db.cpu_avg_normal
(
    k_timestamp          timestamp(6) not null,
    usage_user_system    bigint       not null,
    usage_idel_nice      bigint       not null,
    usage_iowait_irq     float        not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);`)
	require.NoError(t, err)

	_, err = db.Exec(`create table test_stream_db1.cpu_avg_normal
(
    k_timestamp          timestamptz not null,
    usage_user_system    decimal,
    usage_idel_nice      decimal,
    usage_iowait_irq     decimal,
    hostname             char(30)    not null,
    PRIMARY KEY (k_timestamp ASC, hostname ASC)
);`)
	require.NoError(t, err)

	_, err = db.Exec(`create table test_stream_db.cpu_avg_agg1
(
    w_begin              timestamp(6) not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       bigint    not null,
    usage_system_avg     bigint    not null,
    count                bigint    not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);`)
	require.NoError(t, err)
	_, err = db.Exec(`create table test_stream_db.cpu_avg_agg8
(
    w_begin              timestamptz(6) not null,
    w_end                timestamptz(6) not null,
    w_begin1             timestamptz(6) not null,
    w_end1               timestamptz(6) not null,
    usage_user_avg       bigint    not null,
    usage_system_avg     bigint    not null,
    count                bigint    not null
) attributes (
    hostname char(30) not null
    )
primary attributes (hostname);`)
	require.NoError(t, err)
	_, err = db.Exec(`create table test_stream_db1.cpu_avg_agg7
(
    w_begin              timestamptz(6)   not null,
    w_end                timestamptz(6) not null,
    usage_user_avg       decimal     not null,
    usage_system_avg     decimal     not null,
    count                decimal     not null,
    hostname             char(30)    not null,
    PRIMARY KEY (w_begin ASC, hostname ASC)
);`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_stream_db.cpu_normal values ('2023-05-31 10:00:00.123456789', 58, 8, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', 'beijing', '', '', '', '', '', '', '', '');`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test_stream_db.cpu_normal values ('2023-05-31 10:00:10.123456789', 58, 33, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO test_stream_db.cpu_normal values ('2023-05-31 10:00:20.123456789', 58, 44, 24, 61, 22, 63, 6, 44, 80, 38, 'host_1', 'shanghai', '', '', '', '', '', '', '', '');`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE STREAM cpu_stream_normal_1 INTO test_stream_db.cpu_avg_normal WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='off', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT k_timestamp AS k_timestamp, hostname AS hostname, usage_user+usage_system AS usage_user_system, usage_idle*usage_nice AS usage_idel_nice, usage_iowait/usage_irq AS usage_iowait_irq FROM test_stream_db.cpu_normal where usage_system>11;`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE STREAM cpu_stream_agg_rel_7 INTO test_stream_db1.cpu_avg_agg7 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='off',IGNORE_EXPIRED='on', MAX_DELAY='5s',SYNC_TIME='3s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp), last(k_timestamp), avg(usage_user), avg(usage_system), count(*), hostname FROM test_stream_db.cpu_normal WHERE usage_system>0 GROUP BY hostname, state_window(usage_idle);`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE STREAM cpu_stream_agg_ts_3 INTO test_stream_db.cpu_avg_agg1 WITH OPTIONS(recalculate_delay_rounds='1',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='off', MAX_DELAY='10s',SYNC_TIME='5s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end, avg(usage_system) as usage_system_avg, hostname as hostname, count(*) as count, avg(usage_user) as usage_user_avg FROM test_stream_db.cpu_normal WHERE abs(usage_system)>0 GROUP BY hostname, count_window(3);`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE STREAM cpu_stream_agg_ts_8 INTO test_stream_db.cpu_avg_agg8 WITH OPTIONS(recalculate_delay_rounds='0',MAX_RETRIES='3',PROCESS_HISTORY='on',IGNORE_EXPIRED='off', MAX_DELAY='10s',SYNC_TIME='5s',BUFFER_SIZE='1024kib',checkpoint_interval='2s',heartbeat_interval='1s') AS SELECT first(k_timestamp) as w_begin, last(k_timestamp) as w_end,first_row(k_timestamp) as w_begin1, last_row(k_timestamp) as w_end1, avg(usage_user) as usage_user_avg, avg(usage_system) as usage_system_avg, count(*) as count, hostname as hostname FROM test_stream_db.cpu_normal WHERE usage_system+usage_system>0 GROUP BY hostname, time_window(k_timestamp, '1m','30s');`)
	require.NoError(t, err)
	time.Sleep(time.Second * 2)
	//
	_, err = db.Exec(`INSERT INTO test_stream_db.cpu_normal values ('2023-05-31 10:00:30.123456789', 58, 10, 24, 61, 22, 63, 6, 44, 80, 38, 'host_1', '', '', '', '', '', '', '', '', '');`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO test_stream_db.cpu_normal values ('2023-05-31 10:00:40.123456789', 58, 55, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO test_stream_db.cpu_normal values ('2023-05-31 10:00:50.123456789', 58, 66, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO test_stream_db.cpu_normal values ('2023-05-31 10:01:00.123456789', 58, 12, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO test_stream_db.cpu_normal values ('2023-05-31 10:01:10.123456789', 58, 77, 25, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO test_stream_db.cpu_normal values ('2023-05-31 10:01:20.123456789', 58, 88, 24, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO test_stream_db.cpu_normal values ('2023-05-31 10:10:00.123456789', 58, 3, 25, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');`)
	require.NoError(t, err)
	time.Sleep(time.Second * 5)
	_, err = db.Exec(`alter stream cpu_stream_agg_ts_3 set enable='off';`)
	require.NoError(t, err)
	time.Sleep(time.Second * 2)
	_, err = db.Exec(`alter stream cpu_stream_agg_ts_3 set enable='on';`)
	require.NoError(t, err)
	time.Sleep(time.Second * 2)
	_, err = db.Exec(`INSERT INTO test_stream_db.cpu_normal values ('2023-05-31 10:10:00.123456789', 58, 3, 25, 61, 22, 63, 6, 44, 80, 38, 'host_0', '', '', '', '', '', '', '', '', '');`)
	require.NoError(t, err)
	time.Sleep(time.Second * 5)
	_, err = db.Exec(`drop table test_stream_db.cpu_normal cascade;`)
	require.NoError(t, err)
	_, err = db.Exec(`drop table test_stream_db.cpu_avg_normal cascade;`)
	require.NoError(t, err)
	_, err = db.Exec(`drop table test_stream_db1.cpu_avg_normal cascade;`)
	require.NoError(t, err)
}
