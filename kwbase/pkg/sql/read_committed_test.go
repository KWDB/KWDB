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
	gosql "database/sql"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/stretchr/testify/require"
)

type AtomicBool struct {
	value int32
}

func (b *AtomicBool) Get() bool {
	return atomic.LoadInt32(&b.value) == 1
}

func (b *AtomicBool) Set(value bool) {
	var v int32
	if value {
		v = 1
	}
	atomic.StoreInt32(&b.value, v)
}

func TestReadCommittedStmtRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params := base.TestServerArgs{}

	var trapReadCommittedWrites AtomicBool
	var trappedReadCommittedWritesOnce sync.Once
	finishedReadCommittedScans := make(chan struct{})
	finishedExternalTxn := make(chan struct{})
	var sawWriteTooOldError AtomicBool
	var kvTableID uint32

	filterFunc := func(ctx context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		if !trapReadCommittedWrites.Get() {
			return nil
		}
		if ba.Txn == nil || ba.Txn.IsoLevel != enginepb.ReadCommitted {
			return nil
		}
		for _, arg := range ba.Requests {
			if req := arg.GetInner(); req.Method() == roachpb.Put {
				put := req.(*roachpb.PutRequest)
				// Only count writes to the kv table.

				_, tableID, err := keys.DecodeTablePrefix(put.Key)
				if err != nil || uint32(tableID) != kvTableID {
					return nil
				}
				trappedReadCommittedWritesOnce.Do(func() {
					close(finishedReadCommittedScans)
					<-finishedExternalTxn
				})
			}
		}

		return nil
	}
	params.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: filterFunc,
	}
	params.Knobs.SQLExecutor = &ExecutorTestingKnobs{
		OnReadCommittedStmtRetry: func(retryReason error) {
			if strings.Contains(retryReason.Error(), "WriteTooOldError") {
				sawWriteTooOldError.Set(true)
			}
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	//codec = s.ApplicationLayer().Codec()

	// Create a table with three rows. Note that k is not the primary key,
	// so locking won't be pushed into the initial scan of the UPDATEs below.
	_, err := sqlDB.Exec(`CREATE TABLE kv (k TEXT, v INT);`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO kv VALUES ('a', 1);`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO kv VALUES ('b', 2);`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO kv VALUES ('c', 3);`)
	require.NoError(t, err)
	err = sqlDB.QueryRow("SELECT 'kv'::regclass::oid").Scan(&kvTableID)
	require.NoError(t, err)

	g := ctxgroup.WithContext(ctx)

	// Create a read committed transaction that writes to key "a" in its first
	// statement before hitting a retryable error during the second statement.
	tx, err := sqlDB.BeginTx(ctx, &gosql.TxOptions{Isolation: gosql.LevelReadCommitted})
	require.NoError(t, err)
	// Write to "a" in the first statement.
	_, err = tx.Exec(`UPDATE kv SET v = v+10 WHERE k = 'a'`)
	require.NoError(t, err)

	// Start blocking writes in the read committed transaction.
	trapReadCommittedWrites.Set(true)

	// Perform a series of reads and writes in the second statement.
	// Read from "b" and "c" to establish refresh spans.
	// Write to "b" in the transaction, without issue.
	// Write to "c" in the transaction to hit the write-write conflict, which
	// causes the statement to need to retry.
	g.GoCtx(func(ctx context.Context) error {
		_, err = tx.Exec(`UPDATE kv SET v = v+10 WHERE k = 'b' OR k = 'c'`)
		sqlutils.MakeSQLRunner(sqlDB).CheckQueryResults(t, `SELECT k, v FROM kv ORDER BY k`, [][]string{
			{"a", "1"},
			{"b", "2"},
			{"c", "13"},
		})
		return err
	})

	// Wait for the table to be scanned first.
	<-finishedReadCommittedScans

	// Write to "c" outside the transaction to create a write-write conflict.
	_, err = sqlDB.Exec(`UPDATE kv SET v = v+10 WHERE k = 'c'`)
	require.NoError(t, err)
	sqlutils.MakeSQLRunner(sqlDB).CheckQueryResults(t, `SELECT k, v FROM kv ORDER BY k`, [][]string{
		{"a", "1"},
		{"b", "2"},
		{"c", "13"},
	})

	// Now let the READ COMMITTED write go through. It should encounter a
	// WriteTooOldError and retry.
	close(finishedExternalTxn)

	err = g.Wait()
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	sqlutils.MakeSQLRunner(sqlDB).CheckQueryResults(t, `SELECT k, v FROM kv ORDER BY k`, [][]string{
		{"a", "11"},
		{"b", "12"},
		{"c", "23"},
	})
	require.True(t, sawWriteTooOldError.Get())
}

// TestReadCommittedReadTimestampNotSteppedOnCommit verifies that the read
// timestamp of a read committed transaction is stepped between SQL statements,
// but not before commit.
func TestReadCommittedReadTimestampNotSteppedOnCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Keep track of the read timestamps of the read committed transaction during
	// each KV operation.
	var txnReadTimestamps []hlc.Timestamp
	filterFunc := func(ba roachpb.BatchRequest, _ *roachpb.BatchResponse) *roachpb.Error {
		if ba.Txn == nil || ba.Txn.IsoLevel != enginepb.ReadCommitted {
			return nil
		}
		req := ba.Requests[0]
		method := req.GetInner().Method()
		if method == roachpb.ConditionalPut || (method == roachpb.EndTxn && req.GetEndTxn().IsParallelCommit()) {
			txnReadTimestamps = append(txnReadTimestamps, ba.Txn.ReadTimestamp)
		}
		return nil
	}

	ctx := context.Background()
	params := base.TestServerArgs{}
	params.Knobs.Store = &kvserver.StoreTestingKnobs{
		// NOTE: we use a TestingResponseFilter and not a TestingRequestFilter to
		// avoid potential flakiness from requests which are redirected or retried.
		TestingResponseFilter: filterFunc,
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`CREATE TABLE kv (k TEXT, v INT);`)
	require.NoError(t, err)

	// Create a read committed transaction that writes to three rows in three
	// different statements and then commits.
	tx, err := sqlDB.BeginTx(ctx, &gosql.TxOptions{Isolation: gosql.LevelReadCommitted})
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO kv VALUES ('a', 1);`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO kv VALUES ('b', 2);`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO kv VALUES ('c', 3);`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Verify that the transaction's read timestamp was not stepped on commit but
	// was stepped between every other statement.
	require.Len(t, txnReadTimestamps, 4)
	require.True(t, txnReadTimestamps[0].Less(txnReadTimestamps[1]))
	require.True(t, txnReadTimestamps[1].Less(txnReadTimestamps[2]))
	require.True(t, txnReadTimestamps[2].Equal(txnReadTimestamps[3]))
}
