// Copyright 2016 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sql_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc/nodedialer"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

type interceptingTransport struct {
	kvcoord.Transport
	sendNext func(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, error)
}

func (t *interceptingTransport) SendNext(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if fn := t.sendNext; fn != nil {
		return fn(ctx, ba)
	} else {
		return t.Transport.SendNext(ctx, ba)
	}
}

// TestAmbiguousCommit verifies that an ambiguous commit error is returned from
// sql.Exec in situations where an EndTxn is part of a batch and the disposition
// of the batch request is unknown after a network failure or timeout. The goal
// here is to prevent spurious transaction retries after the initial transaction
// actually succeeded. In cases where there's an auto- generated primary key,
// this can result in silent duplications. In cases where the primary key is
// specified in advance, it can result in violated uniqueness constraints, or
// duplicate key violations. See #6053, #7604, and #10023.
func TestAmbiguousCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "ambiguousSuccess", func(t *testing.T, ambiguousSuccess bool) {
		var params base.TestServerArgs
		var processed int32
		var tableStartKey atomic.Value

		translateToRPCError := roachpb.NewError(errors.Errorf("%s: RPC error: success=%t", t.Name(), ambiguousSuccess))

		maybeRPCError := func(req *roachpb.ConditionalPutRequest) *roachpb.Error {
			tsk, ok := tableStartKey.Load().([]byte)
			if !ok {
				return nil
			}
			if !bytes.HasPrefix(req.Header().Key, tsk) {
				return nil
			}
			if atomic.AddInt32(&processed, 1) == 1 {
				return translateToRPCError
			}
			return nil
		}

		params.Knobs.KVClient = &kvcoord.ClientTestingKnobs{
			TransportFactory: func(
				opts kvcoord.SendOptions, nodeDialer *nodedialer.Dialer, replicas kvcoord.ReplicaSlice,
			) (kvcoord.Transport, error) {
				transport, err := kvcoord.GRPCTransportFactory(opts, nodeDialer, replicas)
				return &interceptingTransport{
					Transport: transport,
					sendNext: func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
						if ambiguousSuccess {
							br, err := transport.SendNext(ctx, ba)
							// During shutdown, we may get responses that
							// have call.Err set and all we have to do is
							// not crash on those.
							//
							// For the rest, compare and perhaps inject an
							// RPC error ourselves.
							if err == nil && br.Error.Equal(translateToRPCError) {
								// Translate the injected error into an RPC
								// error to simulate an ambiguous result.
								return nil, br.Error.GoError()
							}
							return br, err
						} else {
							if req, ok := ba.GetArg(roachpb.ConditionalPut); ok {
								if pErr := maybeRPCError(req.(*roachpb.ConditionalPutRequest)); pErr != nil {
									// Blackhole the RPC and return an
									// error to simulate an ambiguous
									// result.
									return nil, pErr.GoError()
								}
							}
							return transport.SendNext(ctx, ba)
						}
					},
				}, err
			},
		}

		if ambiguousSuccess {
			params.Knobs.Store = &kvserver.StoreTestingKnobs{
				TestingResponseFilter: func(args roachpb.BatchRequest, _ *roachpb.BatchResponse) *roachpb.Error {
					if req, ok := args.GetArg(roachpb.ConditionalPut); ok {
						return maybeRPCError(req.(*roachpb.ConditionalPutRequest))
					}
					return nil
				},
			}
		}

		testClusterArgs := base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs:      params,
		}

		const numReplicas = 3
		tc := testcluster.StartTestCluster(t, numReplicas, testClusterArgs)
		defer tc.Stopper().Stop(context.TODO())

		// Avoid distSQL so we can reliably hydrate the intended dist
		// sender's cache below.
		for _, server := range tc.Servers {
			st := server.ClusterSettings()
			st.Manual.Store(true)
			sql.DistSQLClusterExecMode.Override(&st.SV, int64(sessiondata.DistSQLOff))
		}

		sqlDB := tc.Conns[0]

		if _, err := sqlDB.Exec(`CREATE DATABASE test`); err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec(`CREATE TABLE test.t (k SERIAL PRIMARY KEY, v INT)`); err != nil {
			t.Fatal(err)
		}

		tableID := sqlutils.QueryTableID(t, sqlDB, "test", "public", "t")
		tableStartKey.Store(keys.MakeTablePrefix(tableID))

		// Wait for new table to split & replication.
		if err := tc.WaitForSplitAndInitialization(tableStartKey.Load().([]byte)); err != nil {
			t.Fatal(err)
		}

		// Ensure that the dist sender's cache is up to date before
		// fault injection.
		if rows, err := sqlDB.Query(`SELECT * FROM test.t`); err != nil {
			t.Fatal(err)
		} else if err := rows.Close(); err != nil {
			t.Fatal(err)
		}

		if _, err := sqlDB.Exec(`INSERT INTO test.t (v) VALUES (1)`); ambiguousSuccess {
			if pqErr, ok := err.(*pq.Error); ok {
				if pqErr.Code != pgcode.StatementCompletionUnknown {
					t.Errorf("expected code %q, got %q (err: %s)",
						pgcode.StatementCompletionUnknown, pqErr.Code, err)
				}
			} else {
				t.Errorf("expected pq error; got %v", err)
			}
		} else {
			if err != nil {
				t.Error(err)
			}
		}

		// Verify a single row exists in the table.
		var rowCount int
		if err := sqlDB.QueryRow(`SELECT count(*) FROM test.t`).Scan(&rowCount); err != nil {
			t.Fatal(err)
		}
		if e := 1; rowCount != e {
			t.Errorf("expected %d row(s) but found %d", e, rowCount)
		}
	})
}
