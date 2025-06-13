// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
//
//	http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package kvcoord

import (
	"context"
	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"sync"
)

// TsSender is a Sender to send TS requests to TS DB
type TsSender struct {
	tsEngine *tse.TsEngine

	isSingleNode     bool
	rpcContext       *rpc.Context
	gossip           *gossip.Gossip
	stopper          *stop.Stopper
	interceptorStack []lockedSender
	interceptorAlloc struct {
		arr [2]txnInterceptor
		tsTxnHeartbeater
		tsTxnCommitter
	}
	txn roachpb.Transaction
}

// TsDBConfig is config for building TsSender
type TsDBConfig struct {
	KvDB         *kv.DB
	Sender       kv.Sender
	TsEngine     *tse.TsEngine
	RPCContext   *rpc.Context
	Gossip       *gossip.Gossip
	Stopper      *stop.Stopper
	IsSingleNode bool
}

var _ kv.Sender = &TsSender{}

// Send implements the Sender interface.
func (s *TsSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	resp := &roachpb.BatchResponse{}
	if s.isSingleNode {
		rangeGroupID := uint64(1)
		var putPayload [][]byte
		for _, ru := range ba.Requests {
			r := ru.GetInner()
			switch tdr := r.(type) {
			case *roachpb.TsPutRequest:
				putPayload = append(putPayload, tdr.Value.RawBytes)
			case *roachpb.TsPutTagRequest:
				putPayload = append(putPayload, tdr.Value.RawBytes)
			case *roachpb.TsTagUpdateRequest:
				var pld [][]byte
				pld = append(pld, tdr.Tags)
				err := s.tsEngine.PutEntity(rangeGroupID, tdr.TableId, pld, 0)
				if err != nil {
					return nil, &roachpb.Error{Message: err.Error()}
				}
				resp.Responses = append(resp.Responses, roachpb.ResponseUnion{
					Value: &roachpb.ResponseUnion_TsTagUpdate{
						TsTagUpdate: &roachpb.TsTagUpdateResponse{
							ResponseHeader: roachpb.ResponseHeader{NumKeys: 1},
						},
					},
				})
			case *roachpb.TsDeleteRequest:
				rows, err := s.tsEngine.DeleteData(tdr.TableId, rangeGroupID, tdr.PrimaryTags, tdr.TsSpans, 0)
				if err != nil {
					return nil, &roachpb.Error{Message: err.Error()}
				}
				resp.Responses = append(resp.Responses, roachpb.ResponseUnion{
					Value: &roachpb.ResponseUnion_TsDelete{
						TsDelete: &roachpb.TsDeleteResponse{
							ResponseHeader: roachpb.ResponseHeader{NumKeys: int64(rows)},
						},
					},
				})
			case *roachpb.TsDeleteEntityRequest:
				cnt, err := s.tsEngine.DeleteEntities(tdr.TableId, rangeGroupID, tdr.PrimaryTags, false, 0)
				if err != nil {
					return nil, &roachpb.Error{Message: err.Error()}
				}
				resp.Responses = append(resp.Responses, roachpb.ResponseUnion{
					Value: &roachpb.ResponseUnion_TsDeleteEntity{
						TsDeleteEntity: &roachpb.TsDeleteEntityResponse{
							ResponseHeader: roachpb.ResponseHeader{NumKeys: int64(cnt)},
						},
					},
				})
			case *roachpb.TsDeleteMultiEntitiesDataRequest:
				var deleteRows uint64
				// only one rangeGroup
				cnt, err := s.tsEngine.DeleteRangeData(tdr.TableId, rangeGroupID, 0, tdr.HashNum-1, tdr.TsSpans, 0)
				if err != nil {
					return nil, &roachpb.Error{Message: err.Error()}
				}
				deleteRows += cnt
				resp.Responses = append(resp.Responses, roachpb.ResponseUnion{
					Value: &roachpb.ResponseUnion_TsDeleteMultiEntitiesData{
						TsDeleteMultiEntitiesData: &roachpb.TsDeleteMultiEntitiesDataResponse{
							ResponseHeader: roachpb.ResponseHeader{NumKeys: int64(deleteRows)},
						},
					},
				})
			default:
				ba.Header.ReadConsistency = roachpb.READ_UNCOMMITTED
				return s.interceptorStack[0].SendLocked(ctx, ba)
			}
		}
		if putPayload != nil {
			dedupRes, entitiesAffect, err := s.tsEngine.PutData(1, putPayload, 0, true)
			if err != nil {
				// todo need to process dedupResult
				return nil, &roachpb.Error{Message: err.Error()}
			}
			resp.Responses = append(resp.Responses, roachpb.ResponseUnion{
				Value: &roachpb.ResponseUnion_TsPut{
					TsPut: &roachpb.TsPutResponse{
						ResponseHeader: roachpb.ResponseHeader{
							NumKeys: int64(dedupRes.DedupRows),
						},
						DedupRule:         int64(dedupRes.DedupRule),
						DiscardBitmap:     dedupRes.DiscardBitmap,
						EntitiesAffected:  uint32(entitiesAffect.EntityCount),
						UnorderedAffected: entitiesAffect.UnorderedCount,
					},
				},
			})
		}
		return resp, nil
	}

	setConsistency := func() {
		for _, ru := range ba.Requests {
			r := ru.GetInner()
			switch r.(type) {
			case *roachpb.TsPutTagRequest,
				*roachpb.TsDeleteRequest,
				*roachpb.TsDeleteEntityRequest,
				*roachpb.TsDeleteMultiEntitiesDataRequest,
				*roachpb.TsTagUpdateRequest,
				*roachpb.TsRowPutRequest:
				ba.Header.ReadConsistency = roachpb.READ_UNCOMMITTED
			}
		}
	}

	setConsistency()
	//ba.Txn = s.txn.Clone()
	return s.interceptorStack[0].SendLocked(ctx, ba)
}

// DB is a database handle to a single ts cluster. A DB is safe for
// concurrent use by multiple goroutines.
type DB struct {
	kdb *kv.DB
	tss *TsSender
	txn *kv.Txn
}

// NewDB returns a new DB.
func NewDB(cfg TsDBConfig) *DB {
	tsDB := DB{
		kdb: cfg.KvDB,
		tss: &TsSender{
			tsEngine:     cfg.TsEngine,
			rpcContext:   cfg.RPCContext,
			gossip:       cfg.Gossip,
			stopper:      cfg.Stopper,
			isSingleNode: cfg.IsSingleNode,
		},
	}
	tsDB.tss.interceptorAlloc.tsTxnCommitter = tsTxnCommitter{
		wrapped: cfg.Sender,
	}
	tsDB.tss.interceptorAlloc.tsTxnHeartbeater = tsTxnHeartbeater{
		wrapped:      &tsDB.tss.interceptorAlloc.tsTxnCommitter,
		loopInterval: base.DefaultTxnHeartbeatInterval,
		stopper:      cfg.Stopper,
		clock:        cfg.KvDB.Clock(),
	}
	tsDB.tss.interceptorAlloc.tsTxnHeartbeater.mu.Locker = &sync.RWMutex{}
	tsDB.tss.interceptorStack = []lockedSender{&tsDB.tss.interceptorAlloc.tsTxnHeartbeater, &tsDB.tss.interceptorAlloc.tsTxnCommitter}
	return &tsDB
}

// Run executes the operations queued up within a batch. Before executing any
// of the operations the batch is first checked to see if there were any errors
// during its construction (e.g. failure to marshal a proto message).
//
// The operations within a batch are run in parallel and the order is
// non-deterministic. It is an unspecified behavior to modify and retrieve the
// same key within a batch.
//
// Upon completion, Batch.Results will contain the results for each
// operation. The order of the results matches the order the operations were
// added to the batch.
func (db *DB) Run(ctx context.Context, b *kv.Batch) error {
	for _, r := range b.Results {
		if r.Err != nil {
			return r.Err
		}
	}
	if b.Txn() != nil {
		db.tss.txn = b.Txn().Sender().Transaction()
		db.tss.interceptorAlloc.tsTxnHeartbeater.txn = b.Txn()
		db.tss.interceptorAlloc.tsTxnCommitter.txn = b.Txn()
		txn := b.Txn().Sender().Transaction()
		db.tss.interceptorAlloc.tsTxnHeartbeater.mu.transaction = txn.Clone()
	}
	return kv.SendAndFill(ctx, db.Send, b)
}

// Send runs the specified calls synchronously in a single batch and returns
// any errors. Returns (nil, nil) for an empty batch.
func (db *DB) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	return db.kdb.SendUsingSender(ctx, ba, db.tss)
}

// CreateTSTable create ts table
func (db *DB) CreateTSTable(
	ctx context.Context, tableID sqlbase.ID, hashNum uint64, nodeID roachpb.NodeID, meta []byte,
) error {
	log.Infof(ctx, "CreateTSTable on node %d", nodeID)
	addr, err := db.tss.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return err
	}
	conn, err := db.tss.rpcContext.GRPCDialNode(addr.String(), nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		log.Errorf(ctx, "could not dial node ID %d", nodeID)
		return err
	}
	client := serverpb.NewAdminClient(conn)
	req := &serverpb.CreateTSTableRequest{
		TableID: uint64(tableID),
		HashNum: hashNum,
		Meta:    meta,
	}
	if _, err := client.CreateTSTable(ctx, req); err != nil {
		log.Errorf(ctx, "create ts table meta failed: %v", err)
		return err
	}
	return nil
}
