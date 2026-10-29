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

package kvserver

import (
	"bytes"
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
)

// Server implements PerReplicaServer.
type Server struct {
	stores *Stores
}

var _ PerReplicaServer = Server{}

// MakeServer returns a new instance of Server.
func MakeServer(descriptor *roachpb.NodeDescriptor, stores *Stores) Server {
	return Server{stores}
}

func (is Server) execStoreCommand(h StoreRequestHeader, f func(*Store) error) error {
	store, err := is.stores.GetStore(h.StoreID)
	if err != nil {
		return err
	}
	return f(store)
}

// CollectChecksum implements PerReplicaServer.
func (is Server) CollectChecksum(
	ctx context.Context, req *CollectChecksumRequest,
) (*CollectChecksumResponse, error) {
	resp := &CollectChecksumResponse{}
	err := is.execStoreCommand(req.StoreRequestHeader,
		func(s *Store) error {
			r, err := s.GetReplica(req.RangeID)
			if err != nil {
				return err
			}
			c, err := r.getChecksum(ctx, req.ChecksumID)
			if err != nil {
				return err
			}
			ccr := c.CollectChecksumResponse
			if !bytes.Equal(req.Checksum, ccr.Checksum) {
				// If this check is false, then this request is the replica carrying out
				// the consistency check. The message is spurious, but we want to leave the
				// snapshot (if present) intact.
				if len(req.Checksum) > 0 {
					log.Errorf(ctx, "consistency check failed on range r%d: expected checksum %x, got %x",
						req.RangeID, req.Checksum, ccr.Checksum)
					// Leave resp.Snapshot alone so that the caller will receive what's
					// in it (if anything).
				}
			} else {
				ccr.Snapshot = nil
			}
			resp = &ccr
			return nil
		})
	return resp, err
}

// CollectReplicaStatus implements PerReplicaServer.
func (is Server) CollectReplicaStatus(
	ctx context.Context, req *CollectReplicaStatusRequest,
) (*CollectReplicaStatusResponse, error) {
	resp := &CollectReplicaStatusResponse{}
	err := is.execStoreCommand(req.StoreRequestHeader,
		func(s *Store) error {
			r, err := s.GetReplica(req.RangeID)
			if err != nil {
				return err
			}
			if !r.isInitializedRLocked() {
				return roachpb.NewRangeNotFoundError(req.RangeID, s.StoreID())
			}
			resp.ReplicaStatus.StartKey = r.mu.state.Desc.StartKey
			curLease, _ := r.getLeaseRLocked()
			resp.ReplicaStatus.StoreID = s.StoreID()
			resp.ReplicaStatus.NodeID = s.nodeDesc.NodeID
			if r.mu.leaderID == r.mu.replicaID {
				resp.ReplicaStatus.Leader = true
			} else {
				resp.ReplicaStatus.Leader = false
			}
			if curLease.Replica.ReplicaID == r.mu.replicaID {
				resp.ReplicaStatus.LeaseHolder = true
			} else {
				resp.ReplicaStatus.LeaseHolder = false
			}
			resp.ReplicaStatus.LeaseEpoch = curLease.Epoch
			last, err := r.raftLastIndexLocked()
			if err != nil {
				return err
			}
			resp.ReplicaStatus.LastIndex = last
			resp.ReplicaStatus.ApplyIndex = r.mu.state.RaftAppliedIndex
			resp.ReplicaStatus.RangeID = req.RangeID
			return nil
		})
	return resp, err
}

// WaitForApplication implements PerReplicaServer.
//
// It is the caller's responsibility to cancel or set a timeout on the context.
// If the context is never canceled, WaitForApplication will retry forever.
func (is Server) WaitForApplication(
	ctx context.Context, req *WaitForApplicationRequest,
) (*WaitForApplicationResponse, error) {
	resp := &WaitForApplicationResponse{}
	err := is.execStoreCommand(req.StoreRequestHeader, func(s *Store) error {
		// TODO(benesch): Once Replica changefeeds land, see if we can implement
		// this request handler without polling.
		retryOpts := retry.Options{InitialBackoff: 10 * time.Millisecond}
		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			// Long-lived references to replicas are frowned upon, so re-fetch the
			// replica on every turn of the loop.
			repl, err := s.GetReplica(req.RangeID)
			if err != nil {
				return err
			}
			repl.mu.RLock()
			leaseAppliedIndex := repl.mu.state.LeaseAppliedIndex
			isTs := repl.isTsLocked()
			repl.mu.RUnlock()
			if leaseAppliedIndex >= req.LeaseIndex {
				// For performance reasons, we don't sync to disk when
				// applying raft commands. This means that if a node restarts
				// after applying but before the next sync, its
				// LeaseAppliedIndex could temporarily regress (until it
				// reapplies its latest raft log entries).
				//
				// Merging relies on the monotonicity of the log applied
				// index, so before returning ensure that rocksdb has synced
				// everything up to this point to disk.
				//
				// https://gitee.com/kwbasedb/kwbase/issues/33120
				if isTs {
					if err = s.TsEngine.CheckpointForTable(repl.Desc().TableId); err != nil {
						log.Warningf(ctx, "checkpoint for table %d failed for %+v", repl.Desc().TableId, err)
					}
				}
				return relational.WriteSyncNoop(ctx, s.engine)
			}
		}
		if ctx.Err() == nil {
			log.Fatal(ctx, "infinite retry loop exited but context has no error")
		}
		return ctx.Err()
	})
	return resp, err
}

// WaitForReplicaInit implements PerReplicaServer.
//
// It is the caller's responsibility to cancel or set a timeout on the context.
// If the context is never canceled, WaitForReplicaInit will retry forever.
func (is Server) WaitForReplicaInit(
	ctx context.Context, req *WaitForReplicaInitRequest,
) (*WaitForReplicaInitResponse, error) {
	resp := &WaitForReplicaInitResponse{}
	err := is.execStoreCommand(req.StoreRequestHeader, func(s *Store) error {
		retryOpts := retry.Options{InitialBackoff: 10 * time.Millisecond}
		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			// Long-lived references to replicas are frowned upon, so re-fetch the
			// replica on every turn of the loop.
			if repl, err := s.GetReplica(req.RangeID); err == nil && repl.IsInitialized() {
				return nil
			}
		}
		if ctx.Err() == nil {
			log.Fatal(ctx, "infinite retry loop exited but context has no error")
		}
		return ctx.Err()
	})
	return resp, err
}
