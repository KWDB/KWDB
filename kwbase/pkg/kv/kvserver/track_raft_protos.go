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
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/apply"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/compactor"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

func funcName(f interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

// TrackRaftProtos instruments proto marshaling to track protos which are
// marshaled downstream of raft. It returns a function that removes the
// instrumentation and returns the list of downstream-of-raft protos.
func TrackRaftProtos() func() []reflect.Type {
	// Grab the name of the function that roots all raft application.
	applyRaftEntryFunc := funcName((*apply.Task).ApplyCommittedEntries)
	// We only need to track protos that could cause replica divergence
	// by being written to disk downstream of raft.
	whitelist := []string{
		// Some raft operations trigger gossip, but we don't require
		// strict consistency there.
		funcName((*gossip.Gossip).AddInfoProto),
		// Compactions are suggested below Raft, but they are local to a store and
		// thus not subject to Raft consistency requirements.
		funcName((*compactor.Compactor).Suggest),
		// Range merges destroy replicas beneath Raft and write replica tombstones,
		// but tombstones are unreplicated and thus not subject to the strict
		// consistency requirements.
		funcName((*Replica).setTombstoneKey),
		// tryReproposeWithNewLeaseIndex is only run on the replica that
		// proposed the command.
		funcName((*Replica).tryReproposeWithNewLeaseIndex),
	}

	belowRaftProtos := struct {
		syncutil.Mutex
		inner map[reflect.Type]struct{}
	}{
		inner: make(map[reflect.Type]struct{}),
	}

	// Hard-coded protos for which we don't want to change the encoding. These
	// are not "below raft" in the normal sense, but instead are used as part of
	// conditional put operations.
	belowRaftProtos.Lock()
	belowRaftProtos.inner[reflect.TypeOf(&roachpb.RangeDescriptor{})] = struct{}{}
	belowRaftProtos.inner[reflect.TypeOf(&storagepb.Liveness{})] = struct{}{}
	belowRaftProtos.Unlock()

	protoutil.Interceptor = func(pb protoutil.Message) {
		t := reflect.TypeOf(pb)

		// Special handling for MVCCMetadata: we expect MVCCMetadata to be
		// marshaled below raft, but MVCCMetadata.Txn should always be nil in such
		// cases.
		if meta, ok := pb.(*enginepb.MVCCMetadata); ok && meta.Txn != nil {
			protoutil.Interceptor(meta.Txn)
		}

		belowRaftProtos.Lock()
		_, ok := belowRaftProtos.inner[t]
		belowRaftProtos.Unlock()
		if ok {
			return
		}

		var pcs [256]uintptr
		if numCallers := runtime.Callers(0, pcs[:]); numCallers == len(pcs) {
			panic(fmt.Sprintf("number of callers %d might have exceeded slice size %d", numCallers, len(pcs)))
		}
		frames := runtime.CallersFrames(pcs[:])
		for {
			f, more := frames.Next()

			whitelisted := false
			for _, s := range whitelist {
				if strings.Contains(f.Function, s) {
					whitelisted = true
					break
				}
			}
			if whitelisted {
				break
			}

			if strings.Contains(f.Function, applyRaftEntryFunc) {
				belowRaftProtos.Lock()
				belowRaftProtos.inner[t] = struct{}{}
				belowRaftProtos.Unlock()
				break
			}
			if !more {
				break
			}
		}
	}

	return func() []reflect.Type {
		protoutil.Interceptor = func(_ protoutil.Message) {}

		belowRaftProtos.Lock()
		types := make([]reflect.Type, 0, len(belowRaftProtos.inner))
		for t := range belowRaftProtos.inner {
			types = append(types, t)
		}
		belowRaftProtos.Unlock()

		return types
	}
}
