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

package kvserver_test

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"go.etcd.io/etcd/raft/raftpb"
)

func verifyHash(b []byte, expectedSum uint64) error {
	hash := fnv.New64a()
	if _, err := hash.Write(b); err != nil {
		return err
	}
	if sum := hash.Sum64(); sum != expectedSum {
		return fmt.Errorf("expected sum %d; got %d", expectedSum, sum)
	}
	return nil
}

// An arbitrary number chosen to seed the PRNGs used to populate the tested
// protos.
const goldenSeed = 1337

// The count of randomly populated protos that will be concatenated and hashed
// per proto type. Given that the population functions have a chance of leaving
// some fields zero-valued, this number must be greater than `1` to give this
// test a reasonable chance of encountering a non-zero value of every field.
const itersPerProto = 20

type fixture struct {
	populatedConstructor   func(*rand.Rand) protoutil.Message
	emptySum, populatedSum uint64
}

var belowRaftGoldenProtos = map[reflect.Type]fixture{
	reflect.TypeOf(&enginepb.MVCCMetadata{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			m := enginepb.NewPopulatedMVCCMetadata(r, false)
			m.Txn = nil // never populated below Raft
			return m
		},
		emptySum:     7551962144604783939,
		populatedSum: 6203354104300541958,
	},
	reflect.TypeOf(&enginepb.RangeAppliedState{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			return enginepb.NewPopulatedRangeAppliedState(r, false)
		},
		emptySum:     615555020845646359,
		populatedSum: 13383288837489742293,
	},
	reflect.TypeOf(&raftpb.HardState{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			type expectedHardState struct {
				Term                 uint64
				Vote                 uint64
				Commit               uint64
				XXX_NoUnkeyedLiteral struct{}
				XXX_unrecognized     []byte
				XXX_sizecache        int32
			}
			// Conversion fails if new fields are added to `HardState`, in which case this method
			// and the expected sums should be updated.
			var _ = expectedHardState(raftpb.HardState{})

			n := r.Uint64()
			return &raftpb.HardState{
				Term:             n % 3,
				Vote:             n % 7,
				Commit:           n % 11,
				XXX_unrecognized: nil,
			}
		},
		emptySum:     13621293256077144893,
		populatedSum: 13375098491754757572,
	},
	reflect.TypeOf(&roachpb.RangeDescriptor{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			return roachpb.NewPopulatedRangeDescriptor(r, false)
		},
		emptySum:     17385950988268029925,
		populatedSum: 7788599348724979518,
	},
	reflect.TypeOf(&storagepb.Liveness{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			return storagepb.NewPopulatedLiveness(r, false)
		},
		emptySum:     892800390935990883,
		populatedSum: 451773449228717946,
	},
	// This is used downstream of Raft only to write it into unreplicated keyspace
	// as part of VersionUnreplicatedRaftTruncatedState.
	// However, it has been sent through Raft for a long time, as part of
	// ReplicatedEvalResult.
	reflect.TypeOf(&roachpb.RaftTruncatedState{}): {
		populatedConstructor: func(r *rand.Rand) protoutil.Message {
			return roachpb.NewPopulatedRaftTruncatedState(r, false)
		},
		emptySum:     5531676819244041709,
		populatedSum: 14781226418259198098,
	},
}

func init() {
	if storage.DefaultStorageEngine != enginepb.EngineTypeRocksDB && storage.DefaultStorageEngine != enginepb.EngineTypeDefault {
		// These are marshaled below Raft by the Pebble merge operator. The Pebble
		// merge operator can be called below Raft whenever a Pebble Iterator is
		// used. Note that we only see these protos marshaled below Raft when the
		// engine type is not RocksDB. If the engine type is RocksDB the marshaling
		// occurs in C++ which is invisible to the tracking mechanism.
		belowRaftGoldenProtos[reflect.TypeOf(&roachpb.InternalTimeSeriesData{})] = fixture{
			populatedConstructor: func(r *rand.Rand) protoutil.Message {
				return roachpb.NewPopulatedInternalTimeSeriesData(r, false)
			},
			emptySum:     5531676819244041709,
			populatedSum: 8911200268508796945,
		}
		belowRaftGoldenProtos[reflect.TypeOf(&enginepb.MVCCMetadataSubsetForMergeSerialization{})] =
			fixture{
				populatedConstructor: func(r *rand.Rand) protoutil.Message {
					return enginepb.NewPopulatedMVCCMetadataSubsetForMergeSerialization(r, false)
				},
				emptySum:     14695981039346656037,
				populatedSum: 7432412240713840291,
			}
	}
}

func TestBelowRaftProtos(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Enable the additional checks in TestMain. NB: running this test by itself
	// will fail those extra checks - such failures are safe to ignore, so long
	// as this test passes when run with the entire package's tests.
	verifyBelowRaftProtos = true

	slice := make([]byte, 1<<20)
	for typ, fix := range belowRaftGoldenProtos {
		if b, err := protoutil.Marshal(reflect.New(typ.Elem()).Interface().(protoutil.Message)); err != nil {
			t.Fatal(err)
		} else if err := verifyHash(b, fix.emptySum); err != nil {
			t.Errorf("%s (empty): %s\n", typ, err)
		}

		randGen := rand.New(rand.NewSource(goldenSeed))

		bytes := slice
		numBytes := 0
		for i := 0; i < itersPerProto; i++ {
			if n, err := fix.populatedConstructor(randGen).MarshalTo(bytes); err != nil {
				t.Fatal(err)
			} else {
				bytes = bytes[n:]
				numBytes += n
			}
		}
		if err := verifyHash(slice[:numBytes], fix.populatedSum); err != nil {
			t.Errorf("%s (populated): %s\n", typ, err)
		}
	}
}
