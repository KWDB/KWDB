// Copyright 2019 The Cockroach Authors.
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
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/rditer"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/stateloader"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

// InitEngine writes a new store ident to the underlying engine. To
// ensure that no crufty data already exists in the engine, it scans
// the engine contents before writing the new store ident. The engine
// should be completely empty. It returns an error if called on a
// non-empty engine.
func InitEngine(
	ctx context.Context,
	eng storage.Engine,
	ident roachpb.StoreIdent,
	cv clusterversion.ClusterVersion,
) error {
	exIdent, err := ReadStoreIdent(ctx, eng)
	if err == nil {
		return errors.Errorf("engine %s is already bootstrapped with ident %s", eng, exIdent.String())
	}
	if _, ok := err.(*NotBootstrappedError); !ok {
		return err
	}

	if err := checkEngineEmpty(ctx, eng); err != nil {
		return errors.Wrap(err, "cannot verify empty engine for bootstrap")
	}

	batch := eng.NewBatch()
	if err := storage.MVCCPutProto(
		ctx,
		batch,
		nil,
		keys.StoreIdentKey(),
		hlc.Timestamp{},
		nil,
		&ident,
	); err != nil {
		batch.Close()
		return err
	}
	if err := WriteClusterVersion(ctx, batch, cv); err != nil {
		batch.Close()
		return errors.Wrap(err, "cannot write cluster version")
	}
	if err := batch.Commit(true /* sync */, storage.NormalCommitType); err != nil {
		return errors.Wrap(err, "persisting bootstrap data")
	}

	return nil
}

// WriteInitialClusterData writes bootstrapping data to an engine. It creates
// system ranges (filling in meta1 and meta2) and the default zone config.
//
// Args:
// eng: the engine to which data is to be written.
// initialValues: an optional list of k/v to be written as well after each
//   value's checksum is initialized.
// bootstrapVersion: the version at which the cluster is bootstrapped.
// numStores: the number of stores this node will have.
// splits: an optional list of split points. Range addressing will be created
//   for all the splits. The list needs to be sorted.
// nowNanos: the timestamp at which to write the initial engine data.
func WriteInitialClusterData(
	ctx context.Context,
	eng storage.Engine,
	initialValues []roachpb.KeyValue,
	bootstrapVersion roachpb.Version,
	numStores int,
	splits []roachpb.RKey,
	nowNanos int64,
) error {
	// Bootstrap version information. We'll add the "bootstrap version" to the
	// list of initialValues, so that we don't have to handle it specially
	// (particularly since we don't want to manually figure out which range it
	// falls into).
	bootstrapVal := roachpb.Value{}
	if err := bootstrapVal.SetProto(&bootstrapVersion); err != nil {
		return err
	}
	initialValues = append(initialValues,
		roachpb.KeyValue{Key: keys.BootstrapVersionKey, Value: bootstrapVal})

	// Initialize various sequence generators.
	var nodeIDVal, storeIDVal, rangeIDVal roachpb.Value
	nodeIDVal.SetInt(1) // This node has id 1.
	// The caller will initialize the stores with ids 1..numStores.
	storeIDVal.SetInt(int64(numStores))
	// The last range has id = len(splits) + 1
	rangeIDVal.SetInt(int64(len(splits) + 1))
	initialValues = append(initialValues,
		roachpb.KeyValue{Key: keys.NodeIDGenerator, Value: nodeIDVal},
		roachpb.KeyValue{Key: keys.StoreIDGenerator, Value: storeIDVal},
		roachpb.KeyValue{Key: keys.RangeIDGenerator, Value: rangeIDVal})

	// firstRangeMS is going to accumulate the stats for the first range, as we
	// write the meta records for all the other ranges.
	firstRangeMS := &enginepb.MVCCStats{}

	// filter initial values for a given descriptor, returning only the ones that
	// pertain to the respective range.
	filterInitialValues := func(desc *roachpb.RangeDescriptor) []roachpb.KeyValue {
		var r []roachpb.KeyValue
		for _, kv := range initialValues {
			if desc.ContainsKey(roachpb.RKey(kv.Key)) {
				r = append(r, kv)
			}
		}
		return r
	}

	// We iterate through the ranges backwards, since they all need to contribute
	// to the stats of the first range (i.e. because they all write meta2 records
	// in the first range), and so we want to create the first range last so that
	// the stats we compute for it are correct.
	startKey := roachpb.RKeyMax
	for i := len(splits) - 1; i >= -1; i-- {
		endKey := startKey
		rangeID := roachpb.RangeID(i + 2) // RangeIDs are 1-based.
		if i >= 0 {
			startKey = splits[i]
		} else {
			startKey = roachpb.RKeyMin
		}

		desc := &roachpb.RangeDescriptor{
			RangeID:              rangeID,
			StartKey:             startKey,
			EndKey:               endKey,
			NextReplicaID:        2,
			GenerationComparable: proto.Bool(true),
		}
		replicas := []roachpb.ReplicaDescriptor{
			{
				NodeID:    1,
				StoreID:   1,
				ReplicaID: 1,
			},
		}
		desc.SetReplicas(roachpb.MakeReplicaDescriptors(replicas))
		if err := desc.Validate(); err != nil {
			return err
		}
		rangeInitialValues := filterInitialValues(desc)
		log.VEventf(
			ctx, 2, "creating range %d [%s, %s). Initial values: %d",
			desc.RangeID, desc.StartKey, desc.EndKey, len(rangeInitialValues))
		batch := eng.NewBatch()
		defer batch.Close()

		now := hlc.Timestamp{
			WallTime: nowNanos,
			Logical:  0,
		}

		// NOTE: We don't do stats computations in any of the puts below. Instead,
		// we write everything and then compute the stats over the whole range.

		// Range descriptor.
		if err := storage.MVCCPutProto(
			ctx, batch, nil /* ms */, keys.RangeDescriptorKey(desc.StartKey),
			now, nil /* txn */, desc,
		); err != nil {
			return err
		}

		// Replica GC timestamp.
		if err := storage.MVCCPutProto(
			ctx, batch, nil /* ms */, keys.RangeLastReplicaGCTimestampKey(desc.RangeID),
			hlc.Timestamp{}, nil /* txn */, &now,
		); err != nil {
			return err
		}
		// Range addressing for meta2.
		meta2Key := keys.RangeMetaKey(endKey)
		if err := storage.MVCCPutProto(ctx, batch, firstRangeMS, meta2Key.AsRawKey(),
			now, nil /* txn */, desc,
		); err != nil {
			return err
		}

		// The first range gets some special treatment.
		if startKey.Equal(roachpb.RKeyMin) {
			// Range addressing for meta1.
			meta1Key := keys.RangeMetaKey(keys.RangeMetaKey(roachpb.RKeyMax))
			if err := storage.MVCCPutProto(
				ctx, batch, nil /* ms */, meta1Key.AsRawKey(), now, nil /* txn */, desc,
			); err != nil {
				return err
			}
		}

		// Now add all passed-in default entries.
		for _, kv := range rangeInitialValues {
			// Initialize the checksums.
			kv.Value.InitChecksum(kv.Key)
			if err := storage.MVCCPut(
				ctx, batch, nil /* ms */, kv.Key, now, kv.Value, nil, /* txn */
			); err != nil {
				return err
			}
		}

		truncStateType := stateloader.TruncatedStateUnreplicated
		lease := roachpb.BootstrapLease()
		_, err := stateloader.WriteInitialState(
			ctx, batch,
			enginepb.MVCCStats{},
			*desc,
			lease,
			hlc.Timestamp{}, /* gcThreshold */
			truncStateType,
		)
		if err != nil {
			return err
		}

		computedStats, err := rditer.ComputeStatsForRange(desc, batch, now.WallTime)
		if err != nil {
			return err
		}

		sl := stateloader.Make(rangeID)
		if err := sl.SetMVCCStats(ctx, batch, &computedStats); err != nil {
			return err
		}

		if err := batch.Commit(true /* sync */, storage.NormalCommitType); err != nil {
			return err
		}
	}

	return nil
}
