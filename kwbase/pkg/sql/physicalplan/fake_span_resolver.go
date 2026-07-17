// Copyright 2017 The Cockroach Authors.
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

package physicalplan

import (
	"bytes"
	"context"
	"math/rand"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

const avgRangesPerNode = 5

// fakeSpanResolver is a SpanResovler which splits spans and distributes them to
// nodes randomly. Each Seek() call generates a random distribution with
// expected avgRangesPerNode ranges for each node.
type fakeSpanResolver struct {
	nodes []*roachpb.NodeDescriptor
}

var _ SpanResolver = &fakeSpanResolver{}

// NewFakeSpanResolver creates a fake span resolver.
func NewFakeSpanResolver(nodes []*roachpb.NodeDescriptor) SpanResolver {
	return &fakeSpanResolver{
		nodes: nodes,
	}
}

// fakeRange indicates that a range between startKey and endKey is owned by a
// certain node.
type fakeRange struct {
	startKey roachpb.Key
	endKey   roachpb.Key
	replica  *roachpb.NodeDescriptor
}

type fakeSpanResolverIterator struct {
	fsr *fakeSpanResolver
	// the fake span resolver needs to perform scans as part of Seek(); these
	// scans are performed in the context of this txn - the same one using the
	// results of the resolver - so that using the resolver doesn't introduce
	// conflicts.
	txn *kv.Txn
	err error

	// ranges are ordered by the key; the start key of the first one is the
	// beginning of the current range and the end key of the last one is the end
	// of the queried span.
	ranges []fakeRange
}

// NewSpanResolverIterator is part of the SpanResolver interface.
func (fsr *fakeSpanResolver) NewSpanResolverIterator(txn *kv.Txn) SpanResolverIterator {
	return &fakeSpanResolverIterator{fsr: fsr, txn: txn}
}

// Seek is part of the SpanResolverIterator interface. Each Seek call generates
// a random distribution of the given span.
func (fit *fakeSpanResolverIterator) Seek(
	ctx context.Context, span roachpb.Span, scanDir kvcoord.ScanDirection,
) {
	// Set aside the last range from the previous seek.
	var prevRange fakeRange
	if fit.ranges != nil {
		prevRange = fit.ranges[len(fit.ranges)-1]
	}

	// Scan the range and keep a list of all potential split keys.
	kvs, err := fit.txn.Scan(ctx, span.Key, span.EndKey, 0)
	if err != nil {
		log.Errorf(ctx, "Error in fake span resolver scan: %s", err)
		fit.err = err
		return
	}

	// Populate split keys and generate splits using helper functions.
	splitKeys, err := fit.collectSplitKeys(span, kvs)
	if err != nil {
		fit.err = err
		return
	}

	splits := fit.generateSplits(splitKeys, span, scanDir)

	// Build ranges corresponding to the fake splits and assign them random
	// replicas.
	fit.ranges = make([]fakeRange, len(splits)-1)
	for i := range fit.ranges {
		fit.ranges[i] = fakeRange{
			startKey: splits[i],
			endKey:   splits[i+1],
			replica:  fit.fsr.nodes[rand.Intn(len(fit.fsr.nodes))],
		}
	}

	// Check for the case where the last range of the previous Seek() describes
	// the same row as this seek. In this case we'll assign the same replica so we
	// don't "split" column families of the same row across different replicas.
	if prevRange.endKey != nil {
		prefix, err := keys.EnsureSafeSplitKey(span.Key)
		// EnsureSafeSplitKey returns an error for keys which do not specify a
		// column family. In this case we don't need to worry about splitting the
		// row.
		if err == nil && len(prevRange.endKey) >= len(prefix) &&
			bytes.Equal(prefix, prevRange.endKey[:len(prefix)]) {
			fit.ranges[0].replica = prevRange.replica
		}
	}
}

// collectSplitKeys extracts candidate split keys from scan results.
// Returns keys strictly between span.Key and span.EndKey.
func (fit *fakeSpanResolverIterator) collectSplitKeys(
	span roachpb.Span, kvs []kv.KeyValue,
) ([]roachpb.Key, error) {
	var splitKeys []roachpb.Key
	lastKey := span.Key
	for _, kv := range kvs {
		splitKey, err := keys.EnsureSafeSplitKey(kv.Key)
		if err != nil {
			return nil, err
		}
		if !splitKey.Equal(lastKey) && span.ContainsKey(splitKey) {
			splitKeys = append(splitKeys, splitKey)
			lastKey = splitKey
		}
	}
	return splitKeys, nil
}

// generateSplits builds the split list from candidate split keys, choosing a
// random subset and ensuring span boundaries are included. The maximum average
// splits is tuned by avgRangesPerNode.
func (fit *fakeSpanResolverIterator) generateSplits(
	splitKeys []roachpb.Key, span roachpb.Span, scanDir kvcoord.ScanDirection,
) []roachpb.Key {
	maxSplits := 2 * len(fit.fsr.nodes) * avgRangesPerNode
	if maxSplits > len(splitKeys) {
		maxSplits = len(splitKeys)
	}
	numSplits := rand.Intn(maxSplits + 1)

	chosen := make(map[int]struct{})
	for j := len(splitKeys) - numSplits; j < len(splitKeys); j++ {
		t := rand.Intn(j + 1)
		if _, alreadyChosen := chosen[t]; !alreadyChosen {
			chosen[t] = struct{}{}
		} else {
			chosen[j] = struct{}{}
		}
	}

	splits := make([]roachpb.Key, 0, numSplits+2)
	splits = append(splits, span.Key)
	for i := range splitKeys {
		if _, ok := chosen[i]; ok {
			splits = append(splits, splitKeys[i])
		}
	}
	splits = append(splits, span.EndKey)

	if scanDir == kvcoord.Descending {
		for i := 0; i < len(splits)/2; i++ {
			j := len(splits) - i - 1
			splits[i], splits[j] = splits[j], splits[i]
		}
	}
	return splits
}

// Valid is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) Valid() bool {
	return fit.err == nil
}

// Error is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) Error() error {
	return fit.err
}

// NeedAnother is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) NeedAnother() bool {
	return len(fit.ranges) > 1
}

// Next is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) Next(_ context.Context) {
	if len(fit.ranges) <= 1 {
		panic("Next called with no more ranges")
	}
	fit.ranges = fit.ranges[1:]
}

// Desc is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) Desc() roachpb.RangeDescriptor {
	return roachpb.RangeDescriptor{
		StartKey: roachpb.RKey(fit.ranges[0].startKey),
		EndKey:   roachpb.RKey(fit.ranges[0].endKey),
	}
}

// ReplicaInfo is part of the SpanResolverIterator interface.
func (fit *fakeSpanResolverIterator) ReplicaInfo(_ context.Context) (kvcoord.ReplicaInfo, error) {
	n := fit.ranges[0].replica
	return kvcoord.ReplicaInfo{
		ReplicaDescriptor: roachpb.ReplicaDescriptor{NodeID: n.NodeID},
		NodeDesc:          n,
	}, nil
}
