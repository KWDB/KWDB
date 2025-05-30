// Copyright 2014 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"github.com/pkg/errors"
)

type metaAction func(*kv.Batch, roachpb.Key, *roachpb.RangeDescriptor)

func putMeta(b *kv.Batch, key roachpb.Key, desc *roachpb.RangeDescriptor) {
	b.Put(key, desc)
}

func delMeta(b *kv.Batch, key roachpb.Key, desc *roachpb.RangeDescriptor) {
	b.Del(key)
}

// splitRangeAddressing creates (or overwrites if necessary) the meta1
// and meta2 range addressing records for the left and right ranges
// caused by a split.
func splitRangeAddressing(b *kv.Batch, left, right *roachpb.RangeDescriptor) error {
	if err := rangeAddressing(b, left, putMeta); err != nil {
		return err
	}
	return rangeAddressing(b, right, putMeta)
}

// mergeRangeAddressing removes subsumed meta1 and meta2 range
// addressing records caused by merging and updates the records for
// the new merged range. Left is the range descriptor for the "left"
// range before merging and merged describes the left to right merge.
func mergeRangeAddressing(b *kv.Batch, left, merged *roachpb.RangeDescriptor) error {
	if err := rangeAddressing(b, left, delMeta); err != nil {
		return err
	}
	return rangeAddressing(b, merged, putMeta)
}

// updateRangeAddressing overwrites the meta1 and meta2 range addressing
// records for the descriptor.
func updateRangeAddressing(b *kv.Batch, desc *roachpb.RangeDescriptor) error {
	return rangeAddressing(b, desc, putMeta)
}

// rangeAddressing updates or deletes the range addressing metadata
// for the range specified by desc. The action to take is specified by
// the supplied metaAction function.
//
// The rules for meta1 and meta2 records are as follows:
//
//  1. If desc.StartKey or desc.EndKey is meta1:
//     - ERROR
//  2. If desc.EndKey is meta2:
//     - meta1(desc.EndKey)
//  3. If desc.EndKey is normal user key:
//     - meta2(desc.EndKey)
//     3a. If desc.StartKey is not normal user key:
//         - meta1(KeyMax)
func rangeAddressing(b *kv.Batch, desc *roachpb.RangeDescriptor, action metaAction) error {
	// 1. handle illegal case of start or end key being meta1.
	if bytes.HasPrefix(desc.EndKey, keys.Meta1Prefix) ||
		bytes.HasPrefix(desc.StartKey, keys.Meta1Prefix) {
		return errors.Errorf("meta1 addressing records cannot be split: %+v", desc)
	}

	// Note that both cases 2 and 3 are handled by keys.RangeMetaKey.
	//
	// 2. the case of the range ending with a meta2 prefix. This means
	// the range is full of meta2. We must update the relevant meta1
	// entry pointing to the end of this range.
	//
	// 3. the range ends with a normal user key, so we must update the
	// relevant meta2 entry pointing to the end of this range.
	action(b, keys.RangeMetaKey(desc.EndKey).AsRawKey(), desc)

	if bytes.Compare(desc.StartKey, keys.MetaMax) < 0 &&
		bytes.Compare(desc.EndKey, keys.MetaMax) >= 0 {
		// 3a. the range spans meta2 and user keys, update the meta1
		// entry for KeyMax. We do this to prevent the 3 levels of
		// descriptor indirection described in #18998.
		action(b, keys.Meta1KeyMax, desc)
	}
	return nil
}
