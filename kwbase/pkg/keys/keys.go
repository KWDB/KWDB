// Copyright 2015 The Cockroach Authors.
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

package keys

import (
	"bytes"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
)

func makeKey(keys ...[]byte) []byte {
	return bytes.Join(keys, nil)
}

// MakeStoreKey creates a store-local key based on the metadata key
// suffix, and optional detail.
func MakeStoreKey(suffix, detail roachpb.RKey) roachpb.Key {
	key := make(roachpb.Key, 0, len(localStorePrefix)+len(suffix)+len(detail))
	key = append(key, localStorePrefix...)
	key = append(key, suffix...)
	key = append(key, detail...)
	return key
}

// DecodeStoreKey returns the suffix and detail portions of a local
// store key.
func DecodeStoreKey(key roachpb.Key) (suffix, detail roachpb.RKey, err error) {
	if !bytes.HasPrefix(key, localStorePrefix) {
		return nil, nil, errors.Errorf("key %s does not have %s prefix", key, localStorePrefix)
	}
	// Cut the prefix, the Range ID, and the infix specifier.
	key = key[len(localStorePrefix):]
	if len(key) < localSuffixLength {
		return nil, nil, errors.Errorf("malformed key does not contain local store suffix")
	}
	suffix = roachpb.RKey(key[:localSuffixLength])
	detail = roachpb.RKey(key[localSuffixLength:])
	return suffix, detail, nil
}

// StoreIdentKey returns a store-local key for the store metadata.
func StoreIdentKey() roachpb.Key {
	return MakeStoreKey(localStoreIdentSuffix, nil)
}

// StoreGossipKey returns a store-local key for the gossip bootstrap metadata.
func StoreGossipKey() roachpb.Key {
	return MakeStoreKey(localStoreGossipSuffix, nil)
}

// StoreClusterVersionKey returns a store-local key for the cluster version.
func StoreClusterVersionKey() roachpb.Key {
	return MakeStoreKey(localStoreClusterVersionSuffix, nil)
}

// StoreLastUpKey returns the key for the store's "last up" timestamp.
func StoreLastUpKey() roachpb.Key {
	return MakeStoreKey(localStoreLastUpSuffix, nil)
}

// StoreHLCUpperBoundKey returns the store-local key for storing an upper bound
// to the wall time used by HLC.
func StoreHLCUpperBoundKey() roachpb.Key {
	return MakeStoreKey(localStoreHLCUpperBoundSuffix, nil)
}

// StoreSuggestedCompactionKey returns a store-local key for a
// suggested compaction. It combines the specified start and end keys.
func StoreSuggestedCompactionKey(start, end roachpb.Key) roachpb.Key {
	var detail roachpb.RKey
	detail = encoding.EncodeBytesAscending(detail, start)
	detail = encoding.EncodeBytesAscending(detail, end)
	return MakeStoreKey(localStoreSuggestedCompactionSuffix, detail)
}

// DecodeStoreSuggestedCompactionKey returns the start and end keys of
// the suggested compaction's span.
func DecodeStoreSuggestedCompactionKey(key roachpb.Key) (start, end roachpb.Key, err error) {
	var suffix, detail roachpb.RKey
	suffix, detail, err = DecodeStoreKey(key)
	if err != nil {
		return nil, nil, err
	}
	if !suffix.Equal(localStoreSuggestedCompactionSuffix) {
		return nil, nil, errors.Errorf("key with suffix %q != %q", suffix, localStoreSuggestedCompactionSuffix)
	}
	detail, start, err = encoding.DecodeBytesAscending(detail, nil)
	if err != nil {
		return nil, nil, err
	}
	detail, end, err = encoding.DecodeBytesAscending(detail, nil)
	if err != nil {
		return nil, nil, err
	}
	if len(detail) != 0 {
		return nil, nil, errors.Errorf("invalid key has trailing garbage: %q", detail)
	}
	return start, end, nil
}

// NodeLivenessKey returns the key for the node liveness record.
func NodeLivenessKey(nodeID roachpb.NodeID) roachpb.Key {
	key := make(roachpb.Key, 0, len(NodeLivenessPrefix)+9)
	key = append(key, NodeLivenessPrefix...)
	key = encoding.EncodeUvarintAscending(key, uint64(nodeID))
	return key
}

// NodeStatusKey returns the key for accessing the node status for the
// specified node ID.
func NodeStatusKey(nodeID roachpb.NodeID) roachpb.Key {
	key := make(roachpb.Key, 0, len(StatusNodePrefix)+9)
	key = append(key, StatusNodePrefix...)
	key = encoding.EncodeUvarintAscending(key, uint64(nodeID))
	return key
}

func makePrefixWithRangeID(prefix []byte, rangeID roachpb.RangeID, infix roachpb.RKey) roachpb.Key {
	// Size the key buffer so that it is large enough for most callers.
	key := make(roachpb.Key, 0, 32)
	key = append(key, prefix...)
	key = encoding.EncodeUvarintAscending(key, uint64(rangeID))
	key = append(key, infix...)
	return key
}

// MakeRangeIDPrefix creates a range-local key prefix from
// rangeID for both replicated and unreplicated data.
func MakeRangeIDPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return makePrefixWithRangeID(LocalRangeIDPrefix, rangeID, nil)
}

// MakeRangeIDReplicatedPrefix creates a range-local key prefix from
// rangeID for all Raft replicated data.
func MakeRangeIDReplicatedPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return makePrefixWithRangeID(LocalRangeIDPrefix, rangeID, LocalRangeIDReplicatedInfix)
}

// makeRangeIDReplicatedKey creates a range-local key based on the range's
// Range ID, metadata key suffix, and optional detail.
func makeRangeIDReplicatedKey(rangeID roachpb.RangeID, suffix, detail roachpb.RKey) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}

	key := MakeRangeIDReplicatedPrefix(rangeID)
	key = append(key, suffix...)
	key = append(key, detail...)
	return key
}

// DecodeRangeIDKey parses a local range ID key into range ID, infix,
// suffix, and detail.
func DecodeRangeIDKey(
	key roachpb.Key,
) (rangeID roachpb.RangeID, infix, suffix, detail roachpb.Key, err error) {
	if !bytes.HasPrefix(key, LocalRangeIDPrefix) {
		return 0, nil, nil, nil, errors.Errorf("key %s does not have %s prefix", key, LocalRangeIDPrefix)
	}
	// Cut the prefix, the Range ID, and the infix specifier.
	b := key[len(LocalRangeIDPrefix):]
	b, rangeInt, err := encoding.DecodeUvarintAscending(b)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	if len(b) < localSuffixLength+1 {
		return 0, nil, nil, nil, errors.Errorf("malformed key does not contain range ID infix and suffix")
	}
	infix = b[:1]
	b = b[1:]
	suffix = b[:localSuffixLength]
	b = b[localSuffixLength:]

	return roachpb.RangeID(rangeInt), infix, suffix, b, nil
}

// AbortSpanKey returns a range-local key by Range ID for an
// AbortSpan entry, with detail specified by encoding the
// supplied transaction ID.
func AbortSpanKey(rangeID roachpb.RangeID, txnID uuid.UUID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).AbortSpanKey(txnID)
}

// DecodeAbortSpanKey decodes the provided AbortSpan entry,
// returning the transaction ID.
func DecodeAbortSpanKey(key roachpb.Key, dest []byte) (uuid.UUID, error) {
	_, _, suffix, detail, err := DecodeRangeIDKey(key)
	if err != nil {
		return uuid.UUID{}, err
	}
	if !bytes.Equal(suffix, LocalAbortSpanSuffix) {
		return uuid.UUID{}, errors.Errorf("key %s does not contain the AbortSpan suffix %s",
			key, LocalAbortSpanSuffix)
	}
	// Decode the id.
	detail, idBytes, err := encoding.DecodeBytesAscending(detail, dest)
	if err != nil {
		return uuid.UUID{}, err
	}
	if len(detail) > 0 {
		return uuid.UUID{}, errors.Errorf("key %q has leftover bytes after decode: %s; indicates corrupt key", key, detail)
	}
	txnID, err := uuid.FromBytes(idBytes)
	return txnID, err
}

// RangeAppliedStateKey returns a system-local key for the range applied state key.
// This key has subsumed the responsibility of the following three keys:
// - RaftAppliedIndexLegacyKey
// - LeaseAppliedIndexLegacyKey
// - RangeStatsLegacyKey
func RangeAppliedStateKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeAppliedStateKey()
}

// RaftAppliedIndexLegacyKey returns a system-local key for a raft applied index.
// The key is no longer written to. Its responsibility has been subsumed by the
// RangeAppliedStateKey.
func RaftAppliedIndexLegacyKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftAppliedIndexLegacyKey()
}

// LeaseAppliedIndexLegacyKey returns a system-local key for a lease applied index.
// The key is no longer written to. Its responsibility has been subsumed by the
// RangeAppliedStateKey.
func LeaseAppliedIndexLegacyKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).LeaseAppliedIndexLegacyKey()
}

// TsFlushedIndexKey returns a system-local key for a ts flushed index.
func TsFlushedIndexKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).TsFlushedIndexKey()
}

// RaftTruncatedStateLegacyKey returns a system-local key for a RaftTruncatedState.
// See VersionUnreplicatedRaftTruncatedState.
func RaftTruncatedStateLegacyKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftTruncatedStateLegacyKey()
}

// RangeLeaseKey returns a system-local key for a range lease.
func RangeLeaseKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeLeaseKey()
}

// RangeStatsLegacyKey returns the key for accessing the MVCCStats struct for
// the specified Range ID. The key is no longer written to. Its responsibility
// has been subsumed by the RangeAppliedStateKey.
func RangeStatsLegacyKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeStatsLegacyKey()
}

// RangeLastGCKey returns a system-local key for last used GC threshold on the
// user keyspace. Reads and writes <= this timestamp will not be served.
//
// TODO(tschottdorf): should be renamed to RangeGCThresholdKey.
func RangeLastGCKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeLastGCKey()
}

// MakeRangeIDUnreplicatedPrefix creates a range-local key prefix from
// rangeID for all unreplicated data.
func MakeRangeIDUnreplicatedPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return makePrefixWithRangeID(LocalRangeIDPrefix, rangeID, localRangeIDUnreplicatedInfix)
}

// makeRangeIDUnreplicatedKey creates a range-local unreplicated key based
// on the range's Range ID, metadata key suffix, and optional detail.
func makeRangeIDUnreplicatedKey(
	rangeID roachpb.RangeID, suffix roachpb.RKey, detail roachpb.RKey,
) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}

	key := MakeRangeIDUnreplicatedPrefix(rangeID)
	key = append(key, suffix...)
	key = append(key, detail...)
	return key
}

// RangeTombstoneKey returns a system-local key for a range tombstone.
func RangeTombstoneKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeTombstoneKey()
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
func RaftTruncatedStateKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftTruncatedStateKey()
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
func RaftHardStateKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftHardStateKey()
}

// RaftLogPrefix returns the system-local prefix shared by all Entries
// in a Raft log.
func RaftLogPrefix(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftLogPrefix()
}

// RaftLogKey returns a system-local key for a Raft log entry.
func RaftLogKey(rangeID roachpb.RangeID, logIndex uint64) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RaftLogKey(logIndex)
}

// RangeLastReplicaGCTimestampKey returns a range-local key for
// the range's last replica GC timestamp.
func RangeLastReplicaGCTimestampKey(rangeID roachpb.RangeID) roachpb.Key {
	return MakeRangeIDPrefixBuf(rangeID).RangeLastReplicaGCTimestampKey()
}

// MakeRangeKey creates a range-local key based on the range
// start key, metadata key suffix, and optional detail (e.g. the
// transaction ID for a txn record, etc.).
func MakeRangeKey(key, suffix, detail roachpb.RKey) roachpb.Key {
	if len(suffix) != localSuffixLength {
		panic(fmt.Sprintf("suffix len(%q) != %d", suffix, localSuffixLength))
	}
	buf := MakeRangeKeyPrefix(key)
	buf = append(buf, suffix...)
	buf = append(buf, detail...)
	return buf
}

// MakeRangeKeyPrefix creates a key prefix under which all range-local keys
// can be found.
func MakeRangeKeyPrefix(key roachpb.RKey) roachpb.Key {
	buf := make(roachpb.Key, 0, len(LocalRangePrefix)+len(key)+1)
	buf = append(buf, LocalRangePrefix...)
	buf = encoding.EncodeBytesAscending(buf, key)
	return buf
}

// DecodeRangeKey decodes the range key into range start key,
// suffix and optional detail (may be nil).
func DecodeRangeKey(key roachpb.Key) (startKey, suffix, detail roachpb.Key, err error) {
	if !bytes.HasPrefix(key, LocalRangePrefix) {
		return nil, nil, nil, errors.Errorf("key %q does not have %q prefix",
			key, LocalRangePrefix)
	}
	// Cut the prefix and the Range ID.
	b := key[len(LocalRangePrefix):]
	b, startKey, err = encoding.DecodeBytesAscending(b, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(b) < localSuffixLength {
		return nil, nil, nil, errors.Errorf("key %q does not have suffix of length %d",
			key, localSuffixLength)
	}
	// Cut the suffix.
	suffix = b[:localSuffixLength]
	detail = b[localSuffixLength:]
	return
}

// RangeDescriptorKey returns a range-local key for the descriptor
// for the range with specified key.
func RangeDescriptorKey(key roachpb.RKey) roachpb.Key {
	return MakeRangeKey(key, LocalRangeDescriptorSuffix, nil)
}

// RangeDescriptorJointKey returns a range-local key for the "joint descriptor"
// for the range with specified key. This key is not versioned and it is set if
// and only if the range is in a joint configuration that it yet has to transition
// out of.
func RangeDescriptorJointKey(key roachpb.RKey) roachpb.Key {
	return MakeRangeKey(key, LocalRangeDescriptorJointSuffix, nil)
}

var _ = RangeDescriptorJointKey // silence unused check

// TransactionKey returns a transaction key based on the provided
// transaction key and ID. The base key is encoded in order to
// guarantee that all transaction records for a range sort together.
func TransactionKey(key roachpb.Key, txnID uuid.UUID) roachpb.Key {
	rk, err := Addr(key)
	if err != nil {
		panic(err)
	}
	return MakeRangeKey(rk, LocalTransactionSuffix, roachpb.RKey(txnID.GetBytes()))
}

// QueueLastProcessedKey returns a range-local key for last processed
// timestamps for the named queue. These keys represent per-range last
// processed times.
func QueueLastProcessedKey(key roachpb.RKey, queue string) roachpb.Key {
	return MakeRangeKey(key, LocalQueueLastProcessedSuffix, roachpb.RKey(queue))
}

// IsLocal performs a cheap check that returns true iff a range-local key is
// passed, that is, a key for which `Addr` would return a non-identical RKey
// (or a decoding error).
//
// TODO(tschottdorf): we need a better name for these keys as only some of
// them are local and it's been identified as an area that is not understood
// by many of the team's developers. An obvious suggestion is "system" (as
// opposed to "user") keys, but unfortunately that name has already been
// claimed by a related (but not identical) concept.
func IsLocal(k roachpb.Key) bool {
	return bytes.HasPrefix(k, localPrefix)
}

// Addr returns the address for the key, used to lookup the range containing the
// key. In the normal case, this is simply the key's value. However, for local
// keys, such as transaction records, the address is the inner encoded key, with
// the local key prefix and the suffix and optional detail removed. This address
// unwrapping is performed repeatedly in the case of doubly-local keys. In this
// way, local keys address to the same range as non-local keys, but are stored
// separately so that they don't collide with user-space or global system keys.
//
// Logically, the keys are arranged as follows:
//
// k1 /local/k1/KeyMin ... /local/k1/KeyMax k1\x00 /local/k1/x00/KeyMin ...
//
// However, not all local keys are addressable in the global map. Only range
// local keys incorporating a range key (start key or transaction key) are
// addressable (e.g. range metadata and txn records). Range local keys
// incorporating the Range ID are not (e.g. AbortSpan Entries, and range
// stats).
//
// See AddrUpperBound which is to be used when `k` is the EndKey of an interval.
func Addr(k roachpb.Key) (roachpb.RKey, error) {
	if !IsLocal(k) {
		return roachpb.RKey(k), nil
	}

	for {
		if bytes.HasPrefix(k, localStorePrefix) {
			return nil, errors.Errorf("store-local key %q is not addressable", k)
		}
		if bytes.HasPrefix(k, LocalRangeIDPrefix) {
			return nil, errors.Errorf("local range ID key %q is not addressable", k)
		}
		if !bytes.HasPrefix(k, LocalRangePrefix) {
			return nil, errors.Errorf("local key %q malformed; should contain prefix %q",
				k, LocalRangePrefix)
		}
		k = k[len(LocalRangePrefix):]
		var err error
		// Decode the encoded key, throw away the suffix and detail.
		if _, k, err = encoding.DecodeBytesAscending(k, nil); err != nil {
			return nil, err
		}
		if !IsLocal(k) {
			break
		}
	}
	return roachpb.RKey(k), nil
}

// MustAddr calls Addr and panics on errors.
func MustAddr(k roachpb.Key) roachpb.RKey {
	rk, err := Addr(k)
	if err != nil {
		panic(errors.Wrapf(err, "could not take address of '%s'", k))
	}
	return rk
}

// AddrUpperBound returns the address of an (exclusive) EndKey, used to lookup
// ranges containing the keys strictly smaller than that key. However, unlike
// Addr, it will return the following key that local range keys address to. This
// is necessary because range-local keys exist conceptually in the space between
// regular keys. Addr() returns the regular key that is just to the left of a
// range-local key, which is guaranteed to be located on the same range.
// AddrUpperBound() returns the regular key that is just to the right, which may
// not be on the same range but is suitable for use as the EndKey of a span
// involving a range-local key.
//
// Logically, the keys are arranged as follows:
//
// k1 /local/k1/KeyMin ... /local/k1/KeyMax k1\x00 /local/k1/x00/KeyMin ...
//
// and so any end key /local/k1/x corresponds to an address-resolved end key of
// k1\x00.
func AddrUpperBound(k roachpb.Key) (roachpb.RKey, error) {
	rk, err := Addr(k)
	if err != nil {
		return rk, err
	}
	if IsLocal(k) {
		// The upper bound for a range-local key that addresses to key k
		// is the key directly after k.
		rk = rk.Next()
	}
	return rk, nil
}

// RangeMetaKey returns a range metadata (meta1, meta2) indexing key for the
// given key.
//
// - For RKeyMin, KeyMin is returned.
// - For a meta1 key, KeyMin is returned.
// - For a meta2 key, a meta1 key is returned.
// - For an ordinary key, a meta2 key is returned.
func RangeMetaKey(key roachpb.RKey) roachpb.RKey {
	if len(key) == 0 { // key.Equal(roachpb.RKeyMin)
		return roachpb.RKeyMin
	}
	var prefix roachpb.Key
	switch key[0] {
	case meta1PrefixByte:
		return roachpb.RKeyMin
	case meta2PrefixByte:
		prefix = Meta1Prefix
		key = key[len(Meta2Prefix):]
	default:
		prefix = Meta2Prefix
	}

	buf := make(roachpb.RKey, 0, len(prefix)+len(key))
	buf = append(buf, prefix...)
	buf = append(buf, key...)
	return buf
}

// UserKey returns an ordinary key for the given range metadata (meta1, meta2)
// indexing key.
//
// - For RKeyMin, Meta1Prefix is returned.
// - For a meta1 key, a meta2 key is returned.
// - For a meta2 key, an ordinary key is returned.
// - For an ordinary key, the input key is returned.
func UserKey(key roachpb.RKey) roachpb.RKey {
	if len(key) == 0 { // key.Equal(roachpb.RKeyMin)
		return roachpb.RKey(Meta1Prefix)
	}
	var prefix roachpb.Key
	switch key[0] {
	case meta1PrefixByte:
		prefix = Meta2Prefix
		key = key[len(Meta1Prefix):]
	case meta2PrefixByte:
		key = key[len(Meta2Prefix):]
	}

	buf := make(roachpb.RKey, 0, len(prefix)+len(key))
	buf = append(buf, prefix...)
	buf = append(buf, key...)
	return buf
}

// validateRangeMetaKey validates that the given key is a valid Range Metadata
// key. This checks only the constraints common to forward and backwards scans:
// correct prefix and not exceeding KeyMax.
func validateRangeMetaKey(key roachpb.RKey) error {
	// KeyMin is a valid key.
	if key.Equal(roachpb.RKeyMin) {
		return nil
	}
	// Key must be at least as long as Meta1Prefix.
	if len(key) < len(Meta1Prefix) {
		return NewInvalidRangeMetaKeyError("too short", key)
	}

	prefix, body := key[:len(Meta1Prefix)], key[len(Meta1Prefix):]
	if !prefix.Equal(Meta2Prefix) && !prefix.Equal(Meta1Prefix) {
		return NewInvalidRangeMetaKeyError("not a meta key", key)
	}

	if roachpb.RKeyMax.Less(body) {
		return NewInvalidRangeMetaKeyError("body of meta key range lookup is > KeyMax", key)
	}
	return nil
}

// MetaScanBounds returns the range [start,end) within which the desired meta
// record can be found by means of an engine scan. The given key must be a
// valid RangeMetaKey as defined by validateRangeMetaKey.
// TODO(tschottdorf): a lot of casting going on inside.
func MetaScanBounds(key roachpb.RKey) (roachpb.RSpan, error) {
	if err := validateRangeMetaKey(key); err != nil {
		return roachpb.RSpan{}, err
	}

	if key.Equal(Meta2KeyMax) {
		return roachpb.RSpan{},
			NewInvalidRangeMetaKeyError("Meta2KeyMax can't be used as the key of scan", key)
	}

	if key.Equal(roachpb.RKeyMin) {
		// Special case KeyMin: find the first entry in meta1.
		return roachpb.RSpan{
			Key:    roachpb.RKey(Meta1Prefix),
			EndKey: roachpb.RKey(Meta1Prefix.PrefixEnd()),
		}, nil
	}
	if key.Equal(Meta1KeyMax) {
		// Special case Meta1KeyMax: this is the last key in Meta1, we don't want
		// to start at Next().
		return roachpb.RSpan{
			Key:    roachpb.RKey(Meta1KeyMax),
			EndKey: roachpb.RKey(Meta1Prefix.PrefixEnd()),
		}, nil
	}

	// Otherwise find the first entry greater than the given key in the same meta prefix.
	start := key.Next()
	end := key[:len(Meta1Prefix)].PrefixEnd()
	return roachpb.RSpan{Key: start, EndKey: end}, nil
}

// MetaReverseScanBounds returns the range [start,end) within which the desired
// meta record can be found by means of a reverse engine scan. The given key
// must be a valid RangeMetaKey as defined by validateRangeMetaKey.
func MetaReverseScanBounds(key roachpb.RKey) (roachpb.RSpan, error) {
	if err := validateRangeMetaKey(key); err != nil {
		return roachpb.RSpan{}, err
	}

	if key.Equal(roachpb.RKeyMin) || key.Equal(Meta1Prefix) {
		return roachpb.RSpan{},
			NewInvalidRangeMetaKeyError("KeyMin and Meta1Prefix can't be used as the key of reverse scan", key)
	}
	if key.Equal(Meta2Prefix) {
		// Special case Meta2Prefix: this is the first key in Meta2, and the scan
		// interval covers all of Meta1.
		return roachpb.RSpan{
			Key:    roachpb.RKey(Meta1Prefix),
			EndKey: roachpb.RKey(key.Next().AsRawKey()),
		}, nil
	}

	// Otherwise find the first entry greater than the given key and find the last entry
	// in the same prefix. For MVCCReverseScan the endKey is exclusive, if we want to find
	// the range descriptor the given key specified,we need to set the key.Next() as the
	// MVCCReverseScan`s endKey. For example:
	// If we have ranges [a,f) and [f,z), then we'll have corresponding meta records
	// at f and z. If you're looking for the meta record for key f, then you want the
	// second record (exclusive in MVCCReverseScan), hence key.Next() below.
	start := key[:len(Meta1Prefix)]
	end := key.Next()
	return roachpb.RSpan{Key: start, EndKey: end}, nil
}

// MakeTablePrefix returns the key prefix used for the table's data.
func MakeTablePrefix(tableID uint32) []byte {
	return encoding.EncodeUvarintAscending(nil, uint64(tableID))
}

// DecodeTablePrefix validates that the given key has a table prefix, returning
// the remainder of the key (with the prefix removed) and the decoded descriptor
// ID of the table.
func DecodeTablePrefix(key roachpb.Key) ([]byte, uint64, error) {
	if encoding.PeekType(key) != encoding.Int {
		return key, 0, errors.Errorf("invalid key prefix: %q", key)
	}
	return encoding.DecodeUvarintAscending(key)
}

// DescMetadataPrefix returns the key prefix for all descriptors.
func DescMetadataPrefix() []byte {
	k := MakeTablePrefix(uint32(DescriptorTableID))
	return encoding.EncodeUvarintAscending(k, DescriptorTablePrimaryKeyIndexID)
}

// DescMetadataKey returns the key for the descriptor.
func DescMetadataKey(descID uint32) roachpb.Key {
	k := DescMetadataPrefix()
	k = encoding.EncodeUvarintAscending(k, uint64(descID))
	return MakeFamilyKey(k, DescriptorTableDescriptorColFamID)
}

// DecodeDescMetadataID decodes a descriptor ID from a descriptor metadata key.
func DecodeDescMetadataID(key roachpb.Key) (uint64, error) {
	// Extract object ID from key.
	// TODO(marc): move sql/keys.go to keys (or similar) and use a DecodeDescMetadataKey.
	// We should also check proper encoding.
	remaining, tableID, err := DecodeTablePrefix(key)
	if err != nil {
		return 0, err
	}
	if tableID != DescriptorTableID {
		return 0, errors.Errorf("key is not a descriptor table entry: %v", key)
	}
	// DescriptorTable.PrimaryIndex.ID
	remaining, _, err = encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return 0, err
	}
	// descID
	_, id, err := encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return 0, err
	}
	return id, nil
}

// MakeFamilyKey returns the key for the family in the given row by appending to
// the passed key.
func MakeFamilyKey(key []byte, famID uint32) []byte {
	if famID == 0 {
		// As an optimization, family 0 is encoded without a length suffix.
		return encoding.EncodeUvarintAscending(key, 0)
	}
	size := len(key)
	key = encoding.EncodeUvarintAscending(key, uint64(famID))
	// Note that we assume that `len(key)-size` will always be encoded to a
	// single byte by EncodeUvarint. This is currently always true because the
	// varint encoding will encode 1-9 bytes.
	return encoding.EncodeUvarintAscending(key, uint64(len(key)-size))
}

const (
	// SequenceIndexID is the ID of the single index on each special single-column,
	// single-row sequence table.
	SequenceIndexID = 1
	// SequenceColumnFamilyID is the ID of the column family on each special single-column,
	// single-row sequence table.
	SequenceColumnFamilyID = 0
)

// MakeSequenceKey returns the key used to store the value of a sequence.
func MakeSequenceKey(tableID uint32) []byte {
	key := MakeTablePrefix(tableID)
	key = encoding.EncodeUvarintAscending(key, SequenceIndexID)        // Index id
	key = encoding.EncodeUvarintAscending(key, 0)                      // Primary key value
	key = encoding.EncodeUvarintAscending(key, SequenceColumnFamilyID) // Column family
	return key
}

// GetRowPrefixLength returns the length of the row prefix of the key. A table
// key's row prefix is defined as the maximal prefix of the key that is also a
// prefix of every key for the same row. (Any key with this maximal prefix is
// also guaranteed to be part of the input key's row.)
// For secondary index keys, the row prefix is defined as the entire key.
func GetRowPrefixLength(key roachpb.Key) (int, error) {
	n := len(key)
	if encoding.PeekType(key) != encoding.Int {
		// Not a table key, so the row prefix is the entire key.
		return n, nil
	}
	// The column ID length is encoded as a varint and we take advantage of the
	// fact that the column ID itself will be encoded in 0-9 bytes and thus the
	// length of the column ID data will fit in a single byte.
	buf := key[n-1:]
	if encoding.PeekType(buf) != encoding.Int {
		// The last byte is not a valid column ID suffix.
		return 0, errors.Errorf("%s: not a valid table key", key)
	}

	// Strip off the family ID / column ID suffix from the buf. The last byte of the buf
	// contains the length of the column ID suffix (which might be 0 if the buf
	// does not contain a column ID suffix).
	_, colIDLen, err := encoding.DecodeUvarintAscending(buf)
	if err != nil {
		return 0, err
	}
	// Note how this next comparison (and by extension the code after it) is overflow-safe. There
	// are more intuitive ways of writing this that aren't as safe. See #18628.
	if colIDLen > uint64(n-1) {
		// The column ID length was impossible. colIDLen is the length of
		// the encoded column ID suffix. We add 1 to account for the byte
		// holding the length of the encoded column ID and if that total
		// (colIDLen+1) is greater than the key suffix (n == len(buf))
		// then we bail. Note that we don't consider this an error because
		// EnsureSafeSplitKey can be called on keys that look like table
		// keys but which do not have a column ID length suffix (e.g
		// by SystemConfig.ComputeSplitKey).
		return 0, errors.Errorf("%s: malformed table key", key)
	}
	return len(key) - int(colIDLen) - 1, nil
}

// EnsureSafeSplitKey transforms an SQL table key such that it is a valid split key
// (i.e. does not occur in the middle of a row).
func EnsureSafeSplitKey(key roachpb.Key) (roachpb.Key, error) {
	// The row prefix for a key is unique to keys in its row - no key without the
	// row prefix will be in the key's row. Therefore, we can be certain that
	// using the row prefix for a key as a split key is safe: it doesn't occur in
	// the middle of a row.
	idx, err := GetRowPrefixLength(key)
	if err != nil {
		return nil, err
	}
	return key[:idx], nil
}

// Range returns a key range encompassing the key ranges of all requests.
func Range(reqs []roachpb.RequestUnion) (roachpb.RSpan, error) {
	from := roachpb.RKeyMax
	to := roachpb.RKeyMin
	for _, arg := range reqs {
		req := arg.GetInner()
		h := req.Header()
		if !roachpb.IsRange(req) && len(h.EndKey) != 0 {
			return roachpb.RSpan{}, errors.Errorf("end key specified for non-range operation: %s", req)
		}

		key, err := Addr(h.Key)
		if err != nil {
			return roachpb.RSpan{}, err
		}
		if key.Less(from) {
			// Key is smaller than `from`.
			from = key
		}
		if !key.Less(to) {
			// Key.Next() is larger than `to`.
			if bytes.Compare(key, roachpb.RKeyMax) > 0 {
				return roachpb.RSpan{}, errors.Errorf("%s must be less than KeyMax", key)
			}
			to = key.Next()
		}

		if len(h.EndKey) == 0 {
			continue
		}
		endKey, err := AddrUpperBound(h.EndKey)
		if err != nil {
			return roachpb.RSpan{}, err
		}
		if bytes.Compare(roachpb.RKeyMax, endKey) < 0 {
			return roachpb.RSpan{}, errors.Errorf("%s must be less than or equal to KeyMax", endKey)
		}
		if to.Less(endKey) {
			// EndKey is larger than `to`.
			to = endKey
		}
	}
	return roachpb.RSpan{Key: from, EndKey: to}, nil
}

// RangeIDPrefixBuf provides methods for generating range ID local keys while
// avoiding an allocation on every key generated. The generated keys are only
// valid until the next call to one of the key generation methods.
type RangeIDPrefixBuf roachpb.Key

// MakeRangeIDPrefixBuf creates a new range ID prefix buf suitable for
// generating the various range ID local keys.
func MakeRangeIDPrefixBuf(rangeID roachpb.RangeID) RangeIDPrefixBuf {
	return RangeIDPrefixBuf(MakeRangeIDPrefix(rangeID))
}

func (b RangeIDPrefixBuf) replicatedPrefix() roachpb.Key {
	return append(roachpb.Key(b), LocalRangeIDReplicatedInfix...)
}

func (b RangeIDPrefixBuf) unreplicatedPrefix() roachpb.Key {
	return append(roachpb.Key(b), localRangeIDUnreplicatedInfix...)
}

// AbortSpanKey returns a range-local key by Range ID for an AbortSpan
// entry, with detail specified by encoding the supplied transaction ID.
func (b RangeIDPrefixBuf) AbortSpanKey(txnID uuid.UUID) roachpb.Key {
	key := append(b.replicatedPrefix(), LocalAbortSpanSuffix...)
	return encoding.EncodeBytesAscending(key, txnID.GetBytes())
}

// RangeAppliedStateKey returns a system-local key for the range applied state key.
// See comment on RangeAppliedStateKey function.
func (b RangeIDPrefixBuf) RangeAppliedStateKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeAppliedStateSuffix...)
}

// RaftAppliedIndexLegacyKey returns a system-local key for a raft applied index.
// See comment on RaftAppliedIndexLegacyKey function.
func (b RangeIDPrefixBuf) RaftAppliedIndexLegacyKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRaftAppliedIndexLegacySuffix...)
}

// LeaseAppliedIndexLegacyKey returns a system-local key for a lease applied index.
// See comment on LeaseAppliedIndexLegacyKey function.
func (b RangeIDPrefixBuf) LeaseAppliedIndexLegacyKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalLeaseAppliedIndexLegacySuffix...)
}

// RaftTruncatedStateLegacyKey returns a system-local key for a RaftTruncatedState.
func (b RangeIDPrefixBuf) RaftTruncatedStateLegacyKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRaftTruncatedStateLegacySuffix...)
}

// RangeLeaseKey returns a system-local key for a range lease.
func (b RangeIDPrefixBuf) RangeLeaseKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeLeaseSuffix...)
}

// RangeStatsLegacyKey returns the key for accessing the MVCCStats struct
// for the specified Range ID.
// See comment on RangeStatsLegacyKey function.
func (b RangeIDPrefixBuf) RangeStatsLegacyKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeStatsLegacySuffix...)
}

// RangeLastGCKey returns a system-local key for the last GC.
func (b RangeIDPrefixBuf) RangeLastGCKey() roachpb.Key {
	return append(b.replicatedPrefix(), LocalRangeLastGCSuffix...)
}

// RangeTombstoneKey returns a system-local key for a range tombstone.
func (b RangeIDPrefixBuf) RangeTombstoneKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRangeTombstoneSuffix...)
}

// RaftTruncatedStateKey returns a system-local key for a RaftTruncatedState.
func (b RangeIDPrefixBuf) RaftTruncatedStateKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRaftTruncatedStateLegacySuffix...)
}

// RaftHardStateKey returns a system-local key for a Raft HardState.
func (b RangeIDPrefixBuf) RaftHardStateKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRaftHardStateSuffix...)
}

// RaftLogPrefix returns the system-local prefix shared by all Entries
// in a Raft log.
func (b RangeIDPrefixBuf) RaftLogPrefix() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRaftLogSuffix...)
}

// RaftLogKey returns a system-local key for a Raft log entry.
func (b RangeIDPrefixBuf) RaftLogKey(logIndex uint64) roachpb.Key {
	return encoding.EncodeUint64Ascending(b.RaftLogPrefix(), logIndex)
}

// RangeLastReplicaGCTimestampKey returns a range-local key for
// the range's last replica GC timestamp.
func (b RangeIDPrefixBuf) RangeLastReplicaGCTimestampKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalRangeLastReplicaGCTimestampSuffix...)
}

// TsFlushedIndexKey returns a system-local key for a ts flushed index.
func (b RangeIDPrefixBuf) TsFlushedIndexKey() roachpb.Key {
	return append(b.unreplicatedPrefix(), LocalTsFlushedIndexSuffix...)
}

// ZoneKeyPrefix returns the key prefix for id's row in the system.zones table.
func ZoneKeyPrefix(id uint32) roachpb.Key {
	k := MakeTablePrefix(uint32(ZonesTableID))
	k = encoding.EncodeUvarintAscending(k, uint64(ZonesTablePrimaryIndexID))
	return encoding.EncodeUvarintAscending(k, uint64(id))
}

// ZoneKey returns the key for id's entry in the system.zones table.
func ZoneKey(id uint32) roachpb.Key {
	k := ZoneKeyPrefix(id)
	return MakeFamilyKey(k, uint32(ZonesTableConfigColumnID))
}
