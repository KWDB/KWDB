// Copyright 2018 The Cockroach Authors.
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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"go.etcd.io/etcd/raft/raftpb"
)

// PrintKeyValue attempts to pretty-print the specified MVCCKeyValue to
// os.Stdout, falling back to '%q' formatting.
func PrintKeyValue(kv relational.MVCCKeyValue) {
	fmt.Println(SprintKeyValue(kv, true /* printKey */))
}

// SprintKey pretty-prings the specified MVCCKey.
func SprintKey(key relational.MVCCKey) string {
	return fmt.Sprintf("%s %s (%#x): ", key.Timestamp, key.Key, relational.EncodeKey(key))
}

// SprintKeyValue is like PrintKeyValue, but returns a string. If
// printKey is true, prints the key and the value together; otherwise,
// prints just the value.
func SprintKeyValue(kv relational.MVCCKeyValue, printKey bool) string {
	var sb strings.Builder
	if printKey {
		sb.WriteString(SprintKey(kv.Key))
	}
	decoders := []func(kv relational.MVCCKeyValue) (string, error){
		tryRaftLogEntry,
		tryRangeDescriptor,
		tryMeta,
		tryTxn,
		tryRangeIDKey,
		tryTimeSeries,
		tryIntent,
		func(kv relational.MVCCKeyValue) (string, error) {
			// No better idea, just print raw bytes and hope that folks use `less -S`.
			return fmt.Sprintf("%q", kv.Value), nil
		},
	}
	for _, decoder := range decoders {
		out, err := decoder(kv)
		if err != nil {
			continue
		}
		sb.WriteString(out)
		return sb.String()
	}
	panic("unreachable")
}

func tryRangeDescriptor(kv relational.MVCCKeyValue) (string, error) {
	if err := IsRangeDescriptorKey(kv.Key); err != nil {
		return "", err
	}
	var desc roachpb.RangeDescriptor
	if err := getProtoValue(kv.Value, &desc); err != nil {
		return "", err
	}
	return descStr(desc), nil
}

func tryIntent(kv relational.MVCCKeyValue) (string, error) {
	if len(kv.Value) == 0 {
		return "", errors.New("empty")
	}
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
		return "", err
	}
	s := fmt.Sprintf("%+v", meta)
	if meta.Txn != nil {
		s = meta.Txn.WriteTimestamp.String() + " " + s
	}
	return s, nil
}

func decodeWriteBatch(writeBatch *storagepb.WriteBatch) (string, error) {
	if writeBatch == nil {
		return "<nil>\n", nil
	}

	r, err := relational.NewRocksDBBatchReader(writeBatch.Data)
	if err != nil {
		return "", err
	}

	// NB: always return sb.String() as the first arg, even on error, to give
	// the caller all the info we have (in case the writebatch is corrupted).
	var sb strings.Builder
	for r.Next() {
		switch r.BatchType() {
		case relational.BatchTypeDeletion:
			mvccKey, err := r.MVCCKey()
			if err != nil {
				return sb.String(), err
			}
			sb.WriteString(fmt.Sprintf("Delete: %s\n", SprintKey(mvccKey)))
		case relational.BatchTypeValue:
			mvccKey, err := r.MVCCKey()
			if err != nil {
				return sb.String(), err
			}
			sb.WriteString(fmt.Sprintf("Put: %s\n", SprintKeyValue(relational.MVCCKeyValue{
				Key:   mvccKey,
				Value: r.Value(),
			}, true /* printKey */)))
		case relational.BatchTypeMerge:
			mvccKey, err := r.MVCCKey()
			if err != nil {
				return sb.String(), err
			}
			sb.WriteString(fmt.Sprintf("Merge: %s\n", SprintKeyValue(relational.MVCCKeyValue{
				Key:   mvccKey,
				Value: r.Value(),
			}, true /* printKey */)))
		case relational.BatchTypeSingleDeletion:
			mvccKey, err := r.MVCCKey()
			if err != nil {
				return sb.String(), err
			}
			sb.WriteString(fmt.Sprintf("Single Delete: %s\n", SprintKey(mvccKey)))
		case relational.BatchTypeRangeDeletion:
			mvccStartKey, err := r.MVCCKey()
			if err != nil {
				return sb.String(), err
			}
			mvccEndKey, err := r.MVCCEndKey()
			if err != nil {
				return sb.String(), err
			}
			sb.WriteString(fmt.Sprintf(
				"Delete Range: [%s, %s)\n", SprintKey(mvccStartKey), SprintKey(mvccEndKey),
			))
		default:
			sb.WriteString(fmt.Sprintf("unsupported batch type: %d\n", r.BatchType()))
		}
	}
	return sb.String(), r.Error()
}

func tryRaftLogEntry(kv relational.MVCCKeyValue) (string, error) {
	var ent raftpb.Entry
	if err := maybeUnmarshalInline(kv.Value, &ent); err != nil {
		return "", err
	}

	var cmd storagepb.RaftCommand
	switch ent.Type {
	case raftpb.EntryNormal:
		if len(ent.Data) == 0 {
			return fmt.Sprintf("%s: EMPTY\n", &ent), nil
		}
		_, cmdData := DecodeRaftCommand(ent.Data)
		if err := protoutil.Unmarshal(cmdData, &cmd); err != nil {
			return "", err
		}
	case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
		var c raftpb.ConfChangeI
		if ent.Type == raftpb.EntryConfChange {
			var cc raftpb.ConfChange
			if err := protoutil.Unmarshal(ent.Data, &cc); err != nil {
				return "", err
			}
			c = cc
		} else {
			var cc raftpb.ConfChangeV2
			if err := protoutil.Unmarshal(ent.Data, &cc); err != nil {
				return "", err
			}
			c = cc
		}

		var ctx ConfChangeContext
		if err := protoutil.Unmarshal(c.AsV2().Context, &ctx); err != nil {
			return "", err
		}
		if err := protoutil.Unmarshal(ctx.Payload, &cmd); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("unknown log entry type: %s", &ent)
	}
	ent.Data = nil

	var leaseStr string
	if l := cmd.DeprecatedProposerLease; l != nil {
		leaseStr = l.String() // use full lease, if available
	} else {
		leaseStr = fmt.Sprintf("lease #%d", cmd.ProposerLeaseSequence)
	}

	wbStr, err := decodeWriteBatch(cmd.WriteBatch)
	if err != nil {
		wbStr = "failed to decode: " + err.Error() + "\nafter:\n" + wbStr
	}
	cmd.WriteBatch = nil

	return fmt.Sprintf("%s by %s\n%s\nwrite batch:\n%s", &ent, leaseStr, &cmd, wbStr), nil
}

func tryTxn(kv relational.MVCCKeyValue) (string, error) {
	var txn roachpb.Transaction
	if err := maybeUnmarshalInline(kv.Value, &txn); err != nil {
		return "", err
	}
	return txn.String() + "\n", nil
}

func tryRangeIDKey(kv relational.MVCCKeyValue) (string, error) {
	if kv.Key.Timestamp != (hlc.Timestamp{}) {
		return "", fmt.Errorf("range ID keys shouldn't have timestamps: %s", kv.Key)
	}
	_, _, suffix, _, err := keys.DecodeRangeIDKey(kv.Key.Key)
	if err != nil {
		return "", err
	}

	// All range ID keys are stored inline on the metadata.
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
		return "", err
	}
	value := roachpb.Value{RawBytes: meta.RawBytes}

	// Values encoded as protobufs set msg and continue outside the
	// switch. Other types are handled inside the switch and return.
	var msg protoutil.Message
	switch {
	case bytes.Equal(suffix, keys.LocalLeaseAppliedIndexLegacySuffix):
		fallthrough
	case bytes.Equal(suffix, keys.LocalRaftAppliedIndexLegacySuffix):
		i, err := value.GetInt()
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(i, 10), nil

	case bytes.Equal(suffix, keys.LocalAbortSpanSuffix):
		msg = &roachpb.AbortSpanEntry{}

	case bytes.Equal(suffix, keys.LocalRangeLastGCSuffix):
		msg = &hlc.Timestamp{}

	case bytes.Equal(suffix, keys.LocalRangeTombstoneSuffix):
		msg = &roachpb.RangeTombstone{}

	case bytes.Equal(suffix, keys.LocalRaftTruncatedStateLegacySuffix):
		msg = &roachpb.RaftTruncatedState{}

	case bytes.Equal(suffix, keys.LocalRangeLeaseSuffix):
		msg = &roachpb.Lease{}

	case bytes.Equal(suffix, keys.LocalRangeAppliedStateSuffix):
		msg = &enginepb.RangeAppliedState{}

	case bytes.Equal(suffix, keys.LocalRangeStatsLegacySuffix):
		msg = &enginepb.MVCCStats{}

	case bytes.Equal(suffix, keys.LocalRaftHardStateSuffix):
		msg = &raftpb.HardState{}

	case bytes.Equal(suffix, keys.LocalRangeLastReplicaGCTimestampSuffix):
		msg = &hlc.Timestamp{}

	default:
		return "", fmt.Errorf("unknown raft id key %s", suffix)
	}

	if err := value.GetProto(msg); err != nil {
		return "", err
	}
	return msg.String(), nil
}

func tryMeta(kv relational.MVCCKeyValue) (string, error) {
	if !bytes.HasPrefix(kv.Key.Key, keys.Meta1Prefix) && !bytes.HasPrefix(kv.Key.Key, keys.Meta2Prefix) {
		return "", errors.New("not a meta key")
	}
	value := roachpb.Value{
		Timestamp: kv.Key.Timestamp,
		RawBytes:  kv.Value,
	}
	var desc roachpb.RangeDescriptor
	if err := value.GetProto(&desc); err != nil {
		return "", err
	}
	return descStr(desc), nil
}

func tryTimeSeries(kv relational.MVCCKeyValue) (string, error) {
	if len(kv.Value) == 0 || !bytes.HasPrefix(kv.Key.Key, keys.TimeseriesPrefix) {
		return "", errors.New("empty or not TS")
	}
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
		return "", err
	}
	v := roachpb.Value{RawBytes: meta.RawBytes}
	var ts roachpb.InternalTimeSeriesData
	if err := v.GetProto(&ts); err != nil {
		return "", err
	}
	return fmt.Sprintf("%+v [mergeTS=%s]", &ts, meta.MergeTimestamp), nil
}

// IsRangeDescriptorKey returns nil if the key decodes as a RangeDescriptor.
func IsRangeDescriptorKey(key relational.MVCCKey) error {
	_, suffix, _, err := keys.DecodeRangeKey(key.Key)
	if err != nil {
		return err
	}
	if !bytes.Equal(suffix, keys.LocalRangeDescriptorSuffix) {
		return fmt.Errorf("wrong suffix: %s", suffix)
	}
	return nil
}

func getProtoValue(data []byte, msg protoutil.Message) error {
	value := roachpb.Value{
		RawBytes: data,
	}
	return value.GetProto(msg)
}

func descStr(desc roachpb.RangeDescriptor) string {
	return fmt.Sprintf("[%s, %s)\n\tRaw:%s\n",
		desc.StartKey, desc.EndKey, &desc)
}

func maybeUnmarshalInline(v []byte, dest protoutil.Message) error {
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(v, &meta); err != nil {
		return err
	}
	value := roachpb.Value{
		RawBytes: meta.RawBytes,
	}
	return value.GetProto(dest)
}

type stringifyWriteBatch storagepb.WriteBatch

func (s *stringifyWriteBatch) String() string {
	wbStr, err := decodeWriteBatch((*storagepb.WriteBatch)(s))
	if err == nil {
		return wbStr
	}
	return fmt.Sprintf("failed to stringify write batch (%x): %s", s.Data, err)
}
