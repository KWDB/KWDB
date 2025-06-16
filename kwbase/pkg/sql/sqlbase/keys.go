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

package sqlbase

import (
	"bytes"
	"math"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// MakeNameMetadataKey returns the key for the name, as expected by
// versions >= 20.1.
// Pass name == "" in order to generate the prefix key to use to scan over all
// of the names for the specified parentID.
func MakeNameMetadataKey(parentID ID, parentSchemaID ID, name string) roachpb.Key {
	k := keys.MakeTablePrefix(uint32(NamespaceTable.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(NamespaceTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(parentID))
	k = encoding.EncodeUvarintAscending(k, uint64(parentSchemaID))
	if name != "" {
		k = encoding.EncodeBytesAscending(k, []byte(name))
		k = keys.MakeFamilyKey(k, uint32(NamespaceTable.Columns[3].ID))
	}
	return k
}

// DecodeNameMetadataKey returns the components that make up the
// NameMetadataKey for version >= 20.1.
func DecodeNameMetadataKey(k roachpb.Key) (parentID ID, parentSchemaID ID, name string, err error) {
	k, _, err = keys.DecodeTablePrefix(k)
	if err != nil {
		return 0, 0, "", err
	}

	var buf uint64
	k, buf, err = encoding.DecodeUvarintAscending(k)
	if err != nil {
		return 0, 0, "", err
	}
	if buf != uint64(NamespaceTable.PrimaryIndex.ID) {
		return 0, 0, "", errors.Newf("tried get table %d, but got %d", NamespaceTable.PrimaryIndex.ID, buf)
	}

	k, buf, err = encoding.DecodeUvarintAscending(k)
	if err != nil {
		return 0, 0, "", err
	}
	parentID = ID(buf)

	k, buf, err = encoding.DecodeUvarintAscending(k)
	if err != nil {
		return 0, 0, "", err
	}
	parentSchemaID = ID(buf)

	var bytesBuf []byte
	_, bytesBuf, err = encoding.DecodeBytesAscending(k, nil)
	if err != nil {
		return 0, 0, "", err
	}
	name = string(bytesBuf)

	return parentID, parentSchemaID, name, nil
}

// MakeDeprecatedNameMetadataKey returns the key for a name, as expected by
// versions < 20.1. Pass name == "" in order to generate the prefix key to use
// to scan over all of the names for the specified parentID.
func MakeDeprecatedNameMetadataKey(parentID ID, name string) roachpb.Key {
	k := keys.MakeTablePrefix(uint32(DeprecatedNamespaceTable.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(DeprecatedNamespaceTable.PrimaryIndex.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(parentID))
	if name != "" {
		k = encoding.EncodeBytesAscending(k, []byte(name))
		k = keys.MakeFamilyKey(k, uint32(DeprecatedNamespaceTable.Columns[2].ID))
	}
	return k
}

// MakeAllDescsMetadataKey returns the key for all descriptors.
func MakeAllDescsMetadataKey() roachpb.Key {
	return keys.DescMetadataPrefix()
}

// MakeDescMetadataKey returns the key for the descriptor.
func MakeDescMetadataKey(descID ID) roachpb.Key {
	return keys.DescMetadataKey(uint32(descID))
}

// MakeTsPrimaryTagKey make the key from table descriptor ID and hashPoints
// of a TS table/entity
func MakeTsPrimaryTagKey(tbDescID ID, hashPoints []api.HashPoint) roachpb.Key {
	if len(hashPoints) == 0 {
		return nil
	}
	prefix := keys.MakeTablePrefix(uint32(tbDescID))
	return encoding.EncodeUvarintAscending(prefix, uint64(hashPoints[0]))
}

// MakeTsHashPointKey make the TS key from table descriptor ID and hash point
func MakeTsHashPointKey(tbDescID ID, hashPoint uint64, hashNum uint64) roachpb.Key {
	// first hashPartition -> /Table/tbDescID - /Table/tbDescID/hashPoint
	if hashPoint == 0 {
		return keys.MakeTablePrefix(uint32(tbDescID))
	}
	// last hashPartition -> /Table/tbDescID/hashPoint - /Table/tbDescID+1
	if hashPoint == hashNum {
		return keys.MakeTablePrefix(uint32(tbDescID + 1))
	}
	prefix := keys.MakeTablePrefix(uint32(tbDescID))
	return encoding.EncodeUvarintAscending(prefix, hashPoint)
}

// MakeTsRangeKey make the TS key from table descriptor ID, hash point and timestamp.
func MakeTsRangeKey(tbDescID ID, hashPoint uint64, timestamp int64, hashNum uint64) roachpb.Key {
	// last hashPartition -> /Table/tbDescID/hashPoint - /Table/tbDescID+1
	if hashPoint == hashNum {
		return keys.MakeTablePrefix(uint32(tbDescID + 1))
	}
	prefix := keys.MakeTablePrefix(uint32(tbDescID))
	key := encoding.EncodeUvarintAscending(prefix, hashPoint)

	return encoding.EncodeVarintAscending(key, timestamp)
}

// DecodeTsRangeKey decodes a range key for table id, hash point and timestamp.
func DecodeTsRangeKey(key []byte, isStartKey bool, hashNum uint64) (uint64, uint64, int64, error) {
	// specially handle the /Max key, it is actually out of the ranges.
	if bytes.Compare(key, keys.MaxKey) == 0 {
		return math.MaxUint64, hashNum - 1, math.MaxInt64, nil
	}
	remaining, tableID, err := keys.DecodeTablePrefix(key)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(remaining) == 0 {
		// the first range startKey meet it
		if isStartKey {
			return tableID, 0, math.MinInt64, nil
		}
		return tableID - 1, hashNum - 1, math.MaxInt64, nil
	}

	var hashPoint uint64
	remaining, hashPoint, err = encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(remaining) == 0 {
		if isStartKey {
			return tableID, hashPoint, math.MinInt64, nil
		}
		return tableID, hashPoint - 1, math.MaxInt64, nil
	}

	_, timestamp, err := encoding.DecodeVarintAscending(remaining)
	if err != nil {
		return 0, 0, 0, err
	}
	if timestamp == 0 && !isStartKey {
		return tableID, hashPoint - 1, math.MaxInt64, nil
	}
	return tableID, hashPoint, timestamp, nil
}

// DecodeTSRangeKey decode ts range key
func DecodeTSRangeKey(
	startKey roachpb.RKey, endKey roachpb.RKey, hashNum uint64,
) (uint64, uint64, uint64, int64, int64, error) {
	var tableID, startPoint, endPoint uint64
	var startTs, endTs int64
	var err error
	tableID, startPoint, startTs, err = DecodeTsRangeKey(startKey, true, hashNum)
	if err != nil {
		return 0, 0, 0, 0, 0, errors.Wrap(err, "DecodeTsRangeKey StartKey failed")
	}
	_, endPoint, endTs, err = DecodeTsRangeKey(endKey, false, hashNum)
	if err != nil {
		return 0, 0, 0, 0, 0, errors.Wrap(err, "DecodeTsRangeKey endKey failed")
	}
	if endTs != math.MaxInt64 {
		endTs--
	}
	return tableID, startPoint, endPoint, startTs, endTs, nil
}

// IndexKeyValDirs returns the corresponding encoding.Directions for all the
// encoded values in index's "fullest" possible index key, including directions
// for table/index IDs, the interleaved sentinel and the index column values.
// For example, given
//
//	CREATE INDEX foo ON bar (a, b DESC) INTERLEAVED IN PARENT bar (a)
//
// a typical index key with all values specified could be
//
//	/51/1/42/#/51/2/1337
//
// which would return the slice
//
//	{ASC, ASC, ASC, 0, ASC, ASC, DESC}
func IndexKeyValDirs(index *IndexDescriptor) []encoding.Direction {
	if index == nil {
		return nil
	}

	dirs := make([]encoding.Direction, 0, (len(index.Interleave.Ancestors)+1)*2+len(index.ColumnDirections))

	colIdx := 0
	for _, ancs := range index.Interleave.Ancestors {
		// Table/Index IDs are always encoded ascending.
		dirs = append(dirs, encoding.Ascending, encoding.Ascending)
		for i := 0; i < int(ancs.SharedPrefixLen); i++ {
			d, err := index.ColumnDirections[colIdx].ToEncodingDirection()
			if err != nil {
				panic(err)
			}
			dirs = append(dirs, d)
			colIdx++
		}

		// The interleaved sentinel uses the 0 value for
		// encoding.Direction when pretty-printing (see
		// encoding.go:prettyPrintFirstValue).
		dirs = append(dirs, 0)
	}

	// The index's table/index ID.
	dirs = append(dirs, encoding.Ascending, encoding.Ascending)

	for colIdx < len(index.ColumnDirections) {
		d, err := index.ColumnDirections[colIdx].ToEncodingDirection()
		if err != nil {
			panic(err)
		}
		dirs = append(dirs, d)
		colIdx++
	}

	return dirs
}

// PrettyKey pretty-prints the specified key, skipping over the first `skip`
// fields. The pretty printed key looks like:
//
//	/Table/<tableID>/<indexID>/...
//
// We always strip off the /Table prefix and then `skip` more fields. Note that
// this assumes that the fields themselves do not contain '/', but that is
// currently true for the fields we care about stripping (the table and index
// ID).
func PrettyKey(valDirs []encoding.Direction, key roachpb.Key, skip int) string {
	p := key.StringWithDirs(valDirs, 0 /* maxLen */)
	for i := 0; i <= skip; i++ {
		n := strings.IndexByte(p[1:], '/')
		if n == -1 {
			return ""
		}
		p = p[n+1:]
	}
	return p
}

// PrettySpan returns a human-readable representation of a span.
func PrettySpan(valDirs []encoding.Direction, span roachpb.Span, skip int) string {
	var b strings.Builder
	b.WriteString(PrettyKey(valDirs, span.Key, skip))
	b.WriteByte('-')
	b.WriteString(PrettyKey(valDirs, span.EndKey, skip))
	return b.String()
}

// PrettySpans returns a human-readable description of the spans.
// If index is nil, then pretty print subroutines will use their default
// settings.
func PrettySpans(index *IndexDescriptor, spans []roachpb.Span, skip int) string {
	if len(spans) == 0 {
		return ""
	}

	valDirs := IndexKeyValDirs(index)

	var b strings.Builder
	for i, span := range spans {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(PrettySpan(valDirs, span, skip))
	}
	return b.String()
}
