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

package row

import (
	"context"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"github.com/cockroachdb/errors"
)

// singleKVFetcher is a kvBatchFetcher that returns a single kv.
type singleKVFetcher struct {
	kvs  [1]roachpb.KeyValue
	done bool
}

// nextBatch implements the kvBatchFetcher interface.
func (f *singleKVFetcher) nextBatch(
	_ context.Context,
) (ok bool, kvs []roachpb.KeyValue, batchResponse []byte, span roachpb.Span, err error) {
	if f.done {
		return false, nil, nil, roachpb.Span{}, nil
	}
	f.done = true
	return true, f.kvs[:], nil, roachpb.Span{}, nil
}

// GetRangesInfo implements the kvBatchFetcher interface.
func (f *singleKVFetcher) GetRangesInfo() []roachpb.RangeInfo {
	panic(errors.AssertionFailedf("GetRangesInfo() called on singleKVFetcher"))
}

// ConvertBatchError returns a user friendly constraint violation error.
func ConvertBatchError(
	ctx context.Context, tableDesc *sqlbase.ImmutableTableDescriptor, b *kv.Batch,
) error {
	origPErr := b.MustPErr()
	if origPErr.Index == nil {
		return origPErr.GoError()
	}
	j := origPErr.Index.Index
	if j >= int32(len(b.Results)) {
		return errors.AssertionFailedf("index %d outside of results: %+v", j, b.Results)
	}
	result := b.Results[j]
	if cErr, ok := origPErr.GetDetail().(*roachpb.ConditionFailedError); ok && len(result.Rows) > 0 {
		key := result.Rows[0].Key
		return NewUniquenessConstraintViolationError(ctx, tableDesc, key, cErr.ActualValue)
	}
	return origPErr.GoError()
}

// NewUniquenessConstraintViolationError creates an error that represents a
// violation of a UNIQUE constraint.
func NewUniquenessConstraintViolationError(
	ctx context.Context,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	key roachpb.Key,
	value *roachpb.Value,
) error {
	// TODO(dan): There's too much internal knowledge of the sql table
	// encoding here (and this callsite is the only reason
	// DecodeIndexKeyPrefix is exported). Refactor this bit out.
	indexID, _, err := sqlbase.DecodeIndexKeyPrefix(tableDesc.TableDesc(), key)
	if err != nil {
		return err
	}
	index, err := tableDesc.FindIndexByID(indexID)
	if err != nil {
		return err
	}
	var rf Fetcher

	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(index.ColumnIDs)-1)

	colIdxMap := make(map[sqlbase.ColumnID]int, len(index.ColumnIDs))
	cols := make([]sqlbase.ColumnDescriptor, len(index.ColumnIDs))
	for i, colID := range index.ColumnIDs {
		colIdxMap[colID] = i
		col, err := tableDesc.FindColumnByID(colID)
		if err != nil {
			return err
		}
		cols[i] = *col
	}

	tableArgs := FetcherTableArgs{
		Desc:             tableDesc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: indexID != tableDesc.PrimaryIndex.ID,
		Cols:             cols,
		ValNeededForCol:  valNeededForCol,
	}
	if err := rf.Init(
		false, /* reverse */
		sqlbase.ScanLockingStrength_FOR_NONE,
		false, /* returnRangeInfo */
		false, /* isCheck */
		&sqlbase.DatumAlloc{},
		tableArgs,
	); err != nil {
		return err
	}
	f := singleKVFetcher{kvs: [1]roachpb.KeyValue{{Key: key}}}
	if value != nil {
		f.kvs[0].Value = *value
	}
	// Use the Fetcher to decode the single kv pair above by passing in
	// this singleKVFetcher implementation, which doesn't actually hit KV.
	if err := rf.StartScanFrom(ctx, &f); err != nil {
		return err
	}
	datums, _, _, err := rf.NextRowDecoded(ctx)
	if err != nil {
		return err
	}

	valStrs := make([]string, 0, len(datums))
	for _, val := range datums {
		valStrs = append(valStrs, val.String())
	}

	return pgerror.Newf(pgcode.UniqueViolation,
		"duplicate key value (%s)=(%s) violates unique constraint %q",
		strings.Join(index.ColumnNames, ","),
		strings.Join(valStrs, ","),
		index.Name)
}
