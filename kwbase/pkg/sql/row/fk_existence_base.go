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

package row

import (
	"sort"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/span"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// fkExistenceCheckBaseHelper is an auxiliary struct that facilitates FK existence
// checks for one FK constraint.
//
// TODO(knz): the fact that this captures the txn is problematic. The
// txn should be passed as argument.
type fkExistenceCheckBaseHelper struct {
	// txn is the current KV transaction.
	txn *kv.Txn

	// dir indicates the direction of the check.
	//
	// Note that the helper is used both for forward checks (when
	// INSERTing data in a *referencing* table) and backward checks
	// (when DELETEing data in a *referenced* table). UPDATE uses
	// helpers in both directions.
	//
	// Because it serves both directions, the explanations below
	// avoid using the words "referencing" and "referenced". Instead
	// it uses "searched" for the table/index where the existence
	// is tested; and "mutated" for the table/index being written to.
	//
	dir FKCheckType

	// rf is the row fetcher used to look up rows in the searched table.
	rf *Fetcher

	// searchIdx is the index used for lookups over the searched table.
	searchIdx *sqlbase.IndexDescriptor

	// prefixLen is the number of columns being looked up. In the common
	// case it matches the number of columns in searchIdx, however it is
	// possible to run a lookup check for a prefix of the columns in
	// searchIdx, eg. `(a,b) REFERENCES t(x,y)` with an index on
	// `(x,y,z)`.
	prefixLen int

	// ids maps column IDs in index searchIdx to positions of the `row`
	// array provided to each FK existence check. This tells the checker
	// where to find the values in the row for each column of the
	// searched index.
	ids map[sqlbase.ColumnID]int

	// ref is a copy of the ForeignKeyConstraint object in the table
	// descriptor.  During the check this is used to decide how to check
	// the value (MATCH style).
	//
	// TODO(knz): the entire reference object is not needed during the
	// mutation, only the match style and name. Simplify this.
	ref *sqlbase.ForeignKeyConstraint

	// searchTable is the descriptor of the searched table. Stored only
	// for error messages; lookups use the pre-computed searchPrefix.
	searchTable *sqlbase.ImmutableTableDescriptor

	// mutatedIdx is the descriptor for the target index being mutated.
	// Stored only for error messages.
	mutatedIdx *sqlbase.IndexDescriptor

	// valuesScratch is memory used to populate an error message when the check
	// fails.
	valuesScratch tree.Datums

	// spanBuilder is responsible for constructing spans for FK lookups.
	spanBuilder *span.Builder
}

// makeFkExistenceCheckBaseHelper instantiates a FK helper.
//
// - dir is the direction of the check.
//
//   - ref is a copy of the FK constraint object that points
//     to the table where to perform the existence check.
//
//     For forward checks, this is a copy of the FK
//     constraint placed on the referencing table.
//     For backward checks, this is a copy of the FK
//     constraint placed as backref on the referenced table.
//
//     This is used to derive the searched table/index,
//     and determine the MATCH style.
//
//   - writeIdx is the target index being mutated. This is used
//     to determine prefixLen in combination with searchIdx.
//
//   - colMap maps column IDs in the searched index, to positions
//     in the input `row` of datums during the check.
//
//   - alloc is a suitable datum allocator used to initialize
//     the row fetcher.
//
//   - otherTables is an object that provides schema extraction services.
//     TODO(knz): this should become homogeneous across the 3 packages
//     sql, sqlbase, row. The proliferation is annoying.
func makeFkExistenceCheckBaseHelper(
	txn *kv.Txn,
	otherTables FkTableMetadata,
	ref *sqlbase.ForeignKeyConstraint,
	searchIdx *sqlbase.IndexDescriptor,
	mutatedIdx *sqlbase.IndexDescriptor,
	colMap map[sqlbase.ColumnID]int,
	alloc *sqlbase.DatumAlloc,
	dir FKCheckType,
) (ret fkExistenceCheckBaseHelper, err error) {
	// Look up the searched table.
	searchTable := otherTables[ref.ReferencedTableID].Desc
	if searchTable == nil {
		return ret, errors.AssertionFailedf("referenced table %d not in provided table map %+v", ref.ReferencedTableID, otherTables)
	}
	// Determine the columns being looked up.
	ids, err := computeFkCheckColumnIDs(ref, mutatedIdx, searchIdx, colMap)
	if err != nil {
		return ret, err
	}

	// Initialize the row fetcher.
	tableArgs := FetcherTableArgs{
		Desc:             searchTable,
		Index:            searchIdx,
		ColIdxMap:        searchTable.ColumnIdxMap(),
		IsSecondaryIndex: searchIdx.ID != searchTable.PrimaryIndex.ID,
		Cols:             searchTable.Columns,
	}
	rf := &Fetcher{}
	if err := rf.Init(
		false, /* reverse */
		sqlbase.ScanLockingStrength_FOR_NONE,
		false, /* returnRangeInfo */
		false, /* isCheck */
		alloc,
		tableArgs,
	); err != nil {
		return ret, err
	}

	return fkExistenceCheckBaseHelper{
		txn:           txn,
		dir:           dir,
		rf:            rf,
		ref:           ref,
		searchTable:   searchTable,
		searchIdx:     searchIdx,
		mutatedIdx:    mutatedIdx,
		ids:           ids,
		prefixLen:     len(ref.OriginColumnIDs),
		valuesScratch: make(tree.Datums, len(ref.OriginColumnIDs)),
		spanBuilder:   span.MakeBuilder(searchTable.TableDesc(), searchIdx),
	}, nil
}

// computeFkCheckColumnIDs determines the set of column IDs to use for
// the existence check, depending on the MATCH style.
//
// See https://gitee.com/kwbasedb/kwbase/issues/20305 or
// https://www.postgresql.org/docs/11/sql-createtable.html for details on the
// different composite foreign key matching methods.
func computeFkCheckColumnIDs(
	ref *sqlbase.ForeignKeyConstraint,
	mutatedIdx *sqlbase.IndexDescriptor,
	searchIdx *sqlbase.IndexDescriptor,
	colMap map[sqlbase.ColumnID]int,
) (ids map[sqlbase.ColumnID]int, err error) {
	ids = make(map[sqlbase.ColumnID]int, len(ref.OriginColumnIDs))

	switch ref.Match {
	case sqlbase.ForeignKeyReference_SIMPLE:
		for i, writeColID := range ref.OriginColumnIDs {
			if found, ok := colMap[writeColID]; ok {
				ids[searchIdx.ColumnIDs[i]] = found
			} else {
				return nil, errSkipUnusedFK
			}
		}
		return ids, nil

	case sqlbase.ForeignKeyReference_FULL:
		var missingColumns []string
		for _, writeColID := range ref.OriginColumnIDs {
			colOrdinal := -1
			for i, colID := range mutatedIdx.ColumnIDs {
				if writeColID == colID {
					colOrdinal = i
					break
				}
			}
			if colOrdinal == -1 {
				return nil, errors.AssertionFailedf("index %q on columns %+v does not contain column %d",
					mutatedIdx.Name, mutatedIdx.ColumnIDs, writeColID)
			}
			if found, ok := colMap[writeColID]; ok {
				ids[searchIdx.ColumnIDs[colOrdinal]] = found
			} else {
				missingColumns = append(missingColumns, mutatedIdx.ColumnNames[colOrdinal])
			}
		}

		switch len(missingColumns) {
		case 0:
			return ids, nil

		case 1:
			return nil, pgerror.Newf(pgcode.ForeignKeyViolation,
				"missing value for column %q in multi-part foreign key", missingColumns[0])

		case len(ref.OriginColumnIDs):
			// All the columns are nulls, don't check the foreign key.
			return nil, errSkipUnusedFK

		default:
			sort.Strings(missingColumns)
			return nil, pgerror.Newf(pgcode.ForeignKeyViolation,
				"missing values for columns %q in multi-part foreign key", missingColumns)
		}

	case sqlbase.ForeignKeyReference_PARTIAL:
		return nil, unimplemented.NewWithIssue(20305, "MATCH PARTIAL not supported")

	default:
		return nil, errors.AssertionFailedf("unknown composite key match type: %v", ref.Match)
	}
}

var errSkipUnusedFK = errors.New("no columns involved in FK included in writer")
