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

package row

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// MakeFkMetadata populates a map of FkTableMetadata for all the
// TableDescriptors that might be needed when performing FK checking for delete
// and/or insert operations. It uses the passed in lookup function to perform
// the actual lookup. The caller is expected to create the CheckHelper for the
// given mutatedTable. However, if cascading is required, then the FK code will
// need to create additional CheckHelpers. It does this using the
// AnalyzeExprFunction, TableLookupFunction, and CheckPrivilegeFunction
// functions, so these must be provided if there's a possibility of a cascading
// operation.
func MakeFkMetadata(
	ctx context.Context,
	mutatedTable *sqlbase.ImmutableTableDescriptor,
	startUsage FKCheckType,
	tblLookupFn TableLookupFunction,
	privCheckFn CheckPrivilegeFunction,
	analyzeExprFn sqlbase.AnalyzeExprFunction,
	checkHelper *sqlbase.CheckHelper,
) (FkTableMetadata, error) {
	// Initialize the lookup queue.
	queue := tableLookupQueue{
		result:         make(FkTableMetadata),
		alreadyChecked: make(map[TableID]map[FKCheckType]struct{}),
		tblLookupFn:    tblLookupFn,
		privCheckFn:    privCheckFn,
		analyzeExprFn:  analyzeExprFn,
	}

	// Add the passed in table descriptor to the table lookup.
	//
	// This logic is very close to (*tableLookupQueue).getTable()
	// however differs in two important aspects:
	// - we are not checking the privilege; the caller
	//   will have done that given the type of SQL mutation statement.
	//   For example UPDATE wants to check the table for UPDATE privilege.
	// - we do process the mutatedTable that's given even if it is
	//   in "adding" state or non-public.
	//
	queue.result[mutatedTable.ID] = TableEntry{Desc: mutatedTable, CheckHelper: checkHelper}
	if err := queue.enqueue(ctx, mutatedTable.ID, startUsage); err != nil {
		return nil, err
	}

	// Main lookup queue.
	for {
		// Pop one unit of work.
		tableEntry, usage, hasWork := queue.dequeue()
		if !hasWork {
			return queue.result, nil
		}

		// If the table descriptor is nil it means that there was no actual lookup
		// performed. Meaning there is no need to walk any secondary relationships
		// and the table descriptor lookup will happen later.
		//
		// TODO(knz): the paragraph above is suspicious. A nil table desc
		// indicates the table is non-public. In either case it seems that
		// if there is a descriptor we ought to carry out the FK
		// work. What gives?
		if tableEntry.IsAdding || tableEntry.Desc == nil {
			continue
		}

		// Explore all the FK constraints on the table.
		if usage == CheckInserts || usage == CheckUpdates {
			for i := range tableEntry.Desc.OutboundFKs {
				fk := &tableEntry.Desc.OutboundFKs[i]
				// If the mutation performed is an insertion or an update,
				// we'll need to do existence checks on the referenced
				// table(s), if any.
				if _, err := queue.getTable(ctx, fk.ReferencedTableID); err != nil {
					return nil, err
				}
			}
		}
		if usage == CheckDeletes || usage == CheckUpdates {
			// If the mutation performed is a deletion or an update,
			// we'll need to do existence checks on the referencing
			// table(s), if any, as well as cascading actions.
			for i := range tableEntry.Desc.InboundFKs {
				fk := &tableEntry.Desc.InboundFKs[i]
				// The referencing table is required to know the relationship, so
				// fetch it here.
				referencingTableEntry, err := queue.getTable(ctx, fk.OriginTableID)
				if err != nil {
					return nil, err
				}

				// Again here if the table descriptor is nil it means that there was
				// no actual lookup performed. Meaning there is no need to walk any
				// secondary relationships.
				//
				// TODO(knz): this comment is suspicious for the same
				// reasons as above.
				if referencingTableEntry.IsAdding || referencingTableEntry.Desc == nil {
					continue
				}

				if usage == CheckDeletes {
					var nextUsage FKCheckType
					switch fk.OnDelete {
					case sqlbase.ForeignKeyReference_CASCADE:
						nextUsage = CheckDeletes
					case sqlbase.ForeignKeyReference_SET_DEFAULT, sqlbase.ForeignKeyReference_SET_NULL:
						nextUsage = CheckUpdates
					default:
						// There is no need to check any other relationships.
						continue
					}
					if err := queue.enqueue(ctx, fk.OriginTableID, nextUsage); err != nil {
						return nil, err
					}
				} else {
					// curUsage == CheckUpdates
					if fk.OnUpdate == sqlbase.ForeignKeyReference_CASCADE ||
						fk.OnUpdate == sqlbase.ForeignKeyReference_SET_DEFAULT ||
						fk.OnUpdate == sqlbase.ForeignKeyReference_SET_NULL {
						if err := queue.enqueue(ctx, fk.OriginTableID, CheckUpdates); err != nil {
							return nil, err
						}
					}
				}
			}
		}
	}
}
