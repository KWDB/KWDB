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

package opt

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/util"
)

// ViewDeps contains information about the dependencies of a view.
type ViewDeps []ViewDep

// ViewDep contains information about a view dependency.
type ViewDep struct {
	DataSource cat.DataSource

	// ColumnOrdinals is the set of column ordinals that are referenced by the
	// view for this table. In most cases, this consists of all "public" columns
	// of the table; the only exception is when a table is referenced by table ID
	// with a specific list of column IDs.
	ColumnOrdinals util.FastIntSet

	// ColumnIDToOrd maps a scopeColumn's ColumnID to its ColumnOrdinal. This
	// helps us add only the columns that are actually referenced by the object's
	// query into the dependencies. We add a dependency on a column only when the
	// column is referenced and created as a scopeColumn.
	ColumnIDToOrd map[ColumnID]int

	// If an index is referenced specifically (via an index hint), SpecificIndex
	// is true and Index is the ordinal of that index.
	SpecificIndex bool
	Index         cat.IndexOrdinal
}

// Union returns the union of vp and newDeps as a new ViewDeps.
func (vp ViewDeps) Union(newDeps ViewDeps) ViewDeps {
	for j := range newDeps {
		found := false
		for i := range vp {
			if vp[i].DataSource.ID() == newDeps[j].DataSource.ID() {
				vp[i].ColumnOrdinals.Union(newDeps[j].ColumnOrdinals)
				found = true
				break
			}
		}
		if !found {
			vp = append(vp, newDeps[j])
		}
	}
	return vp
}
