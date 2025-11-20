// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ordering

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

func tsScanCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	ok, _ := TSScanPrivateCanProvide(
		expr.Memo().Metadata(),
		&expr.(*memo.TSScanExpr).TSScanPrivate,
		required,
	)
	return ok
}

func tsScanBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	scan := expr.(*memo.TSScanExpr)
	fds := &scan.Relational().FuncDeps
	constCols := fds.ComputeClosure(opt.ColSet{})
	provided := make(opt.Ordering, 0, 1)
	indexColID := scan.Table.ColumnID(0)
	descending := false
	for right := 0; right < len(required.Columns); {
		if required.Optional.Contains(indexColID) {
			continue
		}
		reqCol := &required.Columns[right]
		descending = reqCol.Descending
		break
	}
	if descending {
		scan.Flags.Direction = tree.Descending
	} else {
		scan.Flags.Direction = tree.Ascending
	}
	// only timestamp col can provide order
	for i := 0; i < 1; i++ {
		colID := scan.Table.ColumnID(0)
		if !scan.Cols.Contains(colID) {
			// Column not in output; we are done.
			break
		}
		if constCols.Contains(colID) {
			// Column constrained to a constant, ignore.
			continue
		}

		provided = append(provided, opt.MakeOrderingColumn(colID, descending))
	}
	return trimProvided(provided, required, fds)
}

// TSScanPrivateCanProvide returns true if the ts scan operator returns rows
// that satisfy the given required ordering; it also returns whether the scan
// needs to be in reverse order to match the required ordering.
func TSScanPrivateCanProvide(
	md *opt.Metadata, s *memo.TSScanPrivate, required *physical.OrderingChoice,
) (ok bool, reverse bool) {
	if !s.Flags.CheckOnlyOnePTagValue() {
		return false, false
	}

	descending := false
	indexColID := s.Table.ColumnID(0)
	for right := 0; right < len(required.Columns); {
		if required.Optional.Contains(indexColID) {
			continue
		}
		reqCol := &required.Columns[right]
		if !reqCol.Group.Contains(indexColID) {
			return false, false
		}
		// The directions of the index column and the required column impose either
		// a forward or a reverse scan.
		if right == 0 {
			descending = reqCol.Descending
		} else if descending != reqCol.Descending {
			// We already determined the direction, and according to it, this column
			// has the wrong direction.
			return false, false
		}
		right = right + 1
	}
	// If direction is either, we prefer forward scan.
	return true, descending
}
