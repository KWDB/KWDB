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
	provided := make(opt.Ordering, 0)
	return trimProvided(provided, required, fds)
}

// TSScanPrivateCanProvide returns true if the ts scan operator returns rows
// that satisfy the given required ordering; it also returns whether the scan
// needs to be in reverse order to match the required ordering.
func TSScanPrivateCanProvide(
	md *opt.Metadata, s *memo.TSScanPrivate, required *physical.OrderingChoice,
) (ok bool, reverse bool) {
	//if s.OrderedScanType == keys.OrderedScan || s.OrderedScanType == keys.SortAfterScan {
	if required.Optional.Contains(md.TableMeta(s.Table).MetaID.ColumnID(0)) {
		return true, false
	}
	//}

	return false, false
}
