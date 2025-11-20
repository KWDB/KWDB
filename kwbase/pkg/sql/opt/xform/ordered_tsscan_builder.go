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

package xform

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
)

type orderedTSScanBuilder struct {
	c           *CustomFuncs
	f           *norm.Factory
	mem         *memo.Memo
	scanPrivate memo.TSScanPrivate
}

func (b *orderedTSScanBuilder) init(c *CustomFuncs, private memo.TSScanPrivate) {
	b.c = c
	b.f = c.e.f
	b.mem = c.e.mem
	b.scanPrivate = private
}

// build constructs the final memo expression by composing together the various
// expressions that were specified by previous calls to various add methods.
func (b *orderedTSScanBuilder) build(grp memo.RelExpr) {
	// 1. Only scan.
	flags := &b.scanPrivate.Flags
	flags.ExploreOrderedScan = false
	switch flags.OrderedScanType {
	case opt.OrderedScan:
		flags.OrderedScanType = opt.SortAfterScan
	case opt.SortAfterScan:
		flags.OrderedScanType = opt.OrderedScan
	}

	b.mem.AddTSScanToGroup(&memo.TSScanExpr{TSScanPrivate: b.scanPrivate}, grp)
}
