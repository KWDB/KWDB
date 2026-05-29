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

package tree

// Vacuum represents a vacuum ts databases statement.
type Vacuum struct {
	// AggregateOnly indicates that vacuum should only recompute pre-aggregates
	// (corresponds to the SQL clause "AGGREGATE ONLY").
	AggregateOnly bool
}

var _ Statement = &Vacuum{}

// Format implements the NodeFormatter interface.
func (node *Vacuum) Format(ctx *FmtCtx) {
	ctx.WriteString("VACUUM TS DATABASES")
	if node.AggregateOnly {
		ctx.WriteString(" AGGREGATE ONLY")
	}
}
