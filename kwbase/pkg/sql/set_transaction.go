// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// SetTransaction sets a transaction's isolation level, priority, ro/rw state,
// and as of timestamp.
func (p *GenericPlanner) SetTransaction(n *tree.SetTransaction) (PlanNode, error) {
	var asOfTs hlc.Timestamp
	if n.Modes.AsOf.Expr != nil {
		var err error
		asOfTs, err = p.EvalAsOfTimestamp(n.Modes.AsOf)
		if err != nil {
			return nil, err
		}
		p.semaCtx.AsOfTimestamp = &asOfTs
	}

	if err := p.extendedEvalCtx.TxnModesSetter.setTransactionModes(n.Modes, asOfTs); err != nil {
		return nil, err
	}
	return NewZeroNode(nil /* columns */), nil
}
