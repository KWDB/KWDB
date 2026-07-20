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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
)

// SetSessionCharacteristics configures session-level characteristics for the transaction
func (p *GenericPlanner) SetSessionCharacteristics(
	n *tree.SetSessionCharacteristics,
) (PlanNode, error) {
	// Note: We also support SET DEFAULT_TRANSACTION_ISOLATION TO ' .... ' above.
	// Ensure both versions stay in sync.
	switch n.Modes.Isolation {
	case tree.ReadCommittedIsolation, tree.SerializableIsolation, tree.RepeatedReadIsolation:
		p.sessionDataMutator.SetDefaultTransactionIsolationLevel(n.Modes.Isolation)
	case tree.UnspecifiedIsolation:
		p.sessionDataMutator.SetDefaultTransactionIsolationLevel(tree.SerializableIsolation)
	default:
		return nil, fmt.Errorf("unsupported default isolation level: %s", n.Modes.Isolation)
	}

	switch n.Modes.ReadWriteMode {
	case tree.ReadOnly:
		p.sessionDataMutator.SetDefaultReadOnly(true)
	case tree.ReadWrite:
		p.sessionDataMutator.SetDefaultReadOnly(false)
	case tree.UnspecifiedReadWriteMode:
	default:
		return nil, fmt.Errorf("unsupported default read write mode: %s", n.Modes.ReadWriteMode)
	}

	switch n.Modes.UserPriority {
	case tree.UnspecifiedUserPriority:
	default:
		return nil, unimplemented.New("default transaction priority",
			"unsupported session default: transaction priority")
	}
	return NewZeroNode(nil /* columns */), nil
}
