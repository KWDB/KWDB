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
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Discard implements the DISCARD statement.
// See https://www.postgresql.org/docs/9.6/static/sql-discard.html for details.
func (p *GenericPlanner) Discard(ctx context.Context, s *tree.Discard) (PlanNode, error) {
	switch s.Mode {
	case tree.DiscardModeAll:
		if !p.autoCommit {
			return nil, pgerror.New(pgcode.ActiveSQLTransaction,
				"DISCARD ALL cannot run inside a transaction block")
		}

		// RESET ALL
		if err := resetSessionVars(ctx, p.sessionDataMutator); err != nil {
			return nil, err
		}

		// DEALLOCATE ALL
		p.preparedStatements.DeleteAll(ctx)
	default:
		return nil, errors.AssertionFailedf("unknown mode for DISCARD: %d", s.Mode)
	}
	return NewZeroNode(nil /* columns */), nil
}

func resetSessionVars(ctx context.Context, m *sessionDataMutator) error {
	for _, varName := range varNames {
		v := varGen[varName]
		if v.Set != nil {
			hasDefault, defVal := getSessionVarDefaultString(varName, v, m)
			if hasDefault {
				if err := v.Set(ctx, m, defVal); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
