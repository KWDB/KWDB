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

package sql

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDiscard(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := makeTestPlanner()
	ctx := context.TODO()

	_, err := p.Discard(ctx, &tree.Discard{})
	require.Error(t, err)
	_, err = p.Discard(ctx, &tree.Discard{Mode: tree.DiscardMode(100)})
	require.Error(t, err)

	p.autoCommit = false
	_, err = p.Discard(ctx, &tree.Discard{Mode: tree.DiscardModeAll})
	require.Error(t, err)

	p.autoCommit = true
	p.preparedStatements = connExPrepStmtsAccessor{}
	_, err = p.Discard(ctx, &tree.Discard{Mode: tree.DiscardModeAll})
	require.NoError(t, err)
}
