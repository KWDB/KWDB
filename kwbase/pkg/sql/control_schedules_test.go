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

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/sql/distsql"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestUpdateSchedule(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := makeTestPlanner()
	runParam := runParams{
		ctx:             context.TODO(),
		extendedEvalCtx: p.extendedEvalCtx.copy(),
		p:               p,
	}
	test := jobs.ScheduledJob{}
	require.NoError(t, updateSchedule(runParam, &test))
}

func TestDeleteSchedule(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	temp := s.ExecutorConfig()
	ecf := temp.(ExecutorConfig)
	p := makeTestPlanner()
	p.extendedEvalCtx.ExecCfg.DistSQLSrv = s.DistSQLServer().(*distsql.ServerImpl)
	p.extendedEvalCtx.ExecCfg.InternalExecutor = ecf.InternalExecutor
	runParam := runParams{
		ctx:             context.TODO(),
		extendedEvalCtx: p.extendedEvalCtx.copy(),
		p:               p,
	}

	require.NoError(t, deleteSchedule(runParam, 1))
}
