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
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

type controlJobsNode struct {
	rows          PlanNode
	desiredStatus jobs.Status
	numRows       int
}

var jobCommandToDesiredStatus = map[tree.JobCommand]jobs.Status{
	tree.CancelJob: jobs.StatusCanceled,
	tree.ResumeJob: jobs.StatusRunning,
	tree.PauseJob:  jobs.StatusPaused,
}

// FastPathResults implements the PlanNodeFastPath inteface.
func (n *controlJobsNode) FastPathResults() (int, bool) {
	return n.numRows, true
}

func (n *controlJobsNode) StartExec(params RunParams) error {
	reg := params.p.ExecCfg().JobRegistry
	for {
		ok, err := n.rows.Next(params)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		jobIDDatum := n.rows.Values()[0]
		if jobIDDatum == tree.DNull {
			continue
		}

		jobID, ok := tree.AsDInt(jobIDDatum)
		if !ok {
			return errors.AssertionFailedf("%q: expected *DInt, found %T", jobIDDatum, jobIDDatum)
		}

		// TODO(liyang):Prohibit users from manually controlling replication related JOBs
		//row, err := params.extendedEvalCtx.InternalExecutor.QueryRow(params.Ctx,
		//	"SELECT JOB TYPE",
		//	params.p.Txn(),
		//	`SELECT job_type from kwdb_internal.jobs where job_id =$1`,
		//	jobID)
		//if strings.HasPrefix(string(tree.MustBeDString(row[0])), "REPLICATION") {
		//	return errors.Newf("CONTROL REPLICATE JOB IS NOT ALLOWED")
		//}
		switch n.desiredStatus {
		case jobs.StatusPaused:
			err = reg.PauseRequested(params.Ctx, params.p.txn, int64(jobID))
		case jobs.StatusRunning:
			err = reg.Resume(params.Ctx, params.p.txn, int64(jobID))
		case jobs.StatusCanceled:
			err = reg.CancelRequested(params.Ctx, params.p.txn, int64(jobID))
		default:
			err = errors.AssertionFailedf("unhandled status %v", n.desiredStatus)
		}
		if err != nil {
			return err
		}
		n.numRows++
		params.GetPlanner().SetAuditTarget(0, strconv.FormatInt(int64(jobID), 10), nil)
	}
	switch n.desiredStatus {
	case jobs.StatusPaused:
		telemetry.Inc(sqltelemetry.SchemaJobControlCounter("pause"))
	case jobs.StatusRunning:
		telemetry.Inc(sqltelemetry.SchemaJobControlCounter("resume"))
	case jobs.StatusCanceled:
		telemetry.Inc(sqltelemetry.SchemaJobControlCounter("cancel"))
	}

	return nil
}

func (*controlJobsNode) Next(RunParams) (bool, error) { return false, nil }

func (*controlJobsNode) Values() tree.Datums { return nil }

func (n *controlJobsNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}
