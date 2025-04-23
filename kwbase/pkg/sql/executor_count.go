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

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
)

// CountExecutorName is the name associated with scheduled job executor which
// runs jobs outstanding -- that is, it doesn't spawn external system.job to do its work.
const CountExecutorName = "scheduled-count-executor"

// ScheduledCountExecutor implements ScheduledJobExecutor interface.
type ScheduledCountExecutor struct{}

var _ jobs.ScheduledJobExecutor = &ScheduledCountExecutor{}

// ExecuteJob implements ScheduledJobExecutor interface.
func (e *ScheduledCountExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	user := security.NodeUser
	phs, cleanup := cfg.PlanHookMaker(CountExecutorName, txn, user)
	defer cleanup()
	innerPlaner := phs.(PlanHookState)
	// create a job and start it
	jobRegistry := innerPlaner.ExecCfg().JobRegistry
	syncDetail := jobspb.SyncMetaCacheDetails{
		Type: count,
	}
	jobRecord := jobs.Record{
		Description: "count ts tables",
		Username:    user,
		CreatedBy: &jobs.CreatedByInfo{
			Name: schedule.ScheduleLabel(),
			ID:   schedule.ScheduleID(),
		},
		Details:  syncDetail,
		Progress: jobspb.SyncMetaCacheProgress{},
	}
	job, err := jobRegistry.CreateJobWithTxn(ctx, jobRecord, txn)
	if err != nil {
		return err
	}
	log.Infof(ctx, "Count table schedule creates new job %d", job.ID())
	return nil
}

// NotifyJobTermination implements ScheduledJobExecutor interface.
func (e *ScheduledCountExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID int64,
	jobStatus jobs.Status,
	_ jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	// For now, only interested in failed status.
	if jobStatus == jobs.StatusFailed {
		jobs.DefaultHandleFailedRun(schedule, "job %d failed", jobID)
	}
	return nil
}

// Metrics implements ScheduledJobExecutor interface
func (e *ScheduledCountExecutor) Metrics() metric.Struct {
	return nil
}
