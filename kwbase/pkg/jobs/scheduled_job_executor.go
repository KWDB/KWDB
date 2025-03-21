// Copyright 2020 The Cockroach Authors.
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

package jobs

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// ScheduledJobExecutor is an interface describing execution of the scheduled job.
type ScheduledJobExecutor interface {
	// ExecuteJob Executes scheduled job;  Implementation may use provided transaction.
	// Modifications to the ScheduledJob object will be persisted.
	ExecuteJob(
		ctx context.Context,
		cfg *scheduledjobs.JobExecutionConfig,
		env scheduledjobs.JobSchedulerEnv,
		schedule *ScheduledJob,
		txn *kv.Txn,
	) error

	// NotifyJobTermination Notifies that the system.job started by the ScheduledJob completed.
	// Implementation may use provided transaction to perform any additional mutations.
	// Modifications to the ScheduledJob object will be persisted.
	NotifyJobTermination(
		ctx context.Context,
		jobID int64,
		jobStatus Status,
		details jobspb.Details,
		env scheduledjobs.JobSchedulerEnv,
		schedule *ScheduledJob,
		ex sqlutil.InternalExecutor,
		txn *kv.Txn,
	) error

	// Metrics returns optional metric.Struct object for this executor.
	Metrics() metric.Struct
}

// ScheduledJobExecutorFactory is a callback to create a ScheduledJobExecutor.
type ScheduledJobExecutorFactory = func() (ScheduledJobExecutor, error)

var executorRegistry struct {
	syncutil.Mutex
	factories map[string]ScheduledJobExecutorFactory
	executors map[string]ScheduledJobExecutor
}

// RegisterScheduledJobExecutorFactory registers callback for creating ScheduledJobExecutor
// with the specified name.
func RegisterScheduledJobExecutorFactory(name string, factory ScheduledJobExecutorFactory) {
	executorRegistry.Lock()
	defer executorRegistry.Unlock()
	if executorRegistry.factories == nil {
		executorRegistry.factories = make(map[string]ScheduledJobExecutorFactory)
	}

	if _, ok := executorRegistry.factories[name]; !ok {
		executorRegistry.factories[name] = factory
	}
}

// newScheduledJobExecutor creates new instance of ScheduledJobExecutor.
func newScheduledJobExecutorLocked(name string) (ScheduledJobExecutor, error) {
	if factory, ok := executorRegistry.factories[name]; ok {
		return factory()
	}
	return nil, errors.Newf("executor %q is not registered", name)
}

// GetScheduledJobExecutor returns a singleton instance of
// ScheduledJobExecutor and a flag indicating if that instance was just created.
func GetScheduledJobExecutor(name string) (ScheduledJobExecutor, error) {
	executorRegistry.Lock()
	defer executorRegistry.Unlock()
	return getScheduledJobExecutorLocked(name)
}

func getScheduledJobExecutorLocked(name string) (ScheduledJobExecutor, error) {
	if executorRegistry.executors == nil {
		executorRegistry.executors = make(map[string]ScheduledJobExecutor)
	}
	if ex, ok := executorRegistry.executors[name]; ok {
		return ex, nil
	}
	ex, err := newScheduledJobExecutorLocked(name)
	if err != nil {
		return nil, err
	}
	executorRegistry.executors[name] = ex
	return ex, nil
}

// RegisterExecutorsMetrics registered the metrics updated by each executor.
func RegisterExecutorsMetrics(registry *metric.Registry) error {
	executorRegistry.Lock()
	defer executorRegistry.Unlock()

	for executorType := range executorRegistry.factories {
		ex, err := getScheduledJobExecutorLocked(executorType)
		if err != nil {
			return err
		}
		if m := ex.Metrics(); m != nil {
			registry.AddMetricStruct(m)
		}
	}

	return nil
}

// retryFailedJobAfter specifies the duration after which to retry
const retryFailedJobAfter = time.Minute

// DefaultHandleFailedRun is a default implementation for handling failed run
// (either system.job failure, or perhaps error processing the schedule itself).
func DefaultHandleFailedRun(schedule *ScheduledJob, fmtOrMsg string, args ...interface{}) {
	switch schedule.ScheduleDetails().OnError {
	case jobspb.ScheduleDetails_RETRY_SOON:
		schedule.SetScheduleStatus("retrying: "+fmtOrMsg, args...)
		schedule.SetNextRun(schedule.env.Now().Add(retryFailedJobAfter)) // TODO(yevgeniy): backoff
	case jobspb.ScheduleDetails_PAUSE_SCHED:
		schedule.Pause()
		schedule.SetScheduleStatus("schedule paused: "+fmtOrMsg, args...)
	case jobspb.ScheduleDetails_RETRY_SCHED:
		schedule.SetScheduleStatus("reschedule: "+fmtOrMsg, args...)
	}
}

// NotifyJobTermination is invoked when the job triggered by specified schedule
// completes
//
// The 'txn' transaction argument is the transaction the job will use to update its
// state (e.g. status, etc).  If any changes need to be made to the scheduled job record,
// those changes are applied to the same transaction -- that is, they are applied atomically
// with the job status changes.
func NotifyJobTermination(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	jobID int64,
	jobStatus Status,
	jobDetails jobspb.Details,
	scheduleID int64,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	if env == nil {
		env = scheduledjobs.ProdJobSchedulerEnv
	}

	schedule, err := LoadScheduledJob(ctx, env, scheduleID, ex, txn)
	if err != nil {
		return err
	}
	executor, err := GetScheduledJobExecutor(schedule.ExecutorType())
	if err != nil {
		return err
	}

	// Delegate handling of the job termination to the executor.
	err = executor.NotifyJobTermination(ctx, jobID, jobStatus, jobDetails, env, schedule, ex, txn)
	if err != nil {
		return err
	}

	// Update this schedule in case executor made changes to it.
	return schedule.Update(ctx, ex, txn)
}
