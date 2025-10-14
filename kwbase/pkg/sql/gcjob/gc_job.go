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

package gcjob

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var (
	// MaxSQLGCInterval is the longest the polling interval between checking if
	// elements should be GC'd.
	MaxSQLGCInterval = 5 * time.Minute
)

// SetSmallMaxGCIntervalForTest sets the MaxSQLGCInterval and then returns a closure
// that resets it.
// This is to be used in tests like:
//
//	defer SetSmallMaxGCIntervalForTest()
func SetSmallMaxGCIntervalForTest() func() {
	oldInterval := MaxSQLGCInterval
	MaxSQLGCInterval = 500 * time.Millisecond
	return func() {
		MaxSQLGCInterval = oldInterval
	}
}

type schemaChangeGCResumer struct {
	jobID int64
}

// performGC GCs any schema elements that are in the DELETING state and returns
// a bool indicating if it GC'd any elements.
func performGC(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) (bool, error) {
	didGC := false
	if details.Indexes != nil {
		if didGCIndex, err := gcIndexes(ctx, execCfg, details.ParentID, progress); err != nil {
			return false, errors.Wrap(err, "attempting to GC indexes")
		} else if didGCIndex {
			didGC = true
		}
	} else if details.Tables != nil {
		if didGCTable, err := gcTables(ctx, execCfg, progress); err != nil {
			return false, errors.Wrap(err, "attempting to GC tables")
		} else if didGCTable {
			didGC = true
		}

		// Drop database zone config when all the tables have been GCed.
		if details.ParentID != sqlbase.InvalidID && isDoneGC(progress) {
			if err := deleteDatabaseZoneConfig(ctx, execCfg.DB, details.ParentID); err != nil {
				return false, errors.Wrap(err, "deleting database zone config")
			}
		}
	}
	return didGC, nil
}

// Resume is part of the jobs.Resumer interface.
func (r schemaChangeGCResumer) Resume(
	ctx context.Context, phs interface{}, _ chan<- tree.Datums,
) error {
	p := phs.(sql.PlanHookState)
	// TODO(pbardea): Wait for no versions.
	execCfg := p.ExecCfg()
	if fn := execCfg.GCJobTestingKnobs.RunBeforeResume; fn != nil {
		if err := fn(r.jobID); err != nil {
			return err
		}
	}
	details, progress, err := initDetailsAndProgress(ctx, execCfg, r.jobID)
	if err != nil {
		return err
	}

	gossipUpdateC, cleanup := execCfg.GCJobNotifier.AddNotifyee(ctx)
	defer cleanup()
	tableDropTimes, indexDropTimes := getDropTimes(details)

	allTables := getAllTablesWaitingForGC(details, progress)
	if len(allTables) == 0 {
		return nil
	}
	expired, earliestDeadline := refreshTables(ctx, execCfg, allTables, tableDropTimes, indexDropTimes, r.jobID, progress)
	timerDuration := timeutil.Until(earliestDeadline)
	if expired {
		timerDuration = 0
	} else if timerDuration > MaxSQLGCInterval {
		timerDuration = MaxSQLGCInterval
	}
	timer := timeutil.NewTimer()
	defer timer.Stop()
	timer.Reset(timerDuration)

	for {
		select {
		case <-gossipUpdateC:
			// Upon notification of a gossip update, update the status of the relevant schema elements.
			if log.V(2) {
				log.Info(ctx, "received a new system config")
			}
			remainingTables := getAllTablesWaitingForGC(details, progress)
			if len(remainingTables) == 0 {
				return nil
			}
			expired, earliestDeadline = refreshTables(ctx, execCfg, remainingTables, tableDropTimes, indexDropTimes, r.jobID, progress)

			if isDoneGC(progress) {
				return nil
			}

			timerDuration := time.Until(earliestDeadline)
			if expired {
				timerDuration = 0
			} else if timerDuration > MaxSQLGCInterval {
				timerDuration = MaxSQLGCInterval
			}

			timer.Reset(timerDuration)
		case <-timer.C:
			timer.Read = true
			if log.V(2) {
				log.Info(ctx, "SchemaChangeGC timer triggered")
			}
			// Refresh the status of all tables in case any GC TTLs have changed.
			remainingTables := getAllTablesWaitingForGC(details, progress)
			_, earliestDeadline = refreshTables(ctx, execCfg, remainingTables, tableDropTimes, indexDropTimes, r.jobID, progress)

			if didWork, err := performGC(ctx, execCfg, details, progress); err != nil {
				return err
			} else if didWork {
				persistProgress(ctx, execCfg, r.jobID, progress)

				if fn := execCfg.GCJobTestingKnobs.RunAfterGC; fn != nil {
					if err := fn(r.jobID); err != nil {
						return err
					}
				}
			}

			if isDoneGC(progress) {
				return nil
			}

			// Schedule the next check for GC.
			timerDuration := time.Until(earliestDeadline)
			if timerDuration > MaxSQLGCInterval {
				timerDuration = MaxSQLGCInterval
			}
			timer.Reset(timerDuration)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r schemaChangeGCResumer) OnFailOrCancel(context.Context, interface{}) error {
	return nil
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &schemaChangeGCResumer{
			jobID: *job.ID(),
		}
	}
	jobs.RegisterConstructor(jobspb.TypeSchemaChangeGC, createResumerFn)
}
