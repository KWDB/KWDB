// Copyright 2016 The Cockroach Authors.
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

package flowinfra

import (
	"container/list"
	"context"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

const flowDoneChanSize = 8

var settingMaxRunningFlows = settings.RegisterPublicIntSetting(
	"sql.distsql.max_running_flows",
	"maximum number of concurrent flows that can be run on a node",
	500,
)

// FlowScheduler manages running flows and decides when to queue and when to
// start flows. The main interface it presents is ScheduleFlows, which passes a
// flow to be run.
type FlowScheduler struct {
	log.AmbientContext
	stopper    *stop.Stopper
	flowDoneCh chan Flow
	metrics    *execinfra.DistSQLMetrics

	mu struct {
		syncutil.Mutex
		numRunning       int
		maxRunningFlows  int
		queue            *list.List
		flowSchedulerAcc *mon.BoundAccount
	}

	mu1 struct {
		tsFlowMap map[execinfrapb.FlowID]Flow
	}
}

// flowWithCtx stores a flow to run and a context to run it with.
// TODO(asubiotto): Figure out if asynchronous flow execution can be rearranged
// to avoid the need to store the context.
type flowWithCtx struct {
	ctx         context.Context
	flow        Flow
	enqueueTime time.Time
}

// NewFlowScheduler creates a new FlowScheduler.
func NewFlowScheduler(
	ambient log.AmbientContext,
	stopper *stop.Stopper,
	settings *cluster.Settings,
	metrics *execinfra.DistSQLMetrics,
	acc *mon.BoundAccount,
) *FlowScheduler {
	fs := &FlowScheduler{
		AmbientContext: ambient,
		stopper:        stopper,
		flowDoneCh:     make(chan Flow, flowDoneChanSize),
		metrics:        metrics,
	}
	fs.mu.queue = list.New()
	fs.mu.maxRunningFlows = int(settingMaxRunningFlows.Get(&settings.SV))
	fs.mu.flowSchedulerAcc = acc
	fs.mu1.tsFlowMap = make(map[execinfrapb.FlowID]Flow)
	settingMaxRunningFlows.SetOnChange(&settings.SV, func() {
		fs.mu.Lock()
		fs.mu.maxRunningFlows = int(settingMaxRunningFlows.Get(&settings.SV))
		fs.mu.Unlock()
	})
	return fs
}

func (fs *FlowScheduler) canRunFlow(_ Flow) bool {
	// TODO(radu): we will have more complex resource accounting (like memory).
	// For now we just limit the number of concurrent flows.
	return fs.mu.numRunning < fs.mu.maxRunningFlows
}

// runFlowNow starts the given flow; does not wait for the flow to complete.
func (fs *FlowScheduler) runFlowNow(ctx context.Context, f Flow) error {
	log.VEventf(
		ctx, 1, "flow scheduler running flow %s, currently running %d", f.GetID(), fs.mu.numRunning,
	)
	fs.mu.numRunning++
	fs.metrics.FlowStart()
	if err := f.Start(ctx, func() { fs.flowDoneCh <- f }); err != nil {
		return err
	}
	// TODO(radu): we could replace the WaitGroup with a structure that keeps a
	// refcount and automatically runs Cleanup() when the count reaches 0.
	go func() {
		f.Wait()
		fs.mu.Lock()
		fs.mu.flowSchedulerAcc.Shrink(ctx, f.GetFlowSpecMemSize())
		fs.mu.Unlock()
		f.Cleanup(ctx)
	}()
	return nil
}

// ScheduleFlow is the main interface of the flow scheduler: it runs or enqueues
// the given flow.
//
// If the flow can start immediately, errors encountered when starting the flow
// are returned. If the flow is enqueued, these error will be later ignored.
func (fs *FlowScheduler) ScheduleFlow(ctx context.Context, f Flow, specMemUsage int64) error {
	return fs.stopper.RunTaskWithErr(
		ctx, "flowinfra.FlowScheduler: scheduling flow", func(ctx context.Context) error {
			fs.mu.Lock()
			defer fs.mu.Unlock()
			if err := fs.mu.flowSchedulerAcc.Grow(ctx, specMemUsage); err != nil {
				log.Errorf(ctx, "remote flow exec failed, reason:%s. flow queue length: %d.\n", err.Error(), fs.mu.queue.Len())
				return err
			}
			f.SetFlowSpecMemSize(specMemUsage)
			if fs.canRunFlow(f) {
				return fs.runFlowNow(ctx, f)
			}
			log.VEventf(ctx, 1, "flow scheduler enqueuing flow %s to be run later", f.GetID())
			fs.metrics.FlowsQueued.Inc(1)
			fs.mu.queue.PushBack(&flowWithCtx{
				ctx:         ctx,
				flow:        f,
				enqueueTime: timeutil.Now(),
			})
			return nil

		})
}

func (fs *FlowScheduler) AddTsFlow(ctx context.Context, f Flow, specMemUsage int64) error {
	return fs.stopper.RunTaskWithErr(
		ctx, "flowinfra.FlowScheduler: scheduling flow", func(ctx context.Context) error {
			fs.mu.Lock()
			defer fs.mu.Unlock()
			if err := fs.mu.flowSchedulerAcc.Grow(ctx, specMemUsage); err != nil {
				log.Errorf(ctx, "remote flow exec failed, reason:%s. flow queue length: %d.\n", err.Error(), fs.mu.queue.Len())
				return err
			}
			f.SetFlowSpecMemSize(specMemUsage)
			log.VEventf(ctx, 1, "flow scheduler enqueuing tsFlowMap %s to be run later", f.GetID())
			fs.mu1.tsFlowMap[f.GetID()] = f
			return nil
		})
}

func (fs *FlowScheduler) GetTsFlow(fid execinfrapb.FlowID) Flow {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if f, ok := fs.mu1.tsFlowMap[fid]; ok {
		delete(fs.mu1.tsFlowMap, fid)
		return f
	}
	return nil
}

// Start launches the main loop of the scheduler.
func (fs *FlowScheduler) Start() {
	ctx := fs.AnnotateCtx(context.Background())
	fs.stopper.RunWorker(ctx, func(context.Context) {
		stopped := false
		fs.mu.Lock()
		defer fs.mu.Unlock()

		for {
			if stopped && fs.mu.numRunning == 0 {
				// TODO(radu): somehow error out the flows that are still in the queue.
				return
			}
			fs.mu.Unlock()
			select {
			case <-fs.flowDoneCh:
				fs.mu.Lock()
				fs.mu.numRunning--
				fs.metrics.FlowStop()
				if !stopped {
					if frElem := fs.mu.queue.Front(); frElem != nil {
						n := frElem.Value.(*flowWithCtx)
						fs.mu.queue.Remove(frElem)
						wait := timeutil.Since(n.enqueueTime)
						log.VEventf(
							n.ctx, 1, "flow scheduler dequeued flow %s, spent %s in queue", n.flow.GetID(), wait,
						)
						fs.metrics.FlowsQueued.Dec(1)
						fs.metrics.QueueWaitHist.RecordValue(int64(wait))
						// Note: we use the flow's context instead of the worker
						// context, to ensure that logging etc is relative to the
						// specific flow.
						if err := fs.runFlowNow(n.ctx, n.flow); err != nil {
							log.Errorf(n.ctx, "error starting queued flow: %s", err)
						}
					}
				}

			case <-fs.stopper.ShouldStop():
				fs.mu.Lock()
				stopped = true
			}
		}
	})
}
