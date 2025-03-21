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

package jobs_test

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/pkg/errors"
)

func TestRoundtripJob(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	registry := s.JobRegistry().(*jobs.Registry)
	defer s.Stopper().Stop(ctx)

	storedJob := registry.NewJob(jobs.Record{
		Description:   "beep boop",
		Username:      "robot",
		DescriptorIDs: sqlbase.IDs{42},
		Details:       jobspb.RestoreDetails{},
		Progress:      jobspb.RestoreProgress{},
	})
	if err := storedJob.Created(ctx); err != nil {
		t.Fatal(err)
	}
	retrievedJob, err := registry.LoadJob(ctx, *storedJob.ID())
	if err != nil {
		t.Fatal(err)
	}
	if e, a := storedJob, retrievedJob; !reflect.DeepEqual(e, a) {
		//diff := strings.Join(pretty.Diff(e, a), "\n")
		t.Fatalf("stored job did not match retrieved job:\n%+v\n%+v", e, a)
	}
}

func TestRegistryResumeExpiredLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer jobs.ResetConstructors()()

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Disable leniency for instant expiration
	jobs.LeniencySetting.Override(&s.ClusterSettings().SV, 0)

	db := s.DB()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	nodeLiveness := jobs.NewFakeNodeLiveness(4)
	newRegistry := func(id roachpb.NodeID) *jobs.Registry {
		const cancelInterval = time.Duration(math.MaxInt64)
		const adoptInterval = time.Nanosecond

		ac := log.AmbientContext{Tracer: tracing.NewTracer()}
		nodeID := &base.NodeIDContainer{}
		nodeID.Reset(id)
		r := jobs.MakeRegistry(
			ac, s.Stopper(), clock, db, s.InternalExecutor().(sqlutil.InternalExecutor),
			nodeID, s.ClusterSettings(), server.DefaultHistogramWindowInterval, jobs.FakePHS, "",
		)
		if err := r.Start(ctx, s.Stopper(), nodeLiveness, cancelInterval, adoptInterval); err != nil {
			t.Fatal(err)
		}
		return r
	}

	const jobCount = 3

	drainAdoptionLoop := func() {
		// Every turn of the registry's adoption loop will generate exactly one call
		// to nodeLiveness.GetLivenesses. Only after we've witnessed one call for
		// each job, plus one more call, can we be sure that all work has been
		// completed.
		//
		// Waiting for only jobCount calls to nodeLiveness.GetLivenesses is racy, as
		// we might perform our assertions just as the last turn of registry loop
		// observes our injected liveness failure, if any.
		for i := 0; i < jobCount+1; i++ {
			<-nodeLiveness.GetLivenessesCalledCh
		}
	}

	// jobMap maps node IDs to job IDs.
	jobMap := make(map[roachpb.NodeID]int64)
	hookCallCount := 0
	// resumeCounts maps jobs IDs to number of start/resumes.
	resumeCounts := make(map[int64]int)
	// done prevents jobs from finishing.
	done := make(chan struct{})
	// resumeCalled does a locked, blocking send when a job is started/resumed. A
	// receive on it will block until a job is running.
	resumeCalled := make(chan struct{})
	var lock syncutil.Mutex
	jobs.RegisterConstructor(jobspb.TypeBackup, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		lock.Lock()
		hookCallCount++
		lock.Unlock()
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context, _ chan<- tree.Datums) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case resumeCalled <- struct{}{}:
				case <-done:
				}
				lock.Lock()
				resumeCounts[*job.ID()]++
				lock.Unlock()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-done:
					return nil
				}
			},
		}
	})

	for i := 0; i < jobCount; i++ {
		nodeid := roachpb.NodeID(i + 1)
		rec := jobs.Record{
			Details:  jobspb.BackupDetails{},
			Progress: jobspb.BackupProgress{},
		}
		job, _, err := newRegistry(nodeid).CreateAndStartJob(ctx, nil, rec)
		if err != nil {
			t.Fatal(err)
		}
		// Wait until the job is running.
		<-resumeCalled
		lock.Lock()
		jobMap[nodeid] = *job.ID()
		lock.Unlock()
	}

	drainAdoptionLoop()
	if e, a := jobCount, hookCallCount; e != a {
		t.Fatalf("expected hookCallCount to be %d, but got %d", e, a)
	}

	drainAdoptionLoop()
	if e, a := jobCount, hookCallCount; e != a {
		t.Fatalf("expected hookCallCount to be %d, but got %d", e, a)
	}

	nodeLiveness.FakeSetExpiration(1, hlc.MinTimestamp)
	drainAdoptionLoop()
	<-resumeCalled
	testutils.SucceedsSoon(t, func() error {
		lock.Lock()
		defer lock.Unlock()
		if hookCallCount <= jobCount {
			return errors.Errorf("expected hookCallCount to be > %d, but got %d", jobCount, hookCallCount)
		}
		return nil
	})

	testutils.SucceedsSoon(t, func() error {
		lock.Lock()
		defer lock.Unlock()
		if e, a := 2, resumeCounts[jobMap[1]]; e != a {
			return errors.Errorf("expected resumeCount to be %d, but got %d", e, a)
		}
		return nil
	})

	// We want to verify that simply incrementing the epoch does not
	// result in the job being rescheduled.
	nodeLiveness.FakeIncrementEpoch(3)
	drainAdoptionLoop()
	select {
	case <-resumeCalled:
		t.Fatal("Incrementing an epoch should not reschedule a job")
	default:
	}

	// When we reset the liveness of the node, though, we should get
	// a reschedule.
	nodeLiveness.FakeSetExpiration(3, hlc.MinTimestamp)
	drainAdoptionLoop()
	<-resumeCalled
	close(done)

	testutils.SucceedsSoon(t, func() error {
		lock.Lock()
		defer lock.Unlock()
		if e, a := 1, resumeCounts[jobMap[3]]; e > a {
			return errors.Errorf("expected resumeCount to be > %d, but got %d", e, a)
		}
		if e, a := 1, resumeCounts[jobMap[2]]; e > a {
			return errors.Errorf("expected resumeCount to be > %d, but got %d", e, a)
		}
		count := 0
		for _, ct := range resumeCounts {
			count += ct
		}

		if e, a := 4, count; e > a {
			return errors.Errorf("expected total jobs to be > %d, but got %d", e, a)
		}
		return nil
	})
}

func TestRegistryResumeActiveLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond

	resumeCh := make(chan int64)
	defer jobs.ResetConstructors()()
	jobs.RegisterConstructor(jobspb.TypeBackup, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return jobs.FakeResumer{
			OnResume: func(ctx context.Context, _ chan<- tree.Datums) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case resumeCh <- *job.ID():
					return nil
				}
			},
		}
	})

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	payload, err := protoutil.Marshal(&jobspb.Payload{
		Lease:   &jobspb.Lease{NodeID: 1, Epoch: 1},
		Details: jobspb.WrapPayloadDetails(jobspb.BackupDetails{}),
	})
	if err != nil {
		t.Fatal(err)
	}

	progress, err := protoutil.Marshal(&jobspb.Progress{
		Details: jobspb.WrapProgressDetails(jobspb.BackupProgress{}),
	})
	if err != nil {
		t.Fatal(err)
	}

	var id int64
	sqlutils.MakeSQLRunner(sqlDB).QueryRow(t,
		`INSERT INTO system.jobs (status, payload, progress) VALUES ($1, $2, $3) RETURNING id`,
		jobs.StatusRunning, payload, progress).Scan(&id)

	if e, a := id, <-resumeCh; e != a {
		t.Fatalf("expected job %d to be resumed, but got %d", e, a)
	}
}
