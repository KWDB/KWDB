// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package stream

import (
	"context"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// streamResumer defines the Job of stream computing.
type streamResumer struct {
	job *jobs.Job
}

// Resume is called when the job starts.
func (s *streamResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	streamDetails := s.job.Details().(jobspb.StreamDetails)

	streamID := streamDetails.StreamMetadata.ID
	streamName := streamDetails.StreamMetadata.Name

	streamSpec := &execinfrapb.StreamReaderSpec{
		Metadata:       streamDetails.StreamMetadata,
		JobID:          *s.job.ID(),
		TargetColTypes: streamDetails.TargetTableColTypes,
	}

	originalPlan := phs.(*sql.GenericPlanner)

	var err error

	parameters, err := sqlutil.ParseStreamParameters(streamDetails.StreamMetadata.Parameters)
	if err != nil {
		return err
	}

	streamOpts, err := sqlutil.ParseStreamOpts(&parameters.Options)
	if err != nil {
		return err
	}

	opts := retry.Options{
		InitialBackoff: 5 * time.Second,
		Multiplier:     2,
		MaxBackoff:     30 * time.Second,
		MaxRetries:     streamOpts.MaxRetries,
	}

	// the initial-startup of stream must send a message to resultsCh
	finishedSetupFn := func() { resultsCh <- tree.Datums(nil) }

	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		err = nil

		if err = sql.MakeAndRunStreamPlan(ctx, s.job, originalPlan, streamSpec, finishedSetupFn); err != nil {
			msg := err.Error()
			if strings.Contains(msg, "stopped successfully") {
				log.Infof(ctx, "stream %q is stopped by user: %s", streamName, msg)
				break
			} else if strings.Contains(msg, "does not exist") {
				log.Info(ctx, msg)
				break
			} else if strings.Contains(msg, "memory budget exceeded") {
				err = errors.Errorf(
					"%s, Increase BUFFER_SIZE or reduce SYNC_TIME in stream options.",
					msg,
				)
				log.Infof(ctx, "stream %q is stopped by error: %s, %s", streamName, err.Error())
				break
			} else {
				log.Errorf(ctx, "stream %q is existed with error: %s, retried times %d", streamName, msg, r.CurrentAttempts())

				// the retrying loop can use 'nil' finishedSetupFn to run the stream physical plan
				finishedSetupFn = nil
				continue
			}
		}
	}

	updateErr := sql.UpdateStreamRunHistory(context.Background(), originalPlan, s.job, err, streamID)
	if updateErr != nil {
		return errors.Wrap(err, updateErr.Error())
	}

	return err
}

// OnFailOrCancel implements the Job interface.
func (s *streamResumer) OnFailOrCancel(_ context.Context, _ interface{}) error {
	return nil
}

var _ jobs.Resumer = &streamResumer{}

func init() {
	streamResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &streamResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeStream, streamResumerFn)
}
