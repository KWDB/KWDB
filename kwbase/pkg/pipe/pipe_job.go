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

package pipe

import (
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type pipeWatermarkResumer struct {
	job *jobs.Job
}

func (s *pipeWatermarkResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	p := phs.(*sql.GenericPlanner)
	pipeID := s.job.Details().(jobspb.PipeWatermarkDetails).PipeMetadata.ID
	pipeName := s.job.Details().(jobspb.PipeWatermarkDetails).PipeMetadata.Name

	dsp := p.DistSQLPlanner()
	// Prepare the planning context.
	evalCtx := p.ExtendedEvalContext()
	ci := sqlbase.ColTypeInfoFromColTypes([]types.T{})
	rows := rowcontainer.NewRowContainer(evalCtx.Mon.MakeBoundAccount(), ci, 0)
	defer func() {
		if rows != nil {
			rows.Close(ctx)
		}
	}()

	if err := dsp.PlanAndRunCreatePipe(
		ctx, evalCtx, p, s.job, sql.NewRowResultWriter(rows), resultsCh,
	); err != nil {
		// ctx maybe canceled
		if strings.Contains(err.Error(), "stopped successfully") {
			log.Infof(ctx, "successful to stop pipe %s.", pipeName)
		} else {
			log.Infof(ctx, "pipe %q failed with error %s", pipeName, err.Error())
		}

		updateErr := sql.UpdatePipeRunHistory(context.Background(), p, s.job, err, pipeID)
		if updateErr != nil {
			return errors.Wrap(err, updateErr.Error())
		}

		return err
	}

	return nil
}

func (s *pipeWatermarkResumer) OnFailOrCancel(_ context.Context, _ interface{}) error {
	return nil
}

var _ jobs.Resumer = &pipeWatermarkResumer{}

func init() {
	pipeWatermarkResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &pipeWatermarkResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypePipeWatermark, pipeWatermarkResumerFn)
}

// BuildPipeJobRecord builds a job record for creating or managing a pipe job
func BuildPipeJobRecord(
	params sql.RunParams,
	name tree.Name,
	opt *cdcpb.PipeOptions,
	pipeTableInfo []cdcpb.CDCTableInfo,
	tableIDList []uint64,
	pipeID uint64,
) (*jobs.Record, error) {
	metadata := &cdcpb.PipeMetadata{
		ID:            pipeID,
		Name:          string(name),
		Sink:          opt.Sink,
		MessageFormat: sqlconst.MessageFormatJSON,
		IgnoreHistory: opt.IgnoreHistory == sqlconst.OptOn,
		BufferSize:    uint64(opt.BufferSize * 1024 * 1024),
		Publish:       opt.Publish,
	}

	metadata.TableList = make([]*cdcpb.CDCTable, len(pipeTableInfo))
	for i := range pipeTableInfo {
		metadata.TableList[i] = &cdcpb.CDCTable{
			TableID:               tableIDList[i],
			Database:              pipeTableInfo[i].Database,
			Schema:                pipeTableInfo[i].Schema,
			Table:                 pipeTableInfo[i].Table,
			TsColumn:              pipeTableInfo[i].TsColumnName,
			TsColumnPrecision:     pipeTableInfo[i].TsColumnPrecision,
			OutputColumns:         pipeTableInfo[i].ColIDs,
			OutputColumnTypes:     pipeTableInfo[i].ColTypes,
			OutputColumnNames:     pipeTableInfo[i].ColNames,
			NeedNormalTag:         pipeTableInfo[i].NeedNormalTag,
			PrimaryTagColumnNames: pipeTableInfo[i].PrimaryTagCols,
			NormalTagColumnNames:  pipeTableInfo[i].NormalTagCols,
		}
	}

	if len(pipeTableInfo) == 1 {
		metadata.Filter = pipeTableInfo[0].Filter
		// extract and fill in the column ids, metrics and tag filter expressions.
		if err := sql.MarshalPipeFilter(params, metadata, &pipeTableInfo[0]); err != nil {
			return nil, err
		}
	}

	return &jobs.Record{
		Description: fmt.Sprintf(
			"computes and persists global low-water mark for pipe %v, id %d", name, pipeID),
		Statement: "",
		Username:  params.GetPlanner().User(),
		Details: jobspb.PipeWatermarkDetails{
			PipeMetadata: metadata,
		},
		Progress: jobspb.PipeWatermarkProgress{},
	}, nil
}
