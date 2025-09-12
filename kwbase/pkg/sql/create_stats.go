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
//
// This file contains methods for creating statistics at compile time.
// First we initialize the createStatsNode based on tree.CreateStats,
// createStatsNode is the implementation of the planNode interface.
// createStatsNode starts the task of collecting statistics in the form of a job,
// the startJob function starts a Job during Start, and the remainder of
// the CREATE STATISTICS planning and execution is performed within the job framework.
//
// Usage:
//   - The core functionalities provided by this file include the setup and execution
//     of statistics collection as background jobs in the database, helping maintain
//     optimal query planning and execution performance.
//
// Design:
//   - The primary structure used here is `createStatsNode`, which implements the `planNode`
//     interface necessary for executing SQL commands within the KaiwuDB system.
//   - `createStatsNode` serves to initiate and manage jobs for statistics collection using
//     the database's internal job framework, which allows for controlled execution,
//     failure recovery, and concurrency control.
//   - This file includes error handling specifically tailored to the concurrency and
//     integrity needs of statistics collection, including checks against redundant job
//     executions and ensuring exclusive access to statistical data during updates.

package sql

import (
	"context"
	"fmt"
	"sort"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// createStatsPostEvents controls the cluster setting for logging
// automatic table statistics collection to the event log.
var createStatsPostEvents = settings.RegisterPublicBoolSetting(
	"sql.stats.post_events.enabled",
	"if set, an event is logged for every CREATE STATISTICS job",
	false,
)

// SortHistogramBuckets represents sort histogram maximum bucket counts.
const SortHistogramBuckets = 200

func (p *planner) CreateStatistics(ctx context.Context, n *tree.CreateStats) (planNode, error) {
	return &createStatsNode{
		CreateStats: *n,
		p:           p,
	}, nil
}

// createStatsNode is a planNode implemented in terms of a function. The
// startJob function starts a Job during Start, and the remainder of the
// CREATE STATISTICS planning and execution is performed within the jobs
// framework.
type createStatsNode struct {
	tree.CreateStats
	p *planner

	run createStatsRun
}

// createStatsRun contains the run-time state of createStatsNode during local
// execution.
type createStatsRun struct {
	resultsCh chan tree.Datums
	errCh     chan error
}

func (n *createStatsNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("stats"))
	n.run.resultsCh = make(chan tree.Datums)
	n.run.errCh = make(chan error)
	go func() {
		err := n.startJob(params.ctx, n.run.resultsCh)
		select {
		case <-params.ctx.Done():
		case n.run.errCh <- err:
		}
		close(n.run.errCh)
		close(n.run.resultsCh)
	}()
	return nil
}

func (n *createStatsNode) Next(params runParams) (bool, error) {
	select {
	case <-params.ctx.Done():
		return false, params.ctx.Err()
	case err := <-n.run.errCh:
		return false, err
	case <-n.run.resultsCh:
		return true, nil
	}
}

func (*createStatsNode) Close(context.Context) {}
func (*createStatsNode) Values() tree.Datums   { return nil }

// startJob starts a CreateStats job to plan and execute statistics creation.
func (n *createStatsNode) startJob(ctx context.Context, resultsCh chan<- tree.Datums) error {
	record, err := n.makeJobRecord(ctx)
	if err != nil {
		return err
	}

	if n.Name == stats.AutoStatsName {
		// Don't start the job if there is already a CREATE STATISTICS job running.
		// (To handle race conditions we check this again after the job starts,
		// but this check is used to prevent creating a large number of jobs that
		// immediately fail).
		if err := checkRunningJobs(ctx, nil /* job */, n.p); err != nil {
			return err
		}
	} else {
		telemetry.Inc(sqltelemetry.CreateStatisticsUseCounter)
	}

	job, errCh, err := n.p.ExecCfg().JobRegistry.CreateAndStartJob(ctx, resultsCh, *record)
	if err != nil {
		return err
	}

	if err = <-errCh; err != nil {
		if errors.Is(err, stats.ConcurrentCreateStatsError) {
			// Delete the job so users don't see it and get confused by the error.
			const stmt = `DELETE FROM system.jobs WHERE id = $1`
			if _ /* cols */, delErr := n.p.ExecCfg().InternalExecutor.Exec(
				ctx, "delete-job", nil /* txn */, stmt, *job.ID(),
			); delErr != nil {
				log.Warningf(ctx, "failed to delete job %d: %v", *job.ID(), delErr)
			}
		}
	}
	return err
}

// checkStatsLegal is used to check whether it is legal to create statistics
func (n *createStatsNode) checkTable(tableDesc *ImmutableTableDescriptor) error {
	if tableDesc.IsVirtualTable() {
		return pgerror.Newf(
			pgcode.WrongObjectType, "cannot create statistics on virtual table \"%s\"", tableDesc.Name,
		)
	}

	if tableDesc.IsView() {
		return pgerror.Newf(
			pgcode.WrongObjectType, "cannot create statistics on view \"%s\"", tableDesc.Name,
		)
	}

	switch tableDesc.TableType {
	case tree.TemplateTable:
		return pgerror.Newf(
			pgcode.WrongObjectType, "cannot create statistics on template table \"%s\"", tableDesc.Name,
		)
	case tree.InstanceTable:
		return pgerror.Newf(
			pgcode.WrongObjectType, "cannot create statistics on instance table \"%s\"", tableDesc.Name,
		)
	default:
		return nil
	}
	return nil
}

// makeJobRecord creates a CreateStats job record which can be used to plan and
// execute statistics creation.
//
// This function serves as the key function for parsing the table and column information
// in CREATE STATISTICS, providing information for planning and execution of the CREATE STATISTICS.
//
// In addition, some necessary checks will be done, such as the type of the table,
// the type of the column, the user privileges, etc., and an error will be returned during the check
//
// Parameters:
// - ctx: The context in which this function operates, typically including things like timeout.
//
// Returns:
// - A job record of CreateStats, which contains details for each column stat to be created and the descriptor of the table.
// - An error if there is any issue in creating stats for the specified columns.
func (n *createStatsNode) makeJobRecord(ctx context.Context) (*jobs.Record, error) {
	var tableDesc *ImmutableTableDescriptor
	var fqTableName string
	var err error
	switch t := n.Table.(type) {
	// During the yacc, only table name and table ID types are supported
	case *tree.UnresolvedObjectName:
		tableDesc, err = n.p.ResolveExistingObjectEx(ctx, t, true /*required*/, ResolveRequireTableDesc)
		if err != nil {
			return nil, err
		}
		fqTableName = n.p.ResolvedName(t).FQString()

	case *tree.TableRef:
		tableID := sqlbase.ID(t.TableID)
		sID, err1 := sqlbase.GetTmplTableIDByInstID(ctx, n.p.txn, uint32(tableID))
		if err1 == nil {
			tableID = sID
		}
		flags := tree.ObjectLookupFlags{CommonLookupFlags: tree.CommonLookupFlags{
			AvoidCached: n.p.avoidCachedDescriptors,
		}}
		tableDesc, err = n.p.Tables().getTableVersionByID(ctx, n.p.txn, tableID, flags)
		if err != nil {
			return nil, err
		}
		// For the timing table, the current table is undergoing alter operation, so the statistics collection is skipped
		if tableDesc.TableType == tree.TimeseriesTable && tableDesc.State != sqlbase.TableDescriptor_PUBLIC {
			return nil, sqlbase.NewAlterTSTableError(tableDesc.Name)
		}

		fqTableName, err = n.p.getQualifiedTableName(ctx, &tableDesc.TableDescriptor)
		if err != nil {
			return nil, err
		}
	}

	isTsStats := tableDesc.TableType == tree.TimeseriesTable

	// Check whether it is legal to create statistics
	if err := n.checkTable(tableDesc); err != nil {
		return nil, err
	}

	if err := n.p.CheckPrivilege(ctx, tableDesc, privilege.SELECT); err != nil {
		return nil, err
	}

	// Identify which columns we should create statistics for.
	colStats, err := n.identifyColumnsForStats(tableDesc)
	if err != nil {
		return nil, err
	}

	// Evaluate the AS OF time, if any.
	var asOf *hlc.Timestamp
	if n.Options.AsOf.Expr != nil {
		if isTsStats {
			return nil, pgerror.Newf(pgcode.FeatureNotSupported, "create statistics do not support `as of system time` on time series tables")
		}
		asOfTs, err := n.p.EvalAsOfTimestamp(n.Options.AsOf)
		if err != nil {
			return nil, err
		}
		asOf = &asOfTs
	}

	// Create a job to run statistics creation.
	statement := tree.AsStringWithFQNames(n, n.p.EvalContext().Annotations)
	var description string
	if n.Name == stats.AutoStatsName {
		// Use a user-friendly description for automatic statistics.
		description = fmt.Sprintf("Table statistics refresh for %s", fqTableName)
	} else {
		// This must be a user query, so use the statement (for consistency with
		// other jobs triggered by statements).
		description = statement
		statement = ""
	}

	// Collect sorted histogram for entities in tag table.
	if isTsStats && n.Options.SortedHistogram {
		// Init sorted histogram
		if err := n.initSortedHistogram(ctx, colStats, tableDesc, fqTableName); err != nil {
			return nil, err
		}
	}

	return &jobs.Record{
		Description: description,
		Statement:   statement,
		Username:    n.p.User(),
		Details: jobspb.CreateStatsDetails{
			Name:            string(n.Name),
			FQTableName:     fqTableName,
			Table:           tableDesc.TableDescriptor,
			ColumnStats:     colStats,
			Statement:       n.String(),
			AsOf:            asOf,
			MaxFractionIdle: n.Options.Throttling,
			IsTsStats:       tableDesc.IsTSTable(),
			TimeZone:        n.p.EvalContext().SessionData.DataConversion.Location.String(),
		},
		Progress: jobspb.CreateStatsProgress{},
	}, nil
}

// maxNonIndexCols is the maximum number of non-index columns that we will use
// when choosing a default set of column statistics.
const maxNonIndexCols = 100

// createStatsDefaultColumns creates column statistics on a default set of
// column lists when no columns were specified by the caller.
//
// To determine a useful set of default column statistics, we rely on
// information provided by the schema. In particular, the presence of an index
// on a particular set of columns indicates that the workload likely contains
// queries that involve those columns (e.g., for filters), and it would be
// useful to have statistics on prefixes of those columns. For example, if a
// table abc contains indexes on (a ASC, b ASC) and (b ASC, c ASC), we will
// collect statistics on a, {a, b}, b, and {b, c}.
//
// In addition to the index columns, we collect stats on up to maxNonIndexCols
// other columns from the table. We only collect histograms for index columns,
// plus any other boolean columns (where the "histogram" is tiny).
//
// TODO(rytaft): This currently only generates one single-column stat per
// index. Add code to collect multi-column stats once they are supported.
func createStatsDefaultColumns(
	desc *ImmutableTableDescriptor,
) ([]jobspb.CreateStatsDetails_ColStat, error) {
	colStats := make([]jobspb.CreateStatsDetails_ColStat, 0, len(desc.Indexes)+1)

	var requestedCols util.FastIntSet
	var primaryTagCols util.FastIntSet
	var hasAllPTag bool

	// Add a column for the primary key.
	pkCol := desc.PrimaryIndex.ColumnIDs[0]
	colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
		ColumnIDs:    []sqlbase.ColumnID{pkCol},
		HasHistogram: true,
		ColumnTypes:  []int32{int32(sqlbase.ColumnType_TYPE_DATA)},
	})
	requestedCols.Add(int(pkCol))

	// Add columns for each secondary index.
	for i := range desc.Indexes {
		if desc.Indexes[i].Type == sqlbase.IndexDescriptor_INVERTED {
			// We don't yet support stats on inverted indexes.
			continue
		}
		idxCol := desc.Indexes[i].ColumnIDs[0]
		if !requestedCols.Contains(int(idxCol)) {
			colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
				ColumnIDs:    []sqlbase.ColumnID{idxCol},
				HasHistogram: true,
				ColumnTypes:  []int32{int32(sqlbase.ColumnType_TYPE_DATA)},
			})
			requestedCols.Add(int(idxCol))
		}
	}

	// Add columns for all primary tag.
	var columnIDs []sqlbase.ColumnID
	var columnTypes []int32
	if desc.TableType == tree.TimeseriesTable {
		for _, col := range desc.Columns {
			if col.IsPrimaryTagCol() && !primaryTagCols.Contains(int(col.ID)) {
				primaryTagCols.Add(int(col.ID))
				columnIDs = append(columnIDs, col.ID)
				columnTypes = append(columnTypes, int32(col.TsCol.ColumnType))
			}
		}
		hasAllPTag = true
	}

	// Add all remaining non-json columns in the table, up to maxNonIndexCols.
	nonIdxCols := 0
	for i := 0; i < len(desc.Columns) && nonIdxCols < maxNonIndexCols; i++ {
		col := &desc.Columns[i]
		if col.Type.Family() != types.JsonFamily && !requestedCols.Contains(int(col.ID)) &&
			!primaryTagCols.Contains(int(col.ID)) {
			colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
				ColumnIDs:    []sqlbase.ColumnID{col.ID},
				HasHistogram: col.Type.Family() == types.BoolFamily && desc.TableType == tree.RelationalTable,
				ColumnTypes:  []int32{int32(col.TsCol.ColumnType)},
			})
			nonIdxCols++
		}
	}
	if len(columnIDs) > 0 {
		// Append every statistics of PTag
		if len(columnIDs) > 1 {
			for i := range columnIDs {
				colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
					ColumnIDs:    []sqlbase.ColumnID{columnIDs[i]},
					HasHistogram: false,
					ColumnTypes:  []int32{columnTypes[i]},
				})
			}
		}
		colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
			ColumnIDs:    columnIDs,
			HasHistogram: false,
			ColumnTypes:  columnTypes,
			HasAllPTag:   hasAllPTag,
		})
	}

	return colStats, nil
}

// createTsStatsByColumnIDs creates column statistics on a list of column IDs.
// This function handles different column types such as normal columns, TAG columns,
// and PRIMARY TAG columns in the time series table and determines whether to collect histograms
// based on the column types.
//
// Parameters:
// - desc: Descriptor of the table whose columns are being evaluated.
// - columnIDList: List of column IDs on which statistics are to be created.
//
// Returns:
// - A slice of CreateStatsDetails_ColStat which includes details for statistics to be created for each column.
// - An error if any occurs during the process.
//
// Example:
// Consider a table with the following columns and their physical IDs:
//   - column1, column2, column3 (IDs 1, 2, 3) - normal data columns
//   - tag1, tag2, tag3 (IDs 4, 5, 6) - Tag columns with tag1 as the primary tag
//
// If we need to collect statistics for columns based on their types and roles:
//   - Collect statistics for [1, 2, 3, 5, 6] as normal columns
//   - Collect statistics for [6] specifically as a primary tag column
//
// This setup requires organizing the input columns into their respective types (DATA, TAG, PRIMARY TAG)
// and applying different statistical measurements (e.g., histograms for data columns, count for tag columns)
// depending on their importance and usage in the database schema.
func createTsStatsByColumnIDs(
	desc *ImmutableTableDescriptor, columnIDList tree.ColumnIDList,
) ([]jobspb.CreateStatsDetails_ColStat, error) {
	// Avoid collecting statistics for duplicate columns by sorting and deduplicating the list.
	sort.Slice(columnIDList, func(i, j int) bool { return columnIDList[i] < columnIDList[j] })
	var resColumnIDs tree.ColumnIDList
	for i, v := range columnIDList {
		if i == 0 || v != columnIDList[i-1] {
			resColumnIDs = append(resColumnIDs, v)
		}
	}
	colStats := make([]jobspb.CreateStatsDetails_ColStat, 0, len(resColumnIDs))
	var requestedCols util.FastIntSet
	var primaryTagCols util.FastIntSet
	var normalCols util.FastIntSet

	// Add a column for the primary key.
	pkCol := desc.PrimaryIndex.ColumnIDs[0]
	requestedCols.Add(int(pkCol))

	// Add columns for each secondary index.
	for i := range desc.Indexes {
		if desc.Indexes[i].Type == sqlbase.IndexDescriptor_INVERTED {
			// We don't yet support stats on inverted indexes.
			continue
		}
		idxCol := desc.Indexes[i].ColumnIDs[0]
		if !requestedCols.Contains(int(idxCol)) {
			requestedCols.Add(int(idxCol))
		}
	}

	// Add columns for all primary tag.
	for _, col := range desc.Columns {
		if col.IsPrimaryTagCol() {
			primaryTagCols.Add(int(col.ID))
		}
	}

	var columnIDs []sqlbase.ColumnID
	var columnTypes []int32
	var hasTag bool
	nonIdxCols := 0
	for _, colID := range resColumnIDs {
		columnDesc, err := desc.FindColumnByID(sqlbase.ColumnID(colID))
		if err != nil {
			return nil, err
		}
		columnsType := columnDesc.TsCol.ColumnType

		switch columnsType {
		case sqlbase.ColumnType_TYPE_DATA, sqlbase.ColumnType_TYPE_TAG:
			if columnsType == sqlbase.ColumnType_TYPE_TAG {
				hasTag = true
			}
			// Handle normal and tag columns specifically.
			if requestedCols.Contains(int(colID)) {
				colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
					ColumnIDs: []sqlbase.ColumnID{sqlbase.ColumnID(colID)},
					// Here, we add index column to collect the histogram
					HasHistogram: true,
					ColumnTypes:  []int32{int32(sqlbase.ColumnType_TYPE_DATA)},
				})
			} else {
				if nonIdxCols < maxNonIndexCols {
					// Add all remaining non-json columns in the table, up to maxNonIndexCols.
					if columnDesc.Type.Family() != types.JsonFamily {
						colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
							ColumnIDs:    []sqlbase.ColumnID{sqlbase.ColumnID(colID)},
							HasHistogram: false,
							ColumnTypes:  []int32{int32(columnDesc.TsCol.ColumnType)},
						})
						nonIdxCols++
					}
				}
			}
			normalCols.Add(int(colID))

		case sqlbase.ColumnType_TYPE_PTAG:
			hasTag = true
			// Handle primary tag columns specifically.
			if primaryTagCols.Contains(int(colID)) {
				columnIDs = append(columnIDs, sqlbase.ColumnID(colID))
				columnTypes = append(columnTypes, int32(columnDesc.TsCol.ColumnType))
			}

		default:
			return nil, pgerror.Newf(
				pgcode.DatatypeMismatch,
				"column %s: unsupported column type: %s when creating statistic",
				columnDesc.Name, columnsType.String())
		}
	}

	// Determine if all primary tag columns are included, or exactly one.
	var hasAllPTag bool
	// Append statistics based on the specified column IDs, considering primary tag conditions.
	// This block handles both the scenarios where all primary tags are included or just a single column.
	// It ensures that statistics are only created when all primary tags or exactly one are included.
	if len(columnIDs) > 0 {
		if len(columnIDs) == primaryTagCols.Len() {
			hasAllPTag = true
		} else if len(columnIDs) == 1 {
			hasAllPTag = false
		} else {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "statistics can be created only in all primary tag columns or in one of them")
		}
		// Append every statistics of PTag
		if len(columnIDs) > 1 {
			for i := range columnIDs {
				colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
					ColumnIDs:    []sqlbase.ColumnID{columnIDs[i]},
					HasHistogram: false,
					ColumnTypes:  []int32{columnTypes[i]},
				})
			}
		}
		colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
			ColumnIDs:    columnIDs,
			HasHistogram: false,
			ColumnTypes:  columnTypes,
			HasAllPTag:   hasAllPTag,
		})
	}

	// For statistics of tag, add virtual sketch to get row count
	if hasTag && !hasAllPTag {
		var primaryTagColIDs []sqlbase.ColumnID
		var primaryTagColTypes []int32
		primaryTagCols.ForEach(func(colID int) {
			primaryTagColIDs = append(primaryTagColIDs, sqlbase.ColumnID(colID))
			primaryTagColTypes = append(primaryTagColTypes, int32(sqlbase.ColumnType_TYPE_PTAG))
		})
		colStats = append(colStats, jobspb.CreateStatsDetails_ColStat{
			ColumnIDs:     primaryTagColIDs,
			HasHistogram:  false,
			ColumnTypes:   primaryTagColTypes,
			HasAllPTag:    true,
			VirtualSketch: true,
		})
	}

	return colStats, nil
}

// makePlanForExplainDistSQL is part of the distSQLExplainable interface.
func (n *createStatsNode) makePlanForExplainDistSQL(
	planCtx *PlanningCtx, distSQLPlanner *DistSQLPlanner,
) (PhysicalPlan, error) {
	// Create a job record but don't actually start the job.
	record, err := n.makeJobRecord(planCtx.ctx)
	if err != nil {
		return PhysicalPlan{}, err
	}
	job := n.p.ExecCfg().JobRegistry.NewJob(*record)

	return distSQLPlanner.createPlanForCreateStats(planCtx, job)
}

// createStatsResumer implements the jobs.Resumer interface for CreateStats
// jobs. A new instance is created for each job.
type createStatsResumer struct {
	job     *jobs.Job
	tableID sqlbase.ID
}

var _ jobs.Resumer = &createStatsResumer{}

// Resume is part of the jobs.Resumer interface and handles the resumption
// of paused CREATE STATISTICS jobs. It coordinates the transactional execution
// and error handling necessary to successfully complete the statistics creation.
//
// Parameters:
// - ctx: The context carrying deadlines and cancellation signals.
// - phs: The planner handle, encapsulating state needed to execute SQL plans.
// - resultsCh: A channel for sending the results back to the client, not used in this job type.
//
// Returns:
// - An error if the job cannot be resumed or completes with an error.
func (r *createStatsResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	p := phs.(*planner)
	details := r.job.Details().(jobspb.CreateStatsDetails)
	if details.Name == stats.AutoStatsName {
		// We want to make sure there is only one automatic CREATE STATISTICS job
		// running at a time.
		if err := checkRunningJobs(ctx, r.job, p); err != nil {
			return err
		}
	}

	r.tableID = details.Table.ID
	evalCtx := p.ExtendedEvalContext()

	ci := sqlbase.ColTypeInfoFromColTypes([]types.T{})
	rows := rowcontainer.NewRowContainer(evalCtx.Mon.MakeBoundAccount(), ci, 0)
	defer func() {
		if rows != nil {
			rows.Close(ctx)
		}
	}()

	dsp := p.DistSQLPlanner()
	// Execute the stats creation in a transaction.
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if details.AsOf != nil {
			p.semaCtx.AsOfTimestamp = details.AsOf
			p.extendedEvalCtx.SetTxnTimestamp(details.AsOf.GoTime())
			txn.SetFixedTimestamp(ctx, *details.AsOf)
		}

		// Prepare the planning context.
		planCtx := dsp.NewPlanningCtx(ctx, evalCtx, txn)
		planCtx.planner = p

		// Attempt to plan and run the statistics creation, handling any execution errors.
		if err := dsp.planAndRunCreateStats(
			ctx, evalCtx, planCtx, txn, r.job, NewRowResultWriter(rows),
		); err != nil {
			// Check if this was a context canceled error and restart if it was.
			if s, ok := status.FromError(errors.UnwrapAll(err)); ok {
				if s.Code() == codes.Canceled && s.Message() == context.Canceled.Error() {
					return jobs.NewRetryJobError("node failure")
				}
			}

			// If the job was canceled, any of the distsql processors could have been
			// the first to encounter the .Progress error. This error's string is sent
			// through distsql back here, so we can't examine the err type in this case
			// to see if it's a jobs.InvalidStatusError. Instead, attempt to update the
			// job progress to coerce out the correct error type. If the update succeeds
			// then return the original error, otherwise return this error instead so
			// it can be cleaned up at a higher level.
			if jobErr := r.job.FractionProgressed(
				ctx,
				func(ctx context.Context, _ jobspb.ProgressDetails) float32 {
					// The job failed so the progress value here doesn't really matter.
					return 0
				},
			); jobErr != nil {
				return jobErr
			}
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	// Record this statistics creation in the event log.
	if !createStatsPostEvents.Get(&evalCtx.Settings.SV) {
		return nil
	}
	// TODO(rytaft): This creates a new transaction for the CREATE STATISTICS
	// event. It must be different from the CREATE STATISTICS transaction,
	// because that transaction must be read-only. In the future we may want
	// to use the transaction that inserted the new stats into the
	// system.table_statistics table, but that would require calling
	// MakeEventLogger from the distsqlrun package.
	return evalCtx.ExecCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return MakeEventLogger(evalCtx.ExecCfg).InsertEventRecord(
			ctx,
			txn,
			EventLogCreateStatistics,
			int32(details.Table.ID),
			int32(evalCtx.NodeID),
			struct {
				TableName string
				Statement string
			}{details.FQTableName, details.Statement},
		)
	})
}

// checkRunningJobs checks whether there are any other CreateStats jobs in the
// pending, running, or paused status that started earlier than this one. If
// there are, checkRunningJobs returns an error. If job is nil, checkRunningJobs
// just checks if there are any pending, running, or paused CreateStats jobs.
func checkRunningJobs(ctx context.Context, job *jobs.Job, p *planner) error {
	var jobID int64
	if job != nil {
		jobID = *job.ID()
	}
	const stmt = `SELECT id, payload FROM system.jobs WHERE status IN ($1, $2, $3) ORDER BY created`

	rows, err := p.ExecCfg().InternalExecutor.Query(
		ctx,
		"get-jobs",
		nil, /* txn */
		stmt,
		jobs.StatusPending,
		jobs.StatusRunning,
		jobs.StatusPaused,
	)
	if err != nil {
		return err
	}

	for _, row := range rows {
		payload, err := jobs.UnmarshalPayload(row[1])
		if err != nil {
			return err
		}

		if payload.Type() == jobspb.TypeCreateStats || payload.Type() == jobspb.TypeAutoCreateStats {
			id := (*int64)(row[0].(*tree.DInt))
			if *id == jobID {
				break
			}

			// This is not the first CreateStats job running. This job should fail
			// so that the earlier job can succeed.
			return stats.ConcurrentCreateStatsError
		}
	}
	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r *createStatsResumer) OnFailOrCancel(context.Context, interface{}) error { return nil }

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &createStatsResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeCreateStats, createResumerFn)
	jobs.RegisterConstructor(jobspb.TypeAutoCreateStats, createResumerFn)
}

// identifyColumnsForStats determines which columns statistics should be
// created for based on the input node's properties.
// It handles different scenarios based on whether column IDs or names are specified.
// This function also handles special cases for time series tables.
//
// Parameters:
// - tableDesc: The descriptor of the table for which statistics are being created.
//
// Returns:
// - A slice of CreateStatsDetails_ColStat, which contains details for each column stat to be created.
// - An error if there is any issue in creating stats for the specified columns.
func (n *createStatsNode) identifyColumnsForStats(
	tableDesc *ImmutableTableDescriptor,
) ([]jobspb.CreateStatsDetails_ColStat, error) {
	var colStats []jobspb.CreateStatsDetails_ColStat
	var err error
	// If column IDs are specified, use them to create statistics.
	if len(n.ColumnIDs) > 0 {
		// create stats by ColumnIDs
		if colStats, err = createTsStatsByColumnIDs(tableDesc, n.ColumnIDs); err != nil {
			return nil, err
		}
	} else if len(n.ColumnNames) == 0 {
		// If no column names are specified, use default columns for statistics.
		if colStats, err = createStatsDefaultColumns(tableDesc); err != nil {
			return nil, err
		}
	} else {
		// Find active columns by their names and prepare to generate stats.
		columns, err := tableDesc.FindActiveColumnsByNames(n.ColumnNames)
		if err != nil {
			return nil, err
		}

		columnIDs := make([]sqlbase.ColumnID, len(columns))
		columnTypes := make([]int32, len(columns))
		for i := range columns {
			// JSON columns are not supported for statistics creation.
			if columns[i].Type.Family() == types.JsonFamily {
				return nil, unimplemented.NewWithIssuef(35844,
					"CREATE STATISTICS is not supported for JSON columns")
			}
			columnIDs[i] = columns[i].ID
			columnTypes[i] = int32(columns[i].TsCol.ColumnType)
		}

		// Stores IDs of columns marked as primary tags.
		var primaryTagCols util.FastIntSet
		// Flag to check if there are repeated column IDs.
		var hasAllPTags bool
		var isTag bool
		// Here, we need to check multi-column statistics.
		// Statistics for ts table can be created only for all primary columns or one of them.
		// Check the validity of the input column(s) by `FindActiveColumnsByNames` in the above code block
		if tableDesc.TableType == tree.TimeseriesTable {
			for _, col := range tableDesc.Columns {
				if col.IsPrimaryTagCol() {
					primaryTagCols.Add(int(col.ID))
				}
			}

			// Set to store the IDs of columns to be included in statistics.
			var neededColSet util.FastIntSet
			// Flag to check if there are repeated column IDs.
			var hasRepeatVal bool
			for _, c := range columnIDs {
				if !neededColSet.Contains(int(c)) {
					neededColSet.Add(int(c))
				} else {
					hasRepeatVal = true
					break
				}
			}

			// Multi-column statistics require special conditions for primary tags.
			if len(columnIDs) > 1 && columnTypes[0] != int32(sqlbase.ColumnType_TYPE_PTAG) {
				return nil, pgerror.Newf(pgcode.FeatureNotSupported, "multi-column statistics are not supported yet.")
			}

			if neededColSet.Equals(primaryTagCols) && !hasRepeatVal {
				hasAllPTags = true
			} else {
				if len(columnIDs) != 1 {
					return nil, pgerror.Newf(pgcode.FeatureNotSupported, "statistics can be created only in all primary tag columns or in one of them")
				}
				if columnTypes[0] == int32(sqlbase.ColumnType_TYPE_TAG) || columnTypes[0] == int32(sqlbase.ColumnType_TYPE_PTAG) {
					isTag = true
				}
			}
		}
		colStats = []jobspb.CreateStatsDetails_ColStat{
			{
				ColumnIDs:    columnIDs,
				HasHistogram: false,
				ColumnTypes:  columnTypes,
				HasAllPTag:   hasAllPTags,
			},
		}

		// Append virtual primary tag sketch
		if isTag {
			var primaryTagColIDs []sqlbase.ColumnID
			var primaryTagColTypes []int32
			primaryTagCols.ForEach(func(colID int) {
				primaryTagColIDs = append(primaryTagColIDs, sqlbase.ColumnID(colID))
				primaryTagColTypes = append(primaryTagColTypes, int32(sqlbase.ColumnType_TYPE_PTAG))
			})
			primaryTagcolStats := jobspb.CreateStatsDetails_ColStat{
				ColumnIDs:     primaryTagColIDs,
				HasHistogram:  false,
				ColumnTypes:   primaryTagColTypes,
				HasAllPTag:    true,
				VirtualSketch: true,
			}
			colStats = append(colStats, primaryTagcolStats)
		}

		if len(columnIDs) == 1 && columns[0].Type.Family() != types.ArrayFamily {
			// By default, create histograms on all explicitly requested column stats
			// with a single column. (We cannot create histograms on array columns
			// because we do not support key encoding arrays.)
			colStats[0].HasHistogram = true
		}
	}
	return colStats, nil
}

// initSortedHistogram is used to init sorted histogram.
func (n *createStatsNode) initSortedHistogram(
	ctx context.Context,
	colStats []jobspb.CreateStatsDetails_ColStat,
	tableDesc *ImmutableTableDescriptor,
	fqTableName string,
) error {
	for i, colStat := range colStats {
		if colStat.HasAllPTag {
			newColStat := &colStats[i]

			// Get ts col name
			var tsColName string
			for _, desc := range tableDesc.Columns {
				if desc.TsCol.ColumnType == sqlbase.ColumnType_TYPE_DATA {
					if desc.ID == 1 {
						tsColName = desc.Name
						break
					}
				}
			}

			// TODO(zh): Get timestamp range faster.
			getTsTimeStampInfo := fmt.Sprintf(`select min(%s),max(%s) from %s`, tsColName, tsColName, fqTableName)
			row, err := n.p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
				ctx,
				"read-max/min-timestamp",
				n.p.txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				getTsTimeStampInfo,
			)
			if err != nil {
				log.Error(ctx, errors.New("failed to get min and max tsTimeStamp for automatic stats"))
				return pgerror.Newf(pgcode.Warning, "failed to get min and max tsTimeStamp for automatic stats")
			}
			if row == nil || len(row) != 2 {
				log.Error(ctx, errors.New("failed to get min and max tsTimeStamp for automatic stats: expected 2 columns"))
				return pgerror.Newf(pgcode.Warning, "failed to get min and max tsTimeStamp for stats: expected 2 columns")
			}

			//fmt.Printf("Sorted histogram SQL:[%s]\n minTsTimestamp:[%d],maxTsTimestamp:[%d]\n", getTsTimeStampInfo, row[0].(*tree.DTimestampTZ).UnixMilli(), row[1].(*tree.DTimestampTZ).UnixMilli())
			var minTimeStamp int64
			var maxTimeStamp int64
			if minTimeStampVal, ok := row[0].(*tree.DTimestampTZ); ok {
				minTimeStamp = minTimeStampVal.UnixMilli()
			}
			if maxTimeStampVal, ok := row[1].(*tree.DTimestampTZ); ok {
				maxTimeStamp = maxTimeStampVal.UnixMilli()
			}
			if maxTimeStamp > minTimeStamp {
				newColStat.SortedHistogram = sqlbase.SortedHistogramInfo{
					GenerateSortedHistogram: true,
					FromTimeStamp:           minTimeStamp,
					ToTimeStamp:             maxTimeStamp,
					HistogramMaxBuckets:     SortHistogramBuckets,
				}
			}
		}
	}
	return nil
}
