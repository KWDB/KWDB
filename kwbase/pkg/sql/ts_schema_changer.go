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
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// In order to ensure the consistency of schema changes of time-series objects,
// we implement a two-stage process for schema changes of time-series objects.
// We use the node executing the job as the coordinator to complete the
// time-series engine schema change step by step. Is not available for time-series
// objects in DDL. When we finally complete the DDL safely, the time-series objects
// are available again.
// This is different from the Online-Schema-Change for relational tables.
// We may support Online-Schema-Change for time-series objects in the future

const (
	_ = iota
	createKwdbTsTable
	createKwdbInsTable
	dropKwdbTsTable
	dropKwdbInsTable
	dropKwdbTsDatabase
	alterKwdbAddTag
	alterKwdbDropTag
	alterKwdbAlterTagType
	alterKwdbSetTagValue
	alterKwdbAddColumn
	alterKwdbDropColumn
	alterKwdbAlterColumnType
	alterKwdbAlterPartitionInterval
	compress
	deleteExpiredData
	alterCompressInterval
	autonomy
	compressAll
	compressDB
	compressTable
	vacuum
	alterVacuumInterval
	createTagIndex
	dropTagIndex
	alterTagIndex
	count
)

// tsSchemaChangeResumer implements the jobs.Resumer interface for syncMetaCache
// jobs. A new instance is created for each job.
type tsSchemaChangeResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = &tsSchemaChangeResumer{}

// TSSchemaChangeWorker is used to change the schema on a ts table.
type TSSchemaChangeWorker struct {
	tableID        sqlbase.ID
	mutationID     sqlbase.MutationID
	nodeID         roachpb.NodeID
	db             *kv.DB
	leaseMgr       *LeaseManager
	p              *planner
	distSQLPlanner *DistSQLPlanner
	jobRegistry    *jobs.Registry
	// Keep a reference to the job related to this schema change
	// so that we don't need to read the job again while updating
	// the status of the job.
	job *jobs.Job
	// Caches updated by DistSQL.
	settings *cluster.Settings
	execCfg  *ExecutorConfig
	clock    *hlc.Clock

	healthyNodes []roachpb.NodeID
}

// Resume is part of the jobs.Resumer interface.
func (r *tsSchemaChangeResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	p := phs.(PlanHookState)
	details := r.job.Details().(jobspb.SyncMetaCacheDetails)
	sw := TSSchemaChangeWorker{
		nodeID:         p.ExecCfg().NodeID.Get(),
		db:             p.ExecCfg().DB,
		leaseMgr:       p.ExecCfg().LeaseManager,
		p:              p.(*planner),
		distSQLPlanner: p.DistSQLPlanner(),
		jobRegistry:    p.ExecCfg().JobRegistry,
		job:            r.job,
		settings:       p.ExecCfg().Settings,
		execCfg:        p.ExecCfg(),
		clock:          p.ExecCfg().Clock,
	}
	if details.MutationID != sqlbase.InvalidMutationID {
		sw.tableID = details.SNTable.ID
		sw.mutationID = details.MutationID
	}
	return sw.exec(ctx)
}

// exec executes the entire ts schema change in steps.
func (sw *TSSchemaChangeWorker) exec(ctx context.Context) error {
	d := sw.job.Details().(jobspb.SyncMetaCacheDetails)
	var syncErr error
	// we need to handle the outcome of async DDL action
	defer func() {
		// handle the intermediate state of metadata
		sw.handleResult(ctx, &d, syncErr)
	}()
	opts := retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		Multiplier:     1.5,
	}
	for r := retry.StartWithCtx(ctx, opts); r.Next(); {
		done := false
		// Note that r.Next always returns true on first run so exec will be
		// called at least once before there is a chance for this loop to exit.
		// make distributed exec plan and run
		// if failed and need to rollback WAL, set done = true; otherwise, return err directly.
		// if succeeded, commit WAL
		syncErr = sw.makeAndRunDistPlan(ctx, d)
		switch {
		case syncErr == nil:
			done = true
		case errors.Is(syncErr, sqlbase.ErrDescriptorNotFound):
			// If the table descriptor for the ID can't be found, we assume that
			// another job to drop the table got to it first, and consider this job
			// finished.
			log.Infof(
				ctx,
				"descriptor %d not found for ts schema change processing mutation %d;"+
					"assuming it was dropped, and exiting",
				sw.tableID, sw.mutationID,
			)
			done = true
		case errors.IsAny(syncErr, errSchemaChangeNotFirstInLine):
			// Check if the error is on a whitelist of errors we should retry on,
			// including the schema change not having the first mutation in line.
			log.Warningf(ctx, "error while running ts schema change, retrying: %v", syncErr)
		case strings.Contains(syncErr.Error(), "failed to connect to n"):
			done = true
		default:
			// All other errors lead to a failed job.
			done = true
		}
		if done {
			// Commit or Rollback WAL
			break
		}
	}
	syncErr = sw.completeTsTxn(ctx, syncErr)
	if d.Type == createKwdbTsTable {
		if sw.p.extendedEvalCtx.ExecCfg.TestingKnobs.RunCreateTableFailedAndRollback != nil {
			syncErr = sw.p.extendedEvalCtx.ExecCfg.TestingKnobs.RunCreateTableFailedAndRollback()
		}
	}

	return syncErr
}

// completeTsTxn is used to rollback/commit WAL
func (sw *TSSchemaChangeWorker) completeTsTxn(ctx context.Context, syncErr error) error {
	d := sw.job.Details().(jobspb.SyncMetaCacheDetails)
	opType := getDDLOpType(d.Type)
	retryOpts := retry.Options{
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2,
		MaxRetries:     3,
	}
	var event txnEvent
	event = txnCommit
	if syncErr != nil {
		event = txnRollback
		log.Infof(ctx, "%s start rollback, jobID: %d, syncErr: %s", opType, sw.job.ID(), syncErr.Error())
	}
	var err error
	for opt := retry.Start(retryOpts); opt.Next(); {
		err = sw.sendTsTxn(ctx, d, event)
		if err == nil {
			break
		}
		log.Infof(ctx, "%s rollback failed, reason: %s, jobID: %d", opType, err.Error(), sw.job.ID())
	}
	if err != nil && syncErr == nil {
		syncErr = err
	}
	return syncErr
}

// makeKObjectTableForTs make KObjectTable for AE
// Inparam: SyncMetaCacheDetails
// OutParam: KObjectTable
func makeKObjectTableForTs(d jobspb.SyncMetaCacheDetails) sqlbase.CreateTsTable {
	var kColDescs []sqlbase.KWDBKTSColumn
	var KColumnsID []uint32

	for _, col := range d.SNTable.Columns {
		colName := tree.Name(col.Name)
		kColDesc := sqlbase.KWDBKTSColumn{
			ColumnId:           uint32(col.ID),
			Name:               colName.String(),
			Nullable:           col.IsNullable(),
			StorageType:        col.TsCol.StorageType,
			StorageLen:         col.TsCol.StorageLen,
			ColOffset:          col.TsCol.ColOffset,
			VariableLengthType: col.TsCol.VariableLengthType,
			ColType:            col.TsCol.ColumnType,
		}
		kColDescs = append(kColDescs, kColDesc)
		KColumnsID = append(KColumnsID, uint32(col.ID))
	}
	tableName := tree.Name(d.SNTable.Name)
	kObjectTable := sqlbase.KWDBTsTable{
		TsTableId:         uint64(d.SNTable.ID),
		DatabaseId:        uint32(d.SNTable.ParentID),
		LifeTime:          d.SNTable.TsTable.Lifetime,
		ActiveTime:        d.SNTable.TsTable.ActiveTime,
		KColumnsId:        KColumnsID,
		RowSize:           d.SNTable.TsTable.RowSize,
		BitmapOffset:      d.SNTable.TsTable.BitmapOffset,
		TableName:         tableName.String(),
		Sde:               d.SNTable.TsTable.Sde,
		PartitionInterval: d.SNTable.TsTable.PartitionInterval,
		TsVersion:         uint32(d.SNTable.TsTable.GetTsVersion()),
	}

	nTagIndexInfos := make([]sqlbase.NTagIndexInfo, len(d.SNTable.Indexes))
	for i, idx := range d.SNTable.Indexes {
		columnIDs := make([]uint32, len(idx.ColumnIDs))
		for j, colID := range idx.ColumnIDs {
			columnIDs[j] = uint32(colID)
		}
		nTagIndexInfos[i] = sqlbase.NTagIndexInfo{
			IndexId: uint32(idx.ID),
			ColIds:  columnIDs,
		}
	}

	return sqlbase.CreateTsTable{
		TsTable:   kObjectTable,
		KColumn:   kColDescs,
		IndexInfo: nTagIndexInfos,
	}
}

// handleResult processes metadata based on the results of distributed execution.
// If the execution is successful, it modifies the metadata and changes the intermediate state to public.
// If the execution fails, it rolls back the metadata and changes the intermediate state to public.
func (sw *TSSchemaChangeWorker) handleResult(
	ctx context.Context, d *jobspb.SyncMetaCacheDetails, syncErr error,
) {
	opType := getDDLOpType(d.Type)
	log.Infof(ctx, "%s initial ddl job finished, jobID: %d", opType, sw.job.ID())
	retryOpts := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}
	p := sw.p
	log.Infof(ctx, "%s metadata retry job started, jobID: %d", opType, sw.job.ID())
	// keep trying to process metadata until success
	for opt := retry.Start(retryOpts); opt.Next(); {
		var updateErr error
		switch d.Type {
		case createKwdbTsTable:
			if syncErr != nil {
				log.Infof(ctx, "TS SchemaChange job(create table) failed, reason: %s", syncErr.Error())
			}
			//updateErr = p.handleCreateTSTable(ctx, d.SNTable, syncErr)
		//case dropKwdbTsTable:
		//	updateErr = p.handleDropTsTable(ctx, d.SNTable, sw.jobRegistry, syncErr)
		//case dropKwdbTsDatabase:
		//	updateErr = p.handleDropTsDatabase(ctx, d.Database, d.DropDBInfo, sw.jobRegistry, syncErr)
		case createKwdbInsTable:
			// prepare instance table metadata which is being created
			insTable := sqlbase.InstNameSpace{
				InstName:    d.CTable.CTable.Name,
				InstTableID: sqlbase.ID(d.CTable.CTable.Id),
				TmplTableID: d.SNTable.ID,
				DBName:      d.Database.Name,
				ChildDesc: sqlbase.ChildDesc{
					STableName: d.SNTable.Name,
					State:      sqlbase.ChildDesc_PUBLIC,
				},
			}
			updateErr = p.handleCreateInsTable(ctx, insTable, syncErr)
		//case dropKwdbInsTable:
		//	updateErr = p.handleDropInsTable(
		//		ctx,
		//		d.DropMEInfo[0].DatabaseName,
		//		d.DropMEInfo[0].TableName,
		//		d.DropMEInfo[0].TableID,
		//		syncErr,
		//	)
		case alterKwdbAddColumn, alterKwdbDropColumn, alterKwdbAlterColumnType,
			alterKwdbAddTag, alterKwdbDropTag, alterKwdbAlterTagType, createTagIndex, dropTagIndex:
			updateErr = sw.handleMutationForTSTable(ctx, d, syncErr)
		case alterKwdbAlterPartitionInterval:
			updateErr = p.handleAlterPartitionInterval(
				ctx,
				d.SNTable.ID,
				d.SNTable.TsTable.PartitionInterval,
				d.SNTable.TsTable.PartitionIntervalInput,
				syncErr,
			)
		case alterKwdbSetTagValue:
			// prepare instance table metadata being modified
			insTable := sqlbase.InstNameSpace{
				InstName:    d.SetTag.TableName,
				InstTableID: sqlbase.ID(d.SetTag.TableId),
				TmplTableID: d.SNTable.ID,
				DBName:      d.SetTag.DbName,
				ChildDesc: sqlbase.ChildDesc{
					STableName: d.SNTable.Name,
					State:      sqlbase.ChildDesc_PUBLIC,
				},
			}
			updateErr = p.handleSetTagValue(ctx, d.SNTable, insTable, syncErr)
		case compress, deleteExpiredData, autonomy, vacuum, count:
			updateErr = p.handleSchedule(ctx, sw.job, syncErr)
		default:
		}
		if updateErr == nil {
			break
		} else {
			log.Infof(ctx, "handle metadata failed: %s", updateErr.Error())
			if strings.Contains(updateErr.Error(), context.Canceled.Error()) {
				ctx = context.Background()
			}
		}
	}
	if sw.tableID != 0 {
		if _, err := sw.p.ExecCfg().LeaseManager.WaitForOneVersion(ctx, sw.tableID, base.DefaultRetryOptions()); err != nil {
			log.Warningf(ctx, "ts schema change on table (%d) wait one version error: %s", sw.tableID, err.Error())
		}
	}
	log.Infof(ctx, "%s metadata retry job finished, jobID: %d", opType, sw.job.ID())
}

// handleSchedule changes schedule status when job done.
func (p *planner) handleSchedule(ctx context.Context, job *jobs.Job, syncErr error) error {
	jobStatus := jobs.StatusSucceeded
	if syncErr != nil {
		jobStatus = jobs.StatusFailed
	}
	env := jobSchedulerEnv(p.RunParams(ctx))
	if job.CreatedBy() == nil {
		log.Errorf(ctx, "can not find create_by_id for job %d", *job.ID())
		return nil
	}
	err := jobs.NotifyJobTermination(ctx, env, *job.ID(), jobStatus, job.Details(), job.CreatedBy().ID, p.execCfg.InternalExecutor, p.Txn())
	if err != nil {
		return err
	}
	return nil
}

// handleSetTagValue restore instance table metadata is available,
// and the time-series engine completes setting the tag value.
func (p *planner) handleSetTagValue(
	ctx context.Context, desc sqlbase.TableDescriptor, insTable sqlbase.InstNameSpace, syncErr error,
) error {
	updateErr := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		p.txn = txn
		// rewrite instance table
		if err := writeInstTableMeta(ctx, p.Txn(), []sqlbase.InstNameSpace{insTable}, true); err != nil {
			return err
		}
		if syncErr == nil {
			tableDesc := sqlbase.NewMutableExistingTableDescriptor(desc)
			tableDesc.TableType = tree.TemplateTable
			if err := p.writeTableDesc(ctx, tableDesc); err != nil {
				return err
			}
		}
		return nil
	})
	return updateErr
}

// handleAlterPartitionInterval restore time-series table metadata is available,
// and the time-series engine completes setting the PartitionInterval.
func (p *planner) handleAlterPartitionInterval(
	ctx context.Context, tableID sqlbase.ID, partitionInterval uint64, input *string, syncErr error,
) error {
	_, updateDescErr := p.ExecCfg().LeaseManager.Publish(
		ctx,
		tableID,
		func(tableDesc *sqlbase.MutableTableDescriptor) error {
			tableDesc.State = sqlbase.TableDescriptor_PUBLIC
			if syncErr == nil {
				tableDesc.TsTable.PartitionInterval = partitionInterval
				tableDesc.TsTable.PartitionIntervalInput = input
			}
			return nil
		},
		func(txn *kv.Txn) error { return nil })
	return updateDescErr
}

// handleDropTsDatabase processes metadata based on the result of AE execution.
// If AE drops all the tables in this database success, delete corresponding metadata.
// If AE fails, rollback the metadata.
func (p *planner) handleDropTsDatabase(
	ctx context.Context,
	dbDesc sqlbase.DatabaseDescriptor,
	tables []sqlbase.TableDescriptor,
	jr *jobs.Registry,
	syncErr error,
) error {
	var sj *jobs.StartableJob
	updateDescErr := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		p.txn = txn
		if err := getDescriptorByID(ctx, txn, dbDesc.ID, &dbDesc); err != nil {
			if strings.Contains(err.Error(), "is not a database") {
				return nil
			}
			return err
		}
		if syncErr != nil {
			idKey := sqlbase.MakeDatabaseNameKey(ctx, p.ExecCfg().Settings, dbDesc.Name)
			// If the database fails to be dropped, roll it back
			if err := p.Txn().Put(ctx, idKey.Key(), dbDesc.ID); err != nil {
				return err
			}
			for _, desc := range tables {
				tableDesc := sqlbase.NewMutableExistingTableDescriptor(desc)
				tableDesc.State = sqlbase.TableDescriptor_PUBLIC
				if err := p.writeTableDesc(ctx, tableDesc); err != nil {
					return err
				}
			}
			return nil
		}
		if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionSchemaChangeJob) {
			if ctx.Value(migrationSchemaChangeRequiredHint{}) == nil {
				return errSchemaChangeDisallowedInMixedState
			}
		}
		// TODO (lucy): This should probably be deleting the queued jobs for all the
		// tables being dropped, so that we don't have duplicate schema changers.
		droppedDetails := make([]jobspb.DroppedTableDetails, 0, len(tables))
		descriptorIDs := make([]sqlbase.ID, 0, len(tables))

		for _, desc := range tables {
			tableDesc := sqlbase.NewMutableExistingTableDescriptor(desc)
			droppedDetails = append(droppedDetails, jobspb.DroppedTableDetails{
				Name: desc.Name,
				ID:   desc.ID,
			})
			descriptorIDs = append(descriptorIDs, desc.ID)
			jobDesc := "handle drop table " + desc.Name
			if _, err := p.dropTableImpl(ctx, tableDesc, false, jobDesc); err != nil {
				return err
			}
		}
		jobDesc := "handle drop database " + dbDesc.Name
		jobRecord := jobs.Record{
			Description:   jobDesc,
			Username:      p.User(),
			DescriptorIDs: descriptorIDs,
			Details: jobspb.SchemaChangeDetails{
				DroppedTables:     droppedDetails,
				DroppedDatabaseID: dbDesc.ID,
				FormatVersion:     jobspb.JobResumerFormatVersion,
			},
			Progress: jobspb.SchemaChangeProgress{},
		}
		descKey := sqlbase.MakeDescMetadataKey(dbDesc.ID)

		b := &kv.Batch{}
		if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
			log.VEventf(ctx, 2, "Del %s", descKey)
		}
		b.Del(descKey)

		schemaToDelete := sqlbase.ResolvedSchema{
			ID:   keys.PublicSchemaID,
			Kind: sqlbase.SchemaPublic,
			Name: tree.PublicSchema,
		}
		if err := p.dropSchemaImpl(ctx, b, dbDesc.ID, &schemaToDelete); err != nil {
			return err
		}

		err := sqlbase.RemoveDatabaseNamespaceEntry(
			ctx, p.txn, dbDesc.Name, p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		)
		if err != nil {
			return err
		}
		// No job was created because no tables were dropped, so zone config can be
		// immediately removed.
		if len(tables) == 0 {
			zoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(dbDesc.ID))
			if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
				log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
			}
			// Delete the zone config entry for this database.
			b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
		}
		p.Tables().addUncommittedDatabase(dbDesc.Name, dbDesc.ID, dbDropped)

		sj, err = jr.CreateStartableJobWithTxn(ctx, jobRecord, p.txn, nil)
		if err != nil {
			return err
		}
		return p.txn.Run(ctx, b)
	})
	if updateDescErr == nil && sj != nil {
		if err := sj.Run(ctx); err != nil {
			if cleanupErr := sj.CleanupOnRollback(ctx); cleanupErr != nil {
				return cleanupErr
			}
			return err
		}
	}
	return updateDescErr

}

// handleDropTsTable handle result for drop template table and time series table.
// if drop table success, drop table descriptor.
// else if drop table failed, change table state to public.
func (p *planner) handleDropTsTable(
	ctx context.Context, desc sqlbase.TableDescriptor, jr *jobs.Registry, syncErr error,
) error {
	var sj *jobs.StartableJob
	updateDescErr := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		p.txn = txn
		tableDesc := sqlbase.NewMutableExistingTableDescriptor(desc)
		if syncErr != nil {
			tableDesc.State = sqlbase.TableDescriptor_PUBLIC
			return p.writeTableDesc(ctx, tableDesc)
		}
		// execute without error, then delete corresponding metadata
		jobDesc := "handle drop table " + tableDesc.Name
		if _, err := p.dropTableImpl(ctx, tableDesc, false, jobDesc); err != nil {
			return err
		}
		// Queue a new job.
		var spanList []jobspb.ResumeSpanList
		span := tableDesc.PrimaryIndexSpan()
		for i := len(tableDesc.Mutations) + len(spanList); i < len(tableDesc.Mutations); i++ {
			spanList = append(spanList,
				jobspb.ResumeSpanList{
					ResumeSpans: []roachpb.Span{span},
				},
			)
		}
		jobRecord := jobs.Record{
			Description:   jobDesc,
			Username:      p.User(),
			DescriptorIDs: sqlbase.IDs{tableDesc.GetID()},
			Details: jobspb.SchemaChangeDetails{
				TableID:        tableDesc.ID,
				MutationID:     sqlbase.InvalidMutationID,
				ResumeSpanList: spanList,
				FormatVersion:  jobspb.JobResumerFormatVersion,
			},
			Progress: jobspb.SchemaChangeProgress{},
		}
		var err error
		sj, err = jr.CreateStartableJobWithTxn(ctx, jobRecord, p.txn, nil)
		if err != nil {
			return err
		}
		return nil
	})
	if updateDescErr == nil && sj != nil {
		if err := sj.Run(ctx); err != nil {
			if cleanupErr := sj.CleanupOnRollback(ctx); cleanupErr != nil {
				return cleanupErr
			}
			return err
		}
	}
	return updateDescErr
}

// handleDropInsTable handle result for drop instance table.
// if drop table success, delete instance table from system table.
// else drop table failed, change table state to public.
func (p *planner) handleDropInsTable(
	ctx context.Context, dbName string, tableName string, tableID uint32, syncErr error,
) error {
	updateErr := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		p.txn = txn
		if syncErr != nil {
			insTable, found, err := sqlbase.ResolveInstanceName(ctx, p.txn, dbName, tableName)
			if err != nil {
				return err
			} else if !found {
				return sqlbase.NewUndefinedTableError(tableName)
			}
			insTable.State = sqlbase.ChildDesc_PUBLIC
			// rewrite instance table
			if err := writeInstTableMeta(ctx, p.Txn(), []sqlbase.InstNameSpace{insTable}, true); err != nil {
				return err
			}
		} else {
			// remove the relation between template and instance table
			if err := DropInstanceTable(ctx, p.txn, sqlbase.ID(tableID), dbName, tableName); err != nil {
				return err
			}
			// clean up cache to avoid looking up instance table by using cache,
			// which may return instance table already dropped
			p.execCfg.QueryCache.Clear()
		}
		return nil
	})
	return updateErr
}

// handleCreateInsTable handle result for create instance table.
// if create table success, change table state to public.
// else if create table failed, delete table from system table.
func (p *planner) handleCreateInsTable(
	ctx context.Context, insTable sqlbase.InstNameSpace, syncErr error,
) error {
	updateErr := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		p.txn = txn
		if syncErr != nil {
			// delete instance table
			if err := DropInstanceTable(
				ctx, p.txn, insTable.InstTableID, insTable.DBName, insTable.InstName,
			); err != nil {
				return err
			}
		} else {
			// change table state to public.
			insTable.State = sqlbase.ChildDesc_PUBLIC
			if err := writeInstTableMeta(ctx, p.Txn(), []sqlbase.InstNameSpace{insTable}, true); err != nil {
				return err
			}
		}
		return nil
	})
	return updateErr
}

// handleCreateTSTable processes metadata based on the result of AE execution.
// If create table success, change tableState to PUBLIC.
// If create table fails, rollback the metadata.
func (p *planner) handleCreateTSTable(
	ctx context.Context, tab sqlbase.TableDescriptor, syncErr error,
) error {
	updateErr := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		p.txn = txn
		tableDesc := sqlbase.NewMutableExistingTableDescriptor(tab)
		// if create table success, chang table state to public.
		// else if create table failed, delete table descriptor.
		if syncErr != nil {
			b := p.txn.NewBatch()
			// remove table from namespace
			if txnErr := sqlbase.RemoveObjectNamespaceEntry(
				ctx, p.txn, tableDesc.GetParentID(), keys.PublicSchemaID, tableDesc.Name, false,
			); txnErr != nil {
				return txnErr
			}
			// remove descriptor
			descKey := sqlbase.MakeDescMetadataKey(tableDesc.ID)
			b.Del(descKey)
			if txnErr := p.txn.Run(ctx, b); txnErr != nil {
				return txnErr
			}
		} else {
			tableDesc.State = sqlbase.TableDescriptor_PUBLIC
			if txnErr := p.writeTableDesc(ctx, tableDesc); txnErr != nil {
				return txnErr
			}
		}
		return nil
	})
	return updateErr
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r *tsSchemaChangeResumer) OnFailOrCancel(context.Context, interface{}) error { return nil }

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &tsSchemaChangeResumer{job: job}
	}

	jobs.RegisterConstructor(jobspb.TypeSyncMetaCache, createResumerFn)
}

// makeAndRunDistPlan first check healthy nodes, then builds DISTSQL plan and executes by CGO function
func (sw *TSSchemaChangeWorker) makeAndRunDistPlan(
	ctx context.Context, d jobspb.SyncMetaCacheDetails,
) error {
	var newPlanNode planNode
	//var nodeID []roachpb.NodeID
	opType := getDDLOpType(d.Type)
	switch d.Type {
	//case dropKwdbTsTable, dropKwdbTsDatabase:
	//	switch d.Type {
	//	case dropKwdbTsTable:
	//		log.Infof(ctx, "%s job start, name: %s, id: %d, jobID: %d",
	//			opType, d.SNTable.Name, d.SNTable.ID, sw.job.ID())
	//	case dropKwdbTsDatabase:
	//		log.Infof(ctx, "%s job start, name: %s, id: %d, jobID: %d",
	//			opType, d.Database.Name, d.Database.ID, sw.job.ID())
	//	}
	//	nodeList, err := api.GetHealthyNodeIDs(ctx)
	//	if err != nil {
	//		return err
	//	}
	//	for _, td := range d.DropMEInfo {
	//		log.Infof(ctx, "%s, jobID: %d, waitForOneVersion start", opType, sw.job.ID())
	//		// Wait for DML execution to complete on this table
	//		if _, err := sw.p.ExecCfg().LeaseManager.WaitForOneVersion(
	//			ctx,
	//			sqlbase.ID(td.TableID),
	//			base.DefaultRetryOptions(),
	//		); err != nil {
	//			return err
	//		}
	//		log.Infof(ctx, "%s, jobID: %d, waitForOneVersion finished", opType, sw.job.ID())
	//		log.Infof(ctx, "%s, jobID: %d, checkReplica start", opType, sw.job.ID())
	//		if err := sw.checkReplica(ctx, sqlbase.ID(td.TableID)); err != nil {
	//			return err
	//		}
	//		log.Infof(ctx, "%s, jobID: %d, checkReplica finished", opType, sw.job.ID())
	//	}
	//	newPlanNode = &tsDDLNode{d: d, nodeID: nodeList}
	case alterKwdbAddColumn, alterKwdbDropColumn, alterKwdbAlterColumnType,
		alterKwdbAddTag, alterKwdbDropTag, alterKwdbAlterTagType, createTagIndex, dropTagIndex:
		log.Infof(ctx, "%s job start, name: %s, id: %d, column/tag name: %s, jobID: %d",
			opType, d.SNTable.Name, d.SNTable.ID, d.AlterTag.Name, sw.job.ID())

		//needCheckReplica := d.Type == alterKwdbAddTag || d.Type == alterKwdbDropTag ||
		//	d.Type == alterKwdbAlterTagType

		tableDesc, notFirst, err := sw.notFirstInLine(ctx)
		if err != nil {
			return err
		}
		if notFirst {
			log.Infof(ctx,
				"schema change on ts table %q (v%d): another change is still in progress",
				tableDesc.Name, tableDesc.Version,
			)
			return errSchemaChangeNotFirstInLine
		}
		d.SNTable = *tableDesc
		// Get all healthy nodes.
		var nodeList []roachpb.NodeID
		var retErr error
		nodeList, retErr = api.GetTableNodeIDs(ctx, sw.db, uint32(d.SNTable.GetID()))
		if retErr != nil {
			return retErr
		}
		log.Infof(ctx, "alter table on nodes list: %v", nodeList)
		sw.healthyNodes = nodeList
		if _, err := sw.p.ExecCfg().LeaseManager.WaitForOneVersion(ctx, sw.tableID, base.DefaultRetryOptions()); err != nil {
			return err
		}
		//if needCheckReplica {
		//	if err := sw.checkReplica(ctx, d.SNTable.ID); err != nil {
		//		return err
		//	}
		//}
		txnID := strconv.AppendInt([]byte{}, *sw.job.ID(), 10)
		miniTxn := tsTxn{txnID: txnID, txnEvent: txnStart}
		newPlanNode = &tsDDLNode{d: d, nodeID: nodeList, tsTxn: miniTxn}
	case alterKwdbAlterPartitionInterval:
		log.Infof(ctx, "%s job start, name: %s, id: %d, jobID: %d", opType, d.SNTable.Name, d.SNTable.ID, sw.job.ID())
		// Get all healthy nodes.
		var nodeList []roachpb.NodeID
		var retErr error
		nodeList, retErr = api.GetTableNodeIDs(ctx, sw.db, uint32(d.SNTable.GetID()))
		if retErr != nil {
			return retErr
		}
		log.Infof(ctx, "%s, jobID: %d, waitForOneVersion start", opType, sw.job.ID())
		if _, err := sw.p.ExecCfg().LeaseManager.WaitForOneVersion(
			ctx,
			d.SNTable.ID,
			base.DefaultRetryOptions(),
		); err != nil {
			return err
		}
		log.Infof(ctx, "%s, jobID: %d, waitForOneVersion finished", opType, sw.job.ID())
		log.Infof(ctx, "%s, jobID: %d, checkReplica start", opType, sw.job.ID())
		if err := sw.checkReplica(ctx, d.SNTable.ID); err != nil {
			return err
		}
		log.Infof(ctx, "%s, jobID: %d, checkReplica finished", opType, sw.job.ID())
		txnID := strconv.AppendInt([]byte{}, *sw.job.ID(), 10)
		miniTxn := tsTxn{txnID: txnID, txnEvent: txnStart}
		newPlanNode = &tsDDLNode{d: d, nodeID: nodeList, tsTxn: miniTxn}
	case createKwdbTsTable:
		log.Infof(ctx, "%s job start, name: %s, id: %d, jobID: %d",
			opType, d.SNTable.Name, d.SNTable.ID, sw.job.ID())
		var nodeList []roachpb.NodeID
		var retErr error
		nodeList, retErr = api.GetTableNodeIDs(ctx, sw.db, uint32(d.SNTable.GetID()))
		if retErr != nil {
			return retErr
		}
		log.Infof(ctx, "create table on nodes list: %v", nodeList)
		newPlanNode = &tsDDLNode{d: d, nodeID: nodeList}
	//case dropKwdbInsTable:
	//	log.Infof(ctx, "%s job start, name: %s, id: %d, jobID: %d",
	//		opType, d.SNTable.Name, d.SNTable.ID, sw.job.ID())
	//	for _, dropInfo := range d.DropMEInfo {
	//		hashRouter, err := api.GetHashRouterWithTable(0, dropInfo.TemplateID, false, sw.p.txn)
	//		if err != nil {
	//			return err
	//		}
	//		nodeIDs, err := hashRouter.GetLeaseHolderNodeIDs(ctx, false)
	//		if err != nil {
	//			return err
	//		}
	//		nodeID = append(nodeID, nodeIDs...)
	//	}
	//	newPlanNode = &tsDDLNode{d: d, nodeID: nodeID}
	case createKwdbInsTable:
		tsIns := tsInsertNodePool.Get().(*tsInsertNode)
		payInfo := []*sqlbase.SinglePayloadInfo{{
			Payload:       d.CTable.CTable.Payloads[0],
			RowNum:        1,
			PrimaryTagKey: d.CTable.CTable.PrimaryKeys[0],
		}}
		*tsIns = tsInsertNode{
			nodeIDs:             []roachpb.NodeID{roachpb.NodeID(d.CTable.CTable.NodeIDs[0])},
			allNodePayloadInfos: [][]*sqlbase.SinglePayloadInfo{payInfo},
		}
		newPlanNode = tsIns
	case compress, deleteExpiredData, autonomy, vacuum:
		log.Infof(ctx, "%s job start, jobID: %d", opType, *sw.job.ID())
		if d.Type == vacuum {
			if !tsAutoVacuum.Get(&sw.execCfg.Settings.SV) {
				log.Infof(ctx, "%s job skip, jobID: %d", opType, *sw.job.ID())
				return nil
			}
		}
		var desc []sqlbase.TableDescriptor
		var allDesc []sqlbase.DescriptorProto
		nodeList, err := api.GetHealthyNodeIDs(ctx)
		if err != nil {
			return err
		}
		if err = sw.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			allDesc, err = GetAllDescriptors(ctx, txn)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		for _, table := range allDesc {
			tableDesc, ok := table.(*sqlbase.TableDescriptor)
			// can not compress table if table has mutations
			if d.Type == compress || d.Type == vacuum {
				if ok && tableDesc.IsTSTable() && tableDesc.State == sqlbase.TableDescriptor_PUBLIC && len(tableDesc.Mutations) == 0 {
					desc = append(desc, *tableDesc)
				}
			} else {
				if ok && tableDesc.IsTSTable() && tableDesc.State == sqlbase.TableDescriptor_PUBLIC {
					desc = append(desc, *tableDesc)
				}
			}
		}
		if len(desc) == 0 {
			return nil
		}
		newPlanNode = &operateDataNode{d.Type, nodeList, desc}
	default:
		return pgerror.Newf(pgcode.FeatureNotSupported, "unsupported feature for now: %s", opType)
	}
	log.Infof(ctx, "%s AE execution start, jobID: %d", opType, sw.job.ID())
	return sw.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := sw.p.makeNewPlanAndRun(ctx, txn, newPlanNode)
		return err
	})
}

func (p *planner) makeNewPlanAndRun(
	ctx context.Context, txn *kv.Txn, newPlanNode planNode,
) (int, error) {
	// Create an internal planner as the planner used to serve the user query
	// would have committed by this point.
	plan := *p
	localPlanner := &plan
	localPlanner.curPlan.plan = newPlanNode
	defer localPlanner.curPlan.close(ctx)
	res := roachpb.BulkOpSummary{}
	rw := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		var counts roachpb.BulkOpSummary
		if err := protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &counts); err != nil {
			return err
		}
		res.Add(counts)
		return nil
	})
	recv := MakeDistSQLReceiver(
		ctx,
		rw,
		tree.DDL,
		p.execCfg.RangeDescriptorCache,
		p.execCfg.LeaseHolderCache,
		txn,
		func(ts hlc.Timestamp) {
			p.execCfg.Clock.Update(ts)
		},
		// Make a session tracing object on-the-fly. This is OK
		// because it sets "enabled: false" and thus none of the
		// other fields are used.
		&SessionTracing{},
	)
	defer recv.Release()
	rec, err := p.DistSQLPlanner().checkSupportForNode(localPlanner.curPlan.plan)
	var planAndRunErr error
	var rowAffectNum int
	localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
		isLocal := err != nil || rec == cannotDistribute
		evalCtx := localPlanner.ExtendedEvalContext()
		planCtx := p.DistSQLPlanner().NewPlanningCtx(ctx, evalCtx, txn)
		planCtx.isLocal = isLocal
		planCtx.planner = localPlanner
		planCtx.stmtType = recv.stmtType
		// Create a physical plan and execute it.
		cleanup := p.DistSQLPlanner().PlanAndRun(
			ctx,
			evalCtx,
			planCtx,
			txn,
			localPlanner.curPlan.plan,
			recv,
			localPlanner.GetStmt(),
		)
		defer cleanup()
		if planAndRunErr = rw.Err(); planAndRunErr != nil {
			return
		}
		if planAndRunErr = recv.commErr; planAndRunErr != nil {
			return
		}
		if r, ok := recv.resultWriter.(*callbackResultWriter); ok {
			rowAffectNum = r.rowsAffected
		}
	})
	return rowAffectNum, planAndRunErr
}

/*
sendTsTxn makes a new plan to send commit or rollback to wal for ts DDL.
Input:

	ctx:context,
	d(SyncMetaCacheDetails):job detail which contains ddl type and job ID as txn ID.
	event(txnEvent): commit or rollback.

Output:

	error
*/
func (sw *TSSchemaChangeWorker) sendTsTxn(
	ctx context.Context, d jobspb.SyncMetaCacheDetails, event txnEvent,
) error {
	switch d.Type {
	case alterKwdbAddTag, alterKwdbAddColumn, alterKwdbDropColumn, alterKwdbDropTag,
		alterKwdbAlterTagType, alterKwdbAlterColumnType, createTagIndex, dropTagIndex:
		nodeList := sw.healthyNodes
		txnID := strconv.AppendInt([]byte{}, *sw.job.ID(), 10)
		tsTxn := tsTxn{txnID: txnID, txnEvent: event}
		newPlanNode := &tsDDLNode{d: d, nodeID: nodeList, tsTxn: tsTxn}
		return sw.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			_, err := sw.p.makeNewPlanAndRun(ctx, txn, newPlanNode)
			return err
		})
	default:
		return nil
	}
}

func (sw *TSSchemaChangeWorker) checkReplica(ctx context.Context, tableID sqlbase.ID) error {
	// if isComplete is false after check replica status 30 times, return error.
	for r := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
		MaxRetries:     30,
	}); r.Next(); {
		isComplete := true
		startKey := sqlbase.MakeTsHashPointKey(tableID, 0)
		endKey := sqlbase.MakeTsHashPointKey(tableID, api.HashParam)
		if sw.p.ExecCfg().StartMode == StartMultiReplica {
			isComplete, _ = sw.db.AdminReplicaStatusConsistent(ctx, startKey, endKey)
		}
		if isComplete {
			return nil
		}
		for _, nodeID := range sw.healthyNodes {
			if err := sw.distSQLPlanner.nodeHealth.check(ctx, nodeID); err != nil {
				log.Errorf(ctx, "alter tag failed: %s", err.Error())
				return pgerror.Newf(pgcode.ConnectionFailure, "checkReplica() failed to connect to n%s", nodeID.String())
			}
		}
	}
	return pgerror.New(pgcode.Warning, "checkReplica() have tried 30 times, timed out of AdminReplicaVoterStatusConsistent")
}

func getDDLOpType(op int32) string {
	switch op {
	case createKwdbTsTable:
		return "create ts table"
	//case dropKwdbTsTable:
	//	return "drop ts table"
	//case dropKwdbTsDatabase:
	//	return "drop ts database"
	case alterKwdbAddTag:
		return "add tag"
	case alterKwdbDropTag:
		return "drop tag"
	case alterKwdbAlterTagType:
		return "alter tag type"
	case alterKwdbAddColumn:
		return "add column"
	case alterKwdbDropColumn:
		return "drop column"
	case alterKwdbAlterColumnType:
		return "alter column type"
	case alterKwdbAlterPartitionInterval:
		return "alter partition interval"
	case compress:
		return "compress"
	case compressAll:
		return "compress all"
	case compressDB:
		return "compress database"
	case compressTable:
		return "compress table"
	case deleteExpiredData:
		return "clean up expired data"
	case alterCompressInterval:
		return "alter compress interval"
	case autonomy:
		return "autonomy"
	case vacuum:
		return "vacuum"
	case createTagIndex:
		return "create tag index"
	case dropTagIndex:
		return "drop tag index"
	case count:
		return "count"
	}
	return ""
}

// notFirstInLine returns true whenever the schema change has been queued
// up for execution after another schema change.
func (sw *TSSchemaChangeWorker) notFirstInLine(
	ctx context.Context,
) (*sqlbase.TableDescriptor, bool, error) {
	var notFirst bool
	var desc *sqlbase.TableDescriptor
	err := sw.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		notFirst = false
		var err error
		desc, err = sqlbase.GetTableDescFromID(ctx, txn, sw.tableID)
		if err != nil {
			return err
		}
		for i, mutation := range desc.Mutations {
			if mutation.MutationID == sw.mutationID {
				notFirst = i != 0
				break
			}
		}
		return nil
	})
	return desc, notFirst, err
}

// handleMutationForTSTable processes completed mutations in the time-series table,
// removing them from the queue of mutations.
func (sw *TSSchemaChangeWorker) handleMutationForTSTable(
	ctx context.Context, d *jobspb.SyncMetaCacheDetails, syncErr error,
) error {
	var updateFn func(desc *sqlbase.MutableTableDescriptor) error
	var eventFn func(txn *kv.Txn) error
	isSucceeded := true
	switch d.Type {
	case alterKwdbAddColumn, alterKwdbDropColumn, alterKwdbAddTag, alterKwdbDropTag, createTagIndex:
		updateFn = func(tableDesc *sqlbase.MutableTableDescriptor) error {
			i := 0
			for _, mutation := range tableDesc.Mutations {
				if mutation.MutationID != sw.mutationID {
					// Mutations are applied in a FIFO order. Only apply the first set of
					// mutations if they have the mutation ID we're looking for.
					break
				}
				if isSucceeded {
					if err := tableDesc.MakeMutationComplete(mutation); err != nil {
						return err
					}
				} else if mutation.Direction == sqlbase.DescriptorMutation_ADD {
					if !(d.Type == createTagIndex || d.Type == dropTagIndex) {
						// If adding columns fails, roll back ColumnFamilyDescriptor.
						tableDesc.RemoveColumnFromFamily(mutation.GetColumn().ID)
					}
				}
				// If isSucceeded = true, increase TsVersion and NextTsVersion.
				// If isSucceeded = false, just increase NextTsVersion.
				tableDesc.MaybeIncrementTSVersion(ctx, isSucceeded)
				i++
			}
			if i == 0 {
				// The table descriptor is unchanged. Don't let Publish() increment
				// the version.
				return errDidntUpdateDescriptor
			}
			if d.AlterTag.IsTagCol() && tableDesc.State != sqlbase.TableDescriptor_PUBLIC {
				tableDesc.State = sqlbase.TableDescriptor_PUBLIC
			}
			if (d.Type == createTagIndex || d.Type == dropTagIndex) && tableDesc.State != sqlbase.TableDescriptor_PUBLIC {
				tableDesc.State = sqlbase.TableDescriptor_PUBLIC
			}
			// Trim the executed mutations from the descriptor.
			tableDesc.Mutations = tableDesc.Mutations[i:]
			for n, g := range tableDesc.MutationJobs {
				if g.MutationID == sw.mutationID {
					// Trim the executed mutation group from the descriptor.
					tableDesc.MutationJobs = append(tableDesc.MutationJobs[:n], tableDesc.MutationJobs[n+1:]...)
					break
				}
			}
			return nil
		}
	case alterKwdbAlterColumnType, alterKwdbAlterTagType:
		updateFn = func(tableDesc *sqlbase.MutableTableDescriptor) error {
			i := 0
			for _, mutation := range tableDesc.Mutations {
				if mutation.MutationID != sw.mutationID {
					// Mutations are applied in a FIFO order. Only apply the first set of
					// mutations if they have the mutation ID we're looking for.
					break
				}
				if isSucceeded {
					mutaCol := mutation.GetColumn()
					// get origin column
					originCol, dropped, err := tableDesc.FindColumnByName(mutaCol.ColName())
					if err != nil {
						return err
					}
					if dropped {
						return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
							"column %q in the middle of being dropped", mutaCol.ColName())
					}
					// change origin type to new type
					originCol.Type = mutaCol.Type
					originCol.TsCol = mutaCol.TsCol
					originCol.DefaultExpr = mutaCol.DefaultExpr
				}
				// If isSucceeded = true, increase TsVersion and NextTsVersion.
				// If isSucceeded = false, just increase NextTsVersion.
				tableDesc.MaybeIncrementTSVersion(ctx, isSucceeded)
				i++
			}
			if i == 0 {
				// The table descriptor is unchanged. Don't let Publish() increment
				// the version.
				return errDidntUpdateDescriptor
			}
			if d.AlterTag.IsTagCol() && tableDesc.State != sqlbase.TableDescriptor_PUBLIC {
				tableDesc.State = sqlbase.TableDescriptor_PUBLIC
			}
			// Trim the executed mutations from the descriptor.
			tableDesc.Mutations = tableDesc.Mutations[i:]
			for i, g := range tableDesc.MutationJobs {
				if g.MutationID == sw.mutationID {
					// Trim the executed mutation group from the descriptor.
					tableDesc.MutationJobs = append(tableDesc.MutationJobs[:i], tableDesc.MutationJobs[i+1:]...)
					break
				}
			}
			return nil
		}
	case dropTagIndex:
		if isSucceeded {
			idx := d.CreateOrAlterTagIndex
			tableDesc := d.SNTable
			updateErr := sw.p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				span := tableDesc.IndexSpan(idx.ID)
				ranges, err := ScanMetaKVs(ctx, txn, span)
				if err != nil {
					return err
				}
				for _, r := range ranges {
					var desc roachpb.RangeDescriptor
					if err := r.ValueProto(&desc); err != nil {
						return err
					}
					// We have to explicitly check that the range descriptor's start key
					// lies within the span of the index since ScanMetaKVs returns all
					// intersecting spans.
					if (desc.GetStickyBit() != hlc.Timestamp{}) && span.Key.Compare(desc.StartKey.AsRawKey()) <= 0 {
						// Swallow "key is not the start of a range" errors because it would
						// mean that the sticky bit was removed and merged concurrently. DROP
						// INDEX should not fail because of this.
						if err := sw.p.ExecCfg().DB.AdminUnsplit(ctx, desc.StartKey); err != nil && !strings.Contains(err.Error(), "is not the start of a range") {
							return err
						}
					}
				}
				return nil
			})
			if updateErr != nil {
				return updateErr
			}
		}
		updateFn = func(tableDesc *sqlbase.MutableTableDescriptor) error {
			i := 0
			for _, mutation := range tableDesc.Mutations {
				if mutation.MutationID != sw.mutationID {
					// Mutations are applied in a FIFO order. Only apply the first set of
					// mutations if they have the mutation ID we're looking for.
					break
				}
				mutableIdx := mutation.GetIndex()
				if mutableIdx == nil {
					return errDidntUpdateDescriptor
				}
				i++
				if !isSucceeded {
					if err := tableDesc.AddIndex(*mutableIdx, false); err != nil {
						return err
					}
				}
				tableDesc.MaybeIncrementTSVersion(ctx, isSucceeded)
			}
			// Trim the executed mutations from the descriptor.
			tableDesc.Mutations = tableDesc.Mutations[i:]
			for i, g := range tableDesc.MutationJobs {
				if g.MutationID == sw.mutationID {
					// Trim the executed mutation group from the descriptor.
					tableDesc.MutationJobs = append(tableDesc.MutationJobs[:i], tableDesc.MutationJobs[i+1:]...)
					break
				}
			}
			return nil
		}
	default:
		return errors.Errorf("unsupported online DDL type")
	}

	if isSucceeded {
		if d.Type == alterKwdbDropColumn ||
			d.Type == alterKwdbDropTag {
			// remove comment on column
			eventFn = func(txn *kv.Txn) error {
				if err := sw.p.removeColumnComment(
					ctx, txn, sw.tableID, d.AlterTag.ID,
				); err != nil {
					return err
				}
				return nil
			}
		} else if d.Type == dropTagIndex {
			// remove comment on index
			eventFn = func(txn *kv.Txn) error {
				if err := sw.p.removeIndexComment(
					ctx, sw.tableID, d.CreateOrAlterTagIndex.ID,
				); err != nil {
					return err
				}
				return nil
			}
		}
	}

	_, updateDescErr := sw.p.ExecCfg().LeaseManager.Publish(
		ctx,
		sw.tableID,
		// update table descriptor
		updateFn,
		eventFn,
	)

	if err := sw.waitToUpdateLeases(ctx, sw.tableID); err != nil {
		log.Warningf(ctx, "waiting to update ts table(%d) lease error: %+v", sw.tableID, err)
		if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
			return err
		}
		// As we are dismissing the error, go through the recording motions.
		// This ensures that any important error gets reported to Sentry, etc.
		sqltelemetry.RecordError(ctx, err, &sw.settings.SV)
	}

	return updateDescErr
}

// Wait until the entire cluster has been updated to the latest version
// of the table descriptor.
func (sw *TSSchemaChangeWorker) waitToUpdateLeases(ctx context.Context, tableID sqlbase.ID) error {
	// Aggressively retry because there might be a user waiting for the
	// schema change to complete.
	retryOpts := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}
	log.Infof(ctx, "waiting for a single version on ts table(%d)...", tableID)
	version, err := sw.leaseMgr.WaitForOneVersion(ctx, tableID, retryOpts)
	log.Infof(ctx, "waiting for a single version on ts table(%d)... done (at v %d)", tableID, version)
	return err
}
