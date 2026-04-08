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
	"bytes"
	"context"
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mutations"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gorhill/cronexpr"
)

// WriteKWDBDesc writes rows in batch, max batch size is 10000
func WriteKWDBDesc(
	ctx context.Context,
	txn *kv.Txn,
	systemTable sqlbase.TableDescriptor,
	rows []tree.Datums,
	overWrite bool, // action flag on pk conflict, true for overwrite, false for raising error
) error {
	// build Inserter based on systemTable, make sure columns align with rows from input
	imTable := sqlbase.NewImmutableTableDescriptor(systemTable)
	ri, err := row.MakeKWDBInserter(imTable, systemTable.Columns)
	if err != nil {
		return err
	}
	runTxn := func(ctx context.Context, b *kv.Batch) error {
		if runErr := txn.Run(ctx, b); runErr != nil {
			if _, ok := runErr.(*roachpb.ConditionFailedError); ok {
				return row.ConvertBatchError(ctx, imTable, b)
			}
			return runErr
		}
		return nil
	}

	b := txn.NewBatch()
	for i := range rows {
		values := rows[i]
		rowCount := i + 1
		if rowCount%mutations.MaxBatchSize() == 0 {
			if err := runTxn(ctx, b); err != nil {
				return err
			}
			b = txn.NewBatch()
		}
		if len(values) != len(systemTable.Columns) {
			return errors.Errorf("Table %s: got %d values but expected %d", systemTable.Name, len(values), len(ri.InsertCols))
		}
		// LimitValueWidth checking
		if err := enforceLocalColumnConstraints(values, ri.InsertCols); err != nil {
			return err
		}
		// Queue the rowValue in the KV batch.
		if err := ri.InsertRow(ctx, b, values, overWrite, row.SkipFKs, false, &kv.Txn{}); err != nil {
			return err
		}
	}
	if err := runTxn(ctx, b); err != nil {
		return err
	}
	return nil
}

// InitInstDescriptor inits instantce table descriptor.
func InitInstDescriptor(
	childID, stbID sqlbase.ID, instName, dbName, tmplName string, creationTime hlc.Timestamp,
) sqlbase.InstNameSpace {
	instNamespace := sqlbase.InstNameSpace{
		InstTableID: childID,
		DBName:      dbName,
		InstName:    instName,
		TmplTableID: stbID,
		ChildDesc: sqlbase.ChildDesc{
			STableName:       tmplName,
			ModificationTime: creationTime,
		},
	}
	return instNamespace
}

// writeInstTableMeta writes KWDBTagValue descriptor to table of system.
func writeInstTableMeta(
	ctx context.Context, txn *kv.Txn, instNames []sqlbase.InstNameSpace, overWrite bool,
) error {
	var rows []tree.Datums
	var err error
	for _, name := range instNames {
		name.Version++
		name.ModificationTime, err = txn.CommitTimestamp()
		if err != nil {
			return err
		}
		childDesc, err := protoutil.Marshal(&name.ChildDesc)
		if err != nil {
			return err
		}
		row := tree.Datums{
			tree.NewDInt(tree.DInt(name.InstTableID)),
			tree.NewDString(name.DBName),
			tree.NewDString(name.InstName),
			tree.NewDInt(tree.DInt(name.TmplTableID)),
			tree.NewDBytes(tree.DBytes(childDesc)),
		}
		rows = append(rows, row)
	}
	// system.kwdb_ts_table
	if err := WriteKWDBDesc(ctx, txn, sqlbase.KWDBTsTableTable, rows, overWrite); err != nil {
		return err
	}
	return nil
}

// DropInstanceTable deletes instance table from system.kwdb_ts_table.
func DropInstanceTable(
	ctx context.Context, txn *kv.Txn, instID sqlbase.ID, dbName string, instName string,
) error {
	b := txn.NewBatch()
	var idxVal []tree.Datum
	idxVal = append(idxVal, tree.NewDString(dbName), tree.NewDString(instName))
	tdKeys, err := sqlbase.MakeDropKWDBMetadataKeyInt(sqlbase.KWDBTsTableTable, []uint64{uint64(instID)}, idxVal)
	if err != nil {
		return err
	}
	for _, key := range tdKeys {
		b.Del(key)
	}
	return txn.Run(ctx, b)
}

// generate a hash value using the name and parameters of whitelist to compare whether the whitelist is consistent.
func rowToHashKey(row tree.Datums) uint32 {
	var buffer bytes.Buffer
	buffer.WriteString(string(tree.MustBeDString(row[0])))
	for _, t := range tree.MustBeDArray(row[1]).Array {
		buffer.WriteString(strconv.Itoa(int(tree.MustBeDInt(t))))
	}
	return memo.StringHash(buffer.String())
}

func deleteWhiteList(ctx context.Context, txn *kv.Txn, ie sqlutil.InternalExecutor) error {
	const deleteStmt = `delete from system.bo_black_list`
	_, err := ie.Exec(
		ctx, `delete bo_black_list`, txn, deleteStmt)
	return err
}

// WriteWhiteListDesc writes bo_white_list
func WriteWhiteListDesc(
	ctx context.Context,
	txn *kv.Txn,
	ie sqlutil.InternalExecutor,
	nodeID roachpb.NodeID,
	whiteList []sqlbase.WhiteList,
	overWrite bool,
) error {
	const selectStmt = `select name, arg_type, position, enabled, arg_opt from system.bo_black_list`
	resRows, err := ie.Query(
		ctx, `select white_list`, txn, selectStmt)
	if err != nil {
		return err
	}
	insertFlag := true
	if !overWrite && resRows != nil {
		// we add whiteList or delete whiteList
		if len(resRows) != len(whiteList) {
			if err := deleteWhiteList(ctx, txn, ie); err != nil {
				return err
			}
		} else {
			// the number of whitelist is equal to the number of query records
			whiteListMap := make(map[uint32]struct{}, 0)
			for _, row := range resRows {
				whiteListMap[rowToHashKey(row)] = struct{}{}
			}
			for _, b := range whiteList {
				argType := tree.NewDArray(types.Int2)
				var argTypeArray tree.Datums
				for _, val := range b.ArgType {
					argTypeArray = append(argTypeArray, tree.NewDInt(tree.DInt(val)))
				}
				argType.Array = argTypeArray
				key := rowToHashKey(tree.Datums{tree.NewDString(b.Name), argType})
				if _, ok := whiteListMap[key]; !ok {
					whiteListMap[key] = struct{}{}
				}
			}

			// we write the system table whitelist and the local whitelist into whiteListMap,
			// and compare the count of local whitelist.
			// if they are not equal, it indicates that the local whitelist has been modified.
			// eg:
			//	init local whiteList: 1,2,3,4,5
			// 	init system table whiteList: 1,2,3,4,5
			// 	update local whiteList: 1,2,3,4,6( delete 5, add 6, count has not changed.)
			// 	write system table whiteList into whiteListMap: 1,2,3,4,5
			// 	write local whiteList into whiteListMap: 1,2,3,4,5,6
			//  count of whiteListMap > count of local whiteList
			if len(whiteListMap) != len(whiteList) {
				if err := deleteWhiteList(ctx, txn, ie); err != nil {
					return err
				}
			} else {
				insertFlag = false
			}
		}
	}

	if insertFlag {
		// rewrite the whitelist.
		var rows []tree.Datums
		for _, b := range whiteList {
			argType := tree.NewDArray(types.Int2)
			var argTypeArray tree.Datums
			for _, val := range b.ArgType {
				argTypeArray = append(argTypeArray, tree.NewDInt(tree.DInt(val)))
			}
			argType.Array = argTypeArray
			// generate rowID
			rowID := builtins.GenerateUniqueInt(nodeID)
			row := tree.Datums{
				tree.NewDInt(rowID),
				tree.NewDString(b.Name),
				tree.NewDInt(tree.DInt(b.ArgNum)),
				argType,
				tree.NewDInt(tree.DInt(b.Position)),
				tree.MakeDBool(tree.DBool(b.Enabled)),
				tree.NewDInt(tree.DInt(b.ArgOpt)),
			}
			rows = append(rows, row)
		}

		// system.bo_white_list
		if err := WriteKWDBDesc(ctx, txn, sqlbase.BoBlackListTable, rows, overWrite); err != nil {
			return err
		}
	}
	return nil
}

// UpdateWhiteListMap updates bo_white_list to map
func UpdateWhiteListMap(
	ctx context.Context, txn *kv.Txn, ie sqlutil.InternalExecutor, wl *sqlbase.WhiteListMap,
) error {
	wl.Map = make(map[uint32]sqlbase.FilterInfo, 0)
	const selectStmt = `select name, arg_num, arg_type, position, enabled, arg_opt from system.bo_black_list`
	rows, err := ie.Query(
		ctx, `select white_list`, txn, selectStmt)
	if err != nil {
		return err
	}

	wl.Mu.Lock()
	var buffer bytes.Buffer
	for _, row := range rows {
		buffer.WriteString(string(tree.MustBeDString(row[0])))
		for _, t := range tree.MustBeDArray(row[2]).Array {
			buffer.WriteString(strconv.Itoa(int(tree.MustBeDInt(t))))
		}
		key := memo.StringHash(buffer.String())
		buffer.Reset()
		wl.Map[key] = sqlbase.FilterInfo{Pos: uint32(tree.MustBeDInt(row[3])), Typ: uint32(tree.MustBeDInt(row[5]))}
	}
	wl.Mu.Unlock()
	return nil
}

// SQLAddWhiteList adds data to bo_white_list.
var SQLAddWhiteList = settings.RegisterStringSetting(
	"sql.add_white_list",
	"add data to white_list",
	"",
)

// SQLDelWhiteList deletes data to bo_white_list.
var SQLDelWhiteList = settings.RegisterStringSetting(
	"sql.delete_white_list",
	"del data to white_list",
	"",
)

// InitTSMetaData initializes some metadata required by TS database into the system table.
func InitTSMetaData(
	ctx context.Context, db *kv.DB, ie sqlutil.InternalExecutor, execCfg *ExecutorConfig,
) error {
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// init bo_white_list
		var boWhiteListMap sqlbase.WhiteListMap
		execCfg.TSWhiteListMap = &boWhiteListMap
		if err := WriteWhiteListDesc(ctx, txn, ie, execCfg.NodeID.Get(), sqlbase.InitWhiteList, false); err != nil {
			return err
		}

		// update bo_white_list
		if err1 := UpdateWhiteListMap(ctx, txn, ie, &boWhiteListMap); err1 != nil {
			return err1
		}
		execCfg.TSWhiteListMap = &boWhiteListMap
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// ScheduleDetail schedule detail
type ScheduleDetail struct {
	Name     string
	Executor string
	CronExpr string
}

// Schedules all currently supported schedules
var Schedules = map[string]ScheduleDetail{
	ScheduleVacuum: {
		Name:     ScheduleVacuum,
		Executor: VacuumExecutorName,
		CronExpr: "@hourly",
	},
}

// InitScheduleForKWDB inits schedules
func InitScheduleForKWDB(ctx context.Context, db *kv.DB, ie sqlutil.InternalExecutor) error {
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		const selectStmt = `select schedule_name from system.scheduled_jobs`
		rows, err := ie.Query(
			ctx, `select scheduled_jobs`, txn, selectStmt)
		if err != nil {
			return err
		}
		sches := make(map[string]ScheduleDetail, len(Schedules))
		for name, sch := range Schedules {
			sches[name] = sch
		}
		for _, row := range rows {
			delete(sches, string(tree.MustBeDString(row[0])))
		}
		for _, sched := range sches {
			schedule := jobs.NewScheduledJob(scheduledjobs.ProdJobSchedulerEnv)
			schedule.SetScheduleLabel(sched.Name)
			schedule.SetOwner(security.NodeUser)
			expr, err := cronexpr.Parse(sched.CronExpr)
			if err != nil {
				return errors.Wrapf(err, "parsing schedule expression: %q", sched.CronExpr)
			}
			schedule.SetNextRun(expr.Next(scheduledjobs.ProdJobSchedulerEnv.Now()))
			schedule.SetScheduleDetails(jobspb.ScheduleDetails{Wait: jobspb.ScheduleDetails_SKIP, OnError: jobspb.ScheduleDetails_RETRY_SCHED})
			if err := schedule.SetSchedule(sched.CronExpr); err != nil {
				return err
			}
			schedule.SetExecutionDetails(sched.Executor, jobspb.ExecutionArguments{})
			err = schedule.Create(ctx, ie, txn)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// InitTsTxnJob creates a handle ts txn job when server start.
func InitTsTxnJob(
	ctx context.Context, db *kv.DB, ie sqlutil.InternalExecutor, registry *jobs.Registry,
) error {
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		stmt := `select job_type, status from kwdb_internal.jobs`
		rows, err := ie.Query(
			ctx, `select jobs`, txn, stmt)
		if err != nil {
			return err
		}
		for _, row := range rows {
			// if there is already arunning job, do nothing
			if tree.MustBeDString(row[0]) == "TS TXN" && tree.MustBeDString(row[1]) == "running" {
				return nil
			}
		}
		// Create a Job to handle ts txn record.
		tsTxnDetail := jobspb.TsTxnDetails{}
		jobRecord := jobs.Record{
			Description:   "handle ts txn record",
			Username:      "root",
			Details:       tsTxnDetail,
			Progress:      jobspb.TsTxnProgress{},
			NonCancelable: true,
		}
		_, err = registry.CreateJobWithTxn(ctx, jobRecord, txn)
		if err != nil {
			return err
		}
		return nil

	})
}

// createTSSchemaChangeJob creates a new job to synchronize the metadata cache with the agent.
func (p *planner) createTSSchemaChangeJob(
	ctx context.Context, details jobspb.SyncMetaCacheDetails, jobDesc string, txn *kv.Txn,
) (int64, error) {
	// Queue a new job.
	jobRecord := jobs.Record{
		Description: jobDesc,
		Username:    p.User(),
		Details:     details,
		Progress:    jobspb.SyncMetaCacheProgress{},
		// ts schema change job can not be canceled
		NonCancelable: true,
	}
	job, err := p.extendedEvalCtx.ExecCfg.JobRegistry.CreateJobWithTxn(ctx, jobRecord, txn)
	//job, err := p.extendedEvalCtx.QueueJob(jobRecord)
	if err != nil {
		return 0, err
	}
	return *job.ID(), err
}
