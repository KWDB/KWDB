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

package importer

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobsprotectedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/storage/cloud"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	csvOptionDelimiter = "delimiter"
	optionComment      = "comment"
	optionPrivileges   = "privileges"
	csvOptionNullIf    = "nullif"
	csvOptionSkip      = "skip"
	csvOptionEscaped   = "escaped"
	csvOptionEnclosed  = "enclosed"
	rejectedRows       = "rejected_rows"
	// csvOptionLogColumn  = "log_column"
	csvOptionThreads    = "thread_concurrency"
	csvOptionBatchRows  = "batch_rows"
	csvOptionAutoShrink = "auto_shrink"
	csvOptionCharset    = "charset"
	optionWriteWAL      = "writewal"
)

var importOptionExpectValues = map[string]sql.KVStringOptValidate{
	csvOptionDelimiter: sql.KVStringOptRequireValue,
	optionComment:      sql.KVStringOptRequireNoValue,
	optionPrivileges:   sql.KVStringOptRequireNoValue,
	csvOptionNullIf:    sql.KVStringOptRequireValue,
	csvOptionSkip:      sql.KVStringOptRequireValue,
	csvOptionEscaped:   sql.KVStringOptRequireValue,
	csvOptionEnclosed:  sql.KVStringOptRequireValue,
	rejectedRows:       sql.KVStringOptRequireValue,
	// csvOptionLogColumn:  sql.KVStringOptRequireValue,
	csvOptionThreads:    sql.KVStringOptRequireValue,
	csvOptionBatchRows:  sql.KVStringOptRequireValue,
	csvOptionAutoShrink: sql.KVStringOptRequireNoValue,
	csvOptionCharset:    sql.KVStringOptRequireValue,
	optionWriteWAL:      sql.KVStringOptRequireNoValue,
}

// importHeader is the header for RESTORE stmt results.
var importHeader = sqlbase.ResultColumns{
	{Name: "job_id", Typ: types.String},
	{Name: "status", Typ: types.String},
	{Name: "fraction_completed", Typ: types.Float},
	{Name: "rows", Typ: types.Int},
	{Name: "abandon_rows", Typ: types.String},
	{Name: "reject_rows", Typ: types.String},
	{Name: "note", Typ: types.String},
}

// clusterHeader is the header for IMPORT CLUSTER SETTING stmt results.
var clusterHeader = sqlbase.ResultColumns{
	{Name: "prompt_information", Typ: types.String},
}

// userHeader is the header for IMPORT USER stmt results.
var userHeader = sqlbase.ResultColumns{
	{Name: "job_id", Typ: types.String},
	{Name: "status", Typ: types.String},
	{Name: "rows", Typ: types.Int},
}

var escapedMap = map[rune]bool{'"': true, '\\': true}
var enclosedMap = map[rune]bool{'"': true, '\'': true}
var delimiterMap = map[rune]bool{'"': false, '\n': false}

// importPlanHook implements sql.PlanHookFn.
func importPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	importStmt, ok := stmt.(*tree.Import)
	if !ok {
		return nil, nil, nil, false, nil
	}
	telemetry.Inc(sqltelemetry.ImportCounter("total.attempted"))
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.VersionPartitionedBackup) {
		return nil, nil, nil, false, errors.Errorf("IMPORT requires a cluster which has the version >= 19.2")
	}

	filesFn, err := p.TypeAsStringArray(importStmt.Files, "IMPORT FILES")
	if err != nil {
		return nil, nil, nil, false, err
	}

	var createFileFn func() (string, error)
	if importStmt.CreateFile != nil {
		createFileFn, err = p.TypeAsString(importStmt.CreateFile, "IMPORT CREATE")
		if err != nil {
			return nil, nil, nil, false, err
		}
	}

	opts, err := getImportOpts(p, importStmt)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fileFormat := roachpb.IOFileFormat{}
	if importStmt.FileFormat != "" {
		fileFormat, err = getOptsParas(importStmt.FileFormat, opts)
		if err != nil {
			return nil, nil, nil, false, err
		}
	}
	_, hasComment := opts[optionComment]
	_, withPrivileges := opts[optionPrivileges]
	_, writeWAL := opts[optionWriteWAL]

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		// TODO(dan): Move this span into sql.
		var targetType target.AuditObjectType
		var seed int32
		if p.ExecCfg() != nil && p.ExecCfg().NodeID != nil {
			seed = int32(p.ExecCfg().NodeID.Get())
		}
		ctx, span := tracing.ChildSpan(ctx, importStmt.StatementTag(), seed)
		defer tracing.FinishSpan(span)

		targetType = target.ObjectTable
		if importStmt.IsDatabase {
			targetType = target.ObjectDatabase
		}
		// 1.IMPORT requires admin role.
		if err = p.RequireAdminRole(ctx, "IMPORT"); err != nil {
			return err
		}
		// 2.IMPORT cannot be used inside a transaction
		if !p.ExtendedEvalContext().TxnImplicit {
			err = errors.Errorf("IMPORT cannot be used inside a transaction")
			return err
		}
		var dbName string
		var scNames []string
		var tableDetails []sqlbase.ImportTable
		var files []string
		var databaseComment string
		var privileges []string
		files, err = filesFn()
		if err != nil {
			return err
		}
		if importStmt.Settings {
			if fileFormat.Format != roachpb.IOFileFormat_SQL {
				return errors.Errorf("unsupported file format:%s while import cluster settings", fileFormat.Format)
			}
			// Read the SQL file of cluster parameter settings and execute it
			if err := execClusterSettingSQL(ctx, p, files[0]); err != nil {
				return err
			}
			resultsCh <- tree.Datums{
				tree.NewDString("The cluster settings have been set"),
			}
			return nil
		}
		if importStmt.Users {
			if len(opts) != 0 {
				return errors.Errorf("cannot use options while import users or roles")
			}
			if fileFormat.Format != roachpb.IOFileFormat_SQL {
				return errors.Errorf("unsupported file format:%s while import users or roles", fileFormat.Format)
			}
			// Read the SQL file of cluster parameter settings and execute it
			rows, err := execUserSQL(ctx, p, files[0])
			if err != nil {
				return err
			}
			resultsCh <- tree.Datums{
				tree.NewDString("-"),
				tree.NewDString(string(jobs.StatusSucceeded)),
				tree.NewDInt(tree.DInt(rows)),
			}
			return nil
		}
		var timeSeriesImport bool
		// 3.do some check and get tableDetails in different IMPORT
		if importStmt.IsDatabase {
			timeSeriesImport, dbName, scNames, tableDetails, databaseComment, privileges, err = checkAndGetDetailsInDatabase(ctx, p, files, hasComment, withPrivileges)
			if err != nil {
				return err
			}

			p.SetAuditTargetAndType(0, dbName, nil, targetType)
		} else { /* Single table import */
			if importStmt.Into {
				if hasComment {
					return errors.New("'WITH COMMENT' can only be used for the 'IMPORT CREATE USING' and 'IMPORT DATABASE', the current syntax is 'IMPORT INTO'")
				}
				if withPrivileges {
					return errors.New("'WITH PRIVILEGES' can only be used for the 'IMPORT CREATE USING' and 'IMPORT DATABASE', the current syntax is 'IMPORT INTO'")
				}
				var tableDesc *sql.MutableTableDescriptor
				intoCols := make([]string, 0, len(importStmt.IntoCols))
				if len(importStmt.IntoCols) > 0 {
					for _, col := range importStmt.IntoCols {
						intoCols = append(intoCols, col.String())
					}
				}
				// get desc
				tableDesc, err = p.ResolveMutableTableDescriptor(ctx, importStmt.Table, true, sql.ResolveRequireTableDesc)
				if err != nil {
					return err
				}
				// We have a target table, so it might specify a DB in its name.
				if _, err = checkDatabaseExist(ctx, p, importStmt.Table); err != nil {
					return err
				}
				p.SetAuditTargetAndType(uint32(tableDesc.GetID()), tableDesc.GetName(), nil, targetType)
				if tableDesc.TableType == tree.RelationalTable {
					if tableDetails, err = checkAndGetDetailsInTableInto(tableDesc, intoCols); err != nil {
						return err
					}
				} else {
					timeSeriesImport = true
					tableDesc, err = p.ResolveMutableTableDescriptor(ctx, importStmt.Table, true, sql.ResolveRequireTableDesc)
					if err != nil {
						return err
					}
					if tableDetails, err = checkAndGetTSDetailsInTableInto(ctx, p, importStmt, tableDesc, files, intoCols); err != nil {
						return err
					}
					if err = checkIntoColumns(tableDesc, intoCols); err != nil {
						return err
					}
				}
			} else {
				if importStmt.CreateFile != nil {
					/*
						IMPORT TABLE CREATE USING string_or_placeholder import_format DATA '(' string_or_placeholder_list ')' opt_with_options
						IMPORT TABLE CREATE USING string_or_placeholder
					*/
					var stmts parser.Statements
					stmts, err = checkTableMetaFile(ctx, p, createFileFn)
					if err != nil {
						return err
					}
					tbCreate, ok := stmts[0].AST.(*tree.CreateTable)
					if !ok {
						err = errors.New("expected CREATE TABLE statement in table file")
						return err
					}
					var hasTableComment bool
					var hasPrivileges bool
					if tbCreate.IsTS() {
						timeSeriesImport = true
						if dbName, tableDetails, hasTableComment, privileges, hasPrivileges, err = checkAndGetDetailsInTable(ctx, p, stmts, hasComment, withPrivileges); err != nil {
							return err
						}
						// Check if there are comments in SQL
						if hasComment && !hasTableComment {
							return errors.New("NO COMMENT statement in the SQL file")
						}
						// Check if there are privileges in SQL
						if withPrivileges && !hasPrivileges {
							return errors.New("NO GRANT statement in the SQL file")
						}
					}
				}
				if !timeSeriesImport {
					// Relation table import table create using / import table tableName(table elements)
					if dbName, tableDetails, privileges, err = checkAndGetDetailsInTableNew(ctx, p, importStmt, createFileFn, hasComment, withPrivileges); err != nil {
						return err
					}
					for _, t := range tableDetails {
						if t.Desc != nil {
							p.SetAuditTargetAndType(uint32(t.Desc.GetID()), t.Desc.Name, nil, targetType)
						}
					}
				}

			}
		}
		// 6.Get import job description.
		var jobDesc string
		jobDesc, err = importJobDescription(p, importStmt, files, opts)
		if err != nil {
			return err
		}
		telemetry.CountBucketed("import.files", int64(len(files)))
		// Here we create the job and protected timestamp records in a side
		// transaction and then kick off the job. This is awful. Rather we should be
		// disallowing this statement in an explicit transaction and then we should
		// create the job in the user's transaction here and then in a post-commit
		// hook we should kick of the StartableJob which we attached to the
		// connExecutor somehow.
		importDetails := jobspb.ImportDetails{
			URIs:             files,
			Format:           fileFormat,
			Tables:           tableDetails,
			IsDatabase:       importStmt.IsDatabase,
			DatabaseName:     dbName,
			SchemaNames:      scNames,
			TimeSeriesImport: timeSeriesImport,
			OnlyMeta:         importStmt.OnlyMeta,
			WithComment:      hasComment,
			DatabaseComment:  databaseComment,
			Privileges:       privileges,
			WriteWAL:         writeWAL,
		}
		walltime := p.ExecCfg().Clock.Now().WallTime
		// Prepare the protected timestamp record.
		var spansToProtect []roachpb.Span
		if !timeSeriesImport {
			for i := range tableDetails {
				if td := tableDetails[i]; !td.IsNew {
					spansToProtect = append(spansToProtect, td.Desc.TableSpan())
				}
			}
			if len(spansToProtect) > 0 {
				protectedtsID := uuid.MakeV4()
				importDetails.ProtectedTimestampRecord = &protectedtsID
			}
		}
		jr := jobs.Record{
			Details:     importDetails,
			Description: jobDesc,
			Progress:    jobspb.ImportProgress{},
			Username:    p.User(),
		}
		var sj *jobs.StartableJob
		if err = p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			sj, err = p.ExecCfg().JobRegistry.CreateStartableJobWithTxn(ctx, jr, txn, resultsCh)
			if err != nil {
				return err
			}

			if len(spansToProtect) > 0 {
				// NB: We protect the timestamp preceding the import statement timestamp
				// because that's the timestamp to which we want to revert.
				tsToProtect := hlc.Timestamp{WallTime: walltime}.Prev()
				rec := jobsprotectedts.MakeRecord(*importDetails.ProtectedTimestampRecord,
					*sj.ID(), tsToProtect, spansToProtect)
				return p.ExecCfg().ProtectedTimestampProvider.Protect(ctx, txn, rec)
			}
			return nil
		}); err != nil {
			if sj != nil {
				if cleanupErr := sj.CleanupOnRollback(ctx); cleanupErr != nil {
					log.Warningf(ctx, "failed to cleanup Startable Job: %v", cleanupErr)
				}
			}
			return err
		}
		err = sj.Run(ctx)
		return err
	}
	if importStmt.Settings {
		return fn, clusterHeader, nil, false, nil
	}
	if importStmt.Users {
		return fn, userHeader, nil, false, nil
	}
	return fn, importHeader, nil, false, nil
}

func checkAndGetTSDetailsInTableInto(
	ctx context.Context,
	p sql.PlanHookState,
	importStmt *tree.Import,
	tableDesc *sqlbase.MutableTableDescriptor,
	files []string,
	intoCols []string,
) ([]sqlbase.ImportTable, error) {
	switch tableDesc.TableType {
	//case sqlbase.TableType_CHILD_TABLE:
	//	return nil, errors.Errorf("can't import into instance table %v", importStmt.Table.Table())
	//case sqlbase.TableType_SUPER_TABLE:
	//	return getTemplateTableDetails(ctx, p, importStmt, files)
	case tree.TimeseriesTable:
		return []sqlbase.ImportTable{{Desc: tableDesc.TableDesc(), TableName: tableDesc.Name, IsNew: false, TableType: tree.TimeseriesTable, IntoCols: intoCols}}, nil
	default:
		return nil, errors.Errorf("Unknown table type %v", tableDesc.TableType)
	}
}

// checkIntoColumns is used to judge whether target columns are all in table's columns
func checkIntoColumns(tableDesc *sqlbase.MutableTableDescriptor, intoCols []string) error {
	if len(intoCols) > 0 {
		// judgeMap is used to judge whether target columns are all in table columns
		// colSet is used to judge whether target columns are multiple assignments to the same column
		judgeMap := make(map[string]struct{}, len(tableDesc.Columns))
		colSet := make(map[string]struct{})
		for _, col := range tableDesc.Columns {
			judgeMap[col.Name] = struct{}{}
		}
		for _, intoCol := range intoCols {
			if _, ok := judgeMap[intoCol]; !ok {
				return errors.Errorf("target column %v is not in table %v", intoCol, tableDesc.Name)
			}
			if _, ok := colSet[intoCol]; ok {
				return errors.Errorf("multiple assignments to the same column %v", intoCol)
			}
			colSet[intoCol] = struct{}{}
		}
	}
	return nil
}

func getImportOpts(p sql.PlanHookState, importStmt *tree.Import) (map[string]string, error) {
	optsFn, err := p.TypeAsStringOpts(importStmt.Options, importOptionExpectValues)
	if err != nil {
		return nil, err
	}
	opts, err := optsFn()
	if err != nil {
		return nil, err
	}
	return opts, nil
}

func getOptsParas(fileFormat string, opts map[string]string) (roachpb.IOFileFormat, error) {
	ioFileFormat := roachpb.IOFileFormat{}
	if fileFormat != "CSV" && fileFormat != "SQL" {
		return ioFileFormat, unimplemented.Newf("import.format", "unsupported import format: %q", fileFormat)
	}

	if fileFormat == "CSV" {
		ioFileFormat.Format = roachpb.IOFileFormat_CSV
	} else {
		ioFileFormat.Format = roachpb.IOFileFormat_SQL
	}
	// Set the default CSV separator for the cases when it is not overwritten.
	ioFileFormat.Csv.Comma = ','
	ioFileFormat.Csv.Enclosed = '"'
	ioFileFormat.Csv.Escaped = '"'
	ioFileFormat.Csv.Charset = "UTF-8"
	// Set the import CSV charset from options
	if override, ok := opts[csvOptionCharset]; ok {
		if _, has := types.CsvOptionCharset[override]; has {
			ioFileFormat.Csv.Charset = override
		} else {
			return ioFileFormat, pgerror.Wrap(errors.New("charset value must be 'UTF-8''GBK''GB18030''BIG5'"),
				pgcode.InvalidParameterValue, "invalid charset value")
		}
	}
	_, ioFileFormat.Csv.AutoShrink = opts[csvOptionAutoShrink]
	if override, ok := opts[csvOptionDelimiter]; ok {
		comma, err := util.GetSingleRune(override)
		if err != nil {
			return ioFileFormat, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid delimiter value")
		}
		if err := sql.CheckDelimiterValue(comma); err != nil {
			return ioFileFormat, err
		}
		ioFileFormat.Csv.Comma = comma
	}
	if override, ok := opts[csvOptionEscaped]; ok {
		escape, err := util.GetSingleRune(override)
		if err != nil {
			return ioFileFormat, pgerror.Wrap(err, pgcode.InvalidParameterValue, fmt.Sprintf("invalid escaped value %v", escape))
		}
		if err := sql.CheckEscapeValue(escape); err != nil {
			return ioFileFormat, err
		}
		ioFileFormat.Csv.Escaped = escape
	}
	if override, ok := opts[csvOptionEnclosed]; ok {
		enclosed, err := util.GetSingleRune(override)
		if err != nil {
			return ioFileFormat, pgerror.Wrap(err, pgcode.InvalidParameterValue, fmt.Sprintf("invalid enclosed value %v", enclosed))
		}
		if err := sql.CheckEnclosedValue(enclosed); err != nil {
			return ioFileFormat, err
		}
		ioFileFormat.Csv.Enclosed = enclosed
	}

	if err := sql.CheckImpExpInfoConflict(ioFileFormat.Csv); err != nil {
		return ioFileFormat, err
	}

	if override, ok := opts[csvOptionNullIf]; ok {
		ioFileFormat.Csv.NullEncoding = &override
	}

	if override, ok := opts[csvOptionSkip]; ok {
		skip, err := strconv.Atoi(override)
		if err != nil {
			return ioFileFormat, pgerror.Wrapf(err, pgcode.Syntax, "invalid %s value", csvOptionSkip)
		}
		if skip < 0 {
			return ioFileFormat, pgerror.Newf(pgcode.Syntax, "%s must be >= 0", csvOptionSkip)
		}
		ioFileFormat.Csv.Skip = uint32(skip)
	}

	// if override, ok := opts[csvOptionLogColumn]; ok {
	// 	logColumn, err := strconv.Atoi(override)
	// 	if err != nil {
	// 		return ioFileFormat, pgerror.Newf(pgcode.Syntax, "invalid log_column value")
	// 	}
	// ioFileFormat.Csv.LogColumn = int32(logColumn)
	// ioFileFormat.Csv.LogColumn = int32(0)
	// }

	if override, ok := opts[csvOptionThreads]; ok {
		threads, err := strconv.Atoi(override)
		if err != nil {
			return ioFileFormat, pgerror.Newf(pgcode.Syntax, "invalid thread_concurrency value")
		}
		ioFileFormat.Csv.Threads = int32(threads)
	}

	if override, ok := opts[csvOptionBatchRows]; ok {
		batchRows, err := strconv.Atoi(override)
		if err != nil {
			return ioFileFormat, pgerror.Newf(pgcode.Syntax, "invalid batch_rows value")
		}
		ioFileFormat.Csv.BatchRows = int32(batchRows)
	}
	// log.Infof(context.Background(), "logColumn %d, threads %d, batchRows %d",
	// 	ioFileFormat.Csv.LogColumn, ioFileFormat.Csv.Threads, ioFileFormat.Csv.BatchRows)
	return ioFileFormat, nil
}

func importJobDescription(
	p sql.PlanHookState, orig *tree.Import, files []string, opts map[string]string,
) (string, error) {
	stmt := *orig
	stmt.Files = nil
	for _, file := range files {
		clean, err := cloud.SanitizeExternalStorageURI(file, nil /* extraParams */)
		if err != nil {
			return "", err
		}
		stmt.Files = append(stmt.Files, tree.NewDString(clean))
	}
	stmt.Options = nil
	for k, v := range opts {
		opt := tree.KVOption{Key: tree.Name(k)}
		val := importOptionExpectValues[k] == sql.KVStringOptRequireValue
		val = val || (importOptionExpectValues[k] == sql.KVStringOptAny && len(v) > 0)
		if val {
			opt.Value = tree.NewDString(v)
		}
		stmt.Options = append(stmt.Options, opt)
	}
	sort.Slice(stmt.Options, func(i, j int) bool { return stmt.Options[i].Key < stmt.Options[j].Key })
	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(&stmt, ann), nil
}

type importResumer struct {
	job      *jobs.Job
	settings *cluster.Settings
	res      RowCount

	testingKnobs struct {
		afterImport               func(summary RowCount) error
		alwaysFlushJobProgress    bool
		ignoreProtectedTimestamps bool
	}
}

// Resume is part of the jobs.Resumer interface.
func (r *importResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	telemetry.Inc(sqltelemetry.ImportCounter("started"))

	details := r.job.Details().(jobspb.ImportDetails)
	p := phs.(sql.PlanHookState)
	if p.ExecCfg().StartMode != sql.StartSingleNode {
		p.ExtendedEvalContext().EvalContext.StartDistributeMode = true
	} else {
		p.ExtendedEvalContext().EvalContext.StartSinglenode = true
	}
	cfg := p.ExecCfg()
	if details.TimeSeriesImport {
		return r.timeSeriesResume(ctx, p, details, cfg, resultsCh)
	}
	return r.relationalResume(ctx, p, details, cfg, resultsCh)
}

// timeSeriesResume does timeSeries Resume job.
// 1. Check if create table(database) is complete, if not create table here.
// 2. If only meta is true means we only need to create table, so return result and skip import data.
// 3. Generate a distributed import execution plan and import table data.
func (r *importResumer) timeSeriesResume(
	ctx context.Context,
	p sql.PlanHookState,
	details jobspb.ImportDetails,
	cfg *sql.ExecutorConfig,
	resultsCh chan<- tree.Datums,
) error {
	if details.TimeSeriesEverExecute > 0 {
		return errors.Errorf("time series Import cannot be execute more than once")
	}
	details.TimeSeriesEverExecute++
	if err := r.job.WithTxn(nil).SetDetails(ctx, details); err != nil {
		return err
	}
	if details.Tables == nil {
		return errors.Errorf("details.Tables cannot be nil.")
	}
	// Skip prepare stage on job resumption, if it has already been completed.
	if !details.PrepareComplete {
		tableCollection := sql.GetTableCollection(p.ExecCfg(), p.ExecCfg().InternalExecutor)
		p.ExtendedEvalContext().Tables = tableCollection
		defer p.ExtendedEvalContext().Tables.ReleaseTSTables(ctx)
		if err := r.prepareTSTableDescsForIngestion(ctx, p, details); err != nil {
			return err
		}
		// Re-initialize details after prepare step.
		details = r.job.Details().(jobspb.ImportDetails)
	}
	if details.OnlyMeta {
		showResult(resultsCh, *r.job.ID(), r.res.Rows, 0, 0, true)
		return nil
	}
	if details.IsDatabase {
		var rets RowCount
		count := 0
		for _, table := range details.Tables {
			count++
			dataPath, err := getURIPrefix(details.URIs[0], table)
			if err != nil {
				return err
			}
			dataFiles, err := getDataFilesInURI(ctx, p, dataPath, details.Format, table)
			if err != nil {
				if _, ok := err.(*importPathError); ok {
					continue
				} else {
					return err
				}
			}
			res, err := sql.DistIngest(ctx, p, table, dataFiles, r.job, len(details.Tables))
			if err != nil {
				return err
			}
			rets.Rows += res.TimeSeriesCounts
			rets.RejectRows += res.RejectedCounts
			rets.AbandonRows += res.AbandonCounts
			atomic.AddInt64(&rets.DataSize, res.DataSize)
		}
		r.res.Rows = rets.Rows
		r.res.RejectRows = rets.RejectRows
	} else {
		dataFiles, err := getDataFilesInURI(ctx, p, details.URIs[0], details.Format, details.Tables[0])
		if err != nil {
			return err
		}
		res, err := sql.DistIngest(ctx, p, details.Tables[0], dataFiles, r.job, 1)
		if err != nil {
			return err
		}
		r.res.Rows = res.TimeSeriesCounts
		r.res.RejectRows = res.RejectedCounts
		r.res.AbandonRows = res.AbandonCounts
	}

	showResult(resultsCh, *r.job.ID(), r.res.Rows, int(r.res.AbandonRows), int(r.res.RejectRows), true)
	return nil
}

// relationalResume does relation Resume job.
// 1. Check if create table(database) is complete, if not create table here.
// 2. Record Walltime for rollback if import job is failed.
// 3. If only meta is true means we only need to create table, so return result and skip import data.
// 4. Generate a distributed import execution plan and import table data.
// 5. Set tables' State public then tables can provide services.
func (r *importResumer) relationalResume(
	ctx context.Context,
	p sql.PlanHookState,
	details jobspb.ImportDetails,
	cfg *sql.ExecutorConfig,
	resultsCh chan<- tree.Datums,
) error {
	ptsID := details.ProtectedTimestampRecord
	if ptsID != nil && !r.testingKnobs.ignoreProtectedTimestamps {
		if err := cfg.ProtectedTimestampProvider.Verify(ctx, *ptsID); err != nil {
			if errors.Is(err, protectedts.ErrNotExists) {
				// No reason to return an error which might cause problems if it doesn't
				// seem to exist.
				log.Warningf(ctx, "failed to release protected which seems not to exist: %v", err)
			} else {
				return err
			}
		}
	}
	if details.Tables == nil {
		return errors.Errorf("details.Tables cannot be nil.")
	}
	// Skip prepare stage on job resumption, if it has already been completed.
	if !details.PrepareComplete {
		if err := r.prepareTableDescsForIngestion(ctx, p, details); err != nil {
			return err
		}
		// Re-initialize details after prepare step.
		details = r.job.Details().(jobspb.ImportDetails)
	}
	if details.OnlyMeta {
		if err := r.publishTables(ctx, cfg); err != nil {
			return err
		}
		// Add COMMENT to the database
		if details.DatabaseComment != "" {
			if err := execCommentOnMeta(ctx, p, details.DatabaseComment, details.DatabaseName); err != nil {
				return err
			}
		}
		// Add COMMENT to the table
		for _, table := range details.Tables {
			if table.IsNew {
				if table.TableComment != "" {
					if err := execCommentOnMeta(ctx, p, table.TableComment, details.DatabaseName); err != nil {
						return err
					}
				}
				// Add COMMENT to the column
				if table.ColumnComment != nil {
					for _, colComment := range table.ColumnComment {
						if err := execCommentOnMeta(ctx, p, colComment, details.DatabaseName); err != nil {
							return err
						}
					}
				}
			}
		}

		// Exec Privileges statement
		if details.Privileges != nil {
			if err := execPrivilegesSQL(ctx, p, details.Privileges, strings.Trim(details.DatabaseName, "\"")); err != nil {
				return err
			}
		}
		showResult(resultsCh, *r.job.ID(), r.res.Rows, 0, 0, false)
		return nil
	}
	// In the case of importing into existing tables we must wait for all nodes
	// to see the same version of the updated table descriptor, after which we
	// shall chose a ts to import from.
	if details.Walltime == 0 {
		// TODO(dt): update job status to mention waiting for tables to go offline.
		for _, i := range details.Tables {
			if !i.IsNew {
				if _, err := cfg.LeaseManager.WaitForOneVersion(ctx, i.Desc.ID, retry.Options{}); err != nil {
					return err
				}
			}
		}
		details.Walltime = cfg.Clock.Now().WallTime
		if err := r.job.WithTxn(nil).SetDetails(ctx, details); err != nil {
			return err
		}
	}
	if details.IsDatabase {
		var rets RowCount
		count := 0
		dbPath := details.URIs[0]
		for _, table := range details.Tables {
			count++
			filePathName := dbPath + string(os.PathSeparator) + table.SchemaName + string(os.PathSeparator) + table.Desc.Name + string(os.PathSeparator)
			dataFiles, err := getDataFilesInURI(ctx, p, filePathName, details.Format, table)
			if err != nil {
				if _, ok := err.(*importPathError); ok {
					continue
				} else {
					return err
				}
			}
			res, err := sql.DistIngest(ctx, p, table, dataFiles, r.job, len(details.Tables))
			if err != nil {
				return err
			}
			atomic.AddInt64(&rets.DataSize, res.DataSize)
			pkIDs := make(map[uint64]struct{}, len(details.Tables))
			for _, t := range details.Tables {
				pkIDs[roachpb.BulkOpSummaryID(uint64(t.Desc.ID), uint64(t.Desc.PrimaryIndex.ID))] = struct{}{}
			}
			for id, count := range res.EntryCounts {
				if _, ok := pkIDs[id]; ok {
					atomic.AddInt64(&rets.Rows, count)
				} else {
					atomic.AddInt64(&rets.IndexEntries, count)
				}
			}
		}
		r.res.Rows = rets.Rows
		r.res.IndexEntries = rets.IndexEntries
		r.res.DataSize = rets.DataSize
	} else {
		// assign tables[i] with files input
		res, err := sql.DistIngest(ctx, p, details.Tables[0], details.URIs, r.job, 1)
		if err != nil {
			return err
		}
		// make result Lists by Tables, every table will return results by {file_id: spec.results}
		pkIDs := make(map[uint64]struct{}, len(details.Tables))
		for _, t := range details.Tables {
			pkIDs[roachpb.BulkOpSummaryID(uint64(t.Desc.ID), uint64(t.Desc.PrimaryIndex.ID))] = struct{}{}
		}
		r.res.DataSize = res.DataSize
		for id, count := range res.EntryCounts {
			if _, ok := pkIDs[id]; ok {
				r.res.Rows += count
			} else {
				r.res.IndexEntries += count
			}
		}
	}
	if r.testingKnobs.afterImport != nil {
		if err := r.testingKnobs.afterImport(r.res); err != nil {
			return err
		}
	}

	if err := r.publishTables(ctx, cfg); err != nil {
		return err
	}
	// TODO(ajwerner): Should this actually return the error? At this point we've
	// successfully finished the import but failed to drop the protected
	// timestamp. The reconciliation loop ought to pick it up.
	if ptsID != nil && !r.testingKnobs.ignoreProtectedTimestamps {
		if err := cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return r.releaseProtectedTimestamp(ctx, cfg.ProtectedTimestampProvider, txn)
		}); err != nil {
			log.Errorf(ctx, "failed to release protected timestamp: %v", err)
		}
	}
	// Add COMMENT to the database
	if details.DatabaseComment != "" {
		if err := execCommentOnMeta(ctx, p, details.DatabaseComment, details.DatabaseName); err != nil {
			return err
		}
	}
	// Add COMMENT to the table
	for _, table := range details.Tables {
		if table.IsNew {
			if table.TableComment != "" {
				if err := execCommentOnMeta(ctx, p, table.TableComment, details.DatabaseName); err != nil {
					return err
				}
			}
			// Add COMMENT to the column
			if table.ColumnComment != nil {
				for _, colComment := range table.ColumnComment {
					if err := execCommentOnMeta(ctx, p, colComment, details.DatabaseName); err != nil {
						return err
					}
				}
			}
		}
	}

	// Exec Privileges statement
	if details.Privileges != nil {
		if err := execPrivilegesSQL(ctx, p, details.Privileges, strings.Trim(details.DatabaseName, "\"")); err != nil {
			return err
		}
	}

	showResult(resultsCh, *r.job.ID(), r.res.Rows, 0, 0, false)
	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface. Removes data that has
// been committed from a import that has failed or been canceled. It does this
// by adding the table descriptors in DROP state, which causes the schema change
// stuff to delete the keys in the background.
func (r *importResumer) OnFailOrCancel(ctx context.Context, phs interface{}) error {
	telemetry.Inc(sqltelemetry.ImportCounter("total.failed"))

	p := phs.(sql.PlanHookState)
	cfg := p.ExecCfg()
	jr := cfg.JobRegistry
	details := r.job.Details().(jobspb.ImportDetails)
	return cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if !details.TimeSeriesImport {
			if err := r.dropTables(ctx, jr, txn, cfg.InternalExecutor); err != nil {
				return err
			}
			return r.releaseProtectedTimestamp(ctx, cfg.ProtectedTimestampProvider, txn)
		}
		return nil
	})
}

// getURIPrefix get sub path from db path.
func getURIPrefix(dbPath string, table sqlbase.ImportTable) (string, error) {
	tsScPath := dbPath + string(os.PathSeparator) + "public" + string(os.PathSeparator)
	switch table.TableType {
	case tree.TimeseriesTable, tree.RelationalTable:
		return tsScPath + table.TableName + string(os.PathSeparator), nil
	default:
		return "", errors.Errorf("unsupported table type %v", table.TableType)
	}
}

// getDataFilesInURI get csv files in a dir.
func getDataFilesInURI(
	ctx context.Context,
	p sql.PlanHookState,
	uri string,
	format roachpb.IOFileFormat,
	table sqlbase.ImportTable,
) ([]string, error) {
	patternSuffix := ""
	switch format.Format {
	case roachpb.IOFileFormat_CSV:
		patternSuffix = ".csv"
	}
	var tableFiles []string
	switch table.TableType {
	case tree.TimeseriesTable, tree.RelationalTable:
		store, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, uri)
		if err != nil {
			return nil, err
		}
		defer store.Close()
		// TODO Currently, HTTP services are not fully supported, which is a major issue.
		// The current implementation only relies on parsing the hrefs in the HTML file
		filesInTbDir, err := store.ListFiles(ctx, "*")
		if err != nil {
			return nil, err
		}

		for _, tableFile := range filesInTbDir {
			if strings.HasSuffix(tableFile, patternSuffix) {
				tableFiles = append(tableFiles, tableFile)
			}
		}
		if len(tableFiles) == 0 {
			return nil, newImportPathError(uri)
		}
		for i := 0; i < len(tableFiles); i++ {
			tableFiles[i] = strings.Join([]string{uri, tableFiles[i]}, string(os.PathSeparator))
		}
	default:
		return nil, errors.Errorf("unsupported table type %v", table.TableType)
	}
	return tableFiles, nil
}

// importPathError is an error type describing malformed import data.
type importPathError struct {
	err error
}

func (e *importPathError) Error() string {
	return e.err.Error()
}

func newImportPathError(uri string) error {
	return &importPathError{err: errors.Errorf("%v is not a dir or has no csv files", uri)}
}

// prepareTableDescsForIngestion prepares table descriptors for the ingestion
// step of import. The descriptors are in an IMPORTING state (offline) on
// successful completion of this method.
func (r *importResumer) prepareTableDescsForIngestion(
	ctx context.Context, p sql.PlanHookState, details jobspb.ImportDetails,
) error {
	dbName := details.DatabaseName
	cleanedDbName := strings.Trim(dbName, "\"")
	return p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		defer p.ExecCfg().InternalExecutor.SetSessionData(new(sessiondata.SessionData))
		importDetails := details
		var err error
		var desc *sqlbase.TableDescriptor
		var createSchema string
		if details.IsDatabase {
			createDatabase := fmt.Sprintf(`CREATE DATABASE %s;`, dbName)
			if _, err := p.ExecCfg().InternalExecutor.Exec(ctx, `import-create-database`, txn, createDatabase); err != nil {
				return err
			}
			p.ExecCfg().InternalExecutor.SetSessionData(&sessiondata.SessionData{Database: cleanedDbName})
			for _, sc := range details.SchemaNames {
				if sql.ContainsUpperCase(sc) {
					createSchema = fmt.Sprintf(`CREATE SCHEMA "%s";`, sc)
				} else {
					createSchema = fmt.Sprintf(`CREATE SCHEMA %s;`, sc)
				}
				if _, err := p.ExecCfg().InternalExecutor.Exec(ctx, `import-create-schema`, txn, createSchema); err != nil {
					return err
				}
			}
		}
		for i, table := range details.Tables {
			if table.IsNew {
				searchPath := sessiondata.SetSearchPath(p.CurrentSearchPath(), []string{table.SchemaName})
				p.ExecCfg().InternalExecutor.SetSessionData(&sessiondata.SessionData{Database: cleanedDbName, SearchPath: searchPath})
				_, err = p.ExecCfg().InternalExecutor.Exec(ctx, `import-create-table`, txn, table.Create)
				if err != nil {
					return err
				}
				desc, err = sqlbase.GetTableDescriptorUseTxn(txn, cleanedDbName, table.SchemaName, table.TableName)
				if err != nil {
					return err
				}
			} else {
				desc = table.Desc
			}
			desc, err = offlineImportTable(ctx, txn, desc)
			if err != nil {
				return err
			}
			importDetails.Tables[i].Desc = desc
		}
		importDetails.PrepareComplete = true
		// Update the job once all descs have been prepared for ingestion.
		err = r.job.WithTxn(txn).SetDetails(ctx, importDetails)
		return err
	})
}

// prepareTSTableDescsForIngestion works the same as prepareTableDescsForIngestion,
// except it uses nil transaction cause can not create timeseries table at a transaction.
func (r *importResumer) prepareTSTableDescsForIngestion(
	ctx context.Context, p sql.PlanHookState, details jobspb.ImportDetails,
) error {
	dbName := details.DatabaseName
	cleanedDbName := strings.Trim(dbName, "\"")
	var err error
	if details.IsDatabase {
		createDatabase := fmt.Sprintf(`CREATE TS DATABASE %s;`, dbName)
		if _, err := p.ExecCfg().InternalExecutor.Exec(ctx, `import-create-ts-database`, nil, createDatabase); err != nil {
			return err
		}
		// Add COMMENT to the database
		if details.DatabaseComment != "" {
			if err = execCommentOnMeta(ctx, p, details.DatabaseComment, cleanedDbName); err != nil {
				return err
			}
		}
	}
	for _, table := range details.Tables {
		if table.IsNew {
			if err = execCreateTableMeta(ctx, p, table.Create, cleanedDbName); err != nil {
				return err
			}
			// Add COMMENT to the table
			if table.TableComment != "" {
				if err = execCommentOnMeta(ctx, p, table.TableComment, cleanedDbName); err != nil {
					return err
				}
			}
			// Add COMMENT to the column
			if table.ColumnComment != nil {
				for _, colComment := range table.ColumnComment {
					if err = execCommentOnMeta(ctx, p, colComment, cleanedDbName); err != nil {
						return err
					}
				}
			}
			// Add INDEX to the column
			if table.ColumnIndex != nil {
				for _, index := range table.ColumnIndex {
					if err = execIndexOnMeta(ctx, p, index, cleanedDbName); err != nil {
						return err
					}
				}
			}
		}
	}
	// Exec Privileges statement
	if details.Privileges != nil {
		if err = execPrivilegesSQL(ctx, p, details.Privileges, strings.Trim(details.DatabaseName, "\"")); err != nil {
			return err
		}
	}

	return p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		defer p.ExecCfg().InternalExecutor.SetSessionData(new(sessiondata.SessionData))
		importDetails := details
		var err error
		var desc *sqlbase.TableDescriptor
		for i, table := range details.Tables {
			if table.IsNew {
				if desc, err = sqlbase.GetTableDescriptorUseTxn(txn, cleanedDbName, tree.PublicSchema, table.TableName); err != nil {
					return err
				}
				importDetails.Tables[i].Desc = desc
			} else {
				importDetails.Tables[i].Desc = table.Desc
			}
		}
		importDetails.PrepareComplete = true
		// Update the job once all descs have been prepared for ingestion.
		err = r.job.WithTxn(txn).SetDetails(ctx, importDetails)
		return err
	})
}

// offlineImportTable set table's State offline.
func offlineImportTable(
	ctx context.Context, txn *kv.Txn, desc *sqlbase.TableDescriptor,
) (*sqlbase.TableDescriptor, error) {
	if len(desc.Mutations) > 0 {
		return nil, errors.Errorf("can not IMPORT INTO a table witch has schema changes in progress -- try again later (pending mutation %s)", desc.Mutations[0].String())
	}
	// TODO(dt): Ensure no other schema changes can start during ingest.
	importing := *desc
	// Take the table offline for import.
	// TODO(dt): audit everywhere we get table descs (leases or otherwise) to
	// ensure that filtering by state handles IMPORTING correctly.
	importing.State = sqlbase.TableDescriptor_OFFLINE
	importing.OfflineReason = "importing"
	importing.Version++
	// TODO(dt): de-validate all the FKs.

	if err := txn.SetSystemConfigTrigger(); err != nil {
		return nil, err
	}

	// Note that this CPut is safe with respect to mixed-version descriptor
	// upgrade and downgrade, because IMPORT does not operate in mixed-version
	// states.
	// TODO(jordan,lucy): remove this comment once 19.2 is released.
	existingDesc, err := sqlbase.ConditionalGetTableDescFromTxn(ctx, txn, desc)
	if err != nil {
		return nil, errors.Wrap(err, "another operation is operating on the table now")
	}
	err = txn.CPut(ctx,
		sqlbase.MakeDescMetadataKey(desc.ID),
		sqlbase.WrapDescriptor(&importing),
		existingDesc)
	if err != nil {
		return nil, errors.Wrap(err, "another operation is operating on the table now")
	}

	return &importing, nil
	// NB: we need to wait for the schema change to show up before it is safe
	// to ingest, but rather than do that here, we'll wait for this schema
	// change in the job's Resume hook, before running the ingest phase. That
	// will hopefully let it get a head start on propagating, plus the more we
	// do in the job, the more that has automatic cleanup on rollback.
}

// showResult send result to client.
func showResult(
	resultsCh chan<- tree.Datums,
	jobID int64,
	rows int64,
	abandonRows int,
	rejectRows int,
	timeSeries bool,
) {
	var jobIDStr string
	if timeSeries {
		jobIDStr = "-"
	} else {
		jobIDStr = strconv.FormatInt(jobID, 10)
	}
	note := "None"
	if rejectRows != 0 {
		note = "There is a REJECT FILE, please read and resolve it"
	}
	resultsCh <- tree.Datums{
		tree.NewDString(jobIDStr),
		tree.NewDString(string(jobs.StatusSucceeded)),
		tree.NewDFloat(tree.DFloat(1.0)),
		tree.NewDInt(tree.DInt(rows)),
		tree.NewDString(strconv.Itoa(abandonRows)),
		tree.NewDString(strconv.Itoa(rejectRows)),
		tree.NewDString(note),
	}
}

// publishTables updates the status of imported tables from OFFLINE to PUBLIC.
func (r *importResumer) publishTables(ctx context.Context, execCfg *sql.ExecutorConfig) error {
	details := r.job.Details().(jobspb.ImportDetails)
	// Tables should only be published once.
	if details.TablesPublished {
		return nil
	}
	log.Event(ctx, "making tables live")

	// Needed to trigger the schema change manager.
	err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		b := txn.NewBatch()
		for _, tbl := range details.Tables {
			tableDesc := *tbl.Desc
			tableDesc.State = sqlbase.TableDescriptor_PUBLIC
			tableDesc.Version++

			if !tbl.IsNew {
				// NB: This is not using AllNonDropIndexes or directly mutating the
				// constraints returned by the other usual helpers because we need to
				// replace the `OutboundFKs` and `Checks` slices of tableDesc with copies
				// that we can mutate. We need to do that because tableDesc is a shallow
				// copy of tbl.Desc that we'll be asserting is the current version when we
				// CPut below.
				//
				// Set FK constraints to unvalidated before publishing the table imported
				// into.
				setFKConstraint(&tableDesc, tbl)

				// Set CHECK constraints to unvalidated before publishing the table imported into.
				setCheckConstraints(&tableDesc, tbl)
			}

			// TODO(dt): re-validate any FKs?
			// Note that this CPut is safe with respect to mixed-version descriptor
			// upgrade and downgrade, because IMPORT does not operate in mixed-version
			// states.
			// TODO(jordan,lucy): remove this comment once 19.2 is released.
			existingDesc, err := sqlbase.ConditionalGetTableDescFromTxn(ctx, txn, tbl.Desc)
			if err != nil {
				return errors.WrapWithDepth(1, err, "publishing tables")
			}
			b.CPut(
				sqlbase.MakeDescMetadataKey(tableDesc.ID),
				sqlbase.WrapDescriptor(&tableDesc),
				existingDesc)
		}
		if err := txn.Run(ctx, b); err != nil {
			return errors.WrapWithDepth(1, err, "publishing tables")
		}

		// Update job record to mark tables published state as complete.
		details.TablesPublished = true
		err := r.job.WithTxn(txn).SetDetails(ctx, details)
		if err != nil {
			return errors.WrapWithDepth(1, err, "updating job details after publishing tables")
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Initiate a run of CREATE STATISTICS. We don't know the actual number of
	// rows affected per table, so we use a large number because we want to make
	// sure that stats always get created/refreshed here.
	for i := range details.Tables {
		execCfg.StatsRefresher.NotifyMutation(details.Tables[i].Desc.ID, math.MaxInt32 /* rowsAffected */)
	}

	return nil
}

func setCheckConstraints(tableDesc *sqlbase.TableDescriptor, tbl sqlbase.ImportTable) {
	tableDesc.Checks = make([]*sqlbase.TableDescriptor_CheckConstraint, len(tbl.Desc.Checks))
	for i, c := range tbl.Desc.AllActiveAndInactiveChecks() {
		ck := *c
		ck.Validity = sqlbase.ConstraintValidity_Unvalidated
		tableDesc.Checks[i] = &ck
	}
}

// setFKConstraint set table desc OutboundFKs.Validity unvalidated.
func setFKConstraint(tableDesc *sqlbase.TableDescriptor, tbl sqlbase.ImportTable) {
	tableDesc.OutboundFKs = make([]sqlbase.ForeignKeyConstraint, len(tableDesc.OutboundFKs))
	copy(tableDesc.OutboundFKs, tbl.Desc.OutboundFKs)
	for i := range tableDesc.OutboundFKs {
		tableDesc.OutboundFKs[i].Validity = sqlbase.ConstraintValidity_Unvalidated
	}
}

// releaseProtectedTimestamp release the protected timestamp.
func (r *importResumer) releaseProtectedTimestamp(
	ctx context.Context, pts protectedts.Storage, txn *kv.Txn,
) error {
	ptsID := r.job.Details().(jobspb.ImportDetails).ProtectedTimestampRecord
	// If the job doesn't have a protected timestamp then there's nothing to do.
	if ptsID == nil {
		return nil
	}
	err := pts.Release(ctx, txn, *ptsID)
	if errors.Is(err, protectedts.ErrNotExists) {
		// No reason to return an error which might cause problems if it doesn't
		// seem to exist.
		log.Warningf(ctx, "failed to release protected which seems not to exist: %v", err)
		err = nil
	}
	return err
}

// dropTables implements the OnFailOrCancel logic.
func (r *importResumer) dropTables(
	ctx context.Context, jr *jobs.Registry, txn *kv.Txn, executor *sql.InternalExecutor,
) error {
	details := r.job.Details().(jobspb.ImportDetails)
	// Needed to trigger the schema change manager.
	if err := txn.SetSystemConfigTrigger(); err != nil {
		return err
	}
	// If the prepare step of the import job was not completed then the
	// descriptors do not need to be rolled back as the txn updating them never
	// completed.
	if !details.PrepareComplete {
		return nil
	}

	b := txn.NewBatch()
	for _, tbl := range details.Tables {
		if !tbl.IsNew && details.Walltime != 0 { /* Rolling back data on existing tables */
			// The walltime can be 0 if there is a failure between publishing the tables
			// as OFFLINE and then choosing a ingestion timestamp. This might happen
			// while waiting for the descriptor version to propagate across the cluster
			// for example.
			//
			// In this case, we don't want to rollback the data since data ingestion has
			// not yet begun (since we have not chosen a timestamp at which to ingest.)
			// NB: if a revert fails it will abort the rest of this failure txn, which is
			// also what brings tables back online. We _could_ change the error handling
			// or just move the revert into Resume()'s error return path, however it isn't
			// clear that just bringing a table back online with partially imported data
			// that may or may not be partially reverted is actually a good idea. It seems
			// better to do the revert here so that the table comes back if and only if,
			// it was rolled back to its pre-IMPORT state, and instead provide a manual
			// admin knob (e.g. ALTER TABLE REVERT TO SYSTEM TIME) if anything goes wrong.
			ts := hlc.Timestamp{WallTime: details.Walltime}.Prev()
			if err := sql.RevertTables(ctx, txn.DB(), []*sqlbase.TableDescriptor{tbl.Desc}, ts, sql.RevertTableDefaultBatchSize); err != nil {
				return errors.Wrap(err, "rolling back partially completed IMPORT")
			}
		}
		// Set all table states to public before dropping
		if !details.TablesPublished {
			if err := setTableStatePublic(ctx, txn, b, *tbl.Desc); err != nil {
				return err
			}
		}
	}
	if err := txn.Run(ctx, b); err != nil {
		return errors.Wrap(err, "rolling back tables")
	}
	if details.IsDatabase { /* Delete Database */
		dropDatabase := fmt.Sprintf(`DROP DATABASE %s CASCADE`, details.DatabaseName)
		if _, err := executor.Exec(ctx, `import-drop-database`, txn, dropDatabase); err != nil {
			return err
		}
	} else {
		tbl := details.Tables[0]
		if tbl.IsNew { /* Delete tables created during import */
			dropTable := fmt.Sprintf(`DROP TABLE %s`, strings.Join([]string{details.DatabaseName, tbl.SchemaName, tbl.TableName}, `.`))
			if _, err := executor.Exec(ctx, `import-drop-table`, txn, dropTable); err != nil {
				return err
			}
		}
	}
	return nil
}

// setTableStatePublic set table state public and then this table can provide services.
func setTableStatePublic(
	ctx context.Context, txn *kv.Txn, b *kv.Batch, tableDesc sqlbase.TableDescriptor,
) error {
	existingDesc, err := sqlbase.ConditionalGetTableDescFromTxn(ctx, txn, &tableDesc)
	if err != nil {
		return errors.Wrap(err, "rolling back tables")
	}
	tableDesc.Version++
	// IMPORT did not create this table, so we should not drop it.
	tableDesc.State = sqlbase.TableDescriptor_PUBLIC
	// Note that this CPut is safe with respect to mixed-version descriptor
	// upgrade and downgrade, because IMPORT does not operate in mixed-version
	// states.
	// TODO(jordan,lucy): remove this comment once 19.2 is released.
	b.CPut(
		sqlbase.MakeDescMetadataKey(tableDesc.ID),
		sqlbase.WrapDescriptor(&tableDesc),
		existingDesc)
	return nil
}

var _ jobs.Resumer = &importResumer{}

func init() {
	sql.AddPlanHook(importPlanHook)
	jobs.RegisterConstructor(
		jobspb.TypeImport,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &importResumer{
				job:      job,
				settings: settings,
			}
		},
	)
}

// Read clustersetting.sql files, check file contents, execute SQL statements
func execClusterSettingSQL(ctx context.Context, p sql.PlanHookState, s string) error {
	es, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, s)
	if err != nil {
		return err
	}
	defer es.Close()
	f, err := es.ReadFile(ctx, "")
	if err != nil {
		return err
	}
	defer f.Close()
	settingStr, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	stmts, err := parser.Parse(string(settingStr))
	if err != nil {
		return err
	}
	// Check the syntax in the SQL file
	for i := range stmts {
		// SET CLUSTER SETTING
		if _, ok := stmts[i].AST.(*tree.SetClusterSetting); ok {
			_, err = p.ExecCfg().InternalExecutor.Exec(
				ctx, "import_settings", nil,
				stmts[i].SQL,
			)
			if err != nil {
				return err
			}
			continue
		}
		return errors.Errorf("The cluster setting file does not exist in the path :%s", s)
	}
	return nil
}

// Read user.sql files, check file contents, execute SQL statements
func execUserSQL(ctx context.Context, p sql.PlanHookState, s string) (int, error) {
	es, err := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI(ctx, s)
	var row int
	if err != nil {
		return row, err
	}
	defer es.Close()
	f, err := es.ReadFile(ctx, "")
	if err != nil {
		return row, errors.Errorf("The sql file does not exist in the path: %s", s)
	}
	defer f.Close()
	userStr, err := ioutil.ReadAll(f)
	if err != nil {
		return row, err
	}
	stmts, err := parser.Parse(string(userStr))
	if err != nil {
		return row, err
	}
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Check the syntax in the SQL file
		for i := range stmts {
			// CREATE USER / CREATE ROLE
			if _, ok := stmts[i].AST.(*tree.CreateRole); ok {
				_, err = p.ExecCfg().InternalExecutor.Exec(
					ctx, "import_users", txn,
					stmts[i].SQL,
				)
				if err != nil {
					return err
				}
				continue
			}
			// GRANT TO
			if _, ok := stmts[i].AST.(*tree.GrantRole); ok {
				_, err = p.ExecCfg().InternalExecutor.Exec(
					ctx, "import_grant", txn,
					stmts[i].SQL,
				)
				if err != nil {
					return err
				}
				continue
			}
			return errors.Errorf("The SQL statement in file :%s must be CREATE USER or CREATE ROLE or GRANT TO", s)
		}
		return nil
	}); err != nil {
		return row, err
	}
	return len(stmts), nil
}

// execCreateTableMeta exec grant to sql use InternalExecutor.
func execPrivilegesSQL(
	ctx context.Context, p sql.PlanHookState, privileges []string, dbName string,
) error {
	return p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		p.ExecCfg().InternalExecutor.SetSessionData(&sessiondata.SessionData{Database: dbName})
		defer p.ExecCfg().InternalExecutor.SetSessionData(new(sessiondata.SessionData))
		for _, privilege := range privileges {
			_, err := p.ExecCfg().InternalExecutor.Exec(ctx, "create privileges", txn, privilege)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
