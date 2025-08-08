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

package sql

import (
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	duckdb "github.com/duckdb-go-bindings"
)

// MaxTSDBNameLength represents the maximum length of ts database name.
const MaxTSDBNameLength = 63

// createDatabaseNode represents the planNode for createDatabase
type createDatabaseNode struct {
	n *tree.CreateDatabase
}

// CreateDatabase creates a database.
// Privileges: superuser.
//
// Parameters:
// - n: createDatabase AST
//
// Returns:
// CreateDatabase planNode
//
//	Notes: postgres requires superuser or "CREATEDB".
//	       mysql uses the mysqladmin command.
func (p *planner) CreateDatabase(ctx context.Context, n *tree.CreateDatabase) (planNode, error) {
	if n.EngineType == tree.EngineTypeTimeseries {
		// check the validity of name.
		if sqlbase.ContainsNonAlphaNumSymbol(n.Name.String()) {
			return nil, sqlbase.NewTSNameInvalidError(n.Name.String())
		}
		// check if the name is too long
		if len(n.Name) > MaxTSDBNameLength {
			return nil, sqlbase.NewTSNameOutOfLengthError("database", string(n.Name), MaxTSDBNameLength)
		}
	}
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	if tmpl := n.Template; tmpl != "" {
		// See https://www.postgresql.org/docs/current/static/manage-ag-templatedbs.html
		if !strings.EqualFold(tmpl, "template0") {
			return nil, unimplemented.NewWithIssuef(10151,
				"unsupported template: %s", tmpl)
		}
	}

	if enc := n.Encoding; enc != "" {
		// We only support UTF8 (and aliases for UTF8).
		if !(strings.EqualFold(enc, "UTF8") ||
			strings.EqualFold(enc, "UTF-8") ||
			strings.EqualFold(enc, "UNICODE")) {
			return nil, unimplemented.NewWithIssueDetailf(35882, "create.db.encoding",
				"unsupported encoding: %s", enc)
		}
	}

	if col := n.Collate; col != "" {
		// We only support C and C.UTF-8.
		if col != "C" && col != "C.UTF-8" {
			return nil, unimplemented.NewWithIssueDetailf(16618, "create.db.collation",
				"unsupported collation: %s", col)
		}
	}

	if ctype := n.CType; ctype != "" {
		// We only support C and C.UTF-8.
		if ctype != "C" && ctype != "C.UTF-8" {
			return nil, unimplemented.NewWithIssueDetailf(35882, "create.db.classification",
				"unsupported character classification: %s", ctype)
		}
	}

	if err := p.RequireAdminRole(ctx, "CREATE DATABASE"); err != nil {
		return nil, err
	}
	return &createDatabaseNode{n: n}, nil
}

func (n *createDatabaseNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("database"))
	log.Infof(params.ctx, "create database %s start, type: %s", n.n.Name, tree.EngineName(n.n.EngineType))
	desc := makeDatabaseDesc(n.n)
	desc.EngineType = n.n.EngineType

	// deal with the retentions of ts database
	if n.n.TSDatabase.DownSampling != nil {
		dbRetention, err := checkRetention(nil, nil, *n.n.TSDatabase.DownSampling)
		if err != nil {
			return err
		}
		desc.TsDb.Lifetime = dbRetention.lifetime
	} else {
		desc.TsDb.Lifetime = 0
	}
	// deal with the partition interval of ts database
	if n.n.TSDatabase.PartitionInterval != nil {
		switch n.n.TSDatabase.PartitionInterval.Unit {
		case "s", "second", "m", "minute", "h", "hour":
			return pgerror.Newf(pgcode.InvalidParameterValue, "unsupported partition interval unit: %s",
				n.n.TSDatabase.PartitionInterval.Unit)
		}
		partitionInterval := getTimeFromTimeInput(*n.n.TSDatabase.PartitionInterval)
		if partitionInterval <= 0 || partitionInterval > MaxLifeTime {
			return pgerror.Newf(pgcode.InvalidParameterValue, "partition interval %d%s is invalid, the time range is [1day, 1000year]",
				n.n.TSDatabase.PartitionInterval.Value, n.n.TSDatabase.PartitionInterval.Unit)
		}
		desc.TsDb.PartitionInterval = uint64(partitionInterval)
	} else {
		desc.TsDb.PartitionInterval = DefaultPartitionInterval
	}

	desc.TsDb.CreateTime = params.p.txn.ReadTimestamp()
	desc.TsDb.Creator = params.p.sessionDataMutator.data.User

	created, err := params.p.createDatabase(
		params.ctx, &desc, n.n.IfNotExists, tree.AsStringWithFQNames(n.n, params.Ann()))
	if err != nil {
		return err
	}
	if created {
		params.extendedEvalCtx.Tables.addUncommittedDatabase(
			desc.Name, desc.ID, dbCreated)
	}
	if n.n.Comment != "" {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"set-db-comment",
			params.p.Txn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"UPSERT INTO system.comments VALUES ($1, $2, 0, $3)",
			keys.DatabaseCommentType,
			desc.ID,
			n.n.Comment)
		if err != nil {
			return err
		}
	}

	params.p.SetAuditTarget(uint32(desc.GetID()), desc.GetName(), nil)
	if api.MppMode && !api.SingleNode {
		stmt := fmt.Sprintf("alter database %s CONFIGURE ZONE USING num_replicas = 1;", desc.Name)
		if _, err := params.ExecCfg().InternalExecutor.Exec(params.ctx, "set num_replicas", params.p.txn, stmt); err != nil {
			log.Errorf(params.ctx, "failed alter CONFIGURE ZONE USING num_replicas = 1")
		}
	}
	if desc.EngineType == tree.EngineTypeAP {
		dbPath := params.p.DistSQLPlanner().distSQLSrv.ServerConfig.ApEngine.DbPath
		var config duckdb.Config
		dsn := dbPath + "/" + desc.Name
		defer duckdb.DestroyConfig(&config)
		if duckdb.CreateConfig(&config) == duckdb.StateError {
			return pgerror.Newf(pgcode.Warning, "create config failed")
		}
		var db duckdb.Database
		defer duckdb.Close(&db)
		var errMsg string
		if duckdb.OpenExt(dsn, &db, config, &errMsg) == duckdb.StateError {
			return pgerror.Newf(pgcode.Warning, errMsg)
		}
	}
	log.Infof(params.ctx, "create database %s finished, type: %s, id: %d", desc.Name, tree.EngineName(n.n.EngineType), desc.ID)
	return nil
}

func (*createDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (*createDatabaseNode) Values() tree.Datums          { return tree.Datums{} }
func (*createDatabaseNode) Close(context.Context)        {}

// TSDatabaseUnsupportedErr return error if feature is not supported in timeseries database.
func TSDatabaseUnsupportedErr(engineType tree.EngineType, op string) error {
	if engineType == tree.EngineTypeTimeseries {
		return sqlbase.TSUnsupportedError(op)
	}
	return nil
}
