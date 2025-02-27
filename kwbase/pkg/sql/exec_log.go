// Copyright 2018 The Cockroach Authors.
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
	"net"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/server"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/setting"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

// This file contains facilities to report SQL activities to separate
// log files.
//
// The log format is currently as follows:
//
// Example audit log line:
// I180211 07:30:48.832004 317 sql/exec_log.go:90  [client=127.0.0.1:62503,user=root,n1] 13 exec "kwbase" {"ab"[53]:READ} "SELECT * FROM ab" {} 123.45 12 OK 0
// I180211 07:30:48.832004 317 sql/exec_log.go:90  [client=127.0.0.1:62503,user=root,n1] 13 exec "kwbase" {"ab"[53]:READ} "SELECT nonexistent FROM ab" {} 0.123 12 ERROR 0
// Example execution log:
// I180211 07:30:48.832004 317 sql/exec_log.go:90  [client=127.0.0.1:62503,user=root,n1] 13 exec "kwbase" {} "SELECT * FROM ab" {} 123.45 12 OK 0
// I180211 07:30:48.832004 317 sql/exec_log.go:90  [client=127.0.0.1:62503,user=root,n1] 13 exec "kwbase" {} "SELECT nonexistent FROM ab" {} 0.123 0 "column \"nonexistent\" not found" 0
//
// Explanation of fields:
// I180211 07:30:48.832004 317 sql/exec_log.go:90  [client=127.0.0.1:62503,user=root,n1] 13 exec "kwbase" {"ab"[53]:READ} "SELECT nonexistent FROM ab" {} 0.123 12 ERROR 0
// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  \                                                                                                                                   .../
//   '- prefix generated by CockroachDB's standard log package. Contains:
//
// - date and time
//   - incidentally, this data is needed for auditing.
// - goroutine ID
// - where in this file the message was generated
// - the logging tags [...] - this contains the client address,
//   username, and node ID
//   - tags were populated in the logging context when setting up
//     the pgwire session.
//   - generally useful and actually a requirement for auditing.
// - a counter for the logging entry, monotonically increasing
//   from the point the process started.
//   - this is a requiredement for auditing too.
//
//  .-----------------------------------------------------------------------------------------------------------------------------.../
//  |
//  '- log message generated in this file. Includes:
//
//  - a label indicating where the data was generated - useful for troubleshooting.
//    - distinguishes e.g. exec, prepare, internal-exec, etc.
//  - the current value of `application_name`
//    - required for auditing, also helps filter out messages from a specific app.
//  - the logging trigger.
//    - "{}" for execution logs: any activity is worth logging in the exec log
//    - the list of triggering tables and access modes for audit
//      events. This is needed for auditing.
//  - the full text of the query.
//    - audit logs really ought to only "identify the data that's accessed but
//      without revealing PII". We don't know how to separate those two things
//      yet so the audit log contains the full SQL of the query even though
//      this may yield PII. This may need to be addressed later.
//  - the placeholder values. Useful for queries using placehodlers.
//    - "{}" when there are no placeholders.
//  - the query execution time in milliseconds. For troubleshooting.
//  - the number of rows that were produced. For troubleshooting.
//  - the status of the query (OK for success, ERROR or full error
//    message upon error). Needed for auditing and troubleshooting.
//  - the number of times the statement was retried automatically
//    by the server so far.

// logStatementsExecuteEnabled causes the Executor to log executed
// statements and, if any, resulting errors.
var logStatementsExecuteEnabled = settings.RegisterPublicBoolSetting(
	"sql.trace.log_statement_execute",
	"set to true to enable logging of executed statements",
	false,
)

var slowQueryLogThreshold = settings.RegisterPublicDurationSetting(
	"sql.log.slow_query.latency_threshold",
	"when set to non-zero, log statements whose service latency exceeds "+
		"the threshold to a secondary logger on each node",
	0,
)

type executorType int

const (
	executorTypeExec executorType = iota
	executorTypeInternal
)

// vLevel returns the vmodule log level at which logs from the given executor
// should be written to the logs.
func (s executorType) vLevel() int32 { return int32(s) + 2 }

var logLabels = []string{"exec", "exec-internal"}

// logLabel returns the log label for the given executor type.
func (s executorType) logLabel() string { return logLabels[s] }

// maybeLogStatement conditionally records the current statement
// (p.curPlan) to the exec / audit logs.
func (p *planner) maybeLogStatement(
	ctx context.Context,
	execType executorType,
	numRetries, rows int,
	err error,
	queryReceived time.Time,
) {
	p.maybeLogStatementInternal(ctx, execType, numRetries, rows, err, queryReceived)
}

func (p *planner) maybeLogStatementInternal(
	ctx context.Context, execType executorType, numRetries, rows int, err error, startTime time.Time,
) {
	// Note: if you find the code below crashing because p.execCfg == nil,
	// do not add a test "if p.execCfg == nil { do nothing }" !
	// Instead, make the logger work. This is critical for auditing - we
	// can't miss any statement.
	logV := log.V(2)
	logExecuteEnabled := logStatementsExecuteEnabled.Get(&p.execCfg.Settings.SV)
	slowLogThreshold := slowQueryLogThreshold.Get(&p.execCfg.Settings.SV)
	slowQueryLogEnabled := slowLogThreshold != 0
	auditLogEnabled := setting.AuditEnabled.Get(&p.execCfg.Settings.SV)

	// all type need to audit will set auditEventsDetected to true
	var auditEventsDetected bool
	targetType := p.curPlan.stmt.AST.StatTargetType()
	if auditLogEnabled {
		auditEventsDetected = true
		if len(targetType) == 0 {
			auditEventsDetected = false
		}
		if !auditEventsDetected {
			if p.curPlan.auditInfo != nil {
				auditEventsDetected = len(p.curPlan.auditInfo.GetTargetType()) != 0
			}
		}
		if target.OperationType(p.curPlan.stmt.AST.StatOp()) == target.Export {
			auditEventsDetected = false
		}

	}

	if !logV && !logExecuteEnabled && !auditEventsDetected && !slowQueryLogEnabled {
		return
	}
	// Logged data, in order:

	// label passed as argument.
	appName := p.EvalContext().SessionData.ApplicationName
	stmtStr := p.curPlan.stmt.String()

	logTrigger := "{}"
	plStr := p.extendedEvalCtx.Placeholders.Values.String()
	queryDuration := timeutil.Now().Sub(startTime)
	age := float64(queryDuration.Nanoseconds()) / 1e6

	// rows passed as argument.

	execErrStr := ""
	if err != nil {
		execErrStr = err.Error()
	}

	if auditLogEnabled {
		var auditTxn *kv.Txn
		if p.Txn() != nil && p.Txn().IsOpen() {
			auditTxn = p.Txn()
		} else {
			auditTxn = p.execCfg.DB.NewTxn(ctx, "audit txn")
		}

		objDesc, e := p.PhysicalSchemaAccessor().GetObjectDesc(ctx, auditTxn, p.ExecCfg().Settings, &auditTableName,
			p.ObjectLookupFlags(true /*required*/, false /*requireMutable*/))
		if e != nil {
			log.Warningf(ctx, "got error when set audit strategy version, err:%s", e)
		} else {
			tableVersion := uint32(objDesc.TableDesc().Version)
			if p.execCfg.AuditServer.GetHandler().GetTableVersion() != tableVersion {
				p.execCfg.AuditServer.GetHandler().SetTableVersion(tableVersion)
			}
		}

		p.LogAudit(ctx, auditTxn, rows, err, startTime)
	}

	lbl := execType.logLabel()
	if slowQueryLogEnabled && queryDuration > slowLogThreshold {
		logger := p.execCfg.SlowQueryLogger
		logger.Logf(ctx, "%.3fms %s %q %s %q %s %d %q %d",
			age, lbl, appName, logTrigger, stmtStr, plStr, rows, execErrStr, numRetries)
	}
	if logExecuteEnabled {
		logger := p.execCfg.ExecLogger
		logger.Logf(ctx, "%s %q %s %q %s %.3f %d %q %d",
			lbl, appName, logTrigger, stmtStr, plStr, age, rows, execErrStr, numRetries)
	}
	if logV {
		// Copy to the main log.
		log.VEventf(ctx, execType.vLevel(), "%s %q %s %q %s %.3f %d %q %d",
			lbl, appName, logTrigger, stmtStr, plStr, age, rows, execErrStr, numRetries)
	}
}

// maybeAudit marks the current plan being constructed as flagged
// for auditing if the table being touched has an auditing mode set.
// This is later picked up by maybeLogStatement() above.
//
// It is crucial that this gets checked reliably -- we don't want to
// miss any statements! For now, we call this from CheckPrivilege(),
// as this is the function most likely to be called reliably from any
// caller that also uses a descriptor. Future changes that move the
// call to this method elsewhere must find a way to ensure that
// contributors who later add features do not have to remember to call
// this to get it right.

func (p *planner) maybeAudit(desc sqlbase.DescriptorProto, priv privilege.Kind) {
	if sqlbase.IsReservedID(desc.GetID()) {
		return
	}
	var targetType target.AuditObjectType
	if d, ok := desc.(*sqlbase.ImmutableTableDescriptor); ok {
		if d.IsVirtualTable() || d.IsSequence() {
			return
		}
		targetType = target.ObjectTable
		if d.IsView() {
			targetType = target.ObjectView
		}
	}

	switch priv {
	case privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE:
		if p.curPlan.stmt != nil && priv.String() == p.curPlan.stmt.AST.StatOp() {
			p.SetAuditTargetAndType(uint32(desc.GetID()), desc.GetName(), nil, targetType)
		}
	}
}

// auditEvent represents an audit event for a single table.
type auditEvent struct {
	targetType string
	id         map[uint64]bool
}

func (p *planner) SetAuditTargetAndType(
	id uint32, name string, cascade []string, targetType target.AuditObjectType,
) {
	p.SetAuditTarget(id, name, cascade)
	p.SetTargetType(targetType)
}

func (p *planner) SetAuditTarget(id uint32, name string, cascade []string) {
	if p.curPlan.auditInfo == nil {
		p.InitAuditInfo()
	}
	p.curPlan.auditInfo.SetTarget(id, name, cascade)
}

func (p *planner) SetTargetType(objType target.AuditObjectType) {
	if p.curPlan.auditInfo == nil {
		p.InitAuditInfo()
	}
	p.curPlan.auditInfo.SetTargetType(objType)
}

func (p *planner) SetAuditEvent() {
	if p.curPlan.auditInfo == nil {
		p.curPlan.auditInfo = &server.AuditInfo{}
	}
	oprType := target.OperationType(p.curPlan.stmt.AST.StatOp())
	p.curPlan.auditInfo.SetEventType(oprType)
}

func (p *planner) SetAuditLevel(level target.AuditLevelType) {
	if p.curPlan.auditInfo == nil {
		p.curPlan.auditInfo = &server.AuditInfo{}
	}
	p.curPlan.auditInfo.SetAuditLevel(level)
}

func (p *planner) InitAuditInfo() {
	p.curPlan.auditInfo = &server.AuditInfo{}
}

func (p *planner) LogAudit(
	ctx context.Context, txn *kv.Txn, rows int, err error, startTime time.Time,
) {
	if p.curPlan.auditInfo == nil {
		p.InitAuditInfo()
	}

	auditInfo := p.curPlan.auditInfo
	p.SetBasicAuditInfo(startTime, rows, err)
	if p.curPlan.stmt != nil && p.curPlan.stmt.AST != nil {
		auditInfo.SetTargetType(target.AuditObjectType(p.curPlan.stmt.AST.StatTargetType()))
		auditInfo.SetEventType(target.OperationType(p.curPlan.stmt.AST.StatOp()))
	}
	if e := p.execCfg.AuditServer.LogAudit(ctx, txn, auditInfo); e != nil {
		log.Warningf(ctx, "got error when log audit info, err:%s", e)
	}
}

func (p *planner) SetAuditInfo(
	ctx context.Context,
	txn *kv.Txn,
	startTime time.Time,
	objTyp target.AuditObjectType,
	oprTyp target.OperationType,
	err error,
) {
	if p.curPlan.auditInfo == nil {
		p.InitAuditInfo()
	}

	auditInfo := p.curPlan.auditInfo
	p.SetBasicAuditInfo(startTime, 0, err)
	if p.curPlan.stmt != nil && p.curPlan.stmt.AST != nil {
		auditInfo.SetTargetType(objTyp)
		auditInfo.SetEventType(oprTyp)
	}
	if e := p.execCfg.AuditServer.LogAudit(ctx, txn, auditInfo); e != nil {
		log.Warningf(ctx, "got error when log audit info, err:%s", e)
	}
}

func (p *planner) SetBasicAuditInfo(startTime time.Time, rows int, err error) {
	if p.curPlan.auditInfo == nil {
		p.InitAuditInfo()
	}

	auditInfo := p.curPlan.auditInfo
	auditInfo.SetTimeAndElapsed(startTime)
	// set user info value
	if p.SessionData() != nil {
		auditInfo.SetUser(p.SessionData().User, p.SessionData().Roles)
	}
	auditInfo.SetClient(p.SessionData())
	auditInfo.SetResult(err, rows)
	if p.curPlan.stmt != nil {
		auditInfo.SetCommand(p.curPlan.stmt.String())
	}
	auditInfo.SetCommandOptions(p.extendedEvalCtx.Placeholders)
	if p.ExecCfg() != nil {
		nodeInfo := p.ExecCfg().NodeInfo
		hostIP, hostPort, _ := net.SplitHostPort(p.ExecCfg().Addr)
		auditInfo.SetReporter(nodeInfo.ClusterID(), nodeInfo.NodeID.Get(), hostIP, hostPort, "", 0)
	}
}
