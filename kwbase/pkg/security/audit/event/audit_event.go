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

package event

import (
	"context"
	"errors"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/actions"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target/object"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/rules"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/server"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

type auditTableVersion struct {
	version uint32
	syncutil.Mutex
}

// AuditEvent map event to metric and with logger for table write
type AuditEvent struct { // used by log
	ctx          context.Context
	eventMetric  map[target.AuditObjectType]target.AuditMetrics // map for event and metrics for it
	eventAction  map[actions.ActionID]actions.AuditAction
	execCfg      *sql.ExecutorConfig
	auditQuery   rules.AuditInquirers
	tableVersion auditTableVersion
}

// InitEvents init audit event
func InitEvents(
	ctx context.Context, execCfg *sql.ExecutorConfig, registry *metric.Registry,
) *AuditEvent {
	// set audit log file mode
	execCfg.AuditLogger.SetFileMode(0600)
	// get new instance
	var auditEvent = &AuditEvent{
		ctx:        ctx,
		execCfg:    execCfg,
		auditQuery: rules.NewAuditStrategyInquirers(ctx, execCfg.InternalExecutor),
		eventAction: map[actions.ActionID]actions.AuditAction{
			//actions.MailSend:       actions.NewMailAction(mail),
			actions.LogRecord: actions.NewRecordToLog(execCfg.AuditLogger),
			//actions.WriteIntoTable: actions.NewRecodeToTable(execCfg.DB, eventlog.MakeEventLogger(execCfg)),
		},
		eventMetric: map[target.AuditObjectType]target.AuditMetrics{
			target.ObjectNode:           object.NewNodeMetric(ctx),
			target.ObjectCluster:        object.NewClusterMetric(ctx),
			target.ObjectClusterSetting: object.NewClusterSettingMetric(ctx),
			target.ObjectStore:          object.NewStoreMetric(ctx),
			target.ObjectConn:           object.NewConnMetric(ctx),
			target.ObjectUser:           object.NewUserMetric(ctx),
			target.ObjectRole:           object.NewRoleMetric(ctx),
			target.ObjectDatabase:       object.NewDatabaseMetric(ctx),
			target.ObjectSchema:         object.NewSchemaMetric(ctx),
			target.ObjectTable:          object.NewTableMetric(ctx),
			target.ObjectView:           object.NewViewMetric(ctx),
			target.ObjectIndex:          object.NewIndexMetric(ctx),
			target.ObjectFunction:       object.NewFunctionMetric(ctx),
			target.ObjectProcedure:      object.NewProcedureMetric(ctx),
			target.ObjectTrigger:        object.NewTriggerMetric(ctx),
			target.ObjectSequence:       object.NewSequenceMetric(ctx),
			target.ObjectPrivilege:      object.NewPrivilegeMetric(ctx),
			target.ObjectAudit:          object.NewAuditMetric(ctx),
			target.ObjectRange:          object.NewRangeMetric(ctx),
			target.ObjectCDC:            object.NewChangeFeedMetric(ctx),
			target.ObjectQuery:          object.NewQueryMetric(ctx),
			target.ObjectJob:            object.NewJobMetric(ctx),
			target.ObjectSchedule:       object.NewScheduleMetric(ctx),
			target.ObjectSession:        object.NewSessionMetric(ctx),
		},
	} // register all event metric to metric.Registry
	auditEvent.tableVersion.version = 1
	if registry != nil {
		for _, ms := range auditEvent.eventMetric {
			metrics := ms.RegisterMetric()
			for _, v := range metrics {
				registry.AddMetricStruct(v)
			}
		}
	}
	return auditEvent
}

// SetTableVersion update the table version to refresh auditStrategyCache.
func (ae *AuditEvent) SetTableVersion(version uint32) {
	ae.tableVersion.Lock()
	defer ae.tableVersion.Unlock()
	ae.tableVersion.version = version
}

// GetTableVersion returns table version.
func (ae *AuditEvent) GetTableVersion() uint32 {
	ae.tableVersion.Lock()
	v := ae.tableVersion.version
	ae.tableVersion.Unlock()
	return v
}

// LogAudit : HandleEvent check event, log and metric it
func (ae *AuditEvent) LogAudit(txn *kv.Txn, auditInfo *server.AuditInfo) error {
	if auditInfo == nil {
		return errors.New("auditInfo is a nil pointer")
	}
	if auditInfo.GetTargetInfo() == nil || auditInfo.IsTargetEmpty() || auditInfo.IsOperationEmpty() {
		return nil
	}
	// metric event
	ms, exist := ae.eventMetric[auditInfo.GetTargetInfo().GetTargetType()]
	if !exist {
		return nil
	}
	if !ms.Metric(auditInfo.GetEvent()) {
		return nil
	}
	auditInfo.SetAuditLevel(ms.GetAuditLevel(auditInfo.GetEvent()))

	// determine if audit
	if !ae.IsAudit(txn, auditInfo) {
		return nil
	}

	for _, ac := range ae.eventAction {
		if err := ac.Action(ae.ctx, auditInfo); err != nil {
			return err
		}
	}
	return nil
}

// IsAudit returns true if the current statement has corresponding strategy.
func (ae *AuditEvent) IsAudit(txn *kv.Txn, info *server.AuditInfo) bool {
	if info == nil {
		return false
	}
	targetInfo, userInfo, retInfo := info.GetTargetInfo(), info.GetAuditUser(), info.GetAuditResult()
	if targetInfo == nil || userInfo == nil {
		return false
	}

	if info.GetAuditLevel() == target.UnknownLevel {
		return false
	}
	if info.GetAuditLevel() == target.SystemLevel {
		return true
	}

	strategies := ae.AuditsOfWithOption(ae.ctx, txn)
	for _, es := range strategies {
		if es.CheckAuditTargetType(targetInfo.GetTargetType()) &&
			es.CheckAuditOperation(info.GetEvent()) &&
			es.CheckAuditUser(userInfo.GetUserName()) &&
			es.CheckEventResult(retInfo.GetResult()) {
			if info.GetAuditLevel() == target.StmtLevel {
				return true
			}
			if es.CheckAuditTargetIDs(targetInfo.GetTargetIDs()) {
				return true
			}
		}
	}
	return false
}

// GetMetric return metrics for specified event type
func (ae *AuditEvent) GetMetric(eventType target.AuditObjectType) target.AuditMetrics {
	return ae.eventMetric[eventType]
}

// InitAuditStrategy returns audit strategy
func (ae *AuditEvent) InitAuditStrategy() {
	auditsStrategy.Lock()
	auditsStrategy.auditStrategy = make(map[string]rules.Strategy)
	auditsStrategy.tableVersion = 1
	auditsStrategy.Unlock()

	s, err := ae.auditQuery.QueryAuditPolicy(ae.ctx, nil)
	if err != nil {
		log.Errorf(ae.ctx, "get error when init audit strategy, err: %s", err)
	}
	auditsStrategy.Lock()
	auditsStrategy.auditStrategy = s
	auditsStrategy.Unlock()
}
