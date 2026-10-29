// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mlmgt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
)

// This file wires the sql package's planner into the mlmgt package. The
// mlmgt package deliberately does not depend on the sql package so that the
// ML management logic stays independent of the execution engine; everything
// it needs from the planner is funnelled through
// mlmgt.GenericPlannerAdapter.

// mlSettingsAdapter adapts cluster settings to mlmgt.SettingsAdapter.
type mlSettingsAdapter struct {
	s *cluster.Settings
}

// SV implements mlmgt.SettingsAdapter.
func (s *mlSettingsAdapter) SV() *settings.Values {
	return &s.s.SV
}

// mlRPCContextAdapter adapts the RPC context to mlmgt.RPCContextAdapter.
type mlRPCContextAdapter struct {
	r *rpc.Context
}

// AdvertiseAddr implements mlmgt.RPCContextAdapter.
func (r *mlRPCContextAdapter) AdvertiseAddr() string {
	return r.r.AdvertiseAddr
}

// GetClientTLSConfig implements mlmgt.RPCContextAdapter.
func (r *mlRPCContextAdapter) GetClientTLSConfig() (interface{}, error) {
	return r.r.GetClientTLSConfig()
}

// InternalExecutor implements mlmgt.GenericPlannerAdapter.
func (p *planner) InternalExecutor() sqlutil.InternalExecutor {
	return p.execCfg.InternalExecutor
}

// Settings implements mlmgt.GenericPlannerAdapter.
func (p *planner) Settings() mlmgt.SettingsAdapter {
	return &mlSettingsAdapter{s: p.execCfg.Settings}
}

// MLPredictMetrics implements mlmgt.GenericPlannerAdapter.
func (p *planner) MLPredictMetrics() *mlmgt.MLPredictMetrics {
	return p.execCfg.MLPredictMetrics
}

// MLPrivilegesCache implements mlmgt.GenericPlannerAdapter.
func (p *planner) MLPrivilegesCache() *mlmgt.MLPrivilegesCache {
	return p.execCfg.MLPrivilegesCache
}

// RPCContext implements mlmgt.GenericPlannerAdapter.
func (p *planner) RPCContext() mlmgt.RPCContextAdapter {
	return &mlRPCContextAdapter{r: p.execCfg.RPCContext}
}

// MLAgent implements mlmgt.GenericPlannerAdapter.
func (p *planner) MLAgent() string {
	return p.execCfg.MLAgent()
}

// ResolveTableDescriptorWithTableName resolves a table descriptor without
// using the descriptor cache. It is a wrapper around the original method that
// accepts tree.TableName and an int for requiredType, satisfying the
// mlmgt.GenericPlannerAdapter interface.
func (p *planner) ResolveTableDescriptorWithTableName(
	ctx context.Context, tn *tree.TableName, required bool, requiredType int,
) (*sqlbase.ImmutableTableDescriptor, error) {
	return p.ResolveUncachedTableDescriptor(ctx, tn, required, ResolveRequiredType(requiredType))
}

// GetMLModelName returns the ML model name of the statement being executed.
// The name is taken from the "USING MODEL" clause of the SELECT statement; it
// is empty when the statement does not carry one.
func (p *planner) GetMLModelName() string {
	if p.curPlan.mem == nil {
		return ""
	}
	return p.curPlan.mem.Metadata().MLModelName()
}

// The planner satisfies the mlmgt adapter contract.
var _ mlmgt.GenericPlannerAdapter = (*planner)(nil)
