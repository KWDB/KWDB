// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

/*
Package mlmgt defines the mlmgt features.
*/
package mlmgt

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
)

// SettingsAdapter provides access to cluster settings needed by ML operations.
type SettingsAdapter interface {
	SV() *settings.Values
}

// RPCContextAdapter provides access to RPC configuration needed by ML operations.
type RPCContextAdapter interface {
	AdvertiseAddr() string
	GetClientTLSConfig() (interface{}, error)
}

// GenericPlannerAdapter defines the subset of GenericPlanner methods
// needed by ML operations.
type GenericPlannerAdapter interface {
	User() string
	Txn() *kv.Txn
	InternalExecutor() sqlutil.InternalExecutor
	Settings() SettingsAdapter
	MLPredictMetrics() *MLPredictMetrics
	MLPrivilegesCache() *MLPrivilegesCache
	RPCContext() RPCContextAdapter
	MLAgent() string
	HasAdminRole(ctx context.Context) (bool, error)
	UserHasAdminRole(ctx context.Context, user string) (bool, error)
	MemberOfWithAdminOption(ctx context.Context, member string) (map[string]bool, error)
	SessionData() *sessiondata.SessionData
	CurrentDatabase() string
	CurrentSearchPath() sessiondata.SearchPath
	LookupSchema(ctx context.Context, dbName, scName string) (bool, tree.SchemaMeta, error)
	ResolveTableDescriptorWithTableName(ctx context.Context, tn *tree.TableName, required bool, requiredType int) (*sqlbase.ImmutableTableDescriptor, error)
	CheckPrivilege(ctx context.Context, descriptor sqlbase.DescriptorProto, privilege privilege.Kind) error
	GetMLModelName() string
}

// ResolveRequiredType is the type of the resolution required for table descriptors.
type ResolveRequiredType = int

const (
	// ResolveRequiredTableDesc indicates a table descriptor is required.
	ResolveRequiredTableDesc ResolveRequiredType = iota
	// ResolveAnyTableDesc indicates any table descriptor is acceptable.
	ResolveAnyTableDesc
)

// ResolveTargetObject determines a valid target path for an object
// that may not exist yet. It returns the descriptor for the database
// where the target object lives.
//
// The object name is modified in-place with the result of the name
// resolution.
func ResolveTargetObject(
	ctx context.Context, sc GenericPlannerAdapter, tn *tree.TableName,
) (res *sqlbase.ResolvedObjectPrefix, err error) {
	found, scMeta, err := tn.ResolveTarget(ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, err
	}
	if !found {
		if !tn.ExplicitSchema && !tn.ExplicitCatalog {
			return nil, pgerror.New(pgcode.InvalidName, "no database specified")
		}
		return nil, pgerror.Newf(pgcode.InvalidSchemaName,
			"cannot create %q because the target database or schema does not exist",
			tree.ErrString(tn))
	}
	scInfo := scMeta.(*sqlbase.ResolvedObjectPrefix)
	return scInfo, nil
}

// MLManager implements MLOperator by delegating to a GenericPlannerAdapter.
type MLManager struct {
	Planner GenericPlannerAdapter
}

// NewMLManager creates a new Manager from a GenericPlannerAdapter.
func NewMLManager(p GenericPlannerAdapter) *MLManager {
	return &MLManager{Planner: p}
}
