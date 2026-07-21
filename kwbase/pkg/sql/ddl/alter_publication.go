// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Shanghai Yunxi Technology Co, Ltd makes no representations or warranties about the suitability
// of the software, either express or implied, including but not limited to the implied warranties
// of merchantability, fitness for a particular purpose, or non-infringement. Shanghai Yunxi
// Technology Co, Ltd shall not be liable for any damages suffered by licensee as a result
// of using, modifying or distributing this software or its derivatives.
//

package ddl

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/metadata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
)

type alterPubNode struct {
	n             *tree.AlterPub
	databaseID    uint64
	tableDescList []*MutableTableDescriptor
	pubOpts       func() (map[string]string, error)
	pubMeta       *PubMetadata
}

// NewAlterPubNode creates a new alterPubNode. This func is added only for testing.
// nolint:unexportedreturn
func NewAlterPubNode(
	n *tree.AlterPub,
	databaseID uint64,
	tableDescList []*MutableTableDescriptor,
	pubOpts func() (map[string]string, error),
	pubMeta *PubMetadata,
) *alterPubNode {
	return &alterPubNode{
		n:             n,
		databaseID:    databaseID,
		tableDescList: tableDescList,
		pubOpts:       pubOpts,
		pubMeta:       pubMeta,
	}
}

// AlterPublication creates a publication node for exec.
func AlterPublication(
	ctx context.Context, p *GenericPlanner, n *tree.AlterPub,
) (sql.PlanNode, error) {
	// 1. load the old publication meta.
	pubMeta, err := loadPubByName(ctx, p, n.PubName)
	if err != nil {
		return nil, err
	}
	if pubMeta == nil {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "publication %q does not exist", n.PubName)
	}
	// 1.1 check whether the current user is admin/sysadmin or the owner of pub.
	if err = p.CheckPubSubDDLPrivilege(ctx, privilege.CREATE, "pub "+pubMeta.name.String(), pubMeta.createBy); err != nil {
		return nil, err
	}

	pubMeta.paraInfo, err = cdcpb.UnmarshalPubParameters(pubMeta.parameters)
	if err != nil {
		return nil, err
	}

	//2. check whether current publication has been subscribed(has cdc task).
	isSubscribed := false
	for _, tableInfo := range pubMeta.paraInfo.TableList {
		if isSubscribed = p.ExecCfg().CDCCoordinator.HasTask(sqlbase.CDCInstanceType_Publication, tableInfo.ID, pubMeta.id); isSubscribed {
			return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"publication %s has been subscribed, and cannot be changed", n.PubName)
		}
	}

	var tableDescList []*MutableTableDescriptor
	var pubOpts func() (map[string]string, error)
	hasTableName := n.Table.TableName != ""
	// 3. change publication objects, resolve objects
	if hasTableName {
		// publish database or multiple tables, are not supported to be changed.
		if pubMeta.databaseID != 0 || len(pubMeta.paraInfo.TableList) > 1 {
			return nil, pgerror.Newf(pgcode.FeatureNotSupported,
				"publication %s published database or multipile tables, and cannot be changed", n.PubName)
		}

		// if table name is not null, should resolve table descriptor,
		// and check whether the table is ts table.
		var tableDesc *MutableTableDescriptor
		tableDesc, err = p.ResolveMutableTableDescriptor(
			ctx, &n.Table, true /*required*/, sql.ResolveRequireTableDesc,
		)
		if err != nil {
			return nil, err
		}
		if tableDesc == nil {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "table %s does not exists", n.Table.String())
		}
		// check whether the table to be published is TS table.
		if !tableDesc.IsTSTable() {
			return nil, pgerror.Newf(pgcode.WrongObjectType, "publication is only used on ts table")
		}
		// check privilege
		if err = checkPrivilegeForPubSub(ctx, p, tableDesc, privilege.SELECT); err != nil {
			return nil, err
		}
		tableDescList = append(tableDescList, tableDesc)
	} else {
		// 4. change options
		pubOpts, err = p.TypeAsStringOpts(n.Options, pubOptionExpectValues)
		if err != nil {
			return nil, err
		}
	}

	return &alterPubNode{n: n, tableDescList: tableDescList, pubOpts: pubOpts, pubMeta: pubMeta}, nil
}

func (n *alterPubNode) StartExec(params RunParams) (err error) {
	// change object of publication
	pubTableListChanged := false
	if n.tableDescList != nil {
		var pubTableInfos []cdcpb.CDCTableInfo
		pubTableInfos, _, err = sql.MakeCDCTableInfo(params.Ctx, params.GetPlanner(), n.tableDescList, n.n.Star, n.n.ColNames, true)
		if err != nil {
			return err
		}

		if n.n.Where != nil {
			whereNeedNormalTag := false
			whereNeedNormalTag, err = sql.CheckWhereExprForCDC(
				params.Ctx, params.GetPlanner(), n.n.Table, n.tableDescList[0].TableDescriptor, n.n.Where.Expr)
			if err != nil {
				return err
			}
			if whereNeedNormalTag {
				pubTableInfos[0].NeedNormalTag = true
			}

			pubTableInfos[0].Filter = n.n.Where.Expr.String()
		}

		// delete the cdc watermarks about object in old publication.
		if err = params.GetPlanner().RemoveCDCWatermarks(params.Ctx, sqlbase.CDCInstanceType_Publication, nil, &n.pubMeta.id); err != nil {
			return err
		}
		// update table list in publication metadata.
		n.pubMeta.paraInfo.TableList = pubTableInfos
		pubTableListChanged = true
	}

	// change options of publication
	if n.pubOpts != nil {
		pubOpts, err := n.pubOpts()
		if err != nil {
			return err
		}
		options, err := makePubOptions(pubOpts, &n.pubMeta.paraInfo.PubOptions)
		if err != nil {
			return err
		}
		n.pubMeta.paraInfo.PubOptions = options
	}

	for i := range n.pubMeta.paraInfo.TableList {
		n.pubMeta.paraInfo.TableList[i].NeedNormalTag = n.pubMeta.paraInfo.TableList[i].NeedNormalTag && n.pubMeta.paraInfo.PubOptions.CheckTag == sqlconst.OptOn
	}

	parameters, err := cdcpb.MarshalPubParameters(n.pubMeta.paraInfo)
	if err != nil {
		return err
	}

	if _, err = params.ExecCfg().InternalExecutor.ExecEx(
		params.Ctx,
		"update-pub-metadata",
		params.PlannerTxn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
		`UPDATE system.kwdb_publications SET parameters=$1 WHERE id=$2`,
		parameters, n.pubMeta.id,
	); err != nil {
		return err
	}

	if pubTableListChanged {
		// table list to publish has changed, remove the old and add the new in system.kwdb_cdc_watermark.
		for _, tableInfo := range n.pubMeta.paraInfo.TableList {
			if err = params.GetPlanner().AddCDCWatermark(params.Ctx, metadata.CDCWatermark{
				TableID:      tableInfo.ID,
				TaskID:       n.pubMeta.id,
				TaskType:     sqlbase.CDCInstanceType_Publication,
				LowWatermark: cdcpb.InvalidWatermark,
			}); err != nil {
				return err
			}
		}
	}
	params.GetPlanner().SetAuditTarget(uint32(n.pubMeta.id), n.pubMeta.name.String(), nil)

	return err
}

func (n *alterPubNode) Next(_ RunParams) (bool, error) {
	return false, nil
}

func (n *alterPubNode) Values() tree.Datums { return nil }

func (n *alterPubNode) Close(context.Context) {}
