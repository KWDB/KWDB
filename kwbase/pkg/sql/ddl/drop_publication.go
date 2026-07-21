//
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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

type dropPublicationNode struct {
	pubID   uint64
	pubName tree.Name
	pubMeta *PubMetadata
}

// DropPublication creates a drop publication node for exec.
func DropPublication(
	ctx context.Context, p *GenericPlanner, n *tree.DropPublication,
) (sql.PlanNode, error) {
	pub, err := loadPubByName(ctx, p, n.PubName)
	if err != nil {
		return nil, err
	}

	if pub == nil {
		if n.IfExists {
			return &dropPublicationNode{pubName: ""}, nil
		}
		return nil, pgerror.Newf(pgcode.UndefinedObject, "publication %q does not exist", n.PubName)
	}

	if err = p.CheckPubSubDDLPrivilege(ctx, privilege.DROP, "pub "+pub.name.String(), pub.createBy); err != nil {
		return nil, err
	}

	return &dropPublicationNode{
		pubID:   pub.id,
		pubName: n.PubName,
		pubMeta: pub,
	}, nil
}

func (n *dropPublicationNode) StartExec(params RunParams) error {
	if n.pubName == "" {
		return nil
	}

	p := params.GetPlanner()
	p.SetAuditTarget(uint32(n.pubID), n.pubName.String(), nil)

	// 1. load the cdc watermarks and stop the task related to publication.
	for _, item := range n.pubMeta.paraInfo.TableList {
		p.ExecCfg().CDCCoordinator.StopCDCByLocal(item.ID, n.pubMeta.id, sqlbase.CDCInstanceType_Publication)
	}

	// 2. remove CDCDescriptor from table Descripto.
	for _, table := range n.pubMeta.paraInfo.TableList {
		// remove it from CDC
		if err := p.RemoveCDCDescriptorByTableID(
			params.Ctx, table.ID, sqlbase.CDCInstanceType_Publication, n.pubMeta.id,
		); err != nil {
			// table already be dropped.
			if strings.Contains(err.Error(), "does not exist") {
				continue
			}

			return err
		}
	}

	// 3. delete the record related to publication from system.kwdb_cdc_watermark.
	if err := p.RemoveCDCWatermarks(params.Ctx, sqlbase.CDCInstanceType_Publication, nil, &n.pubMeta.id); err != nil {
		return err
	}

	// 4. delete the publication from system.kwdb_publications.
	err := removePublication(params.Ctx, p, n.pubMeta)
	if err != nil {
		return err
	}

	return err
}

// removePublication deletes the publication from system.kwdb_publications with publication ID.
func removePublication(ctx context.Context, p sql.PlanHookState, pubMeta *PubMetadata) error {
	if _, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"delete-publication",
		p.Txn(),
		InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.kwdb_publications WHERE id = $1",
		pubMeta.id,
	); err != nil {
		return err
	}

	return nil
}

func (*dropPublicationNode) Next(RunParams) (bool, error) { return false, nil }
func (*dropPublicationNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropPublicationNode) Close(context.Context)        {}
