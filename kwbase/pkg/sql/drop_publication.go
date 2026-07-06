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

package sql

import (
	"context"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/security"
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
func (p *planner) DropPublication(ctx context.Context, n *tree.DropPublication) (planNode, error) {
	pub, err := p.loadPubByName(ctx, n.PubName)
	if err != nil {
		return nil, err
	}

	if pub == nil {
		if n.IfExists {
			return &dropPublicationNode{pubName: ""}, nil
		}
		return nil, pgerror.Newf(pgcode.UndefinedObject, "publication %q does not exist", n.PubName)
	}

	if err = p.checkPubSubDDLPrivilege(ctx, privilege.DROP, "pub "+pub.name.String(), pub.createBy); err != nil {
		return nil, err
	}

	return &dropPublicationNode{
		pubID:   pub.id,
		pubName: n.PubName,
		pubMeta: pub,
	}, nil
}

func (n *dropPublicationNode) startExec(params runParams) error {
	if n.pubName == "" {
		return nil
	}

	params.p.SetAuditTarget(uint32(n.pubID), n.pubName.String(), nil)

	err := params.p.removePublication(params.ctx, n.pubMeta)
	if err != nil {
		return err
	}

	return err
}

// removePublication deletes the publication from system.kwdb_publications with publication ID.
// 1. load the cdc watermarks and stop the task related to publication.
// 2. delete the record related to publication from system.kwdb_cdc_watermark.
// 3. delete the publication from system.kwdb_publications.
func (p *planner) removePublication(ctx context.Context, pubMeta *PubMetadata) error {
	if err := p.removeCDCWatermarks(ctx, sqlbase.CDCInstanceType_Publication, nil, &pubMeta.id); err != nil {
		return err
	}

	if _, err := p.ExecCfg().InternalExecutor.ExecEx(
		ctx,
		"delete-publication",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.kwdb_publications WHERE id = $1",
		pubMeta.id,
	); err != nil {
		return err
	}

	for _, table := range pubMeta.paraInfo.TableList {
		// remove it from CDC
		if err := p.removeCDCDescriptorByTableID(
			ctx, table.ID, sqlbase.CDCInstanceType_Publication, pubMeta.id,
		); err != nil {
			// table already be dropped.
			if strings.Contains(err.Error(), "does not exist") {
				continue
			}

			return err
		}
	}

	for _, item := range pubMeta.paraInfo.TableList {
		p.ExecCfg().CDCCoordinator.StopCDCByLocal(item.ID, pubMeta.id, sqlbase.CDCInstanceType_Publication)
	}

	return nil
}

func (*dropPublicationNode) Next(runParams) (bool, error) { return false, nil }
func (*dropPublicationNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropPublicationNode) Close(context.Context)        {}
