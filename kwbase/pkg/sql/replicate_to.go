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
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// ReplTarget ..
type ReplTarget uint32

const (
	// ReplTable is replicate table
	ReplTable ReplTarget = iota

	//RepLDatabase is replicate table
	RepLDatabase
)

type replicationToNode struct {
	replInfo   *tree.ReplicationInformation
	isTs       bool
	replTarget ReplTarget
}

// ReplicationTo ...
// nolint:unexportedreturn
func (p *GenericPlanner) ReplicationTo(
	ctx context.Context, n *tree.ReplicationInformation,
) (*replicationToNode, error) {
	var replTarget ReplTarget
	var isTs bool
	if n.Targets.Tables != nil {
		replTarget = ReplTable
		// TODO(liyang): check if it's ts table

	} else if n.Targets.Databases != nil {
		replTarget = RepLDatabase
		isTs = n.IsTs
	} else {
		return nil, errors.Errorf("ALTER REPLICATE MISSING TARGET")
	}

	// TODO:check privilege If there is no root permission, return an error
	// demo
	//if err := p.CheckPrivilege(); err != nil {
	//	return nil, err
	//}

	return &replicationToNode{replInfo: n, replTarget: replTarget, isTs: isTs}, nil
}

func (n *replicationToNode) StartExec(params RunParams) error {
	if n.isTs {
		// ts table
	} else {
		// relational table
	}
	return nil
}

func (n *replicationToNode) Next(params RunParams) (bool, error) { return false, nil }

func (n *replicationToNode) Values() tree.Datums { return tree.Datums{} }

func (n *replicationToNode) Close(ctx context.Context) {}
