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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

var showDBDistributionColumns = sqlbase.ResultColumns{
	{Name: "node_id", Typ: types.String},
	{Name: "block_num", Typ: types.Int},
	{Name: "block_size", Typ: types.Int},
	{Name: "avg_size", Typ: types.String},
	{Name: "compression_ratio", Typ: types.String},
	{Name: "last_segment_num_level0", Typ: types.Int},
	{Name: "last_segment_num_level1", Typ: types.Int},
	{Name: "last_segment_num_level2", Typ: types.Int},
}

var showTableDistributionColumns = sqlbase.ResultColumns{
	{Name: "node_id", Typ: types.String},
	{Name: "level", Typ: types.String},
	{Name: "block_num", Typ: types.Int},
	{Name: "block_size", Typ: types.Int},
	{Name: "avg_size", Typ: types.String},
	{Name: "compression_ratio", Typ: types.String},
}

// ShowDistribution returns a SHOW DISTRIBUTION statement. The user must have any
// privilege on the database or table.
func (p *planner) ShowDistribution(
	ctx context.Context, n *tree.ShowDistribution,
) (planNode, error) {
	if n.IsDB {
		dbName := string(n.Database)
		dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, dbName, true)
		if err != nil {
			return nil, err
		}
		if dbDesc == nil {
			return nil, sqlbase.NewUndefinedDatabaseError(dbName)
		}
		if dbDesc.EngineType != tree.EngineTypeTimeseries {
			return nil, pgerror.New(pgcode.WrongObjectType, "can not show distributions on non time series database")
		}
		if err = p.CheckAnyPrivilege(ctx, dbDesc); err != nil {
			return nil, err
		}

		req := serverpb.DistributionRequest{IsDB: true, ID: uint64(dbDesc.ID)}
		response, err := p.extendedEvalCtx.StatusServer.Distribution(ctx, &req)
		if err != nil {
			return nil, err
		}
		var capacity int
		var rows []tree.Datums
		var totalNum uint64
		var totalSize uint64
		var totalAvgSize float64
		var totalLevel0 uint32
		var totalLevel1 uint32
		var totalLevel2 uint32
		for _, info := range response.Distribution {
			for _, block := range info.BlockInfo {
				var compressionRatio string
				if block.CompressionRatio == nil {
					compressionRatio = "0"
				} else {
					compressionRatio = fmt.Sprintf("%.2f", *block.CompressionRatio)
				}
				avgSize := fmt.Sprintf("%.2f", block.AvgSize)
				totalNum += uint64(block.BlocksNum)
				totalSize += block.BlocksSize
				row := tree.Datums{
					tree.NewDString(info.NodeID),
					tree.NewDInt(tree.DInt(block.BlocksNum)),
					tree.NewDInt(tree.DInt(block.BlocksSize)),
					tree.NewDString(avgSize),
					tree.NewDString(compressionRatio),
					tree.NewDInt(tree.DInt(block.LastSegLevel0)),
					tree.NewDInt(tree.DInt(block.LastSegLevel1)),
					tree.NewDInt(tree.DInt(block.LastSegLevel2)),
				}
				totalLevel0 += block.LastSegLevel0
				totalLevel1 += block.LastSegLevel1
				totalLevel2 += block.LastSegLevel2

				rows = append(rows, row)
				capacity++
			}
		}
		totalAvgSize = float64(totalSize) / float64(totalNum)
		total := tree.Datums{
			tree.NewDString("total"),
			tree.NewDInt(tree.DInt(totalNum)),
			tree.NewDInt(tree.DInt(totalSize)),
			tree.NewDString(fmt.Sprintf("%.2f", totalAvgSize)),
			tree.NewDString("0"),
			tree.NewDInt(tree.DInt(totalLevel0)),
			tree.NewDInt(tree.DInt(totalLevel1)),
			tree.NewDInt(tree.DInt(totalLevel2)),
		}
		rows = append(rows, total)
		capacity++
		return &delayedNode{
			name:    fmt.Sprintf("SHOW DISTRIBUTION FROM DATABASE %v", dbName),
			columns: showDBDistributionColumns,
			constructor: func(ctx context.Context, p *planner) (planNode, error) {
				v := p.newContainerValuesNode(showDBDistributionColumns, capacity)
				for _, row := range rows {
					if _, err = v.rows.AddRow(ctx, row); err != nil {
						v.Close(ctx)
						return nil, err
					}
				}
				return v, nil
			},
		}, nil
	}
	tblName := n.Table.ToTableName()
	tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &tblName, true, ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, sqlbase.NewUndefinedTableError(n.Table.String())
	}
	if !tableDesc.IsTSTable() {
		return nil, pgerror.New(pgcode.WrongObjectType, "can not show distributions on non time series table")
	}
	if err = p.CheckAnyPrivilege(ctx, tableDesc); err != nil {
		return nil, err
	}

	req := serverpb.DistributionRequest{IsDB: false, ID: uint64(tableDesc.ID)}
	response, err := p.extendedEvalCtx.StatusServer.Distribution(ctx, &req)
	if err != nil {
		return nil, err
	}
	var capacity int
	var rows []tree.Datums
	for _, info := range response.Distribution {
		for _, block := range info.BlockInfo {
			var compressionRatio string
			if block.CompressionRatio == nil {
				compressionRatio = "0"
			} else {
				compressionRatio = fmt.Sprintf("%.2f", *block.CompressionRatio)
			}
			avgSize := fmt.Sprintf("%.2f", block.AvgSize)
			row := tree.Datums{
				tree.NewDString(info.NodeID),
				tree.NewDString(block.Level),
				tree.NewDInt(tree.DInt(block.BlocksNum)),
				tree.NewDInt(tree.DInt(block.BlocksSize)),
				tree.NewDString(avgSize),
				tree.NewDString(compressionRatio),
			}
			rows = append(rows, row)
			capacity++
		}
	}

	return &delayedNode{
		name:    fmt.Sprintf("SHOW DISTRIBUTION FROM TABLE %v", n.Table),
		columns: showTableDistributionColumns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			v := p.newContainerValuesNode(showTableDistributionColumns, capacity)
			for _, row := range rows {
				if _, err = v.rows.AddRow(ctx, row); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}
