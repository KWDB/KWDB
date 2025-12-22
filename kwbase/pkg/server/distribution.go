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

package server

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *statusServer) Distribution(
	ctx context.Context, req *serverpb.DistributionRequest,
) (*serverpb.DistributionResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	response := &serverpb.DistributionResponse{}

	localReq := &serverpb.DistributionRequest{
		NodeID: "local",
		IsDB:   req.IsDB,
		ID:     req.ID,
	}

	if len(req.NodeID) > 0 {
		requestedNodeID, local, err := s.parseNodeID(req.NodeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		if local {
			return s.DistributionLocal(ctx, req.IsDB, req.ID, requestedNodeID.String())
		}
		status, err := s.dialNode(ctx, requestedNodeID)
		if err != nil {
			return nil, err
		}
		return status.Distribution(ctx, localReq)
	}
	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	nodeFn := func(ctx context.Context, client interface{}, _ roachpb.NodeID) (interface{}, error) {
		status := client.(serverpb.StatusClient)
		return status.Distribution(ctx, localReq)
	}
	responseFn := func(nodeID roachpb.NodeID, nodeResp interface{}) {
		sessions := nodeResp.(*serverpb.DistributionResponse)
		response.Distribution = append(response.Distribution, sessions.Distribution...)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {}
	if err := s.iterateNodes(ctx, "distribution list", dialFn, nodeFn, responseFn, errorFn); err != nil {
		return nil, err
	}
	return response, nil
}

// DistributionLocal gets local node block info
func (s *statusServer) DistributionLocal(
	ctx context.Context, isDB bool, id uint64, nodeID string,
) (*serverpb.DistributionResponse, error) {
	resp := &serverpb.DistributionResponse{}
	distribution := &sqlbase.BlocksDistribution{}
	var meta []byte
	var err error
	if isDB {
		meta, err = s.admin.server.tsEngine.GetDBBlocksDistribution(id)
	} else {
		meta, err = s.admin.server.tsEngine.GetTableBlocksDistribution(id)
	}
	if err != nil {
		return resp, err
	}
	err = protoutil.Unmarshal(meta, distribution)
	if err != nil {
		return resp, err
	}
	resp.Distribution = []*serverpb.DistributionInfo{{NodeID: nodeID, BlockInfo: distribution.BlockInfo}}

	return resp, nil
}
