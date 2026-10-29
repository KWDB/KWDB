// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package client

import (
	"context"

	pb "gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/serverpb"
)

// CreateIsvc asks the agent to deploy a model. The request body carries
// the model spec (name, version, framework, ...) and the filter that
// selects the row from the agent's model registry.
func (ml MLGRPCClient) CreateIsvc(ctx context.Context, req *pb.CreateIsvcRequest) (*pb.CreateIsvcResponse, error) {
	res, cli, err := ml.newIsvcServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.CreateIsvc(c, req)
}

// GetIsvc fetches the current deployment status of a model.
func (ml MLGRPCClient) GetIsvc(ctx context.Context, req *pb.GetIsvcRequest) (*pb.GetIsvcResponse, error) {
	res, cli, err := ml.newIsvcServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.GetIsvc(c, req)
}

// GetBatchIsvc fetches the status of many models at once. The planner
// uses this to refresh kwdbml.ml_models.status before show_models().
func (ml MLGRPCClient) GetBatchIsvc(ctx context.Context, req *pb.GetBatchIsvcRequest) (*pb.GetBatchIsvcResponse, error) {
	res, cli, err := ml.newIsvcServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.GetBatchIsvc(c, req)
}

// DeleteIsvc asks the agent to tear down a deployment.
func (ml MLGRPCClient) DeleteIsvc(ctx context.Context, req *pb.DeleteIsvcRequest) error {
	res, cli, err := ml.newIsvcServiceClient()
	if err != nil {
		return err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	_, err = cli.DeleteIsvc(c, req)
	return err
}

// GetResources reports K8s cluster resource usage.
func (ml MLGRPCClient) GetResources(ctx context.Context, req *pb.GetResourcesRequest) (*pb.GetResourcesResponse, error) {
	res, cli, err := ml.newIsvcServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.GetResources(c, req)
}
