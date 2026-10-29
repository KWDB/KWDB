// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package client

import (
	"crypto/tls"
	"time"

	pb "gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/serverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	maxMessageLength = 1024 * 1024 * 1024
	mlClientTimeout  = 10 * time.Minute
)

// MLGRPCClient is the SQL-side handle for the external mlagent gRPC
// service. It is constructed lazily by mlUtil.newClient and shared per
// request. The Address is "host:port" without scheme.
type MLGRPCClient struct {
	Address string
	Config  *tls.Config
}

type connectionRes struct {
	conn *grpc.ClientConn
}

func (ml MLGRPCClient) newTrainingServiceClient() (*connectionRes, pb.MLAgentClient, error) {
	conn, err := ml.grpcClient()
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewMLAgentClient(conn.conn), nil
}

func (ml MLGRPCClient) newIsvcServiceClient() (*connectionRes, pb.IsvcServiceClient, error) {
	conn, err := ml.grpcClient()
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewIsvcServiceClient(conn.conn), nil
}

func (ml MLGRPCClient) newKPredictionServiceClient() (*connectionRes, pb.KPredictionServiceClient, error) {
	conn, err := ml.grpcClient()
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewKPredictionServiceClient(conn.conn), nil
}

func (ml MLGRPCClient) connection() (*grpc.ClientConn, error) {
	return grpc.Dial(ml.Address, ml.grpcDialOptions()...)
}

func (ml MLGRPCClient) grpcClient() (*connectionRes, error) {
	conn, err := ml.connection()
	if err != nil {
		return nil, err
	}
	return &connectionRes{conn}, nil
}

func (ml MLGRPCClient) grpcDialOptions() []grpc.DialOption {
	var dialOpts []grpc.DialOption
	if ml.Config == nil {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(ml.Config)))
	}
	dialOpts = append(dialOpts,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageLength),
			grpc.MaxCallSendMsgSize(maxMessageLength),
		),
	)
	return dialOpts
}

func (res connectionRes) release() {
	if res.conn != nil {
		_ = res.conn.Close()
	}
}
