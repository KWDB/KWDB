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

// KPredict dispatches a kpredict call. The KPrediction service is defined
// in predict.proto and serves the kwdbml.predict builtin when a model is
// served via the agent.
func (ml MLGRPCClient) KPredict(ctx context.Context, req *pb.KPredictRequest) (*pb.KPredictResponse, error) {
	res, cli, err := ml.newKPredictionServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.KPredict(c, req)
}
