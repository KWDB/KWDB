// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package client

import (
	"context"

	pb "gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/serverpb"
	"github.com/cockroachdb/errors"
)

// Heartbeat sends a heartbeat request and returns the agent's version info.
func (ml MLGRPCClient) Heartbeat(ctx context.Context) (*pb.HeartbeatResponse, error) {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return nil, errors.Newf("failed to connect to ML training service: %v", err)
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.Heartbeat(c, &pb.Empty{})
}

// Train dispatches a training job.
func (ml MLGRPCClient) Train(ctx context.Context, req *pb.TrainingRequest) (*pb.CommonResponse, error) {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.Train(c, req)
}

// PostTrain ...
func (ml MLGRPCClient) PostTrain(ctx context.Context, req *pb.PostTrainingRequest) error {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	_, err = cli.PostTrain(c, req)
	return err
}

// Evaluate dispatches an evaluation job.
func (ml MLGRPCClient) Evaluate(ctx context.Context, req *pb.EvaluationRequest) (*pb.CommonResponse, error) {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.Evaluate(c, req)
}

// PostEvaluate ...
func (ml MLGRPCClient) PostEvaluate(ctx context.Context, req *pb.PostEvaluationRequest) error {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	_, err = cli.PostEvaluate(c, req)
	return err
}

// BatchPredict dispatches a batch prediction job.
func (ml MLGRPCClient) BatchPredict(ctx context.Context, req *pb.BatchPredictRequest) (*pb.CommonResponse, error) {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.BatchPredict(c, req)
}

// PostBatchPredict ...
func (ml MLGRPCClient) PostBatchPredict(ctx context.Context, req *pb.PostBatchPredictRequest) error {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	_, err = cli.PostBatchPredict(c, req)
	return err
}

// FineTune dispatches a fine-tune job.
func (ml MLGRPCClient) FineTune(ctx context.Context, req *pb.FineTuneRequest) (*pb.CommonResponse, error) {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.FineTune(c, req)
}

// PostFineTune ...
func (ml MLGRPCClient) PostFineTune(ctx context.Context, req *pb.PostFineTuneRequest) error {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	_, err = cli.PostFineTune(c, req)
	return err
}

// CheckJobStatus asks the agent for the latest status of one or more jobs.
func (ml MLGRPCClient) CheckJobStatus(ctx context.Context, req *pb.JobRequest) (*pb.JobStatusResponse, error) {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.CheckJobStatus(c, req)
}

// DeleteJobs asks the agent to forget a job (used by remove_job).
func (ml MLGRPCClient) DeleteJobs(ctx context.Context, req *pb.JobRequest) (*pb.JobStatusResponse, error) {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.DeleteJobs(c, req)
}

// CancelJob cancels a running job.
func (ml MLGRPCClient) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CommonResponse, error) {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.CancelJob(c, req)
}

// Cleanup ...
func (ml MLGRPCClient) Cleanup(ctx context.Context, req *pb.CleanupRequest) (*pb.CommonResponse, error) {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.Cleanup(c, req)
}

// GetJobResources ...
func (ml MLGRPCClient) GetJobResources(ctx context.Context, req *pb.JobResourcesRequest) (*pb.JobResourcesResponse, error) {
	res, cli, err := ml.newTrainingServiceClient()
	if err != nil {
		return nil, err
	}
	defer res.release()
	c, cancel := context.WithTimeout(ctx, mlClientTimeout)
	defer cancel()
	return cli.GetJobResources(c, req)
}
