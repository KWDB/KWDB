// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
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

/*
Package mlagent defines the mlagent features.
*/
package mlagent

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	// TestTrainResponseRunID is the response run id of the test training service.
	TestTrainResponseRunID = "00000000001111111111222222222231"
	// TestEvaluateResponseRunID is the response run id of the test evaluating service.
	TestEvaluateResponseRunID = "00000000001111111111222222222232"
	// TestBatchPredictResponseRunID is the response run id of the test evaluating service.
	TestBatchPredictResponseRunID = "00000000001111111111222222222233"
	// TestJobID is the response run id of the test checking status service.
	TestJobID = "00000000000000000000000000000001"
	// TestErrorJobID is the response run id on error from the test checking status service.
	TestErrorJobID = "00000000000000000000000000000011"
	// TestNullJobID is the response run id on null from the test checking status service.
	TestNullJobID = "00000000000000000000000000000012"
	// TestModelOutputNull is model name that having null output on response of the test checking status service.
	TestModelOutputNull = "model_output_null"
	// TestModelStatusNull is model name that having null status on response of the test checking status service.
	TestModelStatusNull = "model_status_null"
	// TestModelVersionNotExist is model name that not exist of the test checking status service.
	TestModelVersionNotExist = "modelversionnotexist"
)

func genIsvcModelName(schema, name string) string {
	return fmt.Sprintf("%s-%s", schema, name)
}

func formatUUID(id string) string {
	UID, _ := uuid.FromString(id)
	return UID.String()
}

// TestServer is a mock ml server.
type TestServer struct {
	serverpb.IsvcServiceServer
	serverpb.KPredictionServiceServer
	serverpb.MLAgentServer
}

// GetIsvc is part of the mock ml server interface.
func (s *TestServer) GetIsvc(
	_ context.Context, req *serverpb.GetIsvcRequest,
) (*serverpb.GetIsvcResponse, error) {
	var status *serverpb.IsvcStatus

	switch req.ModelSpec.ModelName {
	case genIsvcModelName("public", TestModelOutputNull):
		return &serverpb.GetIsvcResponse{}, nil
	case genIsvcModelName("public", TestModelStatusNull):
		status = nil
	case genIsvcModelName("public", TestModelVersionNotExist):
		err := serverpb.NewError(serverpb.Code_ModelVersionNotExist, "model not exist")
		//err = errors.Wrap(errors.Wrap(err, "Wrap1"), "Wrap2")
		return nil, err
	default:
		status = serverpb.NewReadyStatus()
	}

	return &serverpb.GetIsvcResponse{
		IsvcOutput: &serverpb.IsvcSpec{
			IsvcName:   req.ModelSpec.GenIsvcName(),
			IsvcStatus: status,
			IsvcUrl:    "",
		},
	}, nil
}

// GetBatchIsvc is part of the mock ml server interface.
func (s *TestServer) GetBatchIsvc(
	_ context.Context, req *serverpb.GetBatchIsvcRequest,
) (*serverpb.GetBatchIsvcResponse, error) {
	isvcList := make([]*serverpb.IsvcSpec, len(req.ModelSpecs))
	for i, model := range req.ModelSpecs {
		switch model.ModelName {
		case genIsvcModelName("public", TestModelOutputNull):
			isvcList[i] = nil
		case genIsvcModelName("public", TestModelStatusNull):
			isvcList[i] = &serverpb.IsvcSpec{}
		default:
			isvcList[i] = &serverpb.IsvcSpec{
				IsvcName:   model.GenIsvcName(),
				IsvcStatus: serverpb.NewReadyStatus(),
				IsvcUrl:    "",
			}
		}
	}

	return &serverpb.GetBatchIsvcResponse{
		Output: isvcList,
	}, nil
}

// CreateIsvc is part of the mock ml server interface.
func (s *TestServer) CreateIsvc(
	_ context.Context, req *serverpb.CreateIsvcRequest,
) (*serverpb.CreateIsvcResponse, error) {
	var status *serverpb.IsvcStatus

	switch req.ModelSpec.ModelName {
	case genIsvcModelName("public", TestModelOutputNull):
		return &serverpb.CreateIsvcResponse{}, nil
	case genIsvcModelName("public", TestModelStatusNull):
		status = nil
	default:
		status = serverpb.NewInitializeStatus("")
	}

	resp := &serverpb.CreateIsvcResponse{}
	resp.IsvcOutput = &serverpb.IsvcSpec{
		IsvcName:   req.ModelSpec.GenIsvcName(),
		IsvcStatus: status,
		IsvcUrl:    "",
	}
	return resp, nil
}

// DeleteIsvc is part of the mock ml server interface.
func (s *TestServer) DeleteIsvc(
	_ context.Context, _ *serverpb.DeleteIsvcRequest,
) (*serverpb.IsvcNull, error) {

	return &serverpb.IsvcNull{}, nil
}

// KPredict is part of the mock ml server interface.
func (s *TestServer) KPredict(
	_ context.Context, req *serverpb.KPredictRequest,
) (*serverpb.KPredictResponse, error) {
	var err error

	switch req.ModelName {
	case genIsvcModelName("public", TestModelOutputNull):
		err = errors.New("failed to get InferenceService")
	case genIsvcModelName("public", TestModelStatusNull):
		err = errors.New("failed to get InferenceService")
	case genIsvcModelName("public", TestModelVersionNotExist):
		err = errors.Wrap(serverpb.NewError(serverpb.Code_ModelVersionNotExist, ""), "Wrap1")
		err = errors.Wrap(err, "Wrap2")
	case genIsvcModelName("public", "powergen2"):
		return &serverpb.KPredictResponse{
			PredictOutputs: []byte(`{"prediction":0.314}`),
		}, nil
	default:
		err = nil
	}

	if err != nil {
		return nil, err
	}

	return &serverpb.KPredictResponse{
		PredictOutputs: []byte(`{"name":"Active_Power", "type":"float", "value":0.5}`),
	}, nil
}

// GetResources is part of the mock ml server interface.
func (s *TestServer) GetResources(
	_ context.Context, req *serverpb.GetResourcesRequest,
) (*serverpb.GetResourcesResponse, error) {
	for i, resource := range req.Resources {
		switch resource.Name {
		case genIsvcModelName("public", TestModelOutputNull),
			genIsvcModelName("public", TestModelStatusNull),
			genIsvcModelName("public", TestModelVersionNotExist):
			req.Resources[i].CPU = serverpb.Unknown
			req.Resources[i].Memory = serverpb.Unknown
			req.Resources[i].Storage = serverpb.Unknown
		default:
			req.Resources[i].CPU = "50m"
			req.Resources[i].Memory = "100Mi"
			req.Resources[i].Storage = "5Gi"
		}
	}

	return &serverpb.GetResourcesResponse{
		Resources: req.Resources,
	}, nil
}

// GetJobResources is part of the mock ml server interface.
func (s *TestServer) GetJobResources(
	_ context.Context, req *serverpb.JobResourcesRequest,
) (*serverpb.JobResourcesResponse, error) {
	for i, resource := range req.Resources {
		switch resource.Name {
		case genIsvcModelName("public", TestModelOutputNull):
			req.Resources[i] = &serverpb.JobResource{}
		case genIsvcModelName("public", TestModelStatusNull),
			genIsvcModelName("public", TestModelVersionNotExist):
			req.Resources[i].CPU = serverpb.Unknown
			req.Resources[i].Memory = serverpb.Unknown
			req.Resources[i].Storage = serverpb.Unknown
		default:
			req.Resources[i].CPU = "50m"
			req.Resources[i].Memory = "100Mi"
			req.Resources[i].Storage = "5Gi"
		}
	}

	return &serverpb.JobResourcesResponse{
		Resources: req.Resources,
	}, nil
}

// Heartbeat is part of the mock ml server interface.
func (s *TestServer) Heartbeat(
	context.Context, *serverpb.Empty,
) (*serverpb.HeartbeatResponse, error) {
	return &serverpb.HeartbeatResponse{}, nil
}

// Train is part of the mock ml server interface.
func (s *TestServer) Train(
	context.Context, *serverpb.TrainingRequest,
) (*serverpb.CommonResponse, error) {
	return &serverpb.CommonResponse{RunId: TestTrainResponseRunID}, nil
}

// PostTrain is part of the mock ml server interface.
func (s *TestServer) PostTrain(
	context.Context, *serverpb.PostTrainingRequest,
) (*serverpb.Empty, error) {
	return &serverpb.Empty{}, nil
}

// Evaluate is part of the mock ml server interface.
func (s *TestServer) Evaluate(
	context.Context, *serverpb.EvaluationRequest,
) (*serverpb.CommonResponse, error) {
	return &serverpb.CommonResponse{RunId: TestEvaluateResponseRunID}, nil
}

// PostEvaluate is part of the mock ml server interface.
func (s *TestServer) PostEvaluate(
	context.Context, *serverpb.PostEvaluationRequest,
) (*serverpb.Empty, error) {
	return &serverpb.Empty{}, nil
}

// BatchPredict is part of the mock ml server interface.
func (s *TestServer) BatchPredict(
	context.Context, *serverpb.BatchPredictRequest,
) (*serverpb.CommonResponse, error) {
	return &serverpb.CommonResponse{RunId: TestBatchPredictResponseRunID}, nil
}

// PostBatchPredict is part of the mock ml server interface.
func (s *TestServer) PostBatchPredict(
	context.Context, *serverpb.PostBatchPredictRequest,
) (*serverpb.Empty, error) {
	return &serverpb.Empty{}, nil
}

// CheckJobStatus is part of the mock ml server interface.
func (s *TestServer) CheckJobStatus(
	_ context.Context, req *serverpb.JobRequest,
) (*serverpb.JobStatusResponse, error) {
	jobs := make([]*serverpb.JobStatus, 0)
	switch {
	case req.JobId == TestErrorJobID:
		return nil, errors.New("job have a error")
	case req.JobId == TestNullJobID:
		return &serverpb.JobStatusResponse{}, nil
	case req.JobId != "":
		status := s.getJobStatus(req.JobId)
		if status == nil {
			return nil, errors.New("job not exist")
		}

		jobs = append(jobs, status)
	case len(req.JobIds) > 0:
		for _, id := range req.JobIds {
			status := s.getJobStatus(id)
			if status != nil {
				jobs = append(jobs, status)
			} else {
				jobs = append(jobs, &serverpb.JobStatus{
					Id:     id,
					Status: serverpb.JobStatusUnknown,
				})
			}
		}
	default:
		return &serverpb.JobStatusResponse{}, nil
	}

	return &serverpb.JobStatusResponse{
		Jobs: jobs,
	}, nil
}

func (s *TestServer) getJobStatus(id string) *serverpb.JobStatus {
	switch id {
	case formatUUID(TestTrainResponseRunID),
		formatUUID(TestEvaluateResponseRunID),
		formatUUID(TestBatchPredictResponseRunID):
		return &serverpb.JobStatus{
			Id:     id,
			Status: serverpb.JobStatusRunning,
		}
	case formatUUID(TestJobID):
		return &serverpb.JobStatus{
			Id:     id,
			Status: serverpb.JobStatusSucceeded,
		}
	default:
		return nil
	}
}

// DeleteJobs is part of the mock ml server interface.
func (s *TestServer) DeleteJobs(
	_ context.Context, req *serverpb.JobRequest,
) (*serverpb.JobStatusResponse, error) {
	switch req.JobId {
	case formatUUID(TestTrainResponseRunID),
		formatUUID(TestEvaluateResponseRunID),
		formatUUID(TestBatchPredictResponseRunID):
		return &serverpb.JobStatusResponse{
			Jobs: []*serverpb.JobStatus{
				{Id: req.JobId},
			},
		}, nil
	case formatUUID(TestJobID):
		return &serverpb.JobStatusResponse{
			Jobs: []*serverpb.JobStatus{
				{Id: req.JobId},
			},
		}, nil
	case formatUUID(TestErrorJobID):
		return nil, errors.New("job has a error")
	default:
		return &serverpb.JobStatusResponse{}, nil
	}
}

// CancelJob is part of the mock ml server interface.
func (s *TestServer) CancelJob(
	_ context.Context, req *serverpb.CancelJobRequest,
) (*serverpb.CommonResponse, error) {
	switch req.RunId {
	case formatUUID(TestTrainResponseRunID),
		formatUUID(TestEvaluateResponseRunID),
		formatUUID(TestBatchPredictResponseRunID):
		return &serverpb.CommonResponse{
			RunId: req.RunId,
		}, nil
	case formatUUID(TestJobID):
		return &serverpb.CommonResponse{
			RunId: req.RunId,
		}, nil
	case formatUUID(TestErrorJobID):
		return nil, errors.New("job has a error")
	default:
		return &serverpb.CommonResponse{}, nil
	}
}

// Cleanup is part of the mock ml server interface.
func (s *TestServer) Cleanup(
	context.Context, *serverpb.CleanupRequest,
) (*serverpb.CommonResponse, error) {
	return &serverpb.CommonResponse{}, nil
}

// StartMLTestServer start ml test server.
func StartMLTestServer(t testing.TB, address string, tlsConfig *tls.Config) *grpc.Server {
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
	}
	opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	mls := grpc.NewServer(opts...)
	go func() {
		lis, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatal(err)
		}

		serverpb.RegisterIsvcServiceServer(mls, &TestServer{})
		serverpb.RegisterKPredictionServiceServer(mls, &TestServer{})
		serverpb.RegisterMLAgentServer(mls, &TestServer{})

		err = mls.Serve(lis)
		if err != nil {
			t.Fatal(err)
		}
	}()

	return mls
}
