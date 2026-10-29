// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	"context"
	"fmt"

	pb "gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// FindJobs searches for ML jobs matching the specified criteria
func (m *MLManager) FindJobs(
	ctx context.Context, modelName string, modelVersion int32, jobTypeStr, jobStatusStr string,
) ([]tree.Datums, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return []tree.Datums{}, err
	}

	var schema, name string
	if modelName != "*" {
		schema, name, err = mgt.extractSchemaObjectName(modelName)
		if err != nil {
			return nil, err
		}
	}

	jobType, err := mlUtils.checkJobType(jobTypeStr)
	if err != nil {
		return nil, err
	}

	jobStatus, err := mlUtils.checkStatusType(jobStatusStr)
	if err != nil {
		return nil, err
	}

	// This build has no separate (sys-admin) deployment tier, so the admin
	// role check is always the one that applies.
	isAdmin, err := m.Planner.UserHasAdminRole(ctx, m.Planner.User())
	if err != nil {
		return nil, err
	}

	if err = mgt.updateBatchJobsStatus(schema, name, modelVersion, jobType, isAdmin); err != nil {
		return nil, err
	}

	return mgt.fetchJobs(schema, name, modelVersion, jobType, jobStatus, isAdmin)
}

// FindJob finds a specific ML job by its ID
func (m *MLManager) FindJob(ctx context.Context, jobID string) ([]tree.Datums, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return []tree.Datums{}, err
	}

	job, err := mgt.getMLJob(jobID)
	if err != nil {
		if pgerror.GetPGCode(err) != pgcode.NoDataFound {
			return nil, err
		}

		return []tree.Datums{}, nil
	}

	return job, nil
}

// RemoveJob removes an ML job and its associated resources
func (m *MLManager) RemoveJob(ctx context.Context, jobID string) (tree.DUuid, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return tree.DUuid{}, err
	}

	rows, err := mgt.getMLJob(jobID)
	if err != nil {
		return tree.DUuid{}, err
	}

	id := *rows[0][0].(*tree.DUuid)
	status := string(*rows[0][2].(*tree.DString))

	switch status {
	case pb.JobStatusSucceeded, pb.JobStatusFailed, pb.JobStatusCanceled:
	default:
		return tree.DUuid{}, errors.Errorf("cannot remove ML job in '%s' status", status)
	}

	if err = mgt.deleteMLJobFromServer(jobID); err != nil {
		return tree.DUuid{}, err
	}

	if err = mgt.deleteMLJob(jobID); err != nil {
		return tree.DUuid{}, err
	}

	return id, nil

}

// CancelJob cancels a running ML job
func (m *MLManager) CancelJob(ctx context.Context, jobID string) (tree.DUuid, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return tree.DUuid{}, err
	}

	rows, err := mgt.getMLJob(jobID)
	if err != nil {
		return tree.DUuid{}, err
	}

	if err = mgt.cancelJob(jobID); err != nil {
		return tree.DUuid{}, err
	}

	if err = mgt.updateJobStatus(jobID, &pb.JobStatus{Id: jobID, Status: pb.JobStatusCanceled}); err != nil {
		return tree.DUuid{}, err
	}

	return *rows[0][0].(*tree.DUuid), nil
}

// FindResources searches for ML resources matching the specified criteria
func (m *MLManager) FindResources(ctx context.Context) ([]tree.Datums, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return nil, err
	}

	_, isAdmin, err := mgt.extractRoleMembersWithAdminFlag(m.Planner.User())
	if err != nil {
		return nil, err
	}

	if !isAdmin {
		return nil, pgerror.Newf(
			pgcode.InsufficientPrivilege, "only admin can use the function",
		)
	}

	modelNames, err := mgt.getModelNames()
	if err != nil {
		return nil, err
	}

	jobIDs, err := mgt.getJobIDs()
	if err != nil {
		return nil, err
	}
	fmt.Println(modelNames, jobIDs)
	return mgt.getResources(modelNames, jobIDs)
}
