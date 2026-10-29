// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	"fmt"
	"strings"

	pb "gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func (mgt *mlManagement) createJob(
	jobID, modelID, modelVersionID, trainingPipelineID interface{},
	jobType, jobStatus, inputData, user string,
) error {
	num, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"create-ml-job",
		mgt.planner.Txn(),
		`INSERT INTO kwdbml.ml_jobs (id, model_id, model_version_id, training_pipeline_id, Creator, created,
			scheduled, job_type, job_status, job_input, settings) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		jobID,
		modelID,
		modelVersionID,
		trainingPipelineID,
		user,
		"now()",
		"now()",
		jobType,
		jobStatus,
		inputData,
		"{}",
	)
	if err != nil {
		return err
	}

	if num != 1 {
		return errors.Newf("unexpected number of rows affected: %d", num)
	}

	return nil
}

func (mgt *mlManagement) getMLJob(jobID string) ([]tree.Datums, error) {
	sql := `SELECT j.id, job_type, job_status, concat_ws('.' , p.schema, p.name), concat_ws('.' , m.schema, m.name), v.version AS model_version, 
						j.creator, j.created, finished, settings, result, error
					FROM kwdbml.ml_jobs j 
					LEFT JOIN kwdbml.ml_models m ON j.model_id = m.id
					LEFT JOIN kwdbml.ml_model_versions v ON j.model_version_id = v.id
					LEFT JOIN kwdbml.ml_training_pipelines p ON j.training_pipeline_id = p.id
					WHERE j.id = $1 `

	rows, err := mgt.planner.InternalExecutor().Query(mgt.ctx, "get-one-ml-job", mgt.planner.Txn(), sql, jobID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch the ML job")
	}

	if len(rows) < 1 {
		return nil, pgerror.Newf(pgcode.NoDataFound, "ML job '%s' does not exist", jobID)
	}

	// This build has no separate (sys-admin) deployment tier, so the admin
	// role check is always the one that applies.
	isAdmin, err := mgt.planner.UserHasAdminRole(mgt.ctx, mgt.planner.User())
	if err != nil {
		return nil, err
	}

	if !isAdmin {
		creator := string(*rows[0][6].(*tree.DString))
		if creator != mgt.planner.User() {
			return nil, pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"user '%s' is not the owner of ML job '%s'",
				mgt.planner.User(),
				jobID,
			)
		}
	}

	return rows, nil
}

func (mgt *mlManagement) checkJobStatus(jobID, _ string) ([]*pb.JobStatus, error) {
	return mgt.batchCheckJobStatus([]string{jobID})
}

func (mgt *mlManagement) batchCheckJobStatus(jobIDs []string) ([]*pb.JobStatus, error) {
	req := &pb.JobRequest{
		JobIds: jobIDs,
	}

	mlClient, err := mlUtils.newClient(mgt.planner)
	if err != nil {
		return nil, err
	}

	res, err := mlClient.CheckJobStatus(mgt.ctx, req)
	if err != nil {
		return nil, err
	}

	if len(res.Jobs) != len(jobIDs) {
		return nil, errors.Newf("check jobs status error, expect result size: %d, given: %d", len(jobIDs), len(res.Jobs))
	}

	return res.Jobs, nil
}

func (mgt *mlManagement) updateBatchJobsStatus(
	schema, name string, modelVersion int32, jobType []string, isAdmin bool,
) error {
	stmt := "SELECT j.id,j.job_status FROM kwdbml.ml_jobs j"

	if schema != "" {
		stmt = fmt.Sprintf(
			"%s LEFT JOIN kwdbml.ml_models m ON j.model_id = m.id AND m.schema = '%s' AND m.name = '%s' ",
			stmt,
			schema,
			name,
		)

		if modelVersion != 0 {
			stmt = fmt.Sprintf(
				"%s LEFT JOIN kwdbml.ml_model_versions v ON j.model_version_id = v.id AND v.version = %d ",
				stmt,
				modelVersion,
			)
		}
	}

	needUpdateStatus := []string{
		pb.JobStatusPending, pb.JobStatusRunning,
	}
	stmt = fmt.Sprintf("%s WHERE job_status IN ('%s') ", stmt, strings.Join(needUpdateStatus, "','"))

	if !isAdmin {
		stmt = fmt.Sprintf("%s AND j.creator = '%s' ", stmt, mgt.planner.User())
	}

	if len(jobType) > 0 {
		stmt = fmt.Sprintf("%s AND job_type IN ('%s') ", stmt, strings.Join(jobType, "','"))
	}

	rows, err := mgt.planner.InternalExecutor().Query(mgt.ctx, "get-update-ml-jobs", mgt.planner.Txn(), stmt)
	if err != nil {
		return err
	}

	var jobIDs []string
	rowPointMap := make(map[string]string, len(rows))

	for _, row := range rows {
		jobID := row[0].(*tree.DUuid).UUID.String()
		status := string(*row[1].(*tree.DString))
		jobIDs = append(jobIDs, jobID)
		rowPointMap[jobID] = status
	}

	if len(jobIDs) > 0 {
		newJobsStatus, err := mgt.batchCheckJobStatus(jobIDs)
		if err != nil {
			return errors.Newf("failed to check the status of ML jobs: %v", err)
		}

		for _, newJobStatus := range newJobsStatus {
			if newJobsStatus == nil {
				continue
			}

			status, ok := rowPointMap[newJobStatus.Id]
			if !ok {
				continue
			}

			// Message is not empty means job is not found
			if newJobStatus.Message != "" {
				log.Warningf(mgt.ctx, "job not found: %v", newJobStatus.Message)

				continue
			}

			if newJobStatus.Status != status {
				if err = mgt.updateJobStatus(newJobStatus.Id, newJobStatus); err != nil {
					log.Warningf(mgt.ctx, "%v", err)
				}

				continue
			}
		}
	}

	return nil
}

func (mgt *mlManagement) updateJobStatus(jobID string, status *pb.JobStatus) error {
	sql := fmt.Sprintf("UPDATE kwdbml.ml_jobs SET job_status = '%s'", status.Status)
	if len(status.FinishedAt) > 0 {
		sql = fmt.Sprintf("%s, finished = '%s'", sql, status.CreatedAt)
	}

	sql = fmt.Sprintf("%s WHERE id = '%s'", sql, jobID)
	if _, err := mgt.planner.InternalExecutor().Exec(mgt.ctx, "update-ml-job", mgt.planner.Txn(), sql); err != nil {
		return err
	}

	return nil
}

func (mgt *mlManagement) fetchJobs(
	schema, name string, modelVersion int32, jobType, jobStatus []string, isAdmin bool,
) ([]tree.Datums, error) {
	stmt := `SELECT j.id, job_type, job_status, concat_ws('.', p.schema, p.name), concat_ws('.', m.schema, m.name), 
				v.version AS model_version, 
				j.creator, j.created, finished, settings, result, error 
				FROM kwdbml.ml_jobs j 
				LEFT JOIN kwdbml.ml_models m ON j.model_id = m.id 
				LEFT JOIN kwdbml.ml_model_versions v ON j.model_version_id = v.id
				LEFT JOIN kwdbml.ml_training_pipelines p ON j.training_pipeline_id = p.id
				WHERE 1=1 `

	if !isAdmin {
		stmt = fmt.Sprintf("%s AND j.creator = '%s' ", stmt, mgt.planner.User())
	}

	if schema != "" {
		stmt = fmt.Sprintf("%s AND m.schema = '%s' AND m.name = '%s' ", stmt, schema, name)

		if modelVersion != 0 {
			stmt = fmt.Sprintf("%s AND v.version = %d ", stmt, modelVersion)
		}
	}

	if len(jobType) > 0 {
		stmt = fmt.Sprintf("%s AND job_type IN ('%s') ", stmt, strings.Join(jobType, "','"))
	}

	if len(jobStatus) > 0 {
		stmt = fmt.Sprintf("%s AND job_status IN ('%s') ", stmt, strings.Join(jobStatus, "','"))
	}

	rows, err := mgt.planner.InternalExecutor().Query(mgt.ctx, "get-all-ml-jobs", mgt.planner.Txn(), stmt)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (mgt *mlManagement) getJobIDs() ([]*pb.JobResource, error) {
	stmt := `SELECT id FROM kwdbml.ml_jobs WHERE job_status=$1`

	rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx, "fetch-job-ids", mgt.planner.Txn(), stmt, pb.JobStatusRunning)
	if err != nil {
		return nil, err
	}

	names := make([]*pb.JobResource, len(rows))

	for i, row := range rows {
		names[i] = &pb.JobResource{
			Name: row[0].(*tree.DUuid).UUID.String(),
			Type: pb.ResourceTypeJob,
		}
	}

	return names, nil
}

func (mgt *mlManagement) cancelJob(jobID string) error {
	req := &pb.CancelJobRequest{
		RunId: jobID,
	}

	mlClient, err := mlUtils.newClient(mgt.planner)
	if err != nil {
		return err
	}

	if _, err = mlClient.CancelJob(mgt.ctx, req); err != nil {
		// job not found means job status will never be updated
		// so ignore this error
		if strings.Contains(err.Error(), " not found") {
			return nil
		}

		return err
	}

	return nil
}

func (mgt *mlManagement) deleteMLJob(jobID string) error {
	if _, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"delete-one-ml-job",
		mgt.planner.Txn(),
		"DELETE FROM kwdbml.ml_jobs WHERE id = $1",
		jobID,
	); err != nil {
		return err
	}

	return nil
}

func (mgt *mlManagement) deleteMLJobFromServer(jobID string) error {
	req := &pb.JobRequest{
		JobId: jobID,
	}

	mlClient, err := mlUtils.newClient(mgt.planner)
	if err != nil {
		return err
	}

	if _, err = mlClient.DeleteJobs(mgt.ctx, req); err != nil {
		// job not found means job status will never be updated
		// so ignore this error
		if strings.Contains(err.Error(), " not found") {
			return nil
		}

		return err
	}

	return nil
}
