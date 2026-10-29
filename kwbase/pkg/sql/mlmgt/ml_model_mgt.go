// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	ejson "encoding/json"
	"fmt"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	pb "gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// PodStatusPending is for k8s pod that have been created but on which work has
	// not yet started.
	PodStatusPending = "PENDING"
	// PodStatusRunning is for k8s pod that are currently in progress.
	PodStatusRunning = "RUNNING"
	// DefaultModelVersionID default model version id
	DefaultModelVersionID = "00000000-0000-0000-0000-000000000000"
)

func (mgt *mlManagement) createModelVersion(model *tree.MLModel) (*modelVersionRecord, error) {

	schema, name, err := mgt.extractSchemaObjectName(string(*model.Name))
	if err != nil {
		return nil, err
	}

	modelVersion := &modelVersionRecord{
		Creator: mgt.planner.User(),
		Schema:  schema,
	}

	modelVersion.Name = name
	if !lex.CheckNameValid(modelVersion.Name) {
		return nil, pgerror.Newf(
			pgcode.InvalidParameterValue, "model name '%s' is unsupported", modelVersion.Name,
		)
	}

	if err = mlUtils.checkIsvcNameLength(modelVersion.genIsvcModelName()); err != nil {
		return nil, err
	}

	modelVersion.Description = string(*model.Description)

	modelVersion.ProblemType, err = mlUtils.checkProblemType(string(*model.ProblemType))
	if err != nil {
		return nil, err
	}

	modelVersion.Framework, err = mlUtils.checkFramework(string(*model.Framework))
	if err != nil {
		return nil, err
	}

	modelVersion.Runtime, err = mlUtils.checkRuntime(string(*model.Runtime))
	if err != nil {
		return nil, err
	}

	if modelVersion.FeaturesJSON, modelVersion.Features, err = mlUtils.checkFeatures(*model.Features); err != nil {
		return nil, err
	}

	if modelVersion.TargetJSON, modelVersion.Target, err = mlUtils.checkTargets(*model.Target); err != nil {
		return nil, err
	}

	if model.Metrics.JSON != nil {
		modelVersion.MetricsJSON, modelVersion.Metrics, err = mlUtils.checkMetrics(*model.Metrics)
		if err != nil {
			return nil, err
		}
	}

	if model.ModelEntity != nil {
		modelVersion.ModelEntity = []byte(*model.ModelEntity)
		if modelVersion.ModelEntityMd5, err = mlUtils.computeChecksum(modelVersion.ModelEntity); err != nil {
			return nil, err
		}
	}

	if model.TransformerEntity != nil {
		modelVersion.TransformerEntity = []byte(*model.TransformerEntity)
		if modelVersion.TransformerMd5, err = mlUtils.computeChecksum(modelVersion.TransformerEntity); err != nil {
			return nil, err
		}
	}

	modelVersion.setStatus(pb.NewUnknownStatus("undeploy"))

	return modelVersion, nil
}

// CreateMLModelRecord creates a machine learning model in kwdbml.ml_models
func (mgt *mlManagement) insertModelRecord(mv *modelVersionRecord) error {
	// Insert into the ml model table and detect collisions.
	// for new model, active_version_id must be empty.
	num, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"create-ml-model",
		mgt.planner.Txn(),
		`INSERT INTO kwdbml.ml_models (id, Schema, name, description, problem_type, features, target, framework,
			runtime, Creator, created, last_updated, active_version_id, status) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		mv.ModelID,
		mv.Schema,
		mv.Name,
		mv.Description,
		mv.ProblemType,
		mv.FeaturesJSON,
		mv.TargetJSON,
		mv.Framework,
		mv.Runtime,
		mv.Creator,
		"now()",
		"now()",
		DefaultModelVersionID,
		mv.StatusJSON,
	)

	if err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			return pgerror.Newf(pgcode.DuplicateObject, "model '%s' already exists", mv.getModelName())
		}
		return errors.Wrap(err, "failed to create a new ml model")
	}

	if num != 1 {
		return errors.Newf("unexpected number of rows affected: %d", num)
	}

	return nil
}

func (mgt *mlManagement) insertModelVersionRecord(mv *modelVersionRecord) error {
	num, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"create-ml-model-version",
		mgt.planner.Txn(),
		`INSERT INTO kwdbml.ml_model_versions(id, model_id, version, Creator, created, 
metrics, description, entity, md5, transformer_entity, transformer_md5) 
VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		mv.VersionID,
		mv.ModelID,
		mv.Version,
		mv.Creator,
		"now()",
		mv.getMetricsOrNull(),
		mv.Description,
		mv.ModelEntity,
		mv.ModelEntityMd5,
		mv.getTransformerEntityOrNull(),
		mv.getTransformerMd5OrNull(),
	)

	if err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			return pgerror.Newf(pgcode.DuplicateObject, "model version '%d' already exists", mv.Version)
		}
		return errors.Wrap(err, "failed to create a new ml model version")
	}

	if num != 1 {
		return errors.Newf("unexpected number of rows affected: %d", num)
	}

	return nil
}

func (mv *modelVersionRecord) checkModelMetadata(old *modelVersionRecord) error {
	if old.ProblemType != mv.ProblemType {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"problem type '%s' is inconsistent with last version '%s'",
			mv.ProblemType,
			old.ProblemType,
		)
	}

	if old.Framework != mv.Framework {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"framework '%s' is inconsistent with last version '%s'",
			mv.Framework,
			old.Framework,
		)
	}

	if old.Runtime != mv.Runtime {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"runtime '%s' is inconsistent with last version '%s'",
			mv.Runtime,
			old.Runtime,
		)
	}

	if old.FeaturesJSON != mv.FeaturesJSON {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"features '%s' is inconsistent with last version '%s'",
			mv.FeaturesJSON,
			old.FeaturesJSON,
		)
	}

	if old.TargetJSON != mv.TargetJSON {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"targets '%s' is inconsistent with last version '%s'",
			mv.TargetJSON,
			old.TargetJSON,
		)
	}

	return nil
}

func (mgt *mlManagement) fillInModelVersionNumber(mv *modelVersionRecord) error {
	// Increment Model version.
	key := roachpb.Key(mlUtils.makeVersionNumberGeneratorKey(keys.ModelVersionPrefix, roachpb.RKey(mv.ModelID)))
	newVal, err := mgt.planner.Txn().Inc(mgt.ctx, key, 1)
	if err != nil {
		return errors.Wrap(err, "failed to generate model version number")
	}

	mv.Version = int32(newVal.ValueInt())
	return nil
}

func (mgt *mlManagement) removeModelVersionNumberGenerator(modelID string) error {
	key := roachpb.Key(mlUtils.makeVersionNumberGeneratorKey(keys.ModelVersionPrefix, roachpb.RKey(modelID)))

	return mgt.planner.Txn().Del(mgt.ctx, key)
}

func (mgt *mlManagement) createAndUpdateIsvcStatus(mv *modelVersionRecord) error {
	status, err := mgt.createIsvc(mv)
	if err != nil {
		return err
	}
	mv.setStatus(status)

	if err := mgt.updateIsvcStatus(mv); err != nil {
		return err
	}

	return nil
}

func (mgt *mlManagement) createIsvc(mv *modelVersionRecord) (*pb.IsvcStatus, error) {
	req := &pb.CreateIsvcRequest{
		ModelSpec: &pb.ModelSpec{
			ModelName:        mv.genIsvcModelName(),
			ModelVersion:     mv.genVersionString(),
			ProblemType:      mv.ProblemType,
			ModelFramework:   mv.Framework,
			ModelContent:     mv.ModelEntity,
			ModelPreparation: mv.TransformerEntity,
		},
	}

	mlClient, err := mlUtils.newClient(mgt.planner)
	if err != nil {
		return nil, err
	}

	res, err := mlClient.CreateIsvc(mgt.ctx, req)
	if err != nil {
		return pb.NewErrorStatus(err.Error()), nil
	}

	if res.IsvcOutput == nil {
		return pb.NewErrorStatus("failed to get service output"), nil
	}

	return res.IsvcOutput.IsvcStatus, nil
}

func (mgt *mlManagement) updateIsvcStatus(mv *modelVersionRecord) error {
	if _, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"update-ml-model-status",
		mgt.planner.Txn(),
		"UPDATE kwdbml.ml_models SET status = $1 WHERE schema = $2 AND name = $3",
		mv.StatusJSON,
		mv.Schema,
		mv.Name,
	); err != nil {
		return err
	}

	return nil
}

func (mgt *mlManagement) getAndUpdateIsvcStatus(mv *modelVersionRecord) (*pb.IsvcStatus, error) {
	status, err := mgt.getIsvcStatus(mv)
	if err != nil {
		return nil, err
	}
	mv.setStatus(status)

	if err := mgt.updateIsvcStatus(mv); err != nil {
		return nil, err
	}

	return mv.Status, nil
}

func (mgt *mlManagement) getIsvcStatus(mv *modelVersionRecord) (*pb.IsvcStatus, error) {
	req := &pb.GetIsvcRequest{
		ModelSpec: &pb.ModelSpec{
			ModelName:    mv.genIsvcModelName(),
			ModelVersion: mv.genVersionString(),
		},
	}

	mlClient, err := mlUtils.newClient(mgt.planner)
	if err != nil {
		return nil, err
	}

	res, err := mlClient.GetIsvc(mgt.ctx, req)
	if err == nil {
		if res.IsvcOutput == nil {
			return pb.NewErrorStatus("failed to get service output"), nil
		}

		return res.IsvcOutput.IsvcStatus, nil
	}

	if !pb.MatchErrorCode(err, pb.Code_ModelVersionNotExist) {
		return pb.NewErrorStatus(err.Error()), nil
	}

	if err = mgt.createAndUpdateIsvcStatus(mv); err != nil {
		return pb.NewErrorStatus(err.Error()), nil
	}

	return pb.NewInitializeStatus("the KaiwuDB ML model is not ready, please try again later"), nil
}

func (mgt *mlManagement) getBatchIsvc(models []*pb.ModelSpec) ([]*pb.IsvcSpec, error) {
	req := &pb.GetBatchIsvcRequest{
		ModelSpecs: models,
	}

	mlClient, err := mlUtils.newClient(mgt.planner)
	if err != nil {
		return nil, err
	}

	res, err := mlClient.GetBatchIsvc(mgt.ctx, req)
	if err != nil {
		return nil, err
	}

	return res.Output, nil
}

func (mgt *mlManagement) getResources(
	names []*pb.Resource, jobs []*pb.JobResource,
) ([]tree.Datums, error) {
	req := &pb.GetResourcesRequest{
		Resources: names,
	}

	mlClient, err := mlUtils.newClient(mgt.planner)
	if err != nil {
		return nil, err
	}

	res, err := mlClient.GetResources(mgt.ctx, req)
	if err != nil {
		return nil, err
	}

	records := make([]tree.Datums, 0, len(res.Resources))

	for _, resource := range res.Resources {
		records = append(records, tree.Datums{
			tree.NewDString(strings.ReplaceAll(resource.Name, "-", ".")),
			tree.NewDString(resource.Type),
			tree.NewDString(resource.CPU),
			tree.NewDString(resource.Memory),
			tree.NewDString(resource.Storage),
		})
	}

	resJob, err := mlClient.GetJobResources(mgt.ctx, &pb.JobResourcesRequest{
		Resources: jobs,
	})
	if err != nil {
		log.Warning(mgt.ctx, err)
		return records, nil
	}

	for _, resource := range resJob.Resources {
		records = append(records, tree.Datums{
			tree.NewDString(resource.Name),
			tree.NewDString(resource.Type),
			tree.NewDString(resource.CPU),
			tree.NewDString(resource.Memory),
			tree.NewDString(resource.Storage),
		})
	}

	return records, nil
}

func (mgt *mlManagement) getModelNames() ([]*pb.Resource, error) {
	stmt := `SELECT sm.schema, sm.name, coalesce(smv.version, 0) 
	FROM kwdbml.ml_models sm INNER JOIN kwdbml.ml_model_versions smv ON sm.active_version_id = smv.id`

	rows, err := mgt.planner.InternalExecutor().Query(mgt.ctx, "fetch-resources-names", mgt.planner.Txn(), stmt)
	if err != nil {
		return nil, err
	}

	names := make([]*pb.Resource, 0)

	for _, row := range rows {
		model := &modelVersionRecord{
			Schema:  string(*row[0].(*tree.DString)),
			Name:    string(*row[1].(*tree.DString)),
			Version: int32(*row[2].(*tree.DInt)),
		}

		names = append(names, &pb.Resource{
			Name: model.genIsvcModelName(),
			Type: pb.ResourceTypeModel,
		})
	}

	return names, nil
}

func (mgt *mlManagement) findModels(roleMembers []string, isAdmin bool) ([]tree.Datums, error) {
	stmt := `
		SELECT sm.schema, sm.name, sm.description, sm.creator, sm.created, sm.last_updated,
			sm.problem_type, sm.features,sm.target, sm.framework, sm.runtime,
			coalesce(smv.version, 0) AS active_version, smv.metrics AS last_metrics, sm.status
		FROM kwdbml.ml_models sm
		LEFT JOIN kwdbml.ml_model_versions smv ON sm.active_version_id = smv.id`
	if !isAdmin {
		stmt = fmt.Sprintf(` %s
			INNER JOIN kwdbml.ml_privileges p
			ON p.schema = sm.schema AND p.name = sm.name AND p.type=%d
			AND p.privilege_type >= %d
			AND p.grantee IN ('%s')`,
			stmt,
			mlModel,
			mlSelectPrivilegeMask,
			strings.Join(roleMembers, "','"),
		)
	}

	stmt = fmt.Sprintf(` %s ORDER BY sm.created`, stmt)

	rows, err := mgt.planner.InternalExecutor().Query(mgt.ctx, "fetch-all-ml-models", mgt.planner.Txn(), stmt)
	if err != nil {
		return rows, err
	}

	if !isAdmin {
		// for general user, display internal model to them
		stmtInternalModel := `
		SELECT sm.schema, sm.name, sm.description, sm.creator, sm.created, sm.last_updated,
			sm.problem_type, sm.features,sm.target, sm.framework, sm.runtime,
			coalesce(smv.version, 0) AS active_version, smv.metrics AS last_metrics, sm.status
		FROM kwdbml.ml_models sm
		LEFT JOIN kwdbml.ml_model_versions smv ON sm.active_version_id = smv.id
		WHERE sm.schema=$1 AND sm.name=$2
		`
		nrows, nerr := mgt.planner.InternalExecutor().Query(
			mgt.ctx,
			"fetch-internal-model",
			mgt.planner.Txn(),
			stmtInternalModel,
			"public",
			TSLLMName,
		)
		if err != nil {
			log.Warning(mgt.ctx, nerr)
			return rows, nil
		}

		return append(rows, nrows...), nil
	}

	return rows, nil
}

func (mgt *mlManagement) updateBatchIsvcStatus(roleMembers []string, isAdmin bool) error {
	stmt := `
		SELECT sm.schema, sm.name, coalesce(smv.version, 0), sm.status FROM kwdbml.ml_models sm 
		LEFT JOIN kwdbml.ml_model_versions smv ON sm.active_version_id = smv.id`
	if !isAdmin {
		stmt = fmt.Sprintf(` %s
			INNER JOIN kwdbml.ml_privileges p
			ON p.schema = sm.schema AND p.name = sm.name AND p.type=%d
			AND p.privilege_type >= %d
			AND p.grantee IN ('%s')`,
			stmt,
			mlModel,
			mlSelectPrivilegeMask,
			strings.Join(roleMembers, "','"),
		)
	}
	stmt = fmt.Sprintf(` %s WHERE sm.status->>'status' = '%s'`, stmt, pb.StatusIsvcInitializing)

	rows, err := mgt.planner.InternalExecutor().Query(mgt.ctx, "fetch-initializing-ml-models", mgt.planner.Txn(), stmt)
	if err != nil {
		return err
	}

	if !isAdmin {
		// for general user, display internal model to them
		nrows, nerr := mgt.planner.InternalExecutor().Query(
			mgt.ctx,
			"fetch-initializing-internal-models",
			mgt.planner.Txn(),
			`SELECT sm.schema, sm.name, coalesce(smv.version, 0), sm.status FROM kwdbml.ml_models sm 
		LEFT JOIN kwdbml.ml_model_versions smv ON sm.active_version_id = smv.id
		WHERE sm.schema=$1 AND sm.name=$2`,
			"public",
			TSLLMName,
		)
		if nerr != nil {
			log.Warning(mgt.ctx, err)
		} else {
			rows = append(rows, nrows...)
		}
	}

	rowCount := len(rows)
	if rowCount == 0 {
		return nil
	}

	modelVersionMap := make(map[string]*modelVersionRecord, rowCount)
	modelSpecs := make([]*pb.ModelSpec, 0)

	for _, row := range rows {
		mv := &modelVersionRecord{
			Schema:  string(*row[0].(*tree.DString)),
			Name:    string(*row[1].(*tree.DString)),
			Version: int32(*row[2].(*tree.DInt)),
		}

		// no active version
		if mv.Version == 0 {
			continue
		}

		if row[3] != tree.DNull {
			mv.StatusJSON = row[3].(*tree.DJSON).JSON.String()
			mv.Status = &pb.IsvcStatus{}
			if err = ejson.Unmarshal([]byte(mv.StatusJSON), mv.Status); err != nil {
				return errors.Wrap(err, "failed to unmarshal the model status")
			}
		}

		spec := &pb.ModelSpec{
			ModelName:    mv.genIsvcModelName(),
			ModelVersion: mv.genVersionString(),
		}

		modelSpecs = append(modelSpecs, spec)
		modelVersionMap[spec.GenIsvcName()] = mv
	}

	isvcList, err := mgt.getBatchIsvc(modelSpecs)
	if err != nil {
		log.Warningf(mgt.ctx, "failed to fetch the model status with error: %v", err)
		return nil
	}

	if isvcList == nil {
		return nil
	}

	for _, isvc := range isvcList {
		if isvc == nil || isvc.IsvcStatus == nil {
			continue
		}

		mv, ok := modelVersionMap[isvc.IsvcName]
		if !ok {
			continue
		}

		if isvc.IsvcStatus.Status != mv.Status.Status {
			mv.setStatus(isvc.IsvcStatus)

			if err = mgt.updateIsvcStatus(mv); err != nil {
				return err
			}
		}
	}

	return nil
}

func (mgt *mlManagement) deleteIsvc(mv *modelVersionRecord) error {
	if mv.Version == 0 {
		return nil
	}

	req := &pb.DeleteIsvcRequest{
		ModelSpec: &pb.ModelSpec{
			ModelName:    mv.genIsvcModelName(),
			ModelVersion: mv.genVersionString(),
		},
	}

	mlClient, err := mlUtils.newClient(mgt.planner)
	if err != nil {
		return err
	}

	if err = mlClient.DeleteIsvc(mgt.ctx, req); err != nil {
		return err
	}

	return nil
}

func (mgt *mlManagement) predict(inputPredict string, mv *modelVersionRecord) (json.JSON, error) {
	output, err := mgt.remotePredict(inputPredict, mv)
	if err == nil {
		res, err := json.ParseJSON(string(output))
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse the predict result")
		}

		return res, nil
	}

	if !pb.MatchErrorCode(err, pb.Code_ModelVersionNotExist) {
		return nil, errors.Wrapf(err, "failed to do the prediction using model '%s'", mv.getModelName())
	}

	if err = mgt.createAndUpdateIsvcStatus(mv); err != nil {
		return nil, errors.Wrapf(err, "failed to do the prediction using model '%s'", mv.getModelName())
	}

	return nil, errors.New("the KaiwuDB ML model is not ready, please try again later")
}

func (mgt *mlManagement) remotePredict(
	inputPredict string, mv *modelVersionRecord,
) ([]byte, error) {
	req := &pb.KPredictRequest{
		ModelName:     mv.genIsvcModelName(),
		ModelVersion:  mv.genVersionString(),
		PredictInputs: []byte(inputPredict),
	}

	mlClient, err := mlUtils.newClient(mgt.planner)
	if err != nil {
		return nil, err
	}

	var res *pb.KPredictResponse
	start := timeutil.Now()
	mgt.planner.MLPredictMetrics().OnlinePredictTotalCount.Inc(1)
	res, err = mlClient.KPredict(mgt.ctx, req)
	if err != nil {
		return nil, err
	}

	mgt.planner.MLPredictMetrics().OnlinePredictResponseTime.RecordValue(int64(timeutil.Since(start)))
	mgt.planner.MLPredictMetrics().OnlinePredictSuccessCount.Inc(1)

	return res.PredictOutputs, nil
}

func (mgt *mlManagement) deleteModel(modelID string) error {
	if _, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"delete-ml-model-versions",
		mgt.planner.Txn(),
		"DELETE FROM kwdbml.ml_model_versions WHERE model_id = $1",
		modelID,
	); err != nil {
		return errors.Wrap(err, "failed to remove the ml model versions")
	}

	if _, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"remove-one-ml-model",
		mgt.planner.Txn(),
		"DELETE FROM kwdbml.ml_models WHERE id = $1 ",
		modelID,
	); err != nil {
		return errors.Wrap(err, "failed to remove the ml model")
	}

	return nil
}

func (mgt *mlManagement) isActiveModel(modelID string) (bool, error) {
	if rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"retrieve-job-status",
		mgt.planner.Txn(),
		"SELECT job_status FROM kwdbml.ml_jobs WHERE model_id = $1 AND (job_status = $2 OR job_status = $3)",
		modelID,
		PodStatusPending,
		PodStatusRunning,
	); err != nil {
		return false, errors.Wrap(err, "failed to set the active ml model")
	} else if len(rows) > 0 {
		return true, nil
	}

	return false, nil
}

func (mgt *mlManagement) deleteOneModelVersion(modelID string, version int32) error {
	if _, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"remove-one-ml-model-version",
		mgt.planner.Txn(),
		"DELETE FROM kwdbml.ml_model_versions WHERE model_id = $1 AND version = $2",
		modelID,
		version,
	); err != nil {
		return errors.Wrap(err, "failed to remove the specified ml model version")
	}

	return nil
}

func (mgt *mlManagement) isActiveModelVersion(modelVersion *modelVersionRecord) (bool, error) {
	if modelVersion.VersionID == modelVersion.ActiveVersionID {
		return true, nil
	}

	if rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"check-job-status",
		mgt.planner.Txn(),
		`SELECT job_status FROM kwdbml.ml_jobs 
		WHERE model_id = $1 AND model_version_id = $2 AND (job_status = $3 OR job_status = $4)`,
		modelVersion.ModelID,
		modelVersion.VersionID,
		PodStatusPending,
		PodStatusRunning,
	); err != nil {
		return false, errors.Wrap(err, "failed to check the active model version")
	} else if len(rows) > 0 {
		return true, nil
	}

	return false, nil
}

func (mgt *mlManagement) fetchActiveModelVersion(schema, name string) (*modelVersionRecord, error) {
	rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"fetch-current-active-version",
		mgt.planner.Txn(),
		`SELECT m.id, v.id, coalesce(v.version, 0), m.Schema, m.name, m.description, m.problem_type, m.framework, 
							m.runtime, m.features, m.target, v.entity, v.transformer_entity, v.metrics, m.active_version_id, m.status
		FROM kwdbml.ml_models m LEFT JOIN kwdbml.ml_model_versions v ON m.active_version_id = v.id
		WHERE m.Schema = $1 AND m.name = $2`,
		schema,
		name,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch the active model version")
	}

	if len(rows) < 1 {
		return nil, pgerror.Newf(pgcode.InvalidSchemaName, "model '%s.%s' does not exist", schema, name)
	}

	return mgt.parseMLModelVersion(rows[0])
}

func (mgt *mlManagement) fetchModelVersion(
	schema, name string, modelVersion int32,
) (*modelVersionRecord, error) {
	var rows []tree.Datums
	var err error
	if modelVersion == 0 {
		rows, err = mgt.planner.InternalExecutor().Query(
			mgt.ctx,
			"fetch-latest-version ",
			mgt.planner.Txn(),
			`SELECT m.id, mv.id, mv.version, m.Schema, m.name, m.description, m.problem_type, m.framework, m.runtime, 
							m.features, m.target, mv.entity, mv.transformer_entity, mv.metrics, m.active_version_id, m.status
			FROM kwdbml.ml_models m INNER JOIN kwdbml.ml_model_versions mv ON mv.model_id = m.id 
			WHERE m.Schema = $1 AND m.name = $2 ORDER BY mv.version DESC LIMIT 1 `,
			schema,
			name,
		)
	} else {
		rows, err = mgt.planner.InternalExecutor().Query(
			mgt.ctx,
			"fetch-model-version",
			mgt.planner.Txn(),
			`SELECT m.id, mv.id, mv.version, m.Schema, m.name, m.description, m.problem_type, m.framework, m.runtime, 
							m.features, m.target, mv.entity, mv.transformer_entity, mv.metrics, m.active_version_id, m.status
			FROM kwdbml.ml_models m INNER JOIN kwdbml.ml_model_versions mv ON mv.model_id = m.id 
			WHERE m.Schema = $1 AND m.name = $2 AND mv.version = $3`,
			schema,
			name,
			modelVersion,
		)
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch the specified model version")
	}

	if len(rows) < 1 {
		return nil, pgerror.Newf(
			pgcode.NoDataFound, "the version '%d' of model '%s.%s' does not exist", modelVersion, schema, name,
		)
	}

	return mgt.parseMLModelVersion(rows[0])

}

func (mgt *mlManagement) hasModel(schema, name string) (*modelVersionRecord, error) {
	rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"fetch-latest-ml-model-version ",
		mgt.planner.Txn(),
		`SELECT m.id, mv.id, mv.version, m.Schema, m.name, m.description, m.problem_type, m.framework, m.runtime, 
							m.features, m.target, mv.entity, mv.transformer_entity, mv.metrics, m.active_version_id, m.status
			FROM kwdbml.ml_models m INNER JOIN kwdbml.ml_model_versions mv ON mv.model_id = m.id 
			WHERE m.Schema = $1 AND m.name = $2 ORDER BY mv.version DESC LIMIT 1 `,
		schema,
		name,
	)

	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch the ml model metadata")
	}

	if len(rows) == 0 {
		return nil, nil
	}

	return mgt.parseMLModelVersion(rows[0])
}

func (mgt *mlManagement) hasMLResource(schema, name string, resType mlResourceType) (bool, error) {
	var stmt string
	switch resType {
	case mlModel:
		stmt = `SELECT id FROM kwdbml.ml_models WHERE schema = $1 AND name = $2 LIMIT 1`
	case mlTrainingPipeline:
		stmt = `SELECT id FROM kwdbml.ml_training_pipelines WHERE schema = $1 AND name = $2 LIMIT 1`
	default:
		return false, errors.Newf("unsupported ml resource type: '%s", resType)
	}

	rows, err := mgt.planner.InternalExecutor().QueryRow(
		mgt.ctx,
		"fetch-one-ml-resource",
		mgt.planner.Txn(),
		stmt,
		schema,
		name,
	)
	if err != nil {
		return false, errors.Wrapf(err, "failed to fetch the ml %s metadata", resType.String())
	}

	if rows.Len() > 0 {
		return true, nil
	}

	return false, nil
}

func (mgt *mlManagement) parseMLModelVersion(row tree.Datums) (*modelVersionRecord, error) {
	model := &modelVersionRecord{
		ModelID:      row[0].(*tree.DUuid).UUID.String(),
		Version:      int32(*row[2].(*tree.DInt)),
		Schema:       string(*row[3].(*tree.DString)),
		Name:         string(*row[4].(*tree.DString)),
		Description:  string(*row[5].(*tree.DString)),
		ProblemType:  string(*row[6].(*tree.DString)),
		Framework:    string(*row[7].(*tree.DString)),
		Runtime:      string(*row[8].(*tree.DString)),
		FeaturesJSON: tree.MustBeDJSON(row[9]).JSON.String(),
		TargetJSON:   tree.MustBeDJSON(row[10]).JSON.String(),
	}
	if row[1] != tree.DNull {
		model.VersionID = row[1].(*tree.DUuid).UUID.String()
	}

	if err := ejson.Unmarshal([]byte(model.FeaturesJSON), &model.Features); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal the model features")
	}

	if err := ejson.Unmarshal([]byte(model.TargetJSON), &model.Target); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal the model target")
	}

	if row[11] != tree.DNull {
		model.ModelEntity = []byte(*row[11].(*tree.DBytes))
	}

	if row[12] != tree.DNull {
		model.TransformerEntity = []byte(*row[12].(*tree.DBytes))
	}

	if row[13] != tree.DNull {
		model.MetricsJSON = tree.MustBeDJSON(row[13]).JSON.String()
		if err := ejson.Unmarshal([]byte(model.MetricsJSON), &model.Metrics); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal the model metrics")
		}
	}

	if row[14] != tree.DNull {
		model.ActiveVersionID = row[14].(*tree.DUuid).UUID.String()
	}

	if row[15] != tree.DNull {
		model.StatusJSON = row[15].(*tree.DJSON).JSON.String()
		if err := ejson.Unmarshal([]byte(model.StatusJSON), &model.Status); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal the model status")
		}
	}

	return model, nil
}

func (mgt *mlManagement) parseMLPredictParam(
	args tree.Datums, features []*pb.Feature, modelName string,
) (string, error) {
	colCount := len(args)
	featureCount := len(features)

	// 使用前缀匹配，支持 tsllm, tsllm_xxx 等模型名称
	isTsllmModel := strings.HasPrefix(modelName, TSLLMName)

	if isTsllmModel && colCount > 10 {
		return "", pgerror.Newf(
			pgcode.InvalidParameterValue,
			"the number of predict columns for time series LLM model exceeds the maximum limit of 10, actual: %d",
			colCount,
		)
	}

	if !isTsllmModel && colCount != featureCount {
		return "", pgerror.Newf(
			pgcode.InvalidColumnDefinition,
			"the column number of predict data is not match with the ML feature definition, expect '%d', but got '%d'",
			featureCount,
			colCount,
		)
	}

	param := &pb.PredictParam{
		Instances: make([][]interface{}, 1),
		Header:    make([]string, featureCount),
	}

	param.Instances[0] = make([]interface{}, colCount)
	if !isTsllmModel {
		for i, f := range features {
			param.Header[i] = f.Name
			value := args[i]

			if err := mlUtils.checkSupportDataType(f.Name, f.Type, value); err != nil {
				return "", err
			}

			param.Instances[0][i] = mlUtils.DatumToString(value)
		}
	} else {
		const maxArrayLength = 50000
		for i := range args {
			if dArr, ok := args[i].(*tree.DArray); ok {
				// Truncate array if exceeds max length
				if len(dArr.Array) > maxArrayLength {
					dArr.Array = dArr.Array[:maxArrayLength]
				}
				// Convert DArray to []interface{} so ejson.Marshal produces a proper JSON array
				arr := make([]interface{}, len(dArr.Array))
				for j, elem := range dArr.Array {
					arr[j] = mlUtils.DatumToString(elem)
				}
				param.Instances[0][i] = arr
				continue
			}
			param.Instances[0][i] = mlUtils.DatumToString(args[i])
		}
	}

	paramString, err := ejson.Marshal(param)
	if err != nil {
		return "", err
	}

	return string(paramString), nil
}

type modelVersionRecord struct {
	ModelID           string
	VersionID         string
	Version           int32
	Schema            string
	Name              string
	Description       string
	ProblemType       string
	Framework         string
	Runtime           string
	FeaturesJSON      string
	Features          []*pb.Feature
	TargetJSON        string
	Target            []*pb.Target
	ModelEntity       []byte
	TransformerEntity []byte
	MetricsJSON       string
	Metrics           []*pb.Metrics
	ActiveVersionID   string
	ModelEntityMd5    string
	TransformerMd5    string
	Creator           string
	Status            *pb.IsvcStatus
	StatusJSON        string
}

func (mv *modelVersionRecord) getTransformerMd5OrNull() interface{} {
	if mv.TransformerMd5 == "" {
		return nil
	}

	return mv.TransformerMd5
}

func (mv *modelVersionRecord) getTransformerEntityOrNull() interface{} {
	if mv.TransformerMd5 == "" {
		return nil
	}

	return mv.TransformerEntity
}

func (mv *modelVersionRecord) getMetricsOrNull() interface{} {
	if mv.MetricsJSON == "" {
		return nil
	}

	return mv.MetricsJSON
}

func (mv *modelVersionRecord) getModelName() string {
	return mlUtils.buildFullName(mv.Schema, mv.Name)
}

func (mv *modelVersionRecord) genIsvcModelName() string {
	return fmt.Sprintf("%s-%s", mv.Schema, mv.Name)
}

func (mv *modelVersionRecord) genVersionString() string {
	return strconv.FormatInt(int64(mv.Version), 10)
}

func (mv *modelVersionRecord) setStatus(status *pb.IsvcStatus) {
	if status == nil {
		mv.Status = pb.NewErrorStatus("the KaiwuDB ML model status is empty")
		mv.StatusJSON = mv.Status.JSONString()
		return
	}

	mv.Status = status
	mv.StatusJSON = mv.Status.JSONString()
}
