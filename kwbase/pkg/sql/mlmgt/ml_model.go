// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ImportModel imports a machine learning model into the system
func (m *MLManager) ImportModel(ctx context.Context, model *tree.MLModel) (string, int32, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return "", 0, err
	}

	newVersion, err := mgt.createModelVersion(model)
	if err != nil {
		return "", 0, err
	}

	var isNewModel bool

	oldVersion, err := mgt.fetchActiveModelVersion(newVersion.Schema, newVersion.Name)
	if err != nil {
		//fetchActiveModelVersion is a common function, it returns error with pgcode.InvalidSchemaName for new model case
		if pgerror.GetPGCode(err) != pgcode.InvalidSchemaName {
			return "", 0, err
		}
		isNewModel = true
	}

	newVersion.VersionID = uuid.MakeV4().String()

	if isNewModel {
		newVersion.ModelID = uuid.MakeV4().String()
		if err = mgt.insertModelRecord(newVersion); err != nil {
			return "", 0, err
		}

		if _, err = mgt.grantPrivileges(
			mlModel, newVersion.Schema, newVersion.Name, m.Planner.User(), mlModelAllPrivilegeMask, true,
		); err != nil {
			return "", 0, err
		}
	} else {
		newVersion.ModelID = oldVersion.ModelID

		if err = newVersion.checkModelMetadata(oldVersion); err != nil {
			return "", 0, err
		}

		if isAuthed, err := mgt.checkPrivileges(
			mlModel, m.Planner.User(), newVersion.Schema, newVersion.Name, mlCreatePrivilegeMask,
		); err != nil {
			return "", 0, err
		} else if !isAuthed {
			return "", 0, errors.Newf(
				"user '%s' does not have '%s' privilege on model '%s'",
				m.Planner.User(),
				mlUtils.constructPrivilegeString(mlModel, mlCreatePrivilegeMask),
				newVersion.getModelName(),
			)
		}
	}

	if err = mgt.fillInModelVersionNumber(newVersion); err != nil {
		return "", 0, err
	}

	if err = mgt.insertModelVersionRecord(newVersion); err != nil {
		return "", 0, err
	}

	if !isNewModel {
		if _, err = mgt.planner.InternalExecutor().Exec(
			mgt.ctx,
			"update-ml-model-last-update-time",
			mgt.planner.Txn(),
			"UPDATE kwdbml.ml_models SET last_updated = now() WHERE schema = $1 AND name = $2",
			newVersion.Schema,
			newVersion.Name,
		); err != nil {
			return "", 0, err
		}
	}

	return newVersion.getModelName(), newVersion.Version, nil
}

// SetActive activates a machine learning model for inference
func (m *MLManager) SetActive(
	ctx context.Context, modelName string, modelVersion int32,
) (int32, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return 0, err
	}

	schema, name, err := mgt.extractSchemaObjectName(modelName)
	if err != nil {
		return 0, err
	}

	if isAuthed, err := mgt.checkPrivileges(mlModel, m.Planner.User(), schema, name, mlUpdatePrivilegeMask); err != nil {
		return 0, err
	} else if !isAuthed {
		return 0, pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"user '%s' does not have '%s' privilege on model '%s'",
			m.Planner.User(),
			mlUtils.constructPrivilegeString(mlModel, mlUpdatePrivilegeMask),
			modelName,
		)
	}

	newVersion, err := mgt.fetchModelVersion(schema, name, modelVersion)
	if err != nil {
		return 0, err
	}

	activeVersion, err := mgt.fetchActiveModelVersion(schema, name)
	if err != nil {
		return 0, err
	}

	if newVersion.Version == activeVersion.Version {
		return newVersion.Version, nil
	}

	if err = mgt.createAndUpdateIsvcStatus(newVersion); err != nil {
		return 0, err
	}

	const sql = "UPDATE kwdbml.ml_models SET active_version_id = $1 WHERE schema = $2 AND name = $3"
	if _, err = m.Planner.InternalExecutor().Exec(
		ctx, "update-ml-model-active-version", m.Planner.Txn(), sql, newVersion.VersionID, schema, name); err != nil {
		return 0, err
	}

	if err = mgt.deleteIsvc(activeVersion); err != nil {
		log.Warning(ctx, "failed to remove the deployment: %v", err)
	}

	return newVersion.Version, nil
}

// Predict performs prediction using a trained machine learning model
func (m *MLManager) Predict(ctx context.Context, args tree.Datums) (json.JSON, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return nil, err
	}

	modelName := m.Planner.GetMLModelName()

	schema, name, err := mgt.extractSchemaObjectName(modelName)
	if err != nil {
		if strings.Contains(err.Error(), "object name is empty") {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue, "the ML model name is required")
		}

		return nil, err
	}

	existing, err := mgt.hasMLResource(schema, name, mlModel)
	if err != nil {
		return nil, err
	} else if !existing {
		return nil, errors.Newf("%s '%s' does not exist", mlModel.String(), modelName)
	}

	isAuthed, err := mgt.checkPredictPrivilege(m.Planner.User(), schema, name)
	if err != nil {
		return nil, err
	}

	if !isAuthed {
		return nil, pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"user '%s' does not have '%s' privilege on model '%s'",
			m.Planner.User(),
			mlUtils.constructPrivilegeString(mlModel, mlPredictPrivilegeMask),
			modelName,
		)
	}

	model, err := mgt.fetchActiveModelVersion(schema, name)
	if err != nil {
		return nil, err
	}

	if model.Version == 0 {
		return nil, errors.Newf("no active version found, schema: %s, name: %s", schema, modelName)
	}

	inputPredict, err := mgt.parseMLPredictParam(args, model.Features, modelName)
	if err != nil {
		return nil, err
	}

	// predict will update the model status with the new txn, and will not rollback on an error of BIF is returned.
	mgtNewTxn, err := newMLManagementWithoutTxn(ctx, m.Planner)
	if err != nil {
		return nil, err
	}

	if model.Status.IsReady() {
		return mgtNewTxn.predict(inputPredict, model)
	}

	status, err := mgtNewTxn.getAndUpdateIsvcStatus(model)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse the prediction result")
	}

	if status.IsReady() {
		return mgtNewTxn.predict(inputPredict, model)
	}

	return nil, errors.Newf(
		"the KaiwuDB ML model is not ready, status: %s, message: %s, use kwdbml.show_models() for detail info",
		status.Status,
		status.Message,
	)
}

// FindModels searches for machine learning models matching criteria
func (m *MLManager) FindModels(ctx context.Context) ([]tree.Datums, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return nil, err
	}

	roleMembers, isAdmin, err := mgt.extractRoleMembersWithAdminFlag(m.Planner.User())
	if err != nil {
		return nil, err
	}

	if err = mgt.updateBatchIsvcStatus(roleMembers, isAdmin); err != nil {
		return nil, err
	}

	return mgt.findModels(roleMembers, isAdmin)
}

// FindModel finds a specific machine learning model by its ID
func (m *MLManager) FindModel(
	ctx context.Context, modelName string, modelVersion int32,
) ([]tree.Datums, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return nil, err
	}

	roleMembers, isAdmin, err := mgt.extractRoleMembersWithAdminFlag(m.Planner.User())
	if err != nil {
		return nil, err
	}

	schema, name, err := mgt.extractSchemaObjectName(modelName)
	if err != nil {
		return nil, err
	}

	stmt := `
SELECT sm.schema, sm.name, smv.description, smv.creator, smv.created, sm.problem_type, sm.features,
	sm.target, sm.framework, sm.runtime,
	CASE WHEN sm.active_version_id = smv.id  THEN 'active' ELSE 'inactive' END AS status,
	smv.version, smj.job_input AS train_data, smv.metrics,sm.status as modelstatus
FROM kwdbml.ml_models sm 
INNER JOIN kwdbml.ml_model_versions smv ON sm.id = smv.model_id 
LEFT JOIN kwdbml.ml_jobs smj ON smj.id = smv.training_job_id`

	// internal model is available for everyone
	if !isAdmin && !mgt.isVisibleInternalModel(schema, name) {
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

	stmt = fmt.Sprintf(` %s WHERE sm.schema = '%s' AND sm.name = '%s'`, stmt, schema, name)

	if modelVersion != 0 {
		stmt = fmt.Sprintf(` %s AND smv.version = %d`, stmt, modelVersion)
	}

	stmt = fmt.Sprintf(` %s ORDER BY smv.version`, stmt)

	results, err := m.Planner.InternalExecutor().Query(ctx, "fetch-ml-model-versions", m.Planner.Txn(), stmt)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		model, err := mgt.hasModel(schema, name)
		if err != nil {
			return nil, err
		}

		// for a non-root user, if it has at least one version for the specified model (without privilege checking),
		// that means the current user have no privilege on it.
		if model != nil && !isAdmin {
			return nil, pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"user '%s' does not have '%s' privilege on model '%s'",
				m.Planner.User(),
				mlUtils.constructPrivilegeString(mlModel, mlSelectPrivilegeMask),
				modelName,
			)
		}
	}

	return results, nil
}

// RemoveModel removes a machine learning model from the system
func (m *MLManager) RemoveModel(
	ctx context.Context, modelName string, modelVersion int32,
) (string, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return "", err
	}

	schema, name, err := mgt.extractSchemaObjectName(modelName)
	if err != nil {
		return "", err
	}

	model, err := mgt.fetchActiveModelVersion(schema, name)
	if err != nil {
		return "", err
	}

	modelFullName := model.getModelName()

	if modelVersion == 0 {
		if isAuthed, err := mgt.checkPrivileges(mlModel, m.Planner.User(), schema, name, mlDeletePrivilegeMask); err != nil {
			return "", err
		} else if !isAuthed {
			return "", pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"user '%s' does not have '%s' privilege on model '%s'",
				m.Planner.User(),
				mlUtils.constructPrivilegeString(mlModel, mlDeletePrivilegeMask),
				modelName,
			)
		}

		if active, err := mgt.isActiveModel(model.ModelID); err != nil {
			return "", err
		} else if active {
			return "", pgerror.Newf(pgcode.Internal,
				"cannot delete the model '%s' since it's used by a long run ml job",
				modelFullName,
			)
		}

		if err = mgt.deleteModel(model.ModelID); err != nil {
			return "", err
		}

		if err = mgt.deleteModelPrivilege(mlModel, schema, name); err != nil {
			return "", err
		}

		if err = mgt.removeModelVersionNumberGenerator(model.ModelID); err != nil {
			return "", err
		}

		if err = mgt.deleteIsvc(model); err != nil {
			return "", err
		}
	} else {
		if isAuthed, err := mgt.checkPrivileges(mlModel, m.Planner.User(), schema, name, mlDeletePrivilegeMask); err != nil {
			return "", err
		} else if !isAuthed {
			return "", pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"user '%s' does not have '%s' privilege on model '%s'",
				m.Planner.User(),
				mlUtils.constructPrivilegeString(mlModel, mlDeletePrivilegeMask),
				modelName,
			)
		}

		model, err = mgt.fetchModelVersion(schema, name, modelVersion)
		if err != nil {
			return "", err
		}

		if active, err := mgt.isActiveModelVersion(model); err != nil {
			return "", err
		} else if active {
			return "", errors.Errorf("failed to delete the active model version '%s,%d'", modelFullName, modelVersion)
		}

		if err = mgt.deleteOneModelVersion(model.ModelID, model.Version); err != nil {
			return "", err
		}
	}

	return modelFullName, nil
}

// GrantModel grants privileges on a machine learning model to a user
func (m *MLManager) GrantModel(
	ctx context.Context, modelName, user string, privilegeStr string,
) (string, error) {
	user = strings.ToLower(user)

	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return "", err
	}
	return mgt.grantMLPrivileges(mlModel, modelName, user, privilegeStr)
}

// RevokeModel revokes privileges on a machine learning model from a user
func (m *MLManager) RevokeModel(
	ctx context.Context, modelName string, user string, privilegeStr string,
) (string, error) {
	user = strings.ToLower(user)

	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return "", err
	}

	return mgt.revokeMLPrivileges(mlModel, modelName, user, privilegeStr)
}

// CheckModelPrivileges checks if a user has the required privileges on a model
func (m *MLManager) CheckModelPrivileges(
	ctx context.Context, modelName string, user string, privilegeStr string,
) (bool, error) {
	user = strings.ToLower(user)

	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return false, err
	}

	return mgt.checkMLPrivileges(mlModel, modelName, user, privilegeStr)
}

// ShowModelPrivileges displays the privileges granted on a machine learning model
func (m *MLManager) ShowModelPrivileges(
	ctx context.Context, modelName string, user string,
) ([]tree.Datums, error) {
	user = strings.ToLower(user)

	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return nil, err
	}

	return mgt.showMLPrivileges(mlModel, modelName, user)
}

// GetAllMLGrantUsers retrieves all users with grants on ML objects
func (m *MLManager) GetAllMLGrantUsers(ctx context.Context) ([]tree.Datums, error) {
	const stmt = `
SELECT schema, name, grantee as user, CASE WHEN type=1 THEN 'model' ELSE 'training pipeline' END
FROM kwdbml.ml_privileges
	`

	rows, err := m.Planner.InternalExecutor().Query(ctx, "query users with ml privileges", m.Planner.Txn(), stmt)
	if err != nil {
		if strings.Contains(err.Error(), "relation \"kwdbml.ml_privileges\" does not exist") {
			return []tree.Datums{}, nil
		}
		return nil, err
	}

	return rows, nil
}
