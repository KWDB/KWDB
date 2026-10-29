// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	"fmt"
	"sort"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func (mgt *mlManagement) grantMLPrivileges(
	resType mlResourceType, resName, user string, privilegeStr string,
) (string, error) {
	schema, name, err := mgt.extractSchemaObjectName(resName)
	if err != nil {
		return "", err
	}

	// internal model privilege
	if resType == mlModel && mgt.isVisibleInternalModel(schema, name) {
		return "", errors.Newf("privileges on internal model can't be granted to other user")
	}

	existing, err := mgt.hasMLResource(schema, name, resType)
	if err != nil {
		return "", err
	} else if !existing {
		return "", errors.Newf("%s '%s' does not exist", resType.String(), resName)
	}

	privileges, err := mlUtils.validateMLPrivileges(resType, privilegeStr)
	if err != nil {
		return "", err
	}

	// only ALL privilege can grant privileges to a specified user.
	allMask, _ := mlUtils.getPrivilegeMask(resType, mlPrivilegeAllStr)

	if isAuthed, err := mgt.checkPrivileges(
		resType, mgt.planner.User(), schema, name, allMask,
	); err != nil {
		return "", err
	} else if !isAuthed {
		return "", pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"user '%s' does not have '%s' privilege on %s '%s'",
			mgt.planner.User(),
			mlPrivilegeAllStr,
			resType.String(),
			resName,
		)
	}

	return mgt.grantPrivileges(resType, schema, name, user, privileges, true)
}

func (mgt *mlManagement) revokeMLPrivileges(
	resType mlResourceType, resName string, user string, privilegeStr string,
) (string, error) {
	if len(privilegeStr) == 0 {
		return "", errors.New("privilege is empty")
	}

	schema, name, err := mgt.extractSchemaObjectName(resName)
	if err != nil {
		return "", err
	}

	existing, err := mgt.hasMLResource(schema, name, resType)
	if err != nil {
		return "", err
	} else if !existing {
		return "", errors.Newf("%s '%s' does not exist", resType.String(), resName)
	}

	privileges, err := mlUtils.validateMLPrivileges(resType, privilegeStr)
	if err != nil {
		return "", err
	}

	// only ALL privilege can revoke privileges from a specified user.
	allMask, _ := mlUtils.getPrivilegeMask(resType, mlPrivilegeAllStr)

	if isAuthed, err := mgt.checkPrivileges(
		resType, mgt.planner.User(), schema, name, allMask,
	); err != nil {
		return "", err
	} else if !isAuthed {
		return "", pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"user '%s' does not have '%s' privilege on %s '%s'",
			mgt.planner.User(),
			mlPrivilegeAllStr,
			resType.String(),
			resName,
		)
	}

	return mgt.revokePrivileges(resType, schema, name, user, privileges)
}

func (mgt *mlManagement) checkMLPrivileges(
	resType mlResourceType, resName string, user string, privilegeStr string,
) (bool, error) {
	schema, name, err := mgt.extractSchemaObjectName(resName)
	if err != nil {
		return false, err
	}

	isUserExist, err := mgt.isUserExist(user)
	if err != nil {
		return false, err
	} else if !isUserExist {
		return false, errors.Newf("user '%s' does not exist", user)
	}

	existing, err := mgt.hasMLResource(schema, name, resType)
	if err != nil {
		return false, err
	} else if !existing {
		return false, errors.Newf("%s '%s' does not exist", resType.String(), resName)
	}

	privileges, err := mlUtils.validateMLPrivileges(resType, privilegeStr)
	if err != nil {
		return false, err
	}

	roles, isAdmin, err := mgt.extractRoleMembersWithAdminFlag(mgt.planner.User())
	if err != nil {
		return false, err
	}

	if !isAdmin {
		// check if the current user has any privilege for the specified ml model/pipeline.
		// if no, return has no any privilege error.
		privilegesMask, err := mgt.fetchPrivilegeMask(resType, roles, schema, name)
		if err != nil {
			return false, err
		}

		if privilegesMask == 0 {
			return false, pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"user '%s' does not have any privilege on %s '%s'",
				mgt.planner.User(),
				resType.String(),
				resName,
			)
		}
	}

	// check if the input user has the specified privileges.
	if isAuthed, err := mgt.checkPrivileges(
		resType, user, schema, name, privileges,
	); err != nil {
		return false, err
	} else if !isAuthed {
		return false, nil
	}

	return true, nil
}

func (mgt *mlManagement) showMLPrivileges(
	resType mlResourceType, resName string, user string,
) ([]tree.Datums, error) {
	isAllUser := user == "*"

	schema, name, err := mgt.extractSchemaObjectName(resName)
	if err != nil {
		return nil, err
	}

	if !isAllUser {
		isUserExist, err := mgt.isUserExist(user)
		if err != nil {
			return nil, err
		} else if !isUserExist {
			return nil, errors.Newf("user '%s' does not exist", user)
		}
	}

	existing, err := mgt.hasMLResource(schema, name, resType)
	if err != nil {
		return nil, err
	} else if !existing {
		return nil, errors.Newf("%s '%s' does not exist", resType.String(), resName)
	}

	roles, isAdmin, err := mgt.extractRoleMembersWithAdminFlag(mgt.planner.User())
	if err != nil {
		return nil, err
	}

	if !isAdmin {
		// check if the current user has any privilege for the specified ml model.
		// if no, return has no any privilege error.
		privilegesMask, err := mgt.fetchPrivilegeMask(resType, roles, schema, name)
		if err != nil {
			return nil, err
		}

		if privilegesMask == 0 {
			return nil, pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"user '%s' does not have any privilege on %s '%s'",
				mgt.planner.User(),
				resType.String(),
				resName,
			)
		}
	}

	if isAllUser {
		privileges, err := mgt.showAllPrivileges(resType, schema, name)
		if err != nil {
			return nil, err
		}

		return privileges, nil
	}

	roles, isAdmin, err = mgt.extractRoleMembersWithAdminFlag(user)
	if err != nil {
		return nil, err
	}

	if isAdmin {
		memberOf := tree.NewDArray(types.String)

		if len(roles) > 1 {
			for _, member := range roles[1:] {
				if err := memberOf.Append(tree.NewDString(member)); err != nil {
					return nil, err
				}
			}
		}

		rootPrivileges := tree.Datums{
			tree.NewDString(user),
			tree.NewDString(schema),
			tree.NewDString(name),
			tree.NewDString(sqlbase.AdminRole),
			tree.NewDString(mlPrivilegeAllStr),
			memberOf,
		}

		return []tree.Datums{rootPrivileges}, nil
	}

	privileges, err := mgt.showPrivileges(resType, roles, schema, name)
	if err != nil {
		return nil, err
	}

	return privileges, nil
}

// privilegeOP operate function, for example, mlGrant, mlRevoke.
type privilegeOP func(*mlManagement, mlResourceType, string, string, string, PrivilegeMask) (bool, error)

// checkPrivileges the basic component of the permission module, which undertakes all built-in functions
// privilege queries, when the interface concurrency is large or have a high real-time requirements,
// cache needs to be added, such as predict
func (mgt *mlManagement) checkPrivileges(
	resType mlResourceType, user, schema, name string, toBeVerify PrivilegeMask,
) (bool, error) {
	roles, isAdmin, err := mgt.extractRoleMembersWithAdminFlag(user)
	if err != nil {
		return false, err
	}

	if isAdmin {
		return true, nil
	}

	// internal model, everybody can update it or use for batchpredict
	if resType == mlModel && mgt.isVisibleInternalModel(schema, name) &&
		(toBeVerify == mlUpdatePrivilegeMask || toBeVerify == mlPredictPrivilegeMask || toBeVerify == mlEvaluatePrivilegeMask) {
		return true, nil
	}

	privileges, err := mgt.fetchPrivilegeMask(resType, roles, schema, name)
	if err != nil {
		return false, err
	}

	return privileges.hasPrivileges(toBeVerify), nil
}

func (mgt *mlManagement) checkPredictPrivilege(user, schema, name string) (bool, error) {
	hasCache, key := mgt.planner.MLPrivilegesCache().Get(user, schema, name)
	if hasCache {
		mgt.planner.MLPrivilegesCache().Claim(key)

		return true, nil
	}

	memberOf, isAdmin, err := mgt.extractRoleMembersWithAdminFlag(user)
	if err != nil {
		return false, err
	}

	// admin or special internal model
	if isAdmin || mgt.isVisibleInternalModel(schema, name) {
		return true, nil
	}

	for _, role := range memberOf {
		hasCache, key = mgt.planner.MLPrivilegesCache().Get(role, schema, name)
		if hasCache {
			mgt.planner.MLPrivilegesCache().Claim(key)

			return true, nil
		}
	}

	user, err = mgt.fetchUserHasPredictPrivilege(memberOf, schema, name)
	if err != nil {
		return false, err
	}

	if user != "" {
		mgt.planner.MLPrivilegesCache().Put(user, schema, name)

		return true, nil
	}

	return false, nil
}

func (mgt *mlManagement) showPrivileges(
	resType mlResourceType, roles []string, schema, name string,
) ([]tree.Datums, error) {
	user := roles[0]
	members := roles[1:]
	sort.Strings(members)

	stmt := fmt.Sprintf(`SELECT grantee, schema, name, grantor, privilege_type, 
            ARRAY (SELECT role FROM system.role_members AS rm WHERE rm.member = grantee ORDER BY 1) AS member_of
            FROM kwdbml.ml_privileges
            WHERE grantee IN ('%s') AND schema=$1 AND name=$2 AND type=$3 ORDER BY grantee=$4 DESC,grantee ASC`,
		strings.Join(roles, "','"),
	)

	rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"fetch-privileges",
		mgt.planner.Txn(),
		stmt,
		schema,
		name,
		resType,
		user,
	)
	if err != nil {
		return nil, err
	}

	if rows == nil {
		return nil, nil
	}

	memberOf := tree.NewDArray(types.String)

	for _, member := range members {
		if err := memberOf.Append(tree.NewDString(member)); err != nil {
			return nil, err
		}
	}

	userIdx := -1
	var pm PrivilegeMask

	for idx, row := range rows {
		grantee := string(tree.MustBeDString(row[0]))
		mask := PrivilegeMask(int16(tree.MustBeDInt(row[4])))
		row[4] = tree.NewDString(mlUtils.constructPrivilegeString(resType, mask))
		if grantee == user {
			userIdx = idx
		}
	}

	if userIdx == -1 {
		rows = append([]tree.Datums{
			{
				tree.NewDString(user),
				tree.NewDString(schema),
				tree.NewDString(name),
				tree.NewDString(sqlbase.AdminRole),
				tree.NewDString(mlUtils.constructPrivilegeString(resType, pm)),
				memberOf,
			},
		}, rows...)
	}

	return rows, nil
}

func (mgt *mlManagement) showAllPrivileges(
	resType mlResourceType, schema, name string,
) ([]tree.Datums, error) {
	stmt := `SELECT grantee, schema, name, grantor, privilege_type, 
ARRAY (SELECT role FROM system.role_members AS rm WHERE rm.member = grantee ORDER BY 1) AS member_of
FROM kwdbml.ml_privileges
WHERE schema=$1 AND name=$2 AND type=$3 ORDER BY grantee`

	rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"fetch-privileges",
		mgt.planner.Txn(),
		stmt,
		schema,
		name,
		resType,
	)
	if err != nil {
		return nil, err
	}

	if rows == nil {
		return nil, nil
	}

	for _, row := range rows {
		mask := PrivilegeMask(int16(tree.MustBeDInt(row[4])))
		row[4] = tree.NewDString(mlUtils.constructPrivilegeString(resType, mask))
	}

	return rows, nil
}

func (mgt *mlManagement) missingPrivileges(
	resType mlResourceType, user, schema, name string, toBeVerify PrivilegeMask,
) (string, error) {
	roles, isAdmin, err := mgt.extractRoleMembersWithAdminFlag(user)
	if err != nil {
		return "", err
	}

	if isAdmin {
		return "", nil
	}

	privileges, err := mgt.fetchPrivilegeMask(resType, roles, schema, name)
	if err != nil {
		return "", err
	}

	missingPrivilegeMask := privileges&toBeVerify ^ toBeVerify

	return mlUtils.constructPrivilegeString(resType, missingPrivilegeMask), nil
}

func (mgt *mlManagement) grantPrivileges(
	resType mlResourceType, schema, name, user string, privileges PrivilegeMask, ignoreAdmin bool,
) (string, error) {
	isUserExist, err := mgt.isUserExist(user)
	if err != nil {
		return "", err
	} else if !isUserExist {
		return "", errors.Newf("user '%s' does not exist", user)
	}

	_, isAdmin, err := mgt.extractRoleMembersWithAdminFlag(user)
	if err != nil {
		return "", err
	} else if isAdmin {
		if ignoreAdmin {
			return user, nil
		}

		return "", errors.Newf(
			`failed to grant '%s' privilege to '%s', the target user '%s' is the member of 'admin' role`,
			mlUtils.constructPrivilegeString(resType, privileges),
			user,
			user,
		)
	}

	return mgt.wrapPrivilegeModify(resType, user, schema, name, privileges, mlGrant)
}

func (mgt *mlManagement) revokePrivileges(
	resType mlResourceType, schema, name, user string, privileges PrivilegeMask,
) (string, error) {
	isUserExist, err := mgt.isUserExist(user)
	if err != nil {
		return "", err
	} else if !isUserExist {
		return "", errors.Newf("user '%s' does not exist", user)
	}

	_, isAdmin, err := mgt.extractRoleMembersWithAdminFlag(user)
	if err != nil {
		return "", err
	} else if isAdmin {
		return "", errors.Newf(
			`failed to revoke '%s' privilege from '%s', should first revoke '%s' from 'admin' role`,
			mlUtils.constructPrivilegeString(resType, privileges),
			user,
			user,
		)
	}

	return mgt.wrapPrivilegeModify(resType, user, schema, name, privileges, mlRevoke)
}

func (mgt *mlManagement) wrapPrivilegeModify(
	resType mlResourceType, user, schema, name string, privileges PrivilegeMask, op privilegeOP,
) (string, error) {
	user = strings.ToLower(user)

	if _, err := op(mgt, resType, user, schema, name, privileges); err != nil {
		return "", err
	}

	return user, nil
}

func (mgt *mlManagement) fetchPrivilegeMask(
	resType mlResourceType, roles []string, schema, name string,
) (PrivilegeMask, error) {
	stmt := fmt.Sprintf(`SELECT privilege_type FROM kwdbml.ml_privileges 
                      WHERE grantee IN ('%s') AND schema=$1 AND name=$2 AND type=$3`,
		strings.Join(roles, "','"),
	)

	rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"fetch-privilege-mask",
		mgt.planner.Txn(),
		stmt,
		schema,
		name,
		resType,
	)
	if err != nil {
		return 0, err
	}

	var pm PrivilegeMask

	for _, row := range rows {
		pm |= PrivilegeMask(tree.MustBeDInt(row[0]))
	}

	return pm, nil
}

func (mgt *mlManagement) fetchUserHasPredictPrivilege(
	roles []string, schema, name string,
) (string, error) {
	stmt := fmt.Sprintf(`SELECT grantee,privilege_type FROM kwdbml.ml_privileges 
                      WHERE grantee IN ('%s') AND schema=$1 AND name=$2 AND type=$3`,
		strings.Join(roles, "','"),
	)

	rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"fetch-privilege-mask",
		mgt.planner.Txn(),
		stmt,
		schema,
		name,
		mlModel,
	)
	if err != nil {
		return "", err
	}

	for _, row := range rows {
		pm := PrivilegeMask(tree.MustBeDInt(row[1]))
		if pm.hasPrivileges(mlPredictPrivilegeMask) {
			return string(tree.MustBeDString(row[0])), err
		}
	}

	return "", nil
}

func (mgt *mlManagement) insertPrivilegeMask(
	resType mlResourceType, privilegeMask PrivilegeMask, grantee, schema, name string,
) error {
	if _, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"insert-privilege-record",
		mgt.planner.Txn(),
		`INSERT INTO kwdbml.ml_privileges (grantee,schema, name, type, privilege_type,grantor)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		grantee,
		schema,
		name,
		resType,
		privilegeMask,
		mgt.planner.User(),
	); err != nil {
		return err
	}

	mgt.planner.MLPrivilegesCache().Reset(grantee, schema, name)

	return nil
}

func (mgt *mlManagement) updatePrivilegeMask(
	resType mlResourceType, privilegeMask PrivilegeMask, grantee, schema, name string,
) error {
	if _, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"update-privilege-record",
		mgt.planner.Txn(),
		`UPDATE kwdbml.ml_privileges SET privilege_type=$1 WHERE grantee=$2 AND schema=$3 AND name=$4 AND type=$5`,
		privilegeMask,
		grantee,
		schema,
		name,
		resType,
	); err != nil {
		return err
	}

	mgt.planner.MLPrivilegesCache().Reset(grantee, schema, name)

	return nil
}

func (mgt *mlManagement) deletePrivilege(
	resType mlResourceType, grantee, schema, name string,
) error {
	if _, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"delete-privilege-record",
		mgt.planner.Txn(),
		`DELETE FROM kwdbml.ml_privileges WHERE grantee=$1 AND schema=$2 AND name=$3 AND type=$4`,
		grantee,
		schema,
		name,
		resType,
	); err != nil {
		return err
	}

	mgt.planner.MLPrivilegesCache().Reset(grantee, schema, name)

	return nil
}

func (mgt *mlManagement) deleteModelPrivilege(resType mlResourceType, schema, name string) error {
	if err := mgt.deleteMLPrivilege(resType, schema, name); err != nil {
		return err
	}

	mgt.planner.MLPrivilegesCache().BatchReset(schema, name)

	return nil
}

func (mgt *mlManagement) deleteMLPrivilege(resType mlResourceType, schema, name string) error {
	if _, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"delete-privilege-record",
		mgt.planner.Txn(),
		`DELETE FROM kwdbml.ml_privileges WHERE schema=$1 AND name=$2 AND type=$3`,
		schema,
		name,
		resType,
	); err != nil {
		return err
	}

	return nil
}

func (*mlUtil) constructPrivilegeString(resType mlResourceType, mask PrivilegeMask) string {
	switch resType {
	case mlModel:
		return ModelPrivilegeMask(mask).String()
	case mlTrainingPipeline:
		return TrainingPipelinePrivilegeMask(mask).String()
	default:
		return ""
	}
}

func (*mlUtil) getPrivilegeMask(resType mlResourceType, privilegeStr string) (PrivilegeMask, bool) {
	switch resType {
	case mlModel:
		mask, ok := mlModelPrivilegeToMask[privilegeStr]

		return mask, ok
	case mlTrainingPipeline:
		mask, ok := mlTrainingPipelinePrivilegeToMask[privilegeStr]

		return mask, ok
	default:
		return 0, false
	}
}

func mlGrant(
	mgt *mlManagement,
	resType mlResourceType,
	grantee, schema, name string,
	newPrivileges PrivilegeMask,
) (bool, error) {
	existingPrivileges, err := mgt.fetchPrivilegeMask(resType, []string{grantee}, schema, name)
	if err != nil {
		return false, err
	}

	if existingPrivileges == 0 {
		if err = mgt.insertPrivilegeMask(resType, newPrivileges, grantee, schema, name); err != nil {
			return false, err
		}

		return true, nil
	}

	updatedPrivileges := newPrivileges | existingPrivileges
	if err = mgt.updatePrivilegeMask(resType, updatedPrivileges, grantee, schema, name); err != nil {
		return false, err
	}

	return true, nil
}

func mlRevoke(
	mgt *mlManagement,
	resType mlResourceType,
	grantee, schema, name string,
	toBeRevoked PrivilegeMask,
) (bool, error) {
	existingPrivileges, err := mgt.fetchPrivilegeMask(resType, []string{grantee}, schema, name)
	if err != nil {
		return false, err
	}

	if existingPrivileges == 0 {
		return false, errors.Newf("user '%s' has no existing privileges on %s '%s.%s'", grantee, resType.String(), schema, name)
	}

	if toBeRevoked == mlModelAllPrivilegeMask {
		if err = mgt.deletePrivilege(resType, grantee, schema, name); err != nil {
			return false, err
		}

		return true, nil
	}

	hasPrivileges := existingPrivileges & toBeRevoked

	if hasPrivileges == 0 {
		return false, errors.Newf("user '%s' has no existing privileges on %s '%s.%s'", grantee, resType.String(), schema, name)
	}

	existingPrivileges &^= toBeRevoked

	if existingPrivileges == 0 {
		if err = mgt.deletePrivilege(resType, grantee, schema, name); err != nil {
			return false, err
		}

		return true, nil
	}

	if err = mgt.updatePrivilegeMask(resType, existingPrivileges, grantee, schema, name); err != nil {
		return false, err
	}

	return true, nil
}
