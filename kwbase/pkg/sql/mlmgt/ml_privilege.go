// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

// kind define the ml privileges.
type mlKind int16

func (k mlKind) mask() PrivilegeMask {
	return 1 << k
}

// PrivilegeMask define the mask code of ml privileges.
type PrivilegeMask int16

// ModelPrivilegeMask define the model privilege mask code of ml privileges.
type ModelPrivilegeMask PrivilegeMask

// TrainingPipelinePrivilegeMask define the training pipeline privilege mask code of ml privileges.
type TrainingPipelinePrivilegeMask PrivilegeMask

const (
	// mlCreate create privilege, mask value 16.
	mlCreate mlKind = iota + 4

	// mlDelete delete privilege, mask value 32.
	mlDelete

	// mlUpdate update privilege, mask value 64.
	mlUpdate

	// mlTrain train privilege, mask value 128.
	mlTrain

	// mlPredict predict privilege, mask value 256.
	mlPredict

	// mlEvaluate evaluate privilege, mask value 512.
	mlEvaluate

	// mlSelect select privilege, mask value 1024.
	mlSelect
)

const (
	mlPrivilegeAllStr    = "ALL"
	mlPrivilegeSeparator = "|"
)

var (
	// mlModelPrivilegeKinds KaiwuDB model privileges value set
	mlModelPrivilegeKinds = [...]mlKind{
		mlCreate,
		mlSelect,
		mlDelete,
		mlUpdate,
		mlPredict,
		mlEvaluate,
	}

	mlTrainingPipelinePrivilegeKinds = [...]mlKind{
		mlSelect,
		mlDelete,
		mlTrain,
	}

	mlCreatePrivilegeMask              = mlCreate.mask()
	mlSelectPrivilegeMask              = mlSelect.mask()
	mlDeletePrivilegeMask              = mlDelete.mask()
	mlUpdatePrivilegeMask              = mlUpdate.mask()
	mlTrainPrivilegeMask               = mlTrain.mask()
	mlPredictPrivilegeMask             = mlPredict.mask()
	mlEvaluatePrivilegeMask            = mlEvaluate.mask()
	mlModelAllPrivilegeMask            = mlUtils.kindToMask(mlModelPrivilegeKinds[:]...)
	mlTrainingPipelineAllPrivilegeMask = mlUtils.kindToMask(mlTrainingPipelinePrivilegeKinds[:]...)

	mlModelPrivilegeToMask = map[string]PrivilegeMask{
		"CREATE":   mlCreatePrivilegeMask,
		"SELECT":   mlSelectPrivilegeMask,
		"DELETE":   mlDeletePrivilegeMask,
		"UPDATE":   mlUpdatePrivilegeMask,
		"PREDICT":  mlPredictPrivilegeMask,
		"EVALUATE": mlEvaluatePrivilegeMask,
		"ALL":      mlModelAllPrivilegeMask,
	}

	mlTrainingPipelinePrivilegeToMask = map[string]PrivilegeMask{
		"SELECT": mlSelectPrivilegeMask,
		"DELETE": mlDeletePrivilegeMask,
		"TRAIN":  mlTrainPrivilegeMask,
		"ALL":    mlTrainingPipelineAllPrivilegeMask,
	}
)

func (privileges PrivilegeMask) hasPrivileges(toBeVerify PrivilegeMask) bool {
	return privileges&toBeVerify == toBeVerify
}

func (privileges ModelPrivilegeMask) String() string {
	var privilegesList = make([]string, 0, len(mlModelPrivilegeKinds))

	modelPrivileges := PrivilegeMask(privileges)

	if modelPrivileges == mlModelAllPrivilegeMask {
		return mlPrivilegeAllStr
	}

	for key, mask := range mlModelPrivilegeToMask {
		if key == mlPrivilegeAllStr {
			continue
		}

		if mask&modelPrivileges != 0 {
			privilegesList = append(privilegesList, key)
		}
	}

	sort.Strings(privilegesList)

	return strings.Join(privilegesList, mlPrivilegeSeparator)
}

func (privileges TrainingPipelinePrivilegeMask) String() string {
	var privilegesList = make([]string, 0, len(mlTrainingPipelinePrivilegeKinds))

	trainingPipelinePrivileges := PrivilegeMask(privileges)

	if trainingPipelinePrivileges == mlTrainingPipelineAllPrivilegeMask {
		return mlPrivilegeAllStr
	}

	for key, mask := range mlTrainingPipelinePrivilegeToMask {
		if key == mlPrivilegeAllStr {
			continue
		}

		if mask&trainingPipelinePrivileges != 0 {
			privilegesList = append(privilegesList, key)
		}
	}

	sort.Strings(privilegesList)

	return strings.Join(privilegesList, mlPrivilegeSeparator)
}

// validateMLPrivileges validate the input privilege string (without pre/post blank space).
func (*mlUtil) validateMLPrivileges(
	resType mlResourceType, privilegeStr string,
) (PrivilegeMask, error) {
	if len(privilegeStr) == 0 {
		return 0, errors.Errorf("privilege is empty")
	}

	privilegeList := strings.Split(privilegeStr, mlPrivilegeSeparator)

	var privileges PrivilegeMask

	for _, value := range privilegeList {
		valueWithoutSpace := strings.TrimSpace(value)
		privilege := strings.ToUpper(valueWithoutSpace)

		if len(privilege) == 0 {
			continue
		}

		if privilege == mlPrivilegeAllStr {
			allMask, _ := mlUtils.getPrivilegeMask(resType, mlPrivilegeAllStr)

			return allMask, nil
		}

		if mask, ok := mlUtils.getPrivilegeMask(resType, privilege); ok {
			privileges |= mask
		} else {
			return 0, errors.Errorf("not a valid KaiwuDB ML privilege for %s: %q", resType.String(), valueWithoutSpace)
		}
	}

	if privileges == 0 {
		return 0, errors.New("empty privilege mask is not allowed")
	}

	return privileges, nil
}

func (*mlUtil) kindToMask(kinds ...mlKind) PrivilegeMask {
	var ret PrivilegeMask

	for _, item := range kinds {
		ret |= item.mask()
	}

	return ret
}
