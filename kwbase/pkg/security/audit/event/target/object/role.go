// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package object

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
)

// RoleOperationMetric is the metric of operation on role
type RoleOperationMetric struct {
	*target.Object
}

// NewRoleMetric create a role metric
func NewRoleMetric(ctx context.Context) *RoleOperationMetric {
	return &RoleOperationMetric{
		Object: &target.Object{
			Ctx:        ctx,
			Name:       "opera_role_count",
			Level:      target.StmtLevel,
			ObjMetrics: target.CreateMetricMap(target.ObjectRole),
		},
	}
}
