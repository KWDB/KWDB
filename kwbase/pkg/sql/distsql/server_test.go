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

package distsql

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestFlowVerIsCompatible(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name               string
		flowVer            execinfrapb.DistSQLVersion
		minAcceptedVersion execinfrapb.DistSQLVersion
		serverVersion      execinfrapb.DistSQLVersion
		expected           bool
	}{
		{
			name:               "version within range",
			flowVer:            2,
			minAcceptedVersion: 1,
			serverVersion:      3,
			expected:           true,
		},
		{
			name:               "version below minimum",
			flowVer:            0,
			minAcceptedVersion: 1,
			serverVersion:      3,
			expected:           false,
		},
		{
			name:               "version above maximum",
			flowVer:            4,
			minAcceptedVersion: 1,
			serverVersion:      3,
			expected:           false,
		},
		{
			name:               "version equal to minimum",
			flowVer:            1,
			minAcceptedVersion: 1,
			serverVersion:      3,
			expected:           true,
		},
		{
			name:               "version equal to maximum",
			flowVer:            3,
			minAcceptedVersion: 1,
			serverVersion:      3,
			expected:           true,
		},
		{
			name:               "version at boundary - flowVer between min and max",
			flowVer:            2,
			minAcceptedVersion: 2,
			serverVersion:      2,
			expected:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FlowVerIsCompatible(tt.flowVer, tt.minAcceptedVersion, tt.serverVersion)
			require.Equal(t, tt.expected, result)
		})
	}
}
