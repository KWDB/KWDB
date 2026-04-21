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
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
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

func TestDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	cfg := s.DistSQLServer().(*ServerImpl).ServerConfig

	distSQLSrv := NewServer(ctx, cfg)

	t.Run("normal drain completes without error", func(t *testing.T) {
		distSQLSrv.Drain(ctx, 5*time.Second, func(numOutstanding int, desc string) {
		})
	})

	t.Run("fast drain completes without error", func(t *testing.T) {
		distSQLSrv.ServerConfig.TestingKnobs.DrainFast = true
		distSQLSrv.Drain(ctx, 5*time.Second, func(numOutstanding int, desc string) {
		})
	})
}

func TestSetupFlow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	cfg := s.DistSQLServer().(*ServerImpl).ServerConfig

	distSQLSrv := NewServer(ctx, cfg)

	t.Run("version mismatch", func(t *testing.T) {
		req := &execinfrapb.SetupFlowRequest{
			Version: 999,
			Flow: execinfrapb.FlowSpec{
				FlowID:  execinfrapb.FlowID{UUID: uuid.MakeV4()},
				Gateway: roachpb.NodeID(1),
			},
		}

		resp, err := distSQLSrv.SetupFlow(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Error)
		require.Contains(t, resp.Error.String(), "version mismatch")
	})

	t.Run("version compatible", func(t *testing.T) {
		req := &execinfrapb.SetupFlowRequest{
			Version: execinfra.Version,
			Flow: execinfrapb.FlowSpec{
				FlowID:  execinfrapb.FlowID{UUID: uuid.MakeV4()},
				Gateway: roachpb.NodeID(1),
			},
		}

		resp, err := distSQLSrv.SetupFlow(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Error)
	})
}
