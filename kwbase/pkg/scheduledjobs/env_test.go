// Copyright 2020 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package scheduledjobs

import (
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestProdJobSchedulerEnvImpl(t *testing.T) {
	t.Run("ScheduledJobsTableName", func(t *testing.T) {
		expected := "system.scheduled_jobs"
		actual := ProdJobSchedulerEnv.ScheduledJobsTableName()
		require.Equal(t, expected, actual)
	})

	t.Run("SystemJobsTableName", func(t *testing.T) {
		expected := "system.jobs"
		actual := ProdJobSchedulerEnv.SystemJobsTableName()
		require.Equal(t, expected, actual)
	})

	t.Run("Now", func(t *testing.T) {
		// Test if Now() method returns a reasonable time
		now := ProdJobSchedulerEnv.Now()
		// Check if the returned time is near the current time (within 1 second)
		currentTime := timeutil.Now()
		diff := currentTime.Sub(now)
		require.True(t, diff >= 0 && diff < time.Second, "Now() should return current time")
	})

	t.Run("NowExpr", func(t *testing.T) {
		expected := "current_timestamp()"
		actual := ProdJobSchedulerEnv.NowExpr()
		require.Equal(t, expected, actual)
	})
}

func TestJobExecutionConfig(t *testing.T) {
	// Test basic functionality of JobExecutionConfig struct
	// Since JobExecutionConfig is mainly a data structure, we test if its fields can be set and accessed correctly
	t.Run("JobExecutionConfigFields", func(t *testing.T) {
		// Create a JobExecutionConfig instance
		config := &JobExecutionConfig{
			StartMode: 1,
		}

		// Verify field value
		require.Equal(t, 1, config.StartMode)
		// Verify other fields are nil by default
		require.Nil(t, config.Settings)
		require.Nil(t, config.InternalExecutor)
		require.Nil(t, config.DB)
		require.Nil(t, config.TestingKnobs)
		require.Nil(t, config.PlanHookMaker)
	})
}
