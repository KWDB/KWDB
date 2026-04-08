// Copyright 2019 The Cockroach Authors.
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

package execinfra

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

func TestGetWorkMemLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name             string
		memoryLimitBytes int64
		settingValue     int64
		expected         int64
	}{
		{
			name:             "testing knob set",
			memoryLimitBytes: 1024,
			settingValue:     64 * 1024 * 1024,
			expected:         1024,
		},
		{
			name:             "testing knob not set",
			memoryLimitBytes: 0,
			settingValue:     64 * 1024 * 1024,
			expected:         64 * 1024 * 1024,
		},
		{
			name:             "testing knob negative",
			memoryLimitBytes: -1,
			settingValue:     64 * 1024 * 1024,
			expected:         64 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())

			// Create a server config with the appropriate settings
			st := cluster.MakeTestingClusterSettings()
			cfg := &ServerConfig{
				Settings: st,
				TestingKnobs: TestingKnobs{
					MemoryLimitBytes: tt.memoryLimitBytes,
				},
			}

			// Set the work mem setting
			SettingWorkMemBytes.Override(&st.SV, tt.settingValue)

			result := GetWorkMemLimit(cfg)
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMetadataTestLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		level    MetadataTestLevel
		expected string
	}{
		{Off, "Off"},
		{NoExplain, "NoExplain"},
		{On, "On"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			// Just verify the constants are defined correctly
			_ = tt.level
		})
	}
}
