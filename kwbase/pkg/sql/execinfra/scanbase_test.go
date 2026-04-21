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

package execinfra

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

func TestScanShouldLimitBatches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name       string
		maxResults uint64
		limitHint  int64
		parallelOn bool
		expected   bool
	}{
		{
			name:       "max results below threshold, no limit hint, parallel on",
			maxResults: ParallelScanResultThreshold - 1,
			limitHint:  0,
			parallelOn: true,
			expected:   false,
		},
		{
			name:       "max results at threshold, no limit hint, parallel on",
			maxResults: ParallelScanResultThreshold,
			limitHint:  0,
			parallelOn: true,
			expected:   true,
		},
		{
			name:       "max results above threshold, no limit hint, parallel on",
			maxResults: ParallelScanResultThreshold + 1,
			limitHint:  0,
			parallelOn: true,
			expected:   true,
		},
		{
			name:       "max results below threshold, with limit hint, parallel on",
			maxResults: ParallelScanResultThreshold - 1,
			limitHint:  100,
			parallelOn: true,
			expected:   true,
		},
		{
			name:       "max results above threshold, with limit hint, parallel on",
			maxResults: ParallelScanResultThreshold + 1,
			limitHint:  100,
			parallelOn: true,
			expected:   true,
		},
		{
			name:       "max results below threshold, no limit hint, parallel off",
			maxResults: ParallelScanResultThreshold - 1,
			limitHint:  0,
			parallelOn: false,
			expected:   true,
		},
		{
			name:       "max results above threshold, no limit hint, parallel off",
			maxResults: ParallelScanResultThreshold + 1,
			limitHint:  0,
			parallelOn: false,
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())

			// Create a flow context with the appropriate settings
			st := cluster.MakeTestingClusterSettings()
			if st == nil {
				t.Fatal("cluster.MakeTestingClusterSettings() returned nil")
			}

			flowCtx := &FlowCtx{
				Cfg: &ServerConfig{
					Stopper:  stopper,
					Settings: st,
				},
			}

			if flowCtx.Cfg == nil {
				t.Fatal("flowCtx.Cfg is nil")
			}
			if flowCtx.Cfg.Settings == nil {
				t.Fatal("flowCtx.Cfg.Settings is nil")
			}

			// Set the parallel scans setting
			sqlbase.ParallelScans.Override(&st.SV, tt.parallelOn)

			result := ScanShouldLimitBatches(tt.maxResults, tt.limitHint, flowCtx)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}
