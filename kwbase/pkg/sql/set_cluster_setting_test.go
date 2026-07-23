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

package sql

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TestCheckTsDedupRule tests the checkTsDedupRule function
func TestCheckTsDedupRule(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value merge", "merge", false},
		{"valid value keep.experimental", "keep.experimental", false},
		{"valid value discard", "discard", false},
		{"valid value override", "override", false},
		{"invalid value", "invalid", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTsDedupRule(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkTsDedupRule(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestCheckTsQueryOptMode tests the checkTsQueryOptMode function
func TestCheckTsQueryOptMode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value 0 (binary)", "0", false},
		{"valid value 63 (binary)", "111111", false},
		{"invalid value -1", "-1", true},
		{"invalid value 64 (binary)", "1000000", true},
		{"invalid value abc", "abc", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTsQueryOptMode(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkTsQueryOptMode(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestCheckBool tests the checkBool function
func TestCheckBool(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value true", "true", false},
		{"valid value false", "false", false},
		{"invalid value", "invalid", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkBool(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkBool(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestCheckCapacityStatsPeriod tests the checkCapacityStatsPeriod function
func TestCheckCapacityStatsPeriod(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value 1", "1", false},
		{"valid value 10000", "10000", false},
		{"invalid value 0", "0", true},
		{"invalid value 10001", "10001", true},
		{"invalid value abc", "abc", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkCapacityStatsPeriod(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkCapacityStatsPeriod(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestCheckTsTableCacheCapacity tests the checkTsTableCacheCapacity function
func TestCheckTsTableCacheCapacity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value 1", "1", false},
		{"valid value 2147483647", "2147483647", false},
		{"invalid value 0", "0", true},
		{"invalid value 2147483648", "2147483648", true},
		{"invalid value abc", "abc", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTsTableCacheCapacity(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkTsTableCacheCapacity(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestCheckTsRowsPerBlockMaxLimit tests the checkTsRowsPerBlockMaxLimit function
func TestCheckTsRowsPerBlockMaxLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value 1", "1", false},
		{"valid value 50000", "50000", false},
		{"invalid value 0", "0", true},
		{"invalid value 50001", "50001", true},
		{"invalid value abc", "abc", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTsRowsPerBlockMaxLimit(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkTsRowsPerBlockMaxLimit(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestCheckTsRowsPerBlockMinLimit tests the checkTsRowsPerBlockMinLimit function
func TestCheckTsRowsPerBlockMinLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value 1", "1", false},
		{"valid value 50000", "50000", false},
		{"invalid value 0", "0", true},
		{"invalid value 50001", "50001", true},
		{"invalid value abc", "abc", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTsRowsPerBlockMinLimit(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkTsRowsPerBlockMinLimit(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestCheckTsCompactLastSegmentMaxLimit tests the checkTsCompactLastSegmentMaxLimit function
func TestCheckTsCompactLastSegmentMaxLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value 1", "1", false},
		{"valid value 1000", "1000", false},
		{"invalid value 0", "0", true},
		{"invalid value 1001", "1001", true},
		{"invalid value abc", "abc", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTsCompactLastSegmentMaxLimit(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkTsCompactLastSegmentMaxLimit(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestCheckTsCompressStage tests the checkTsCompressStage function
func TestCheckTsCompressStage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value 0", "0", false},
		{"valid value 1", "1", false},
		{"valid value 2", "2", false},
		{"valid value 3", "3", false},
		{"invalid value 4", "4", true},
		{"invalid value -1", "-1", true},
		{"invalid value abc", "abc", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTsCompressStage(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkTsCompressStage(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestCheckTsCompressLastSegment tests the checkTsCompressLastSegment function
func TestCheckTsCompressLastSegment(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value true", "true", false},
		{"valid value false", "false", false},
		{"invalid value", "invalid", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTsCompressLastSegment(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkTsCompressLastSegment(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestCheckTsReservedLastSegmentMaxLimit tests the checkTsReservedLastSegmentMaxLimit function
func TestCheckTsReservedLastSegmentMaxLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value 1", "1", false},
		{"valid value 2147483647", "2147483647", false},
		{"invalid value 0", "0", true},
		{"invalid value 2147483648", "2147483648", true},
		{"invalid value abc", "abc", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTsReservedLastSegmentMaxLimit(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkTsReservedLastSegmentMaxLimit(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestCheckTsBlockFilterSamplingRatio tests the checkTsBlockFilterSamplingRatio function
func TestCheckTsBlockFilterSamplingRatio(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value 0.1", "0.1", false},
		{"valid value 1.0", "1.0", false},
		{"invalid value 0", "0", true},
		{"invalid value 1.1", "1.1", true},
		{"invalid value abc", "abc", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTsBlockFilterSamplingRatio(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkTsBlockFilterSamplingRatio(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestCheckTsCountRecalcCycle tests the checkTsCountRecalcCycle function
func TestCheckTsCountRecalcCycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"valid value 0", "0", false},
		{"valid value 86400", "86400", false},
		{"invalid value -1", "-1", true},
		{"invalid value 86401", "86401", true},
		{"invalid value abc", "abc", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTsCountRecalcCycle(tc.value)
			if (err != nil) != tc.expected {
				t.Errorf("checkTsCountRecalcCycle(%s): expected error = %t, got error = %v", tc.value, tc.expected, err)
			}
		})
	}
}

// TestToSettingString tests the toSettingString function
func TestToSettingString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	// Test string setting
	stringSetting := settings.RegisterStringSetting(
		"test.string",
		"test string setting",
		"default",
	)

	// Test bool setting
	boolSetting := settings.RegisterBoolSetting(
		"test.bool.enabled",
		"test bool setting",
		false,
	)

	// Test int setting
	intSetting := settings.RegisterIntSetting(
		"test.int",
		"test int setting",
		0,
	)

	testCases := []struct {
		name     string
		setting  settings.Setting
		datum    tree.Datum
		expected string
		hasError bool
	}{
		{"string setting with string datum", stringSetting, tree.NewDString("test"), "test", false},
		{"bool setting with bool datum", boolSetting, tree.MakeDBool(tree.DBool(true)), "true", false},
		{"int setting with int datum", intSetting, tree.NewDInt(tree.DInt(42)), "42", false},
	}
	p := MakeTestPlanner()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := toSettingString(ctx, p, st, tc.name, tc.setting, tc.datum, nil)
			if (err != nil) != tc.hasError {
				t.Errorf("toSettingString: expected error = %t, got error = %v", tc.hasError, err)
			}
			if !tc.hasError && result != tc.expected {
				t.Errorf("toSettingString: expected result = %s, got result = %s", tc.expected, result)
			}
		})
	}
}

func TestClusterSettingRejectsOutOfRangeValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(conn)
	testCases := []struct {
		name  string
		query string
		errRE string
	}{
		{
			name:  "inside-out ratio below zero",
			query: "SET CLUSTER SETTING sql.opt.inside_out_row_ratio = -0.1",
			errRE: `outside the range \[0, 1\]`,
		},
		{
			name:  "inside-out ratio above one",
			query: "SET CLUSTER SETTING sql.opt.inside_out_row_ratio = 1.5",
			errRE: `outside the range \[0, 1\]`,
		},
		{
			name:  "negative TS parallel degree",
			query: "SET CLUSTER SETTING ts.parallel_degree = -1",
			errRE: `negative value`,
		},
		{
			name:  "non-positive max running flows",
			query: "SET CLUSTER SETTING sql.distsql.max_running_flows = -1",
			errRE: `value < 1`,
		},
		{
			name:  "negative slow query threshold",
			query: "SET CLUSTER SETTING sql.log.slow_query.latency_threshold = '-1s'",
			errRE: `negative duration`,
		},
		{
			name:  "negative statement details threshold",
			query: "SET CLUSTER SETTING sql.metrics.statement_details.threshold = '-1s'",
			errRE: `negative duration`,
		},
		{
			name:  "non-positive cleanup interval",
			query: "SET CLUSTER SETTING sql.temp_object_cleaner.cleanup_interval = '-1s'",
			errRE: `non-positive duration`,
		},
		{
			name:  "negative transaction trace threshold",
			query: "SET CLUSTER SETTING sql.trace.txn.enable_threshold = '-1s'",
			errRE: `negative duration`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r.ExpectErr(t, tc.errRE, tc.query)
		})
	}
}

// TestSetClusterSetting tests the SetClusterSetting method
func TestSetClusterSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logScope := log.Scope(t)
	defer logScope.Close(t)
	ctx := context.Background()

	// Start a test server
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a test database
	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(t, "CREATE DATABASE test_db")

	// Get the executor config
	execCfg := s.ExecutorConfig().(ExecutorConfig)

	// Create a GenericPlanner with admin privileges
	p, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		security.RootUser, // Root user has admin privileges
		&MemoryMetrics{},
		&execCfg,
	)
	GenericPlanner := p.(*GenericPlanner)
	defer cleanup()

	// Test cases for different cluster settings
	testCases := []struct {
		name     string
		setting  string
		value    tree.TypedExpr
		expected bool // expected error when calling SetClusterSetting
	}{
		{"valid ts.rows_per_block.min_limit", "ts.rows_per_block.min_limit", tree.NewDInt(tree.DInt(1024)), false},
		{"valid ts.rows_per_block.max_limit", "ts.rows_per_block.max_limit", tree.NewDInt(tree.DInt(4096)), false},
		{"valid ts.dedup.rule", "ts.dedup.rule", tree.NewDString("merge"), false},
		{"invalid ts.dedup.rule", "ts.dedup.rule", tree.NewDString("invalid"), false}, // SetClusterSetting doesn't validate values
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a SetClusterSetting node
			n := &tree.SetClusterSetting{
				Name:  tc.setting,
				Value: tc.value,
			}

			// Test SetClusterSetting
			node, err := GenericPlanner.SetClusterSetting(ctx, n)
			if (err != nil) != tc.expected {
				t.Errorf("SetClusterSetting for %s: expected error = %t, got error = %v", tc.name, tc.expected, err)
			}

			if !tc.expected {
				// Verify the returned node is a setClusterSettingNode
				setClusterNode, ok := node.(*setClusterSettingNode)
				if !ok {
					t.Error("SetClusterSetting should return a setClusterSettingNode")
					return
				}

				// Test StartExec for invalid values
				if tc.setting == "ts.dedup.rule" && tc.value.String() == "'invalid'" {
					// Create RunParams
					RunParams := RunParams{
						Ctx:             ctx,
						p:               GenericPlanner,
						extendedEvalCtx: GenericPlanner.ExtendedEvalContext(),
					}

					// Test StartExec - should return error for invalid dedup rule
					err := setClusterNode.StartExec(RunParams)
					if err == nil {
						t.Error("StartExec should return error for invalid ts.dedup.rule")
					}
				}
			}
		})
	}
}
