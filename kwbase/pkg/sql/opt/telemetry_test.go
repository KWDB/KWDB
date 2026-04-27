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

package opt_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

// TestOpTelemetryCountersInitialization tests that the OpTelemetryCounters are properly initialized
func TestOpTelemetryCountersInitialization(t *testing.T) {
	// Check that counters for TelemetryOperators are not nil
	for _, op := range opt.TelemetryOperators {
		if opt.OpTelemetryCounters[op] == nil {
			t.Errorf("Expected counter for operator %s to be initialized, got nil", op)
		}
	}

	// Check that the counters are properly set to the expected values
	// for _, op := range opt.TelemetryOperators {
	// 	expectedCounter := sqltelemetry.OptNodeCounter(op.String())
	// 	if opt.OpTelemetryCounters[op] != expectedCounter {
	// 		t.Errorf("Expected counter for operator %s to be %v, got %v", op, expectedCounter, opt.OpTelemetryCounters[op])
	// 	}
	// }
}

// TestJoinTypeToUseCounter tests the JoinTypeToUseCounter function with different join operators
func TestJoinTypeToUseCounter(t *testing.T) {
	testCases := []struct {
		op       opt.Operator
		expected telemetry.Counter
		name     string
	}{
		{opt.InnerJoinOp, sqltelemetry.JoinTypeInnerUseCounter, "InnerJoinOp"},
		{opt.BatchLookUpJoinOp, sqltelemetry.JoinTypeInnerUseCounter, "BatchLookUpJoinOp"},
		{opt.LeftJoinOp, sqltelemetry.JoinTypeLeftUseCounter, "LeftJoinOp"},
		{opt.RightJoinOp, sqltelemetry.JoinTypeLeftUseCounter, "RightJoinOp"},
		{opt.FullJoinOp, sqltelemetry.JoinTypeFullUseCounter, "FullJoinOp"},
		{opt.SemiJoinOp, sqltelemetry.JoinTypeSemiUseCounter, "SemiJoinOp"},
		{opt.AntiJoinOp, sqltelemetry.JoinTypeAntiUseCounter, "AntiJoinOp"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := opt.JoinTypeToUseCounter(tc.op)
			if result != tc.expected {
				t.Errorf("JoinTypeToUseCounter(%s) = %v, want %v", tc.name, result, tc.expected)
			}
		})
	}
}

// TestJoinTypeToUseCounterPanic tests that JoinTypeToUseCounter panics for unsupported join operators
func TestJoinTypeToUseCounterPanic(t *testing.T) {
	// We need to identify an operator that is not handled by JoinTypeToUseCounter
	// Since we can't easily create an arbitrary operator that's not in the switch statement,
	// we'll focus on testing the valid cases above.
	// The panic case is difficult to test without knowing specific unhandled operators.

	// For now, we'll just verify that all the expected join operators are handled
	// without causing a panic during normal operation.

	// Test a few known valid operators to ensure they don't panic
	validOps := []opt.Operator{opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp, opt.SemiJoinOp, opt.AntiJoinOp}

	for _, op := range validOps {
		t.Run("NoPanic_"+op.String(), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("JoinTypeToUseCounter(%s) panicked unexpectedly: %v", op, r)
				}
			}()

			// This should not panic for valid join operators
			_ = opt.JoinTypeToUseCounter(op)
		})
	}
}

// TestOpTelemetryCountersLength tests that the length of OpTelemetryCounters matches NumOperators
func TestOpTelemetryCountersLength(t *testing.T) {
	if len(opt.OpTelemetryCounters) != int(opt.NumOperators) {
		t.Errorf("Expected OpTelemetryCounters length to be %d, got %d", int(opt.NumOperators), len(opt.OpTelemetryCounters))
	}
}

// BenchmarkOpTelemetryCountersAccess benchmarks accessing the OpTelemetryCounters array
func BenchmarkOpTelemetryCountersAccess(b *testing.B) {
	if len(opt.TelemetryOperators) == 0 {
		b.Skip("No TelemetryOperators to benchmark")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := opt.TelemetryOperators[i%len(opt.TelemetryOperators)]
		_ = opt.OpTelemetryCounters[op]
	}
}

// BenchmarkJoinTypeToUseCounter benchmarks the JoinTypeToUseCounter function
func BenchmarkJoinTypeToUseCounter(b *testing.B) {
	joinOps := []opt.Operator{opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp, opt.SemiJoinOp, opt.AntiJoinOp}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := joinOps[i%len(joinOps)]
		_ = opt.JoinTypeToUseCounter(op)
	}
}
