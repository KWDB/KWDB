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

package sqltelemetry_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

func TestCteUseCounter(t *testing.T) {
	counter := sqltelemetry.CteUseCounter
	if counter == nil {
		t.Error("CteUseCounter should not be nil")
	}
}

func TestRecursiveCteUseCounter(t *testing.T) {
	counter := sqltelemetry.RecursiveCteUseCounter
	if counter == nil {
		t.Error("RecursiveCteUseCounter should not be nil")
	}
}

func TestSubqueryUseCounter(t *testing.T) {
	counter := sqltelemetry.SubqueryUseCounter
	if counter == nil {
		t.Error("SubqueryUseCounter should not be nil")
	}
}

func TestCorrelatedSubqueryUseCounter(t *testing.T) {
	counter := sqltelemetry.CorrelatedSubqueryUseCounter
	if counter == nil {
		t.Error("CorrelatedSubqueryUseCounter should not be nil")
	}
}

func TestForeignKeyChecksUseCounter(t *testing.T) {
	counter := sqltelemetry.ForeignKeyChecksUseCounter
	if counter == nil {
		t.Error("ForeignKeyChecksUseCounter should not be nil")
	}
}

func TestForeignKeyCascadesUseCounter(t *testing.T) {
	counter := sqltelemetry.ForeignKeyCascadesUseCounter
	if counter == nil {
		t.Error("ForeignKeyCascadesUseCounter should not be nil")
	}
}

func TestForeignKeyLegacyUseCounter(t *testing.T) {
	counter := sqltelemetry.ForeignKeyLegacyUseCounter
	if counter == nil {
		t.Error("ForeignKeyLegacyUseCounter should not be nil")
	}
}

func TestLateralJoinUseCounter(t *testing.T) {
	counter := sqltelemetry.LateralJoinUseCounter
	if counter == nil {
		t.Error("LateralJoinUseCounter should not be nil")
	}
}

func TestHashJoinHintUseCounter(t *testing.T) {
	counter := sqltelemetry.HashJoinHintUseCounter
	if counter == nil {
		t.Error("HashJoinHintUseCounter should not be nil")
	}
}

func TestMergeJoinHintUseCounter(t *testing.T) {
	counter := sqltelemetry.MergeJoinHintUseCounter
	if counter == nil {
		t.Error("MergeJoinHintUseCounter should not be nil")
	}
}

func TestLookupJoinHintUseCounter(t *testing.T) {
	counter := sqltelemetry.LookupJoinHintUseCounter
	if counter == nil {
		t.Error("LookupJoinHintUseCounter should not be nil")
	}
}

func TestIndexHintUseCounter(t *testing.T) {
	counter := sqltelemetry.IndexHintUseCounter
	if counter == nil {
		t.Error("IndexHintUseCounter should not be nil")
	}
}

func TestIndexHintSelectUseCounter(t *testing.T) {
	counter := sqltelemetry.IndexHintSelectUseCounter
	if counter == nil {
		t.Error("IndexHintSelectUseCounter should not be nil")
	}
}

func TestIndexHintUpdateUseCounter(t *testing.T) {
	counter := sqltelemetry.IndexHintUpdateUseCounter
	if counter == nil {
		t.Error("IndexHintUpdateUseCounter should not be nil")
	}
}

func TestIndexHintDeleteUseCounter(t *testing.T) {
	counter := sqltelemetry.IndexHintDeleteUseCounter
	if counter == nil {
		t.Error("IndexHintDeleteUseCounter should not be nil")
	}
}

func TestInterleavedTableJoinCounter(t *testing.T) {
	counter := sqltelemetry.InterleavedTableJoinCounter
	if counter == nil {
		t.Error("InterleavedTableJoinCounter should not be nil")
	}
}

func TestExplainPlanUseCounter(t *testing.T) {
	counter := sqltelemetry.ExplainPlanUseCounter
	if counter == nil {
		t.Error("ExplainPlanUseCounter should not be nil")
	}
}

func TestExplainDistSQLUseCounter(t *testing.T) {
	counter := sqltelemetry.ExplainDistSQLUseCounter
	if counter == nil {
		t.Error("ExplainDistSQLUseCounter should not be nil")
	}
}

func TestExplainAnalyzeUseCounter(t *testing.T) {
	counter := sqltelemetry.ExplainAnalyzeUseCounter
	if counter == nil {
		t.Error("ExplainAnalyzeUseCounter should not be nil")
	}
}

func TestExplainAnalyzeDebugUseCounter(t *testing.T) {
	counter := sqltelemetry.ExplainAnalyzeDebugUseCounter
	if counter == nil {
		t.Error("ExplainAnalyzeDebugUseCounter should not be nil")
	}
}

func TestExplainOptUseCounter(t *testing.T) {
	counter := sqltelemetry.ExplainOptUseCounter
	if counter == nil {
		t.Error("ExplainOptUseCounter should not be nil")
	}
}

func TestExplainVecUseCounter(t *testing.T) {
	counter := sqltelemetry.ExplainVecUseCounter
	if counter == nil {
		t.Error("ExplainVecUseCounter should not be nil")
	}
}

func TestExplainOptVerboseUseCounter(t *testing.T) {
	counter := sqltelemetry.ExplainOptVerboseUseCounter
	if counter == nil {
		t.Error("ExplainOptVerboseUseCounter should not be nil")
	}
}

func TestCreateStatisticsUseCounter(t *testing.T) {
	counter := sqltelemetry.CreateStatisticsUseCounter
	if counter == nil {
		t.Error("CreateStatisticsUseCounter should not be nil")
	}
}

func TestTurnAutoStatsOnUseCounter(t *testing.T) {
	counter := sqltelemetry.TurnAutoStatsOnUseCounter
	if counter == nil {
		t.Error("TurnAutoStatsOnUseCounter should not be nil")
	}
}

func TestTurnAutoStatsOffUseCounter(t *testing.T) {
	counter := sqltelemetry.TurnAutoStatsOffUseCounter
	if counter == nil {
		t.Error("TurnAutoStatsOffUseCounter should not be nil")
	}
}

func TestJoinAlgoHashUseCounter(t *testing.T) {
	counter := sqltelemetry.JoinAlgoHashUseCounter
	if counter == nil {
		t.Error("JoinAlgoHashUseCounter should not be nil")
	}
}

func TestJoinAlgoMergeUseCounter(t *testing.T) {
	counter := sqltelemetry.JoinAlgoMergeUseCounter
	if counter == nil {
		t.Error("JoinAlgoMergeUseCounter should not be nil")
	}
}

func TestJoinAlgoLookupUseCounter(t *testing.T) {
	counter := sqltelemetry.JoinAlgoLookupUseCounter
	if counter == nil {
		t.Error("JoinAlgoLookupUseCounter should not be nil")
	}
}

func TestJoinAlgoCrossUseCounter(t *testing.T) {
	counter := sqltelemetry.JoinAlgoCrossUseCounter
	if counter == nil {
		t.Error("JoinAlgoCrossUseCounter should not be nil")
	}
}

func TestJoinTypeInnerUseCounter(t *testing.T) {
	counter := sqltelemetry.JoinTypeInnerUseCounter
	if counter == nil {
		t.Error("JoinTypeInnerUseCounter should not be nil")
	}
}

func TestJoinTypeLeftUseCounter(t *testing.T) {
	counter := sqltelemetry.JoinTypeLeftUseCounter
	if counter == nil {
		t.Error("JoinTypeLeftUseCounter should not be nil")
	}
}

func TestJoinTypeFullUseCounter(t *testing.T) {
	counter := sqltelemetry.JoinTypeFullUseCounter
	if counter == nil {
		t.Error("JoinTypeFullUseCounter should not be nil")
	}
}

func TestJoinTypeSemiUseCounter(t *testing.T) {
	counter := sqltelemetry.JoinTypeSemiUseCounter
	if counter == nil {
		t.Error("JoinTypeSemiUseCounter should not be nil")
	}
}

func TestJoinTypeAntiUseCounter(t *testing.T) {
	counter := sqltelemetry.JoinTypeAntiUseCounter
	if counter == nil {
		t.Error("JoinTypeAntiUseCounter should not be nil")
	}
}

func TestCancelQueriesUseCounter(t *testing.T) {
	counter := sqltelemetry.CancelQueriesUseCounter
	if counter == nil {
		t.Error("CancelQueriesUseCounter should not be nil")
	}
}

func TestCancelSessionsUseCounter(t *testing.T) {
	counter := sqltelemetry.CancelSessionsUseCounter
	if counter == nil {
		t.Error("CancelSessionsUseCounter should not be nil")
	}
}

func TestReportJoinReorderLimit(t *testing.T) {
	// Test that we can call ReportJoinReorderLimit without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("ReportJoinReorderLimit should not panic, but it did: %v", r)
		}
	}()

	// Test with different values
	sqltelemetry.ReportJoinReorderLimit(0)
	sqltelemetry.ReportJoinReorderLimit(5)
	sqltelemetry.ReportJoinReorderLimit(10)
	sqltelemetry.ReportJoinReorderLimit(20) // Above the reorderJoinsCounters limit
}

func TestReportMultiModelJoinReorderLimit(t *testing.T) {
	// Test that we can call ReportMultiModelJoinReorderLimit without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("ReportMultiModelJoinReorderLimit should not panic, but it did: %v", r)
		}
	}()

	// Test with different values
	sqltelemetry.ReportMultiModelJoinReorderLimit(0)
	sqltelemetry.ReportMultiModelJoinReorderLimit(5)
	sqltelemetry.ReportMultiModelJoinReorderLimit(10)
	sqltelemetry.ReportMultiModelJoinReorderLimit(20) // Above the multiModelReorderJoinsCounters limit
}

func TestWindowFunctionCounter(t *testing.T) {
	functions := []string{"row_number", "rank", "dense_rank", "lag", "lead", "first_value", "last_value", "nth_value"}

	for _, fn := range functions {
		counter := sqltelemetry.WindowFunctionCounter(fn)
		if counter == nil {
			t.Errorf("WindowFunctionCounter with function '%s' should not return nil", fn)
		}
	}

	// Test with empty string
	counter := sqltelemetry.WindowFunctionCounter("")
	if counter == nil {
		t.Error("WindowFunctionCounter with empty string should not return nil")
	}
}

func TestOptNodeCounter(t *testing.T) {
	nodeTypes := []string{"select", "insert", "update", "delete", "scan", "join", "index-join", "lookup-join"}

	for _, nodeType := range nodeTypes {
		counter := sqltelemetry.OptNodeCounter(nodeType)
		if counter == nil {
			t.Errorf("OptNodeCounter with node type '%s' should not return nil", nodeType)
		}
	}

	// Test with empty string
	counter := sqltelemetry.OptNodeCounter("")
	if counter == nil {
		t.Error("OptNodeCounter with empty string should not return nil")
	}
}
