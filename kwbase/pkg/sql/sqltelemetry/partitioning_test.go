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

func TestPartitioningTelemetryTypeString(t *testing.T) {
	tests := []struct {
		partitioningType sqltelemetry.PartitioningTelemetryType
		expected         string
	}{
		{sqltelemetry.AlterAllPartitions, "alter-all-partitions"},
		{sqltelemetry.PartitionConstrainedScan, "partition-constrained-scan"},
	}

	for _, test := range tests {
		result := test.partitioningType.String()
		if result != test.expected {
			t.Errorf("PartitioningTelemetryType(%d).String() = %s; expected %s", test.partitioningType, result, test.expected)
		}
	}
}

func TestIncrementPartitioningCounter(t *testing.T) {
	// Test that we can call IncrementPartitioningCounter without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("IncrementPartitioningCounter should not panic, but it did: %v", r)
		}
	}()

	// Test with each partitioning type
	partitioningTypes := []sqltelemetry.PartitioningTelemetryType{
		sqltelemetry.AlterAllPartitions,
		sqltelemetry.PartitionConstrainedScan,
	}

	for _, partitioningType := range partitioningTypes {
		sqltelemetry.IncrementPartitioningCounter(partitioningType)
	}
}
