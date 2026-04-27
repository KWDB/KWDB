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

package flowinfra

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestOutboxStatsBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test creating a new OutboxStats
	stats := &OutboxStats{}
	if stats.BytesSent != 0 {
		t.Errorf("expected BytesSent to be 0, got: %d", stats.BytesSent)
	}

	// Test setting BytesSent
	expectedBytes := int64(1024)
	stats.BytesSent = expectedBytes
	if stats.BytesSent != expectedBytes {
		t.Errorf("expected BytesSent to be %d, got: %d", expectedBytes, stats.BytesSent)
	}
}

func TestOutboxStatsReset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a stats with non-zero value
	stats := &OutboxStats{BytesSent: 1024}
	if stats.BytesSent != 1024 {
		t.Errorf("expected BytesSent to be 1024, got: %d", stats.BytesSent)
	}

	// Reset and check
	stats.Reset()
	if stats.BytesSent != 0 {
		t.Errorf("expected BytesSent to be 0 after Reset, got: %d", stats.BytesSent)
	}
}

func TestOutboxStatsString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test String method
	stats := &OutboxStats{BytesSent: 1024}
	str := stats.String()
	if str == "" {
		t.Error("expected String() to return non-empty string")
	}
}

func TestOutboxStatsSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test Size method with zero value
	stats1 := &OutboxStats{}
	size1 := stats1.Size()
	if size1 != 0 {
		t.Errorf("expected Size to be 0 for zero value, got: %d", size1)
	}

	// Test Size method with non-zero value
	stats2 := &OutboxStats{BytesSent: 1024}
	size2 := stats2.Size()
	if size2 == 0 {
		t.Error("expected Size to be non-zero for non-zero value")
	}
}

func TestOutboxStatsMarshalTo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a stats with value
	stats := &OutboxStats{BytesSent: 1024}

	// Marshal to buffer
	buffer := make([]byte, stats.Size())
	n, err := stats.MarshalTo(buffer)
	if err != nil {
		t.Fatalf("expected MarshalTo to succeed, got: %v", err)
	}
	if n == 0 {
		t.Error("expected MarshalTo to write non-zero bytes")
	}
	if n > len(buffer) {
		t.Errorf("expected MarshalTo to write at most %d bytes, wrote %d", len(buffer), n)
	}
}

func TestOutboxStatsProtoMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test ProtoMessage method (interface compliance)
	stats := &OutboxStats{}
	stats.ProtoMessage() // Should not panic
}

func TestOutboxStatsDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test Descriptor method (interface compliance)
	stats := &OutboxStats{}
	_, _ = stats.Descriptor() // Should not panic
}
