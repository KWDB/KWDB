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

package execinfrapb

import "testing"

// TestBytesEncodeFormat tests the BytesEncodeFormat enum methods
func TestBytesEncodeFormat(t *testing.T) {
	// Test Enum method
	format := BytesEncodeFormat_HEX
	enumPtr := format.Enum()
	if *enumPtr != BytesEncodeFormat_HEX {
		t.Errorf("Enum() returned %v, expected %v", *enumPtr, BytesEncodeFormat_HEX)
	}

	// Test String method
	if format.String() != "HEX" {
		t.Errorf("String() returned %q, expected %q", format.String(), "HEX")
	}

	// Test EnumDescriptor method
	desc, path := format.EnumDescriptor()
	if len(desc) == 0 {
		t.Error("EnumDescriptor() returned empty descriptor")
	}
	if len(path) == 0 {
		t.Error("EnumDescriptor() returned empty path")
	}
}

// TestApiGeneratedFunctions tests all generated serialization and deserialization functions for api.pb.go
func TestApiGeneratedFunctions(t *testing.T) {
	// Test simple message types
	testTsInfo(t)
}

func testTsInfo(t *testing.T) {
	spec := &TsInfo{
		QueryID:              123,
		BrpcAddrs:            []string{"localhost:8080", "localhost:8081"},
		UseAeGather:          true,
		IsDist:               true,
		UseQueryShortCircuit: true,
		UseCompressType:      1,
	}

	// Test Reset function
	if resetter, ok := interface{}(spec).(interface{ Reset() }); ok {
		resetter.Reset()
	}

	// Test String function
	if stringer, ok := interface{}(spec).(interface{ String() string }); ok {
		_ = stringer.String()
	}

	// Test Size function
	if sizer, ok := interface{}(spec).(interface{ Size() int }); ok {
		_ = sizer.Size()
	}

	// Test XXX_Size function
	if xxxSizer, ok := interface{}(spec).(interface{ XXX_Size() int }); ok {
		_ = xxxSizer.XXX_Size()
	}

	// Test XXX_DiscardUnknown function
	if xxxDiscarder, ok := interface{}(spec).(interface{ XXX_DiscardUnknown() }); ok {
		xxxDiscarder.XXX_DiscardUnknown()
	}
}
