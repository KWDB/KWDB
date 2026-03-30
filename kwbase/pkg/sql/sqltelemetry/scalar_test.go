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

func TestBuiltinCounter(t *testing.T) {
	counter := sqltelemetry.BuiltinCounter("abs", "(int)")
	if counter == nil {
		t.Error("BuiltinCounter should not return nil")
	}

	// Test with different inputs
	counter2 := sqltelemetry.BuiltinCounter("length", "(text)")
	if counter2 == nil {
		t.Error("BuiltinCounter should not return nil for different inputs")
	}
}

func TestUnaryOpCounter(t *testing.T) {
	counter := sqltelemetry.UnaryOpCounter("+", "int")
	if counter == nil {
		t.Error("UnaryOpCounter should not return nil")
	}

	// Test with different inputs
	counter2 := sqltelemetry.UnaryOpCounter("-", "float")
	if counter2 == nil {
		t.Error("UnaryOpCounter should not return nil for different inputs")
	}
}

func TestCmpOpCounter(t *testing.T) {
	counter := sqltelemetry.CmpOpCounter("=", "int", "int")
	if counter == nil {
		t.Error("CmpOpCounter should not return nil")
	}

	// Test with different inputs
	counter2 := sqltelemetry.CmpOpCounter("<", "text", "text")
	if counter2 == nil {
		t.Error("CmpOpCounter should not return nil for different inputs")
	}
}

func TestBinOpCounter(t *testing.T) {
	counter := sqltelemetry.BinOpCounter("+", "int", "int")
	if counter == nil {
		t.Error("BinOpCounter should not return nil")
	}

	// Test with different inputs
	counter2 := sqltelemetry.BinOpCounter("*", "float", "float")
	if counter2 == nil {
		t.Error("BinOpCounter should not return nil for different inputs")
	}
}

func TestCastOpCounter(t *testing.T) {
	counter := sqltelemetry.CastOpCounter("int", "text")
	if counter == nil {
		t.Error("CastOpCounter should not return nil")
	}

	// Test with different inputs
	counter2 := sqltelemetry.CastOpCounter("text", "int")
	if counter2 == nil {
		t.Error("CastOpCounter should not return nil for different inputs")
	}
}

func TestArrayCastCounter(t *testing.T) {
	counter := sqltelemetry.ArrayCastCounter
	if counter == nil {
		t.Error("ArrayCastCounter should not be nil")
	}
}

func TestArrayConstructorCounter(t *testing.T) {
	counter := sqltelemetry.ArrayConstructorCounter
	if counter == nil {
		t.Error("ArrayConstructorCounter should not be nil")
	}
}

func TestArrayFlattenCounter(t *testing.T) {
	counter := sqltelemetry.ArrayFlattenCounter
	if counter == nil {
		t.Error("ArrayFlattenCounter should not be nil")
	}
}

func TestArraySubscriptCounter(t *testing.T) {
	counter := sqltelemetry.ArraySubscriptCounter
	if counter == nil {
		t.Error("ArraySubscriptCounter should not be nil")
	}
}

func TestIfErrCounter(t *testing.T) {
	counter := sqltelemetry.IfErrCounter
	if counter == nil {
		t.Error("IfErrCounter should not be nil")
	}
}

func TestLargeLShiftArgumentCounter(t *testing.T) {
	counter := sqltelemetry.LargeLShiftArgumentCounter
	if counter == nil {
		t.Error("LargeLShiftArgumentCounter should not be nil")
	}
}

func TestLargeRShiftArgumentCounter(t *testing.T) {
	counter := sqltelemetry.LargeRShiftArgumentCounter
	if counter == nil {
		t.Error("LargeRShiftArgumentCounter should not be nil")
	}
}
