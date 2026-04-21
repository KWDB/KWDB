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

package sessiondata

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDataConversionConfig_GetFloatPrec(t *testing.T) {
	testCases := []struct {
		name             string
		extraFloatDigits int
		want             int
	}{
		{"extra 3 returns -1", 3, -1},
		{"extra 4 returns -1", 4, -1},
		{"extra 0 returns 15", 0, 15},
		{"extra 1 returns 16", 1, 16},
		{"extra -14 returns 1", -14, 1},
		{"extra -15 returns 1", -15, 1},
		{"extra 2 returns 17", 2, 17},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &DataConversionConfig{ExtraFloatDigits: tc.extraFloatDigits}
			got := cfg.GetFloatPrec()
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDataConversionConfig_Equals(t *testing.T) {
	loc1 := time.UTC

	testCases := []struct {
		name string
		cfg1 *DataConversionConfig
		cfg2 *DataConversionConfig
		want bool
	}{
		{
			"same config",
			&DataConversionConfig{
				BytesEncodeFormat: BytesEncodeHex,
				ExtraFloatDigits:  0,
				Location:          loc1,
			},
			&DataConversionConfig{
				BytesEncodeFormat: BytesEncodeHex,
				ExtraFloatDigits:  0,
				Location:          loc1,
			},
			true,
		},
		{
			"different BytesEncodeFormat",
			&DataConversionConfig{
				BytesEncodeFormat: BytesEncodeHex,
				ExtraFloatDigits:  0,
				Location:          loc1,
			},
			&DataConversionConfig{
				BytesEncodeFormat: BytesEncodeEscape,
				ExtraFloatDigits:  0,
				Location:          loc1,
			},
			false,
		},
		{
			"different ExtraFloatDigits",
			&DataConversionConfig{
				BytesEncodeFormat: BytesEncodeHex,
				ExtraFloatDigits:  0,
				Location:          loc1,
			},
			&DataConversionConfig{
				BytesEncodeFormat: BytesEncodeHex,
				ExtraFloatDigits:  1,
				Location:          loc1,
			},
			false,
		},
		{
			"both nil Location",
			&DataConversionConfig{
				BytesEncodeFormat: BytesEncodeHex,
				ExtraFloatDigits:  0,
				Location:          nil,
			},
			&DataConversionConfig{
				BytesEncodeFormat: BytesEncodeHex,
				ExtraFloatDigits:  0,
				Location:          nil,
			},
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.cfg1.Equals(tc.cfg2)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestBytesEncodeFormat_String(t *testing.T) {
	testCases := []struct {
		name string
		val  BytesEncodeFormat
		want string
	}{
		{"BytesEncodeHex", BytesEncodeHex, "hex"},
		{"BytesEncodeEscape", BytesEncodeEscape, "escape"},
		{"BytesEncodeBase64", BytesEncodeBase64, "base64"},
		{"invalid", 999, "invalid (999)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.val.String()
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestBytesEncodeFormatFromString(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		want   BytesEncodeFormat
		wantOk bool
	}{
		{"HEX", "HEX", BytesEncodeHex, true},
		{"hex", "hex", BytesEncodeHex, true},
		{"Hex", "Hex", BytesEncodeHex, true},
		{"ESCAPE", "ESCAPE", BytesEncodeEscape, true},
		{"escape", "escape", BytesEncodeEscape, true},
		{"BASE64", "BASE64", BytesEncodeBase64, true},
		{"base64", "base64", BytesEncodeBase64, true},
		{"invalid", "invalid", -1, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := BytesEncodeFormatFromString(tc.input)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantOk, ok)
		})
	}
}

func TestDistSQLExecMode_String(t *testing.T) {
	testCases := []struct {
		name string
		val  DistSQLExecMode
		want string
	}{
		{"DistSQLOff", DistSQLOff, "off"},
		{"DistSQLAuto", DistSQLAuto, "auto"},
		{"DistSQLOn", DistSQLOn, "on"},
		{"DistSQLAlways", DistSQLAlways, "always"},
		{"invalid", 999, "invalid (999)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.val.String()
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDistSQLExecModeFromString(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		want   DistSQLExecMode
		wantOk bool
	}{
		{"OFF", "OFF", DistSQLOff, true},
		{"off", "off", DistSQLOff, true},
		{"AUTO", "AUTO", DistSQLAuto, true},
		{"auto", "auto", DistSQLAuto, true},
		{"ON", "ON", DistSQLOn, true},
		{"on", "on", DistSQLOn, true},
		{"ALWAYS", "ALWAYS", DistSQLAlways, true},
		{"always", "always", DistSQLAlways, true},
		{"invalid", "invalid", 0, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := DistSQLExecModeFromString(tc.input)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantOk, ok)
		})
	}
}

func TestVectorizeExecMode_String(t *testing.T) {
	testCases := []struct {
		name string
		val  VectorizeExecMode
		want string
	}{
		{"VectorizeOff", VectorizeOff, "off"},
		{"VectorizeAuto", VectorizeAuto, "auto"},
		{"VectorizeOn", VectorizeOn, "on"},
		{"VectorizeExperimentalAlways", VectorizeExperimentalAlways, "experimental_always"},
		{"invalid", 999, "invalid (999)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.val.String()
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestVectorizeExecModeFromString(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		want   VectorizeExecMode
		wantOk bool
	}{
		{"OFF", "OFF", VectorizeOff, true},
		{"off", "off", VectorizeOff, true},
		{"AUTO", "AUTO", VectorizeAuto, true},
		{"auto", "auto", VectorizeAuto, true},
		{"ON", "ON", VectorizeOn, true},
		{"on", "on", VectorizeOn, true},
		{"EXPERIMENTAL_ALWAYS", "EXPERIMENTAL_ALWAYS", VectorizeExperimentalAlways, true},
		{"experimental_always", "experimental_always", VectorizeExperimentalAlways, true},
		{"invalid", "invalid", 0, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := VectorizeExecModeFromString(tc.input)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantOk, ok)
		})
	}
}

func TestSerialNormalizationMode_String(t *testing.T) {
	testCases := []struct {
		name string
		val  SerialNormalizationMode
		want string
	}{
		{"SerialUsesRowID", SerialUsesRowID, "rowid"},
		{"SerialUsesVirtualSequences", SerialUsesVirtualSequences, "virtual_sequence"},
		{"SerialUsesSQLSequences", SerialUsesSQLSequences, "sql_sequence"},
		{"invalid", 999, "invalid (999)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.val.String()
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSerialNormalizationModeFromString(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		want   SerialNormalizationMode
		wantOk bool
	}{
		{"ROWID", "ROWID", SerialUsesRowID, true},
		{"rowid", "rowid", SerialUsesRowID, true},
		{"VIRTUAL_SEQUENCE", "VIRTUAL_SEQUENCE", SerialUsesVirtualSequences, true},
		{"virtual_sequence", "virtual_sequence", SerialUsesVirtualSequences, true},
		{"SQL_SEQUENCE", "SQL_SEQUENCE", SerialUsesSQLSequences, true},
		{"sql_sequence", "sql_sequence", SerialUsesSQLSequences, true},
		{"invalid", "invalid", 0, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := SerialNormalizationModeFromString(tc.input)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantOk, ok)
		})
	}
}
