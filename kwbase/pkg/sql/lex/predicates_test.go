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

package lex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsASCII(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty string", "", true},
		{"ASCII string", "hello", true},
		{"Unicode string", "café", false},
		{"Mixed string", "hello café", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isASCII(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsDigit(t *testing.T) {
	tests := []struct {
		name  string
		input int
		want  bool
	}{
		{"digit 0", '0', true},
		{"digit 9", '9', true},
		{"letter a", 'a', false},
		{"letter Z", 'Z', false},
		{"symbol !", '!', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsDigit(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsHexDigit(t *testing.T) {
	tests := []struct {
		name  string
		input int
		want  bool
	}{
		{"digit 0", '0', true},
		{"digit 9", '9', true},
		{"lowercase a", 'a', true},
		{"lowercase f", 'f', true},
		{"uppercase A", 'A', true},
		{"uppercase F", 'F', true},
		{"letter g", 'g', false},
		{"symbol !", '!', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsHexDigit(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsIdentStart(t *testing.T) {
	tests := []struct {
		name  string
		input int
		want  bool
	}{
		{"uppercase A", 'A', true},
		{"uppercase Z", 'Z', true},
		{"lowercase a", 'a', true},
		{"lowercase z", 'z', true},
		{"underscore", '_', true},
		{"digit 0", '0', false},
		{"symbol !", '!', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsIdentStart(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsIdentMiddle(t *testing.T) {
	tests := []struct {
		name  string
		input int
		want  bool
	}{
		{"uppercase A", 'A', true},
		{"lowercase a", 'a', true},
		{"underscore", '_', true},
		{"digit 0", '0', true},
		{"dollar sign", '$', true},
		{"symbol !", '!', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsIdentMiddle(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsBareIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty string", "", false},
		{"valid identifier", "hello", true},
		{"identifier with underscore", "hello_world", true},
		{"identifier with digit", "hello123", true},
		{"identifier with dollar", "hello$world", true},
		{"identifier starting with digit", "123hello", false},
		{"identifier with uppercase", "Hello", false},
		{"identifier with symbol", "hello!world", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isBareIdentifier(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCheckNameValid(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty string", "", false},
		{"valid identifier", "hello", true},
		{"reserved keyword", "select", false},
		{"identifier with uppercase", "Hello", false},
		{"identifier with symbol", "hello!world", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CheckNameValid(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}
