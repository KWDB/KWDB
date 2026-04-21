// Copyright 2018 The Cockroach Authors.
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

package cat_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// MockTable implements the cat.Table interface for testing purposes.
type MockTable struct {
	name tree.Name
	id   cat.StableID
}

func (m *MockTable) Name() tree.Name {
	return m.name
}

func (m *MockTable) ID() cat.StableID {
	return m.id
}

func TestTableInterface(t *testing.T) {
	table := &MockTable{
		name: tree.Name("test_table"),
		id:   12345,
	}

	if table.ID() != 12345 {
		t.Errorf("Expected ID to be 12345, got %d", table.ID())
	}

	if string(table.Name()) != "test_table" {
		t.Errorf("Expected Name to be 'test_table', got '%s'", string(table.Name()))
	}
}

func TestTableName(t *testing.T) {
	name := tree.Name("test_table_name")

	if string(name) != "test_table_name" {
		t.Errorf("Expected name to be 'test_table_name', got '%s'", string(name))
	}
}
