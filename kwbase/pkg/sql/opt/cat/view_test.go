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

// MockView implements the cat.View interface for testing purposes.
type MockView struct {
	name tree.Name
	id   cat.StableID
}

func (m *MockView) Name() tree.Name {
	return m.name
}

func (m *MockView) ID() cat.StableID {
	return m.id
}

func TestViewInterface(t *testing.T) {
	view := &MockView{
		name: tree.Name("test_view"),
		id:   12345,
	}

	if view.ID() != 12345 {
		t.Errorf("Expected ID to be 12345, got %d", view.ID())
	}

	if string(view.Name()) != "test_view" {
		t.Errorf("Expected Name to be 'test_view', got '%s'", string(view.Name()))
	}
}

func TestViewName(t *testing.T) {
	name := tree.Name("test_view_name")

	if string(name) != "test_view_name" {
		t.Errorf("Expected name to be 'test_view_name', got '%s'", string(name))
	}
}
