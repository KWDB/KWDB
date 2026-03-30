// Copyright 2018 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

// MockZone implements the cat.Zone interface for testing purposes.
type MockZone struct {
	name tree.Name
	id   cat.StableID
}

func (m *MockZone) Name() tree.Name {
	return m.name
}

func (m *MockZone) ID() cat.StableID {
	return m.id
}

func TestZoneInterface(t *testing.T) {
	zone := &MockZone{
		name: tree.Name("test_zone"),
		id:   12345,
	}

	if zone.ID() != 12345 {
		t.Errorf("Expected ID to be 12345, got %d", zone.ID())
	}

	if string(zone.Name()) != "test_zone" {
		t.Errorf("Expected Name to be 'test_zone', got '%s'", string(zone.Name()))
	}
}

func TestZoneName(t *testing.T) {
	name := tree.Name("test_zone_name")

	if string(name) != "test_zone_name" {
		t.Errorf("Expected name to be 'test_zone_name', got '%s'", string(name))
	}
}
