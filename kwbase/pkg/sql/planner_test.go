// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestTypeAsString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := planner{}

	testData := []struct {
		expr        tree.Expr
		expected    string
		expectedErr bool
	}{
		{expr: tree.NewDString("foo"), expected: "foo"},
		{
			expr: &tree.BinaryExpr{
				Operator: tree.Concat, Left: tree.NewDString("foo"), Right: tree.NewDString("bar")},
			expected: "foobar",
		},
		{expr: tree.NewDInt(3), expectedErr: true},
	}

	t.Run("TypeAsString", func(t *testing.T) {
		for _, td := range testData {
			fn, err := p.TypeAsString(td.expr, "test")
			if err != nil {
				if !td.expectedErr {
					t.Fatalf("expected no error; got %v", err)
				}
				continue
			} else if td.expectedErr {
				t.Fatal("expected error; got none")
			}
			s, err := fn()
			if err != nil {
				t.Fatal(err)
			}
			if s != td.expected {
				t.Fatalf("expected %s; got %s", td.expected, s)
			}
		}
	})

	t.Run("TypeAsStringArray", func(t *testing.T) {
		for _, td := range testData {
			fn, err := p.TypeAsStringArray([]tree.Expr{td.expr, td.expr}, "test")
			if err != nil {
				if !td.expectedErr {
					t.Fatalf("expected no error; got %v", err)
				}
				continue
			} else if td.expectedErr {
				t.Fatal("expected error; got none")
			}
			a, err := fn()
			if err != nil {
				t.Fatal(err)
			}
			expected := []string{td.expected, td.expected}
			if !reflect.DeepEqual(a, expected) {
				t.Fatalf("expected %v; got %v", expected, a)
			}
		}
	})
}

func TestTypeAsStringOrNull(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := planner{}

	tests := []struct {
		name        string
		expr        tree.Expr
		expected    string
		expectedErr bool
	}{
		{name: "string value", expr: tree.NewDString("foo"), expected: "foo"},
		{name: "null value", expr: tree.DNull, expected: ""},
		{name: "invalid type", expr: tree.NewDInt(3), expectedErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, err := p.TypeAsStringOrNull(tt.expr, "test")
			if err != nil {
				if !tt.expectedErr {
					t.Fatalf("expected no error; got %v", err)
				}
				return
			} else if tt.expectedErr {
				t.Fatal("expected error; got none")
			}

			isNull, s, err := fn()
			if err != nil {
				t.Fatal(err)
			}

			if tt.expr == tree.DNull {
				if !isNull {
					t.Fatalf("expected isNull=true; got false")
				}
				if s != "" {
					t.Fatalf("expected empty string; got %s", s)
				}
			} else {
				if isNull {
					t.Fatalf("expected isNull=false; got true")
				}
				if s != tt.expected {
					t.Fatalf("expected %s; got %s", tt.expected, s)
				}
			}
		})
	}
}

func TestTypeAsStringOpts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := planner{}

	tests := []struct {
		name        string
		opts        tree.KVOptions
		expected    map[string]string
		expectedErr bool
	}{
		{name: "empty options", opts: tree.KVOptions{}, expected: map[string]string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, err := p.TypeAsStringOpts(tt.opts, map[string]KVStringOptValidate{})
			if err != nil {
				if !tt.expectedErr {
					t.Fatalf("expected no error; got %v", err)
				}
				return
			} else if tt.expectedErr {
				t.Fatal("expected error; got none")
			}

			s, err := fn()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(s, tt.expected) {
				t.Fatalf("expected %v; got %v", tt.expected, s)
			}
		})
	}
}

func TestParseType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := planner{}

	tests := []struct {
		name        string
		sql         string
		expectedErr bool
	}{
		{name: "valid type", sql: "INT", expectedErr: false},
		{name: "invalid type", sql: "INVALID_TYPE", expectedErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := p.ParseType(tt.sql)
			if tt.expectedErr {
				if err == nil {
					t.Errorf("ParseType() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("ParseType() unexpected error: %v", err)
				}
				if result == nil {
					t.Errorf("ParseType() returned nil type")
				}
			}
		})
	}
}

func TestParseQualifiedTableName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := planner{}

	tests := []struct {
		name        string
		sql         string
		expectedErr bool
	}{
		{name: "valid table name", sql: "public.users", expectedErr: false},
		{name: "invalid table name", sql: "invalid..table", expectedErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := p.ParseQualifiedTableName(tt.sql)
			if tt.expectedErr {
				if err == nil {
					t.Errorf("ParseQualifiedTableName() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("ParseQualifiedTableName() unexpected error: %v", err)
				}
				if result == nil {
					t.Errorf("ParseQualifiedTableName() returned nil table name")
				}
			}
		})
	}
}

func TestPlannerGetters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name string
		test func(t *testing.T, p *planner)
	}{
		{
			name: "IsInternalSQL",
			test: func(t *testing.T, p *planner) {
				result := p.IsInternalSQL()
				if result != false {
					t.Errorf("IsInternalSQL() = %v, want false", result)
				}
			},
		},
		{
			name: "ExecutorConfig",
			test: func(t *testing.T, p *planner) {
				result := p.ExecutorConfig()
				if result == nil {
					t.Errorf("ExecutorConfig() = nil, want non-nil")
				}
			},
		},
		{
			name: "ExtendedEvalContext",
			test: func(t *testing.T, p *planner) {
				result := p.ExtendedEvalContext()
				if result == nil {
					t.Errorf("ExtendedEvalContext() = nil, want non-nil")
				}
			},
		},
		{
			name: "ExtendedEvalContextCopy",
			test: func(t *testing.T, p *planner) {
				result := p.ExtendedEvalContextCopy()
				if result == nil {
					t.Errorf("ExtendedEvalContextCopy() = nil, want non-nil")
				}
			},
		},
		{
			name: "EvalContext",
			test: func(t *testing.T, p *planner) {
				result := p.EvalContext()
				if result == nil {
					t.Logf("EvalContext() returned nil as expected")
				}
			},
		},
		{
			name: "Tables",
			test: func(t *testing.T, p *planner) {
				result := p.Tables()
				if result != nil {
					t.Errorf("Tables() = %v, want nil", result)
				}
			},
		},
		{
			name: "GetStmt",
			test: func(t *testing.T, p *planner) {
				result := p.GetStmt()
				if result != "" {
					t.Errorf("GetStmt() = %v, want empty string", result)
				}
			},
		},
		{
			name: "ExecCfg",
			test: func(t *testing.T, p *planner) {
				result := p.ExecCfg()
				if result != nil {
					t.Errorf("ExecCfg() = %v, want nil", result)
				}
			},
		},
		//{
		//	name: "LeaseMgr",
		//	test: func(t *testing.T, p *planner) {
		//		result := p.LeaseMgr()
		//		if result != nil {
		//			t.Errorf("LeaseMgr() = %v, want nil", result)
		//		}
		//	},
		//},
		{
			name: "Txn",
			test: func(t *testing.T, p *planner) {
				result := p.Txn()
				if result != nil {
					t.Errorf("Txn() = %v, want nil", result)
				}
			},
		},
		{
			name: "DistSQLPlanner",
			test: func(t *testing.T, p *planner) {
				result := p.DistSQLPlanner()
				if result != nil {
					t.Errorf("DistSQLPlanner() = %v, want nil", result)
				}
			},
		},
		{
			name: "GetNodeIDNumber",
			test: func(t *testing.T, p *planner) {
				result := p.GetNodeIDNumber()
				if result != 0 {
					t.Errorf("GetNodeIDNumber() = %v, want 0", result)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &planner{}
			tt.test(t, p)
		})
	}
}
