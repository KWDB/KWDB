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

package testexpr

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
)

// Instance is a dummy RelExpr that contains various properties that can be
// extracted via that interface. It can be initialized with whatever subset of
// fields are required for the particular test; for example:
//
//	e := &testexpr.Instance{
//	  Rel: &props.Relational{...},
//	  Provided: &physical.Provided{...},
//	}
type Instance struct {
	Rel      *props.Relational
	Required *physical.Required
	Provided *physical.Provided
	Priv     interface{}

	// We embed a RelExpr to provide implementation for the unexported methods of
	// RelExpr. This should not be initialized (resulting in a panic if any of
	// those methods are called).
	memo.RelExpr
}

var _ memo.RelExpr = &Instance{}

// Relational is part of the RelExpr interface.
func (e *Instance) Relational() *props.Relational { return e.Rel }

// RequiredPhysical is part of the RelExpr interface.
func (e *Instance) RequiredPhysical() *physical.Required { return e.Required }

// ProvidedPhysical is part of the RelExpr interface.
func (e *Instance) ProvidedPhysical() *physical.Provided { return e.Provided }

// Private is part of the RelExpr interface.
func (e *Instance) Private() interface{} { return e.Priv }

// Op is part of the RelExpr interface.
func (e *Instance) Op() opt.Operator {
	// We implement this to keep checkExpr happy. It shouldn't match a real
	// operator.
	return 0xFFFF
}

// The rest of the methods are not implemented. Fields can be added to Instance
// to implement these as necessary.

// ChildCount is part of the RelExpr interface.
func (e *Instance) ChildCount() int { panic("not implemented") }

// Child is part of the RelExpr interface.
func (e *Instance) Child(nth int) opt.Expr { panic("not implemented") }

// String is part of the RelExpr interface.
func (e *Instance) String() string { panic("not implemented") }

// SetChild is part of the RelExpr interface.
func (e *Instance) SetChild(nth int, child opt.Expr) { panic("not implemented") }

// Memo is part of the RelExpr interface.
func (e *Instance) Memo() *memo.Memo { panic("not implemented") }

// FirstExpr is part of the RelExpr interface.
func (e *Instance) FirstExpr() memo.RelExpr { panic("not implemented") }

// NextExpr is part of the RelExpr interface.
func (e *Instance) NextExpr() memo.RelExpr { panic("not implemented") }

// Cost is part of the RelExpr interface.
func (e *Instance) Cost() memo.Cost { panic("not implemented") }

// Walk all interface
func (e *Instance) Walk(param opt.ExprParam) bool { panic("not implemented") }

// IsTSEngine get expr can push to ts engine
func (e *Instance) IsTSEngine() bool { return true }

// SetEngineTS set expr can push to ts engine
func (e *Instance) SetEngineTS() {}

// GetAddSynchronizer get expr can push to ts engine
func (e *Instance) GetAddSynchronizer() bool { return false }

// SetAddSynchronizer set expr add synchronizer
func (e *Instance) SetAddSynchronizer() {}
