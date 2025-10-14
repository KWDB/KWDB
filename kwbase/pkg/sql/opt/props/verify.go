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

//go:build race
// +build race

package props

import (
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Verify runs consistency checks against the shared properties, in order to
// ensure that they conform to several invariants:
//
//  1. The properties must have been built.
//  2. If HasCorrelatedSubquery is true, then HasSubquery must be true as well.
//  3. If Mutate is true, then CanHaveSideEffects must also be true.
func (s *Shared) Verify() {
	if !s.Populated {
		panic(errors.AssertionFailedf("properties are not populated"))
	}
	if s.HasCorrelatedSubquery && !s.HasSubquery {
		panic(errors.AssertionFailedf("HasSubquery cannot be false if HasCorrelatedSubquery is true"))
	}
	if s.CanMutate && !s.CanHaveSideEffects {
		panic(errors.AssertionFailedf("CanHaveSideEffects cannot be false if CanMutate is true"))
	}
}

// Verify runs consistency checks against the relational properties, in order to
// ensure that they conform to several invariants:
//
//  1. Functional dependencies are internally consistent.
//  2. Not null columns are a subset of output columns.
//  3. Outer columns do not intersect output columns.
//  4. If functional dependencies indicate that the relation can have at most
//     one row, then the cardinality reflects that as well.
func (r *Relational) Verify() {
	r.Shared.Verify()
	r.FuncDeps.Verify()

	if !r.NotNullCols.SubsetOf(r.OutputCols) {
		panic(errors.AssertionFailedf("not null cols %s not a subset of output cols %s",
			log.Safe(r.NotNullCols), log.Safe(r.OutputCols)))
	}
	if r.OuterCols.Intersects(r.OutputCols) {
		panic(errors.AssertionFailedf("outer cols %s intersect output cols %s",
			log.Safe(r.OuterCols), log.Safe(r.OutputCols)))
	}
	if r.FuncDeps.HasMax1Row() {
		if r.Cardinality.Max > 1 {
			panic(errors.AssertionFailedf(
				"max cardinality must be <= 1 if FDs have max 1 row: %s", r.Cardinality))
		}
	}
	if r.IsAvailable(PruneCols) {
		if !r.Rule.PruneCols.SubsetOf(r.OutputCols) {
			panic(errors.AssertionFailedf("prune cols %s must be a subset of output cols %s",
				log.Safe(r.Rule.PruneCols), log.Safe(r.OutputCols)))
		}
	}
}

// VerifyAgainst checks that the two properties don't contradict each other.
// Used for testing (e.g. to cross-check derived properties from expressions in
// the same group).
func (r *Relational) VerifyAgainst(other *Relational) {
	if !r.OutputCols.Equals(other.OutputCols) {
		panic(errors.AssertionFailedf("output cols mismatch: %s vs %s", log.Safe(r.OutputCols), log.Safe(other.OutputCols)))
	}

	if r.Cardinality.Max < other.Cardinality.Min ||
		r.Cardinality.Min > other.Cardinality.Max {
		panic(errors.AssertionFailedf("cardinality mismatch: %s vs %s", log.Safe(r.Cardinality), log.Safe(other.Cardinality)))
	}

	// NotNullCols, FuncDeps are best effort, so they might differ.
	// OuterCols, CanHaveSideEffects, and HasPlaceholder might differ if a
	// subexpression containing them was elided.
}

// Verify runs consistency checks against the relational properties, in order to
// ensure that they conform to several invariants:
//
//  1. Functional dependencies are internally consistent.
func (s *Scalar) Verify() {
	s.Shared.Verify()
	s.FuncDeps.Verify()
}
