// Copyright 2020 The Cockroach Authors.
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

package tree

import "github.com/cockroachdb/errors"

// Volatility indicates whether the result of a function is dependent *only*
// on the values of its explicit arguments, or can change due to outside factors
// (such as parameter variables or table contents).
//
// The values are ordered with smaller values being strictly more restrictive
// than larger values.
//
// NOTE: functions having side-effects, such as setval(),
// must be labeled volatile to ensure they will not get optimized away,
// even if the actual return value is not changeable.
type Volatility int8

const (
	// VolatilityLeakProof means that the operator cannot modify the database, the
	// transaction state, or any other state. It cannot depend on configuration
	// settings and is guaranteed to return the same results given the same
	// arguments in any context. In addition, no information about the arguments
	// is conveyed except via the return value. Any function that might throw an
	// error depending on the values of its arguments is not leak-proof.
	//
	// USE THIS WITH CAUTION! The optimizer might call operators that are leak
	// proof on inputs that they wouldn't normally be called on (e.g. pulling
	// expressions out of a CASE). In the future, they may even run on rows that
	// the user doesn't have permission to access.
	//
	// Note: VolatilityLeakProof is strictly stronger than VolatilityImmutable. In
	// principle it could be possible to have leak-proof stable or volatile
	// functions (perhaps now()); but this is not useful in practice as very few
	// operators are marked leak-proof.
	// Examples: integer comparison.
	VolatilityLeakProof Volatility = 1 + iota
	// VolatilityImmutable means that the operator cannot modify the database, the
	// transaction state, or any other state. It cannot depend on configuration
	// settings and is guaranteed to return the same results given the same
	// arguments in any context. ImmutableCopy operators can be constant folded.
	// Examples: log, from_json.
	VolatilityImmutable
	// VolatilityStable means that the operator cannot modify the database or the
	// transaction state and is guaranteed to return the same results given the
	// same arguments whenever it is evaluated within the same statement. Multiple
	// calls to a stable operator can be optimized to a single call.
	// Examples: current_timestamp, current_date.
	VolatilityStable
	// VolatilityVolatile means that the operator can do anything, including
	// modifying database state.
	// Examples: random, crdb_internal.force_error, nextval.
	VolatilityVolatile
)

// String returns the byte representation of Volatility as a string.
func (v Volatility) String() string {
	switch v {
	case VolatilityLeakProof:
		return "leak-proof"
	case VolatilityImmutable:
		return "immutable"
	case VolatilityStable:
		return "stable"
	case VolatilityVolatile:
		return "volatile"
	default:
		return "invalid"
	}
}

// ToPostgres returns the postgres "provolatile" string ("i" or "s" or "v") and
// the "proleakproof" flag.
func (v Volatility) ToPostgres() (provolatile string, proleakproof bool) {
	switch v {
	case VolatilityLeakProof:
		return "i", true
	case VolatilityImmutable:
		return "i", false
	case VolatilityStable:
		return "s", false
	case VolatilityVolatile:
		return "v", false
	default:
		panic(errors.AssertionFailedf("invalid volatility %s", v))
	}
}

// VolatilityFromPostgres returns a Volatility that matches the postgres
// provolatile/proleakproof settings.
func VolatilityFromPostgres(provolatile string, proleakproof bool) (Volatility, error) {
	switch provolatile {
	case "i":
		if proleakproof {
			return VolatilityLeakProof, nil
		}
		return VolatilityImmutable, nil
	case "s":
		return VolatilityStable, nil
	case "v":
		return VolatilityVolatile, nil
	default:
		return 0, errors.AssertionFailedf("invalid provolatile %s", provolatile)
	}
}
