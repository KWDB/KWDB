// Copyright 2018 The Cockroach Authors.
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

package constraint

// KeyExtension is an enumeration used when comparing keys. Key extensions
// specify whether each key is conceptually suffixed with a value that sorts
// before all values (ExtendLow) or with a value that sorts after all values
// (ExtendHigh). This allows span boundaries to be compared with one another.
// See the comment for the Key.Compare method for more details.
type KeyExtension bool

const (
	// ExtendLow specifies that the key is conceptually suffixed with a value
	// that sorts before all values for purposes of comparison.
	ExtendLow KeyExtension = false

	// ExtendHigh specifies that the key is conceptually suffixed with a value
	// that sorts after all values for purposes of comparison.
	ExtendHigh KeyExtension = true
)

const (
	// comparisonDirectionLess is returned when the extended key sorts before another.
	comparisonDirectionLess = -1
	// comparisonDirectionGreater is returned when the extended key sorts after another.
	comparisonDirectionGreater = 1
)

// ToCmp converts from a key extension value to a comparison value. ExtendLow
// maps to -1 because it sorts before all other values. ExtendHigh maps to 1
// because it sorts after all other values.
func (e KeyExtension) ToCmp() int {
	// Map ExtendLow into -1 and ExtendHigh into +1.
	if e == ExtendLow {
		return comparisonDirectionLess
	}
	return comparisonDirectionGreater
}

// isLow returns true if this extension sorts before all values (ExtendLow).
func (e KeyExtension) isLow() bool {
	return e == ExtendLow
}

// invert returns the opposite extension. ExtendLow becomes ExtendHigh and vice versa.
func (e KeyExtension) invert() KeyExtension {
	return !e
}
