// Copyright 2020 The Cockroach Authors.
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

/*
Package generic provides an implementation of a generic immutable interval
B-Tree.

The package uses code generation to create type-safe, zero-allocation
specializations of the ordered tree structure.

# Usage

Users of the package should follow these steps:

 1. Define a type that will be used to parameterize the generic tree structure.
 2. Ensure that the parameter type fulfills the type contract defined in
    internal/contract.go.
 3. Include a go generate declaration that invokes the gen.sh script with the
    type name as the first argument and the package name as the second argument.
 4. Invoke go generate.

# Example

1. The latch type is defined:

	type latch struct {
	    id         uint64
	    span       roachpb.Span
	    ts         hlc.Timestamp
	    done       *signal
	    next, prev *latch // readSet linked-list.
	}

2. Methods are defined to fulfill the type contract.

	func (la *latch) ID() uint64         { return la.id }
	func (la *latch) Key() []byte        { return la.span.Key }
	func (la *latch) EndKey() []byte     { return la.span.EndKey }
	func (la *latch) String() string     { return fmt.Sprintf("%s@%s", la.span, la.ts) }
	func (la *latch) SetID(v uint64)     { la.id = v }
	func (la *latch) SetKey(v []byte)    { la.span.Key = v }
	func (la *latch) SetEndKey(v []byte) { la.span.EndKey = v }

3. The following comment is added near the declaration of the latch type:

	//go:generate ../../util/interval/generic/gen.sh *latch spanlatch

4. Invoking go generate results in the creation of the following files:

  - latch_interval_btree.go
  - latch_interval_btree_test.go

# Working Example

See example_t.go for a working example. Running go generate on this package
generates:
  - example_interval_btree.go
  - example_interval_btree_test.go
*/
package generic
