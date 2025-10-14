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

package colexec

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
)

// limitOp is an operator that implements limit, returning only the first n
// tuples from its input.
type limitOp struct {
	OneInputNode
	closerHelper

	limit int

	// seen is the number of tuples seen so far.
	seen int
	// done is true if the limit has been reached.
	done bool
}

var _ Operator = &limitOp{}
var _ closableOperator = &limitOp{}

// NewLimitOp returns a new limit operator with the given limit.
func NewLimitOp(input Operator, limit int) Operator {
	c := &limitOp{
		OneInputNode: NewOneInputNode(input),
		limit:        limit,
	}
	return c
}

func (c *limitOp) Init() {
	c.input.Init()
}

func (c *limitOp) Next(ctx context.Context) coldata.Batch {
	if c.done {
		return coldata.ZeroBatch
	}
	bat := c.input.Next(ctx)
	length := bat.Length()
	if length == 0 {
		return bat
	}
	newSeen := c.seen + length
	if newSeen >= c.limit {
		c.done = true
		bat.SetLength(c.limit - c.seen)
		return bat
	}
	c.seen = newSeen
	return bat
}

// Close closes the limitOp's input.
// TODO(asubiotto): Remove this method. It only exists so that we can call Close
//
//	from some runTests subtests when not draining the input fully. The test
//	should pass in the testing.T object used so that the caller can decide to
//	explicitly close the input after checking the test.
func (c *limitOp) IdempotentClose(ctx context.Context) error {
	if !c.close() {
		return nil
	}
	if closer, ok := c.input.(IdempotentCloser); ok {
		return closer.IdempotentClose(ctx)
	}
	return nil
}
