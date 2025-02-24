// Copyright 2019 The Cockroach Authors.
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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestBufferOp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	inputTuples := tuples{{int64(1)}, {int64(2)}, {int64(3)}}
	input := newOpTestInput(coldata.BatchSize(), inputTuples, []coltypes.T{coltypes.Int64})
	buffer := NewBufferOp(input).(*bufferOp)
	buffer.Init()

	t.Run("TestBufferReturnsInputCorrectly", func(t *testing.T) {
		buffer.advance(ctx)
		b := buffer.Next(ctx)
		require.Nil(t, b.Selection())
		require.Equal(t, len(inputTuples), b.Length())
		for i, val := range inputTuples {
			require.Equal(t, val[0], b.ColVec(0).Int64()[i])
		}

		// We've read over the batch, so we now should get a zero-length batch.
		b = buffer.Next(ctx)
		require.Nil(t, b.Selection())
		require.Equal(t, 0, b.Length())
	})
}
