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

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestAlterAuditFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		n        *AlterAudit
		expected string
	}{
		{
			n: &AlterAudit{
				Name:   Name("audit1"),
				Enable: true,
			},
			expected: "ALTER AUDIT audit1 ENABLE",
		},
		{
			n: &AlterAudit{
				Name:   Name("audit1"),
				Enable: false,
			},
			expected: "ALTER AUDIT audit1 DISABLE",
		},
		{
			n: &AlterAudit{
				Name:    Name("audit1"),
				NewName: Name("audit2"),
			},
			expected: "ALTER AUDIT audit1 RENAME TO audit2",
		},
		{
			n: &AlterAudit{
				Name:     Name("audit1"),
				Enable:   true,
				IfExists: true,
			},
			expected: "ALTER AUDIT IF EXISTS audit1 ENABLE",
		},
		{
			n: &AlterAudit{
				Name:     Name("audit1"),
				Enable:   false,
				IfExists: true,
			},
			expected: "ALTER AUDIT IF EXISTS audit1 DISABLE",
		},
		{
			n: &AlterAudit{
				Name:     Name("audit1"),
				NewName:  Name("audit2"),
				IfExists: true,
			},
			expected: "ALTER AUDIT IF EXISTS audit1 RENAME TO audit2",
		},
	}

	for i, tc := range testCases {
		ctx := NewFmtCtx(FmtSimple)
		tc.n.Format(ctx)
		result := ctx.CloseAndGetString()
		require.Equal(t, tc.expected, result, "test case %d", i)
	}
}
