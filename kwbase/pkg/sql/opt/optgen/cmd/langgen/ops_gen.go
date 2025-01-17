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

package main

import (
	"fmt"
	"io"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optgen/lang"
)

// generateOps generates the Operator type and enumeration for the Optgen
// AST nodes.
func generateOps(compiled *lang.CompiledExpr, w io.Writer) {
	fmt.Fprintf(w, "type Operator int\n\n")

	fmt.Fprintf(w, "const (\n")
	fmt.Fprintf(w, "  UnknownOp Operator = iota\n\n")

	for _, define := range compiled.Defines {
		fmt.Fprintf(w, "  %sOp\n", define.Name)
	}

	fmt.Fprintf(w, ")\n\n")
}
