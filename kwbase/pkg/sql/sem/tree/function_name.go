// Copyright 2016 The Cockroach Authors.
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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Function names are used in expressions in the FuncExpr node.
// General syntax:
//    [ <context-prefix> . ] <function-name>
//
// The other syntax nodes hold a mutable ResolvableFunctionReference
// attribute.  This is populated during parsing with an
// UnresolvedName, and gets assigned a FunctionDefinition upon the
// first call to its Resolve() method.

// ResolvableFunctionReference implements the editable reference cell
// of a FuncExpr. The FunctionRerence is updated by the Normalize()
// method.
type ResolvableFunctionReference struct {
	FunctionReference
}

// Format implements the NodeFormatter interface.
func (fn *ResolvableFunctionReference) Format(ctx *FmtCtx) {
	ctx.FormatNode(fn.FunctionReference)
}
func (fn *ResolvableFunctionReference) String() string { return AsString(fn) }

// Resolve checks if the function name is already resolved and
// resolves it as necessary.
func (fn *ResolvableFunctionReference) Resolve(
	searchPath sessiondata.SearchPath,
) (*FunctionDefinition, error) {
	switch t := fn.FunctionReference.(type) {
	case *FunctionDefinition:
		return t, nil
	case *UnresolvedName:
		fd, err := t.ResolveFunction(searchPath)
		if err != nil {
			return nil, err
		}
		fn.FunctionReference = fd
		return fd, nil
	default:
		return nil, errors.AssertionFailedf("unknown function name type: %+v (%T)",
			fn.FunctionReference, fn.FunctionReference,
		)
	}
}

// WrapFunction creates a new ResolvableFunctionReference
// holding a pre-resolved function. Helper for grammar rules.
func WrapFunction(n string) ResolvableFunctionReference {
	if fd, ok := FunDefs[n]; ok {
		return ResolvableFunctionReference{fd}
	}
	if fd, isUdf := ConcurrentFunDefs.LookupFunc(n); isUdf {
		return ResolvableFunctionReference{fd}
	}
	panic(errors.AssertionFailedf("function %s() not defined", log.Safe(n)))
}

// FunctionReference is the common interface to UnresolvedName and QualifiedFunctionName.
type FunctionReference interface {
	fmt.Stringer
	NodeFormatter
	functionReference()
	FunctionName() string
}

func (*UnresolvedName) functionReference()     {}
func (*FunctionDefinition) functionReference() {}

const (
	// FuncTimeBucket represents string of "time_bucket"
	FuncTimeBucket = "time_bucket"
	// FuncMatching represents string of "matching"
	FuncMatching = "matching"
	// FuncBucketFill represents string of "time_bucket_gapfill_internal"
	FuncBucketFill = "time_bucket_gapfill_internal"
	// FuncInterpolate represents string of "interpolate"
	FuncInterpolate = "interpolate"
	// FunDiff represents string of "diff"
	FunDiff = "diff"
)

// FunctionName return name of func.
func (n *UnresolvedName) FunctionName() string { return n.Parts[0] }

// FunctionName return name of func.
func (n *FunctionDefinition) FunctionName() string { return n.Name }
