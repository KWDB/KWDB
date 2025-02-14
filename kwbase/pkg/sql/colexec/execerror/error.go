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

package execerror

import (
	"bufio"
	"context"
	"fmt"
	"runtime/debug"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/util/causer"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

const panicLineSubstring = "runtime/panic.go"

// CatchVectorizedRuntimeError executes operation, catches a runtime error if
// it is coming from the vectorized engine, and returns it. If an error not
// related to the vectorized engine occurs, it is not recovered from.
func CatchVectorizedRuntimeError(operation func()) (retErr error) {
	defer func() {
		panicObj := recover()
		if panicObj == nil {
			// No panic happened, so the operation must have been executed
			// successfully.
			return
		}

		// Find where the panic came from and only proceed if it is related to the
		// vectorized engine.
		stackTrace := string(debug.Stack())
		scanner := bufio.NewScanner(strings.NewReader(stackTrace))
		panicLineFound := false
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), panicLineSubstring) {
				panicLineFound = true
				break
			}
		}
		if !panicLineFound {
			panic(fmt.Sprintf("panic line %q not found in the stack trace\n%s", panicLineSubstring, stackTrace))
		}
		if !scanner.Scan() {
			panic(fmt.Sprintf("unexpectedly there is no line below the panic line in the stack trace\n%s", stackTrace))
		}
		panicEmittedFrom := strings.TrimSpace(scanner.Text())
		if !isPanicFromVectorizedEngine(panicEmittedFrom) {
			// Do not recover from the panic not related to the vectorized
			// engine.
			panic(panicObj)
		}

		err, ok := panicObj.(error)
		if !ok {
			// Not an error object. Definitely unexpected.
			retErr = errors.AssertionFailedf("unexpected error from the vectorized runtime: %+v", panicObj)
			return
		}
		retErr = err

		if _, ok := panicObj.(*StorageError); ok {
			// A StorageError was caused by something below SQL, and represents
			// an error that we'd simply like to propagate along.
			// Do nothing.
			return
		}

		annotateErrorWithoutCode := true
		var nvie *notVectorizedInternalError
		if errors.As(err, &nvie) {
			// A notVectorizedInternalError was not caused by the
			// vectorized engine and represents an error that we don't
			// want to annotate in case it doesn't have a valid PG code.
			annotateErrorWithoutCode = false
		}
		if code := pgerror.GetPGCode(err); annotateErrorWithoutCode && code == pgcode.Uncategorized {
			// Any error without a code already is "surprising" and
			// needs to be annotated to indicate that it was
			// unexpected.
			retErr = errors.NewAssertionErrorWithWrappedErrf(err, "unexpected error from the vectorized runtime")
		}
	}()
	operation()
	return retErr
}

const (
	colPackagePrefix          = "gitee.com/kwbasedb/kwbase/pkg/col"
	colcontainerPackagePrefix = "gitee.com/kwbasedb/kwbase/pkg/sql/colcontainer"
	colexecPackagePrefix      = "gitee.com/kwbasedb/kwbase/pkg/sql/colexec"
	colflowsetupPackagePrefix = "gitee.com/kwbasedb/kwbase/pkg/sql/colflow"
	execinfraPackagePrefix    = "gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	rowexecPackagePrefix      = "gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	treePackagePrefix         = "gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// isPanicFromVectorizedEngine checks whether the panic that was emitted from
// panicEmittedFrom line of code (which includes package name as well as the
// file name and the line number) came from the vectorized engine.
// panicEmittedFrom must be trimmed to not have any white spaces in the prefix.
func isPanicFromVectorizedEngine(panicEmittedFrom string) bool {
	const testExceptionPrefix = "gitee.com/kwbasedb/kwbase/pkg/sql/colflow_test.(*testNonVectorizedPanicEmitter)"
	if strings.HasPrefix(panicEmittedFrom, testExceptionPrefix) {
		// Although the panic appears to be coming from the vectorized engine, it
		// is intended to not be caught in order to test the panic propagation, so
		// we say that the panic is not from the vectorized engine.
		return false
	}
	return strings.HasPrefix(panicEmittedFrom, colPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, colcontainerPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, colexecPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, colflowsetupPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, execinfraPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, rowexecPackagePrefix) ||
		strings.HasPrefix(panicEmittedFrom, treePackagePrefix)
}

// StorageError is an error that was created by a component below the sql
// stack, such as the network or storage layers. A StorageError will be bubbled
// up all the way past the SQL layer unchanged.
type StorageError struct {
	error
}

// Cause implements the Causer interface.
func (s *StorageError) Cause() error {
	return s.error
}

// NewStorageError returns a new storage error. This can be used to propagate
// an error through the exec subsystem unchanged.
func NewStorageError(err error) *StorageError {
	return &StorageError{error: err}
}

// notVectorizedInternalError is an error that originated outside of the
// vectorized engine (for example, it was caused by a non-columnar builtin).
// notVectorizedInternalError will be returned to the client not as an
// "internal error" and without the stack trace.
type notVectorizedInternalError struct {
	cause error
}

func newNotVectorizedInternalError(err error) *notVectorizedInternalError {
	return &notVectorizedInternalError{cause: err}
}

var (
	_ causer.Causer  = &notVectorizedInternalError{}
	_ errors.Wrapper = &notVectorizedInternalError{}
)

func (e *notVectorizedInternalError) Error() string {
	return e.cause.Error()
}

func (e *notVectorizedInternalError) Cause() error {
	return e.cause
}

func (e *notVectorizedInternalError) Unwrap() error {
	return e.Cause()
}

func decodeNotVectorizedInternalError(
	_ context.Context, cause error, _ string, _ []string, _ proto.Message,
) error {
	return newNotVectorizedInternalError(cause)
}

func init() {
	errors.RegisterWrapperDecoder(errors.GetTypeKey((*notVectorizedInternalError)(nil)), decodeNotVectorizedInternalError)
}

// VectorizedInternalPanic simply panics with the provided object. It will
// always be returned as internal error to the client with the corresponding
// stack trace. This method should be called to propagate all *unexpected*
// errors that originated within the vectorized engine.
func VectorizedInternalPanic(err interface{}) {
	panic(err)
}

// VectorizedExpectedInternalPanic is the same as NonVectorizedPanic. It should
// be called to propagate all *expected* errors that originated within the
// vectorized engine.
func VectorizedExpectedInternalPanic(err error) {
	NonVectorizedPanic(err)
}

// NonVectorizedPanic panics with the error that is wrapped by
// notVectorizedInternalError which will not be treated as internal error and
// will not have a printed out stack trace. This method should be called to
// propagate all errors that originated outside of the vectorized engine and
// all expected errors from the vectorized engine.
func NonVectorizedPanic(err error) {
	panic(newNotVectorizedInternalError(err))
}
