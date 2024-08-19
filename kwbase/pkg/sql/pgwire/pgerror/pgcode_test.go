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

package pgerror_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/testutils"
)

func TestPGCode(t *testing.T) {
	tt := testutils.T{T: t}

	testData := []struct {
		outerCode    string
		innerCode    string
		innerErr     error
		expectedCode string
	}{
		{"foo", "bar", errors.New("world"), "bar"},
		{"foo", pgcode.Uncategorized, errors.New("world"), "foo"},
		{pgcode.Uncategorized, "foo", errors.New("world"), "foo"},
		{"foo", "bar", errors.WithAssertionFailure(errors.New("world")), pgcode.Internal},
		{"foo", "bar", errors.UnimplementedError(errors.IssueLink{}, "world"), pgcode.FeatureNotSupported},
		{"foo", pgcode.Internal, errors.New("world"), pgcode.Internal},
		{pgcode.Internal, "foo", errors.New("world"), pgcode.Internal},
	}

	for _, t := range testData {
		tt.Run(fmt.Sprintf("%s/%s/%s", t.outerCode, t.innerCode, t.innerErr),
			func(tt testutils.T) {
				origErr := t.innerErr
				origErr = pgerror.WithCandidateCode(origErr, t.innerCode)
				origErr = pgerror.WithCandidateCode(origErr, t.outerCode)

				theTest := func(tt testutils.T, err error) {
					tt.Check(errors.Is(err, t.innerErr))
					tt.CheckEqual(err.Error(), t.innerErr.Error())

					tt.Check(pgerror.HasCandidateCode(err))
					if _, ok := errors.If(err, func(err error) (interface{}, bool) { return nil, pgerror.IsCandidateCode(err) }); !ok {
						tt.Error("woops")
					}

					code := pgerror.GetPGCodeInternal(err, pgerror.ComputeDefaultCode)
					tt.CheckEqual(code, t.expectedCode)

					errV := fmt.Sprintf("%+v", err)
					tt.Check(strings.Contains(errV, "code: "+t.innerCode))
					tt.Check(strings.Contains(errV, "code: "+t.outerCode))
				}

				tt.Run("local", func(tt testutils.T) { theTest(tt, origErr) })

				enc := errors.EncodeError(context.TODO(), origErr)
				newErr := errors.DecodeError(context.TODO(), enc)

				tt.Run("remote", func(tt testutils.T) { theTest(tt, newErr) })

			})
	}

}