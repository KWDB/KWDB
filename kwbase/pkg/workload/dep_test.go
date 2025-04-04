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

package workload

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/testutils/buildutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestDepAllowlist(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We want workload to be lightweight.
	buildutil.VerifyTransitiveWhitelist(t, "gitee.com/kwbasedb/kwbase/pkg/workload",
		[]string{
			`gitee.com/kwbasedb/kwbase/pkg/build`,
			`gitee.com/kwbasedb/kwbase/pkg/col/coldata`,
			`gitee.com/kwbasedb/kwbase/pkg/col/coltypes`,
			`gitee.com/kwbasedb/kwbase/pkg/col/typeconv`,
			`gitee.com/kwbasedb/kwbase/pkg/docs`,
			`gitee.com/kwbasedb/kwbase/pkg/geo/geopb`,
			`gitee.com/kwbasedb/kwbase/pkg/sql/lex`,
			`gitee.com/kwbasedb/kwbase/pkg/sql/oidext`,
			`gitee.com/kwbasedb/kwbase/pkg/sql/types`,
			`gitee.com/kwbasedb/kwbase/pkg/util`,
			`gitee.com/kwbasedb/kwbase/pkg/util/arith`,
			`gitee.com/kwbasedb/kwbase/pkg/util/bufalloc`,
			`gitee.com/kwbasedb/kwbase/pkg/util/duration`,
			`gitee.com/kwbasedb/kwbase/pkg/util/encoding/csv`,
			`gitee.com/kwbasedb/kwbase/pkg/util/envutil`,
			`gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented`,
			`gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil`,
			`gitee.com/kwbasedb/kwbase/pkg/util/protoutil`,
			`gitee.com/kwbasedb/kwbase/pkg/util/randutil`,
			`gitee.com/kwbasedb/kwbase/pkg/util/stacktrace`,
			`gitee.com/kwbasedb/kwbase/pkg/util/stringencoding`,
			`gitee.com/kwbasedb/kwbase/pkg/util/syncutil`,
			`gitee.com/kwbasedb/kwbase/pkg/util/timeutil`,
			`gitee.com/kwbasedb/kwbase/pkg/util/uint128`,
			`gitee.com/kwbasedb/kwbase/pkg/util/uuid`,
			`gitee.com/kwbasedb/kwbase/pkg/util/version`,
			`gitee.com/kwbasedb/kwbase/pkg/workload/histogram`,
			// TODO(dan): These really shouldn't be used in util packages, but the
			// payoff of fixing it is not worth it right now.
			`gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode`,
			`gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror`,
		},
	)
}
