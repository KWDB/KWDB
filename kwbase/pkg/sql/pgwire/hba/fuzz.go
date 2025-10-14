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

//go:build gofuzz
// +build gofuzz

package hba

import (
	"fmt"

	"github.com/kr/pretty"
)

func FuzzParseAndNormalize(data []byte) int {
	conf, err := ParseAndNormalize(string(data))
	if err != nil {
		return 0
	}
	s := conf.String()
	conf2, err := ParseAndNormalize(s)
	if err != nil {
		panic(fmt.Errorf(`-- original:
%s
-- parsed:
%# v
-- new:
%s
-- error:
%v`,
			string(data),
			pretty.Formatter(conf),
			s,
			err))
	}
	s2 := conf2.String()
	if s != s2 {
		panic(fmt.Errorf(`reparse mismatch:
-- original:
%s
-- new:
%# v
-- printed:
%s`,
			s,
			pretty.Formatter(conf2),
			s2))
	}
	return 1
}
