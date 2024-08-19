// Copyright 2020 The Cockroach Authors.
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
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
)

const substringTmpl = "pkg/sql/colexec/substring_tmpl.go"

func genSubstring(wr io.Writer) error {
	t, err := ioutil.ReadFile(substringTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_StartType_T", "coltypes.{{$startType}}", -1)
	s = strings.Replace(s, "_LengthType_T", "coltypes.{{$lengthType}}", -1)
	s = strings.Replace(s, "_StartType", "{{$startType}}", -1)
	s = strings.Replace(s, "_LengthType", "{{$lengthType}}", -1)

	tmpl, err := template.New("substring").Parse(s)
	if err != nil {
		return err
	}

	intToInts := make(map[coltypes.T][]coltypes.T)
	for _, intType := range coltypes.IntTypes {
		intToInts[intType] = coltypes.IntTypes
	}
	return tmpl.Execute(wr, intToInts)
}

func init() {
	registerGenerator(genSubstring, "substring.eg.go", substringTmpl)
}