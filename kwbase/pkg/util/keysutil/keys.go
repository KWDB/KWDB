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

package keysutil

import (
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"github.com/pkg/errors"
)

// PrettyScanner implements  a partial right inverse to keys.PrettyPrint(): it
// takes a key formatted for human consumption and attempts to translate it into
// a roachpb.Key. Not all key types are supported, but a function for decoding
// the SQL table space can be provided (to replace the weak default one).
//
// No optimization has been performed. This is intended for use in debugging and
// tests only.
type PrettyScanner struct {
	// keyComprehension contains pointers to scanner routines for pretty-printed
	// keys from different regions of the key space.
	keyComprehension keys.KeyComprehensionTable
	// validateRoundTrip, if set,
	// makes the scanner validate that calling PrettyPrint on the result yields
	// the scan's input.
	validateRoundTrip bool
}

// MakePrettyScanner creates a PrettyScanner.
//
// If tableParser is not nil, it will replace the default function for scanning
// pretty-printed keys from the table part of the keys space (i.e. inputs
// starting with "/Table"). The supplied function needs to parse the part that
// comes after "/Table".
func MakePrettyScanner(tableParser keys.KeyParserFunc) PrettyScanner {
	dict := keys.KeyDict
	if tableParser != nil {
		dict = customizeKeyComprehension(dict, tableParser)
	}
	return PrettyScanner{
		keyComprehension: dict,
		// If we specified a custom parser, forget about the roundtrip.
		validateRoundTrip: tableParser == nil,
	}
}

// customizeKeyComprehension takes as input a KeyComprehensionTable and
// overwrites the "pretty scanner" function for the tables key space (i.e. for
// keys starting with "/Table"). The modified table is returned.
func customizeKeyComprehension(
	table keys.KeyComprehensionTable, tableParser keys.KeyParserFunc,
) keys.KeyComprehensionTable {
	// Make a deep copy of the table.
	cpy := make(keys.KeyComprehensionTable, len(table))
	copy(cpy, table)
	for i := range table {
		cpy[i].Entries = make([]keys.DictEntry, len(cpy[i].Entries))
		copy(cpy[i].Entries, table[i].Entries)
	}
	table = cpy

	// Find the part of the table that deals with parsing table data.
	// We'll perform surgery on it to apply `tableParser`.
	for i := range table {
		region := &table[i]
		if region.Name == "/Table" {
			if len(region.Entries) != 1 {
				panic(fmt.Sprintf("expected a single entry under \"/Table\", got: %d", len(region.Entries)))
			}
			subRegion := &region.Entries[0]
			subRegion.PSFunc = tableParser
			return table
		}
	}
	panic("failed to find required \"/Table\" entry")
}

// Scan is a partial right inverse to PrettyPrint: it takes a key formatted for
// human consumption and attempts to translate it into a roachpb.Key. Not all
// key types are supported and no optimization has been performed. This is
// intended for use in debugging and tests only.
func (s PrettyScanner) Scan(input string) (_ roachpb.Key, rErr error) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				rErr = err
				return
			}
			rErr = errors.Errorf("%v", r)
		}
	}()

	origInput := input
	var output roachpb.Key

	mkErr := func(err error) (roachpb.Key, error) {
		if err == nil {
			err = errIllegalInput
		}
		err = errors.Errorf(`can't parse "%s" after reading %s: %s`,
			input, origInput[:len(origInput)-len(input)], err)
		return nil, &keys.ErrUglifyUnsupported{Wrapped: err}
	}

	var entries []keys.DictEntry // nil if not pinned to a subrange
outer:
	for len(input) > 0 {
		if entries != nil {
			for _, v := range entries {
				if strings.HasPrefix(input, v.Name) {
					input = input[len(v.Name):]
					if v.PSFunc == nil {
						return mkErr(nil)
					}
					remainder, key := v.PSFunc(input)
					input = remainder
					output = append(output, key...)
					entries = nil
					continue outer
				}
			}
			return nil, &keys.ErrUglifyUnsupported{
				Wrapped: errors.New("known key, but unsupported subtype"),
			}
		}
		for _, v := range keys.ConstKeyDict {
			if strings.HasPrefix(input, v.Name) {
				output = append(output, v.Value...)
				input = input[len(v.Name):]
				continue outer
			}
		}
		for _, v := range s.keyComprehension {
			if strings.HasPrefix(input, v.Name) {
				// No appending to output yet, the dictionary will take care of
				// it.
				input = input[len(v.Name):]
				entries = v.Entries
				continue outer
			}
		}
		return mkErr(errors.New("can't handle key"))
	}
	if s.validateRoundTrip {
		if out := keys.PrettyPrint(nil /* valDirs */, output); out != origInput {
			return nil, errors.Errorf("constructed key deviates from original: %s vs %s", out, origInput)
		}
	}
	return output, nil
}

var errIllegalInput = errors.New("illegal input")
