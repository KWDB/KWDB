// Copyright 2018 The Cockroach Authors.
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
	"strings"
)

// matchWriter keeps track of the indentation level so that callers can nest
// and unnest code in whatever pattern they choose.
type matchWriter struct {
	writer  io.Writer
	nesting int
}

// marker opaquely stores a nesting level. The nest methods return the marker
// for the nesting level that existed before the nest call, and the
// unnestToMarker method returns to that level given the marker.
type marker int

// nest writes a formatted string with no identation and then increases the
// indentation. It returns a marker for the indentation level before the
// increase. This marker can be passed to unnestToMarker to return to that
// level.
func (w *matchWriter) nest(format string, args ...interface{}) marker {
	w.write(format, args...)
	w.nesting++
	return marker(w.nesting - 1)
}

// nestIndent writes an indented formatted string and then increases the
// indentation. It returns a marker for the indentation level before the
// increase. This marker can be passed to unnestToMarker to return to that
// level.
func (w *matchWriter) nestIndent(format string, args ...interface{}) marker {
	w.writeIndent(format, args...)
	w.nesting++
	return marker(w.nesting - 1)
}

// marker returns the a marker for the current nesting level, which can be
// passed to unnestToMarker in order to return to this level.
func (w *matchWriter) marker() marker {
	return marker(w.nesting)
}

func (w *matchWriter) write(format string, args ...interface{}) {
	fmt.Fprintf(w.writer, format, args...)
}

func (w *matchWriter) writeIndent(format string, args ...interface{}) {
	fmt.Fprint(w.writer, strings.Repeat("  ", w.nesting))
	fmt.Fprintf(w.writer, format, args...)
}

func (w *matchWriter) newline() {
	fmt.Fprintf(w.writer, "\n")
}

func (w *matchWriter) unnest(suffix string) {
	w.unnestToMarker(marker(w.nesting-1), suffix)
}

func (w *matchWriter) unnestToMarker(marker marker, suffix string) {
	for w.nesting > int(marker) {
		w.nesting--
		fmt.Fprint(w.writer, strings.Repeat("  ", w.nesting))
		fmt.Fprint(w.writer, suffix)
	}
}
