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

// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

package csv

import (
	"bufio"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

// A Writer writes records to a CSV encoded file.
//
// As returned by NewWriter, a Writer writes records terminated by a
// newline and uses ',' as the field delimiter. The exported fields can be
// changed to customize the details before the first call to Write or WriteAll.
//
// Comma is the field delimiter.
//
// If UseCRLF is true, the Writer ends each record with \r\n instead of \n.
type Writer struct {
	Comma    rune // Field delimiter (set to ',' by NewWriter)
	UseCRLF  bool // True to use \r\n as the line terminator
	w        *bufio.Writer
	Enclosed rune
	Escaped  rune
	Nullas   string
	IsSQL    bool // True to use '' as the line terminator
}

// NewWriter returns a new Writer that writes to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		Comma:    ',',
		Enclosed: '"',
		Escaped:  '"',
		w:        bufio.NewWriter(w),
	}
}

// GetBufio Return a bufio.Writer
func (w *Writer) GetBufio() *bufio.Writer {
	return w.w
}

// WriteSQL writes a single SQL record to w along with any necessary quoting.
// A record is a slice of strings with each string being one field.
func (w *Writer) WriteSQL(record []string, nilMap map[int]struct{}) error {
	for n, field := range record {
		if n > 0 {
			if _, err := w.w.WriteRune(','); err != nil {
				return err
			}
		}
		if nilMap != nil {
			// this means filed is a null value, continue this filed.
			if _, ok := nilMap[n]; ok {
				if _, err := w.w.WriteString("NULL"); err != nil {
					return err
				}
				continue
			}
		}
		if _, err := w.w.WriteString(field); err != nil {
			return err
		}
	}
	return nil
}

// Writer writes a single CSV record to w along with any necessary quoting.
// A record is a slice of strings with each string being one field.
func (w *Writer) Write(record []string, nilMap map[int]struct{}) error {
	if !validDelim(w.Comma) {
		return errInvalidDelim
	}

	for n, field := range record {
		if n > 0 {
			if _, err := w.w.WriteRune(w.Comma); err != nil {
				return err
			}
		}
		if nilMap != nil {
			// this means filed is a null value, continue this filed.
			if _, ok := nilMap[n]; ok {
				if w.Nullas != "" {
					if _, err := w.w.WriteString(w.Nullas); err != nil {
						return err
					}
				}
				continue
			}
		}
		// If we don't have to have a quoted field then just
		// write out the field and continue to the next field.
		if !w.fieldNeedsQuotes(field) {
			if w.Nullas != "" && field == w.Nullas {
				field = string(w.Enclosed) + field + string(w.Enclosed)
				if _, err := w.w.WriteString(field); err != nil {
					return err
				}
			} else if _, err := w.w.WriteString(field); err != nil {
				return err
			}
			continue
		}
		if err := w.w.WriteByte(byte(w.Enclosed)); err != nil {
			return err
		}

		for _, r1 := range field {
			var err error
			switch r1 {
			case w.Enclosed:
				err = w.w.WriteByte(byte(w.Escaped))
				if err != nil {
					return err
				}
				err = w.w.WriteByte(byte(w.Enclosed))
			case w.Escaped:
				err = w.w.WriteByte(byte(w.Escaped))
				if err != nil {
					return err
				}
				err = w.w.WriteByte(byte(w.Escaped))
			case '\r':
				if !w.UseCRLF {
					err = w.w.WriteByte('\r')
				}
			case '\n':
				if w.UseCRLF {
					_, err = w.w.WriteString("\r\n")
				} else {
					err = w.w.WriteByte('\n')
				}
			default:
				_, err = w.w.WriteRune(r1)
			}
			if err != nil {
				return err
			}
		}

		if err := w.w.WriteByte(byte(w.Enclosed)); err != nil {
			return err
		}
	}
	var err error
	if w.IsSQL {
		return err
	}
	if w.UseCRLF {
		_, err = w.w.WriteString("\r\n")
	} else {
		err = w.w.WriteByte('\n')
	}
	return err
}

// Flush writes any buffered data to the underlying io.Writer.
// To check if an error occurred during the Flush, call Error.
func (w *Writer) Flush() {
	w.w.Flush()
}

// Error reports any error that has occurred during a previous Write or Flush.
func (w *Writer) Error() error {
	_, err := w.w.Write(nil)
	return err
}

// WriteAll writes multiple CSV records to w using Write and then calls Flush.
func (w *Writer) WriteAll(records [][]string) error {
	for _, record := range records {
		err := w.Write(record, nil)
		if err != nil {
			return err
		}
	}
	return w.w.Flush()
}

// fieldNeedsQuotes reports whether our field must be enclosed in quotes.
// Fields with a Comma, fields with a quote or newline, and
// fields which start with a space must be enclosed in quotes.
// We used to quote empty strings, but we do not anymore (as of Go 1.4).
// The two representations should be equivalent, but Postgres distinguishes
// quoted vs non-quoted empty string during database imports, and it has
// an option to force the quoted behavior for non-quoted CSV but it has
// no option to force the non-quoted behavior for quoted CSV, making
// CSV with quoted empty strings strictly less useful.
// Not quoting the empty string also makes this package match the behavior
// of Microsoft Excel and Google Drive.
// For Postgres, quote the data terminating string `\.`.
func (w *Writer) fieldNeedsQuotes(field string) bool {
	// if field is "" ,also write it to buf,
	// this is done to distinguish between null values and empty strings.
	if field == "" || field == `\.` ||
		strings.ContainsRune(field, w.Comma) || strings.ContainsRune(field, w.Enclosed) || strings.ContainsRune(field, w.Escaped) ||
		strings.ContainsAny(field, "\"\r\n") {
		return true
	}

	r1, _ := utf8.DecodeRuneInString(field)
	return unicode.IsSpace(r1)
}
