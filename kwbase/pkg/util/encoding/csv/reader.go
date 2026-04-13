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

// Package csv reads and writes comma-separated values (CSV) files.
// There are many kinds of CSV files; this package supports the format
// described in RFC 4180.
//
// A csv file contains zero or more records of one or more fields per record.
// Each record is separated by the newline character. The final record may
// optionally be followed by a newline character.
//
//	field1,field2,field3
//
// White space is considered part of a field.
//
// Carriage returns before newline characters are silently removed.
//
// Blank lines are ignored. A line with only whitespace characters (excluding
// the ending newline character) is not considered a blank line.
//
// Fields which start and stop with the quote character " are called
// quoted-fields. The beginning and ending quote are not part of the
// field.
//
// The source:
//
//	normal string,"quoted-field"
//
// results in the fields
//
//	{`normal string`, `quoted-field`}
//
// Within a quoted-field a quote character followed by a second quote
// character is considered a single quote.
//
//	"the ""word"" is true","a ""quoted-field"""
//
// results in
//
//	{`the "word" is true`, `a "quoted-field"`}
//
// Newlines and commas may be included in a quoted-field
//
//	"Multi-line
//	field","comma is ,"
//
// results in
//
//	{`Multi-line
//	field`, `comma is ,`}
package csv

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Azure/go-autorest/autorest/to"
	"github.com/cockroachdb/errors"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/transform"
)

// A ParseError is returned for parsing errors.
// Line numbers are 1-indexed and columns are 0-indexed.
type ParseError struct {
	StartLine int   // Line where the record starts
	Line      int   // Line where the error occurred
	Column    int   // Column (rune index) where the error occurred
	Err       error // The actual error
}

var _ error = (*ParseError)(nil)
var _ fmt.Formatter = (*ParseError)(nil)
var _ errors.Formatter = (*ParseError)(nil)

// Error implements error.
func (e *ParseError) Error() string { return fmt.Sprintf("%v", e) }

// Cause implements causer.
func (e *ParseError) Cause() error { return e.Err }

// Format implements fmt.Formatter.
func (e *ParseError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// FormatError implements errors.Formatter.
func (e *ParseError) FormatError(p errors.Printer) error {
	if e.Err == ErrFieldCount {
		p.Printf("record on line %d", e.Line)
	} else if e.StartLine != e.Line {
		p.Printf("record on line %d; parse error on line %d, column %d", e.StartLine, e.Line, e.Column)
	} else {
		p.Printf("parse error on line %d, column %d", e.Line, e.Column)
	}
	return e.Err
}

// These are the errors that can be returned in ParseError.Err.
var (
	ErrBareQuote  = errors.New("bare \" in non-quoted-field")
	ErrQuote      = errors.New("extraneous or missing \" in quoted-field")
	ErrFieldCount = errors.New("wrong number of fields")
)

var errInvalidDelim = errors.New("csv: invalid field or comment delimiter")
var errInvalidEnclose = errors.New("csv: invalid field or comment enclose")
var errInvalidEscape = errors.New("csv: invalid field or comment escape")

func validDelim(r rune) bool {
	return r != 0 && r != '\r' && r != '\n' && utf8.ValidRune(r) && r != utf8.RuneError
}

func validEnclose(r rune) bool {
	return r != 0 && r != '"' && r != '\'' && utf8.ValidRune(r) && r != utf8.RuneError
}

func validEscape(r rune) bool {
	return r != 0 && r != '\\' && r != '"' && utf8.ValidRune(r) && r != utf8.RuneError
}

// A Reader reads records from a CSV-encoded file.
//
// As returned by NewReader, a Reader expects input conforming to RFC 4180.
// The exported fields can be changed to customize the details before the
// first call to Read or ReadAll.
type Reader struct {
	// Comma is the field delimiter.
	// It is set to comma (',') by NewReader.
	Comma rune
	// Escape is set by user, used to convert this item as '\\'
	Escape rune
	// Enclose is set by user, used to set dataItem's start && end
	Enclose rune
	// Comment, if not 0, is the comment character. Lines beginning with the
	// Comment character without preceding whitespace are ignored.
	// With leading whitespace the Comment character becomes part of the
	// field, even if TrimLeadingSpace is true.
	Comment rune

	// FieldsPerRecord is the number of expected fields per record.
	// If FieldsPerRecord is positive, Read requires each record to
	// have the given number of fields. If FieldsPerRecord is 0, Read sets it to
	// the number of fields in the first record, so that future records must
	// have the same field count. If FieldsPerRecord is negative, no check is
	// made and records may have a variable number of fields.
	FieldsPerRecord int

	// If LazyQuotes is true, a quote may appear in an unquoted field and a
	// non-doubled quote may appear in a quoted field.
	LazyQuotes bool

	// If TrimLeadingSpace is true, leading white space in a field is ignored.
	// This is done even if the field delimiter, Comma, is white space.
	TrimLeadingSpace bool

	// ReuseRecord controls whether calls to Read may return a slice sharing
	// the backing array of the previous call's returned slice for performance.
	// By default, each call to Read returns newly allocated memory owned by the caller.
	ReuseRecord bool

	r *bufio.Reader

	// numLine is the current line being read in the CSV file.
	numLine int

	// rawBuffer is a line buffer only used by the readLine method.
	rawBuffer []byte

	// recordBuffer holds the unescaped fields, one after another.
	// The fields can be accessed by using the indexes in fieldIndexes.
	// E.g., For the row `a,"b","c""d",e`, recordBuffer will contain `abc"de`
	// and fieldIndexes will contain the indexes [1, 2, 5, 6].
	recordBuffer []byte

	// fieldIndexes is an index of fields inside recordBuffer.
	// The i'th field ends at offset fieldIndexes[i] in recordBuffer.
	fieldIndexes []int

	// lastRecord is a record cache and only used when ReuseRecord == true.
	lastRecord []*string

	// Nullif is set by user, used to set col replace to NULL
	Nullif string

	// Record the byte size that the current read has read
	readSize int64

	// The charset of CSV
	Charset string

	// Does the line being processed by the current thread contain line breaks
	HasSwap bool

	// Number of columns with FLOAT type
	AdjustColumnSet map[uint32]struct{}

	TsColNumber int32
}

// NewReader returns a new Reader that reads from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		Comma:   ',',
		Escape:  '"',
		Enclose: '"',
		r:       bufio.NewReader(r),
	}
}

// SetReader is used to set csvReader's reader.
func (r *Reader) SetReader(reader io.Reader) Reader {
	r.r = bufio.NewReader(reader)
	return *r
}

// Read reads one record (a slice of fields) from r.
// If the record has an unexpected number of fields,
// Read returns the record along with the error ErrFieldCount.
// Except for that case, Read always returns either a non-nil
// record or a non-nil error, but not both.
// If there is no data left to be read, Read returns nil, io.EOF.
// If ReuseRecord is true, the returned slice may be shared
// between multiple calls to Read.
func (r *Reader) Read() (record []*string, err error) {
	if r.ReuseRecord {
		record, err = r.readRecord(r.lastRecord)
		r.lastRecord = record
	} else {
		record, err = r.readRecord(nil)
	}
	return record, err
}

// ReadAll reads all the remaining records from r.
// Each record is a slice of fields.
// A successful call returns err == nil, not err == io.EOF. Because ReadAll is
// defined to read until EOF, it does not treat end of file as an error to be
// reported.
func (r *Reader) ReadAll() (records [][]string, err error) {
	for {
		record, err := r.readRecord(nil)
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		records = append(records, StrPtrToStr(record))
	}
}

// ReadSize returns the size of the read data in Byte for the current Reader
func (r *Reader) ReadSize() int64 {
	return r.readSize
}

// ReadUntil read current line util end.
func (r *Reader) ReadUntil(fileIndex int64) (int64, error) {
	record, err := r.readLine()
	if err != nil {
		return 0, err
	}
	length := int64(len(record))
	fileIndex += length
	return fileIndex, nil
}

// GbkToUtf8 make gbk to utf8
func GbkToUtf8(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), simplifiedchinese.GBK.NewDecoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	// fmt.Println("GbkToUtf8", string(s), "to ", string(d))
	return d, nil
}

// Gb18030ToUtf8 make gb18030 to utf8
func Gb18030ToUtf8(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), simplifiedchinese.GB18030.NewDecoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	// fmt.Println("Gb18030ToUtf8", string(s), "to ", string(d))
	return d, nil
}

// Big5ToUtf8 make big5 to utf8
func Big5ToUtf8(s []byte) ([]byte, error) {
	reader := transform.NewReader(bytes.NewReader(s), traditionalchinese.Big5.NewDecoder())
	d, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil, e
	}
	// fmt.Println("Big5ToUtf8", string(s), "to ", string(d))
	return d, nil
}

// transferCharset transfer CSV	from GBK/GB18030/BIG5 charset to UTF8
func (r *Reader) transferCharset(line []byte) ([]byte, error) {
	switch r.Charset {
	case "GBK":
		utfLine, errE := GbkToUtf8(line)
		if errE != nil {
			return nil, errE
		}
		return utfLine, nil
	case "GB18030":
		utfLine, errE := Gb18030ToUtf8(line)
		if errE != nil {
			return nil, errE
		}
		return utfLine, nil
	case "BIG5":
		utfLine, errE := Big5ToUtf8(line)
		if errE != nil {
			return nil, errE
		}
		return utfLine, nil
	default:
		return line, nil
	}
}

// readLine reads the next line (with the trailing endline).
// If EOF is hit without a trailing endline, it will be omitted.
// If some bytes were read, then the error is never io.EOF.
// The result is only valid until the next call to readLine.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.r.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		r.rawBuffer = append(r.rawBuffer[:0], line...)
		for err == bufio.ErrBufferFull {
			line, err = r.r.ReadSlice('\n')
			r.rawBuffer = append(r.rawBuffer, line...)
		}
		line = r.rawBuffer
	}
	if len(line) > 0 && err == io.EOF {
		err = nil
		// For backwards compatibility, drop trailing \r before EOF.
		if line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
	}
	r.numLine++
	r.readSize += int64(len(line))
	if r.Charset != "UTF-8" && r.Charset != "" {
		// transfer charset of line to GBK/GB18030/BIG5
		utfLine, charErr := r.transferCharset(line)
		if charErr != nil {
			return nil, charErr
		}
		line = utfLine
	}
	return line, err
}

// lengthCRLF reports the number of bytes for a trailing "\r\n".
func lengthCRLF(b []byte) int {
	if j := len(b) - 1; j >= 0 && b[j] == '\n' {
		if j := len(b) - 2; j >= 0 && b[j] == '\r' {
			return 2
		}
		return 1
	}
	return 0
}

// nextRune returns the next rune in b or utf8.RuneError.
func nextRune(b []byte) rune {
	r, _ := utf8.DecodeRune(b)
	return r
}

type wrapState int

const (
	_ wrapState = iota
	left
	right
	both
)

type encloseState int

const (
	_ encloseState = iota
	close
	open
)

// Parser is used to record some format csv options to use,
// such as comma, enclose and escape
type Parser struct {
	comma        byte
	enclose      byte
	escape       byte
	encloseState encloseState
	wrappedState wrapState
}

func (p *Parser) meetEnclosed(start byte) bool {
	return p.enclose == start
}
func (p *Parser) meetEscape(pre byte) bool {
	return pre == p.escape
}
func (p *Parser) noEscape(line []byte, idx int) bool {
	if idx == 0 {
		return false
	}
	if line[idx] == p.escape {
		return !p.noEscape(line[:idx], idx-1)
	}
	return false
}

func (p *Parser) handleEscape(src []byte) []byte {
	filed := src[:0]
	remain := src
	for {
		nextIdx := bytes.IndexByte(remain, p.escape)
		if nextIdx == -1 {
			filed = append(filed, remain...)
			break
		}
		nextByte := byte(nextRune(remain[1:]))
		if p.meetEscape(remain[0]) {
			if len(remain) < 2 {
				filed = append(filed, remain...)
				break
			}
			if p.escape == '\\' && nextByte == 'x' {
				filed = append(filed, p.escape)
			}
			filed = append(filed, nextByte)
			remain = remain[2:]
			continue
		}
		filed = append(filed, remain[:nextIdx]...)
		remain = remain[nextIdx:]
	}
	switch p.wrappedState {
	case left:
		filed = filed[1:]
	case right:
		filed = filed[:len(filed)-1]
	case both:
		if lengthCRLF(filed[len(filed)-1:]) == len(filed[len(filed)-1:]) {
			filed = filed[:len(filed)-1]
		}
		filed = filed[1 : len(filed)-1]
	}
	return filed
}

func (p *Parser) getOneField(line []byte) ([]byte, []byte) {
	idx := 0
	nextIdx := idx
	p.encloseState = close
	if p.meetEnclosed(line[0]) {
		p.encloseState = open
		p.wrappedState = left
		idx++
	} else if p.wrappedState == left {
		p.wrappedState = right
	}
	for {
		if len(line[idx:]) == lengthCRLF(line[idx:]) {
			if p.encloseState == open {
				idx += lengthCRLF(line[idx:])
				nextIdx = idx
			}
			break
		}
		if p.meetEnclosed(line[idx]) {
			if p.encloseState == open && !p.noEscape(line[:idx], idx-1) {
				p.encloseState = close
				p.wrappedState = both
			}
			idx++
		}
		if (line[idx] == p.comma && p.encloseState == close) || lengthCRLF(line[idx:]) == len(line[idx:]) {
			nextIdx = idx + 1
			break
		}
		if p.meetEscape(line[idx]) {
			idx++
		}
		idx++
	}
	return line[:idx], line[nextIdx:]
}

// readRecord use []*string is to distinguish between null values and empty strings.
func (r *Reader) readRecord(dst []*string) ([]*string, error) {
	// initialize a map to record the position of empty values in a recordBuffer.
	nilMap := make(map[int]struct{})
	if r.Comma == r.Comment || !validDelim(r.Comma) || (r.Comment != 0 && !validDelim(r.Comment)) {
		return nil, errInvalidDelim
	}

	if validEscape(r.Escape) {
		return nil, errInvalidEscape
	}

	if validEnclose(r.Enclose) {
		return nil, errInvalidEnclose
	}

	// Read line (automatically skipping past empty lines and any comments).
	var line, fullLine []byte
	var errRead error
	for errRead == nil {
		line, errRead = r.readLine()
		if r.Comment != 0 && nextRune(line) == r.Comment {
			line = nil
			continue // Skip comment lines
		}
		if errRead == nil && len(line) == lengthCRLF(line) {
			line = nil
			continue // Skip empty lines
		}
		fullLine = line
		break
	}
	if errRead == io.EOF {
		return nil, errRead
	}

	// Parse each field in the record.
	var err error
	const quoteLen = len(`"`)
	commaLen := utf8.RuneLen(r.Comma)
	recLine := r.numLine // Starting line for record
	r.recordBuffer = r.recordBuffer[:0]
	r.fieldIndexes = r.fieldIndexes[:0]
	p := Parser{}
	p.comma = byte(r.Comma)
	p.escape = byte(r.Escape)
	p.enclose = byte(r.Enclose)
	replaceNull := true
	p.encloseState = close
parseField:
	for {
		if r.TrimLeadingSpace {
			line = bytes.TrimLeftFunc(line, unicode.IsSpace)
		}
		var enclosed bool
		if len(line) != 0 {
			enclosed = p.meetEnclosed(line[0])
			if enclosed {
				replaceNull = false
			} else {
				replaceNull = true
			}
		}
		if len(line) == 0 || (!enclosed && p.encloseState == close) { // no enclosed = "
			// Non-quoted string field
			nextDelimiterPos := bytes.IndexRune(line, r.Comma) // first delimiter pos
			field := line
			// find delimiter or not, if find, field set to this delimiter
			// if not ,filed set from this pos to end
			if nextDelimiterPos >= 0 {
				field = field[:nextDelimiterPos]
			} else { // not found delimiter in remain line
				field = field[:len(field)-lengthCRLF(field)]
			}
			// Check to make sure a quote does not appear in field.
			if !r.LazyQuotes {
				// there is no " in field
				if j := bytes.IndexByte(field, '"'); j >= 0 {
					// but found another ", break for error
					col := utf8.RuneCount(fullLine[:len(fullLine)-len(line[j:])])
					r.HasSwap = true
					err = &ParseError{StartLine: recLine, Line: r.numLine, Column: col, Err: ErrBareQuote}
					break parseField
				}
			}
			if len(field) == 0 {
				nilMap[len(r.fieldIndexes)] = struct{}{}
			}
			if r.Nullif != "" && replaceNull && string(field) == r.Nullif {
				nilMap[len(r.fieldIndexes)] = struct{}{}
			} else {
				r.recordBuffer = append(r.recordBuffer, field...)
			}
			// record filed pos to r.fieldIndexes
			r.fieldIndexes = append(r.fieldIndexes, len(r.recordBuffer))
			if nextDelimiterPos >= 0 {
				// skip to next field
				line = line[nextDelimiterPos+commaLen:]
				continue parseField
			}
			// i = -1; there is no delimiter in line[:end]
			break parseField
		} else if r.Enclose == r.Escape { // enclosed by "
			// Quoted string field
			line = line[quoteLen:] // skip first "
			for {
				i := bytes.IndexByte(line, byte(r.Enclose)) // find next " pos
				if i >= 0 {                                 // found " in remain line
					if r.Nullif != "" && replaceNull && string(line[:i]) == r.Nullif {
						nilMap[len(r.fieldIndexes)] = struct{}{}
					}
					// Hit next quote.
					r.recordBuffer = append(r.recordBuffer, line[:i]...) // do not record line[i] {"}
					line = line[i+quoteLen:]
					nextChar := nextRune(line)
					if nextChar == r.Enclose { // skip " is escape
						r.recordBuffer = append(r.recordBuffer, byte(r.Enclose))
						line = line[quoteLen:]
					} else if nextChar == r.Comma {
						line = line[commaLen:]
						r.fieldIndexes = append(r.fieldIndexes, len(r.recordBuffer))
						replaceNull = false
						continue parseField
					} else if lengthCRLF(line) == len(line) {
						r.fieldIndexes = append(r.fieldIndexes, len(r.recordBuffer))
						break parseField
					} else if r.LazyQuotes {
						r.recordBuffer = append(r.recordBuffer, byte(r.Enclose))
					} else {
						col := utf8.RuneCount(fullLine[:len(fullLine)-len(line)-quoteLen])
						err = &ParseError{StartLine: recLine, Line: r.numLine, Column: col, Err: ErrQuote}
						break parseField
					}

				} else if len(line) > 0 { // filed start with " but no " in field, just add record. and read next line
					/*
						"aaaaaabbcc
						ccddeeddff", "bbbb
					*/
					// Hit end of line (copy all data so far).
					r.recordBuffer = append(r.recordBuffer, line...)
					if errRead != nil {
						break parseField
					}
					line, errRead = r.readLine()
					if errRead == io.EOF {
						errRead = nil
					}
					fullLine = line
				} else { // there is no " in line && no left bytes; means
					/*
						"
					*/
					// Abrupt end of file (EOF or error).
					if !r.LazyQuotes && errRead == nil {
						col := utf8.RuneCount(fullLine)
						err = &ParseError{StartLine: recLine, Line: r.numLine, Column: col, Err: ErrQuote}
						break parseField
					}
					r.fieldIndexes = append(r.fieldIndexes, len(r.recordBuffer))
					break parseField
				}
			}
		} else {
			field, remain := p.getOneField(line)
			newField := p.handleEscape(field)
			r.recordBuffer = append(r.recordBuffer, newField...)
			if p.encloseState == open {
				if errRead != nil {
					break parseField
				}
				if len(remain) > 0 {
					if line[len(field)] == p.comma {
						r.fieldIndexes = append(r.fieldIndexes, len(r.recordBuffer))
					} else {
						r.recordBuffer = append(r.recordBuffer, remain...)
					}
				}
				remain, errRead = r.readLine()
				if errRead == io.EOF {
					errRead = nil
				}
				fullLine = remain
				// if !r.LazyQuotes && errRead == nil {
				// 	col := utf8.RuneCount(fullLine)
				// 	err = &ParseError{StartLine: recLine, Line: r.numLine, Column: col, Err: ErrQuote}
				// 	break parseField
				// }
				// r.fieldIndexes = append(r.fieldIndexes, len(r.recordBuffer))
				// break parseField
			} else {
				r.fieldIndexes = append(r.fieldIndexes, len(r.recordBuffer))
			}
			line = remain
			replaceNull = true
			if len(line) > 0 {
				continue parseField
			}
			break parseField
		}
	}
	if err == nil {
		err = errRead
	}

	// Create a single string and create slices out of it.
	// This pins the memory of the fields together, but allocates once.
	str := string(r.recordBuffer) // Convert to string once to batch allocations
	dst = dst[:0]
	if cap(dst) < len(r.fieldIndexes) {
		dst = make([]*string, len(r.fieldIndexes))
	}
	dst = dst[:len(r.fieldIndexes)]
	var preIdx int
	for i, idx := range r.fieldIndexes {
		if _, ok := nilMap[i]; ok {
			continue
		}
		var strLine string
		var strNoLine string
		strLine = str[preIdx:idx]
		if r.TsColNumber < 0 || int(r.TsColNumber) != i {
			strNoLine = strLine
		} else {
			strNoLine = strings.Replace(strLine, "'", "", -1)
		}

		if _, ok := r.AdjustColumnSet[uint32(i)]; ok {
			index := strings.IndexFunc(strLine, func(r rune) bool {
				return !unicode.IsSpace(r)
			})
			if index != -1 {
				strNoLine = strLine[index:]
			}
		}
		dst[i] = to.StringPtr(strNoLine)
		preIdx = idx
	}

	// Check or update the expected fields per record.
	if r.FieldsPerRecord > 0 {
		if len(dst) != r.FieldsPerRecord && err == nil {
			err = &ParseError{StartLine: recLine, Line: recLine, Err: ErrFieldCount}
		}
	} else if r.FieldsPerRecord == 0 {
		r.FieldsPerRecord = len(dst)
	}
	return dst, err
}

// StrPtrToStr convert []*string to []string.
func StrPtrToStr(record []*string) []string {
	strRecord := make([]string, len(record))
	for i, field := range record {
		if field != nil {
			strRecord[i] = *field
		} else {
			strRecord[i] = ""
		}
	}
	return strRecord
}
