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

package parser

import (
	"bytes"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	unimp "gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type lexer struct {
	in string
	// tokens contains tokens generated by the scanner.
	tokens []sqlSymType

	// The type that should be used when an INT or SERIAL is encountered.
	nakedIntType *types.T

	// lastPos is the position into the tokens slice of the last
	// token returned by Lex().
	lastPos int

	stmt tree.Statement
	// numPlaceholders is 1 + the highest placeholder index encountered.
	numPlaceholders int
	numAnnotations  tree.AnnotationIdx

	lastError error
}

func (l *lexer) init(sql string, tokens []sqlSymType, nakedIntType *types.T) {
	l.in = sql
	l.tokens = tokens
	l.lastPos = -1
	l.stmt = nil
	l.numPlaceholders = 0
	l.numAnnotations = 0
	l.lastError = nil

	l.nakedIntType = nakedIntType
}

// cleanup is used to avoid holding on to memory unnecessarily (for the cases
// where we reuse a scanner).
func (l *lexer) cleanup() {
	l.tokens = nil
	l.stmt = nil
	l.lastError = nil
}

// Lex lexes a token from input.
func (l *lexer) Lex(lval *sqlSymType) int {
	l.lastPos++
	// The core lexing takes place in the scanner. Here we do a small bit of post
	// processing of the lexical tokens so that the grammar only requires
	// one-token lookahead despite SQL requiring multi-token lookahead in some
	// cases. These special cases are handled below and the returned tokens are
	// adjusted to reflect the lookahead (LA) that occurred.
	if l.lastPos >= len(l.tokens) {
		lval.id = 0
		lval.pos = int32(len(l.in))
		lval.str = "EOF"
		return 0
	}
	*lval = l.tokens[l.lastPos]

	switch lval.id {
	case NOT, WITH, AS:
		nextID := int32(0)
		if l.lastPos+1 < len(l.tokens) {
			nextID = l.tokens[l.lastPos+1].id
		}

		// If you update these cases, update lex.lookaheadKeywords.
		switch lval.id {
		case AS:
			switch nextID {
			case OF:
				lval.id = AS_LA
			}
		case NOT:
			switch nextID {
			case BETWEEN, IN, LIKE, ILIKE, SIMILAR:
				lval.id = NOT_LA
			}

		case WITH:
			switch nextID {
			case TIME, ORDINALITY:
				lval.id = WITH_LA
			}
		}
	}

	return int(lval.id)
}

func (l *lexer) lastToken() sqlSymType {
	if l.lastPos < 0 {
		return sqlSymType{}
	}

	if l.lastPos >= len(l.tokens) {
		return sqlSymType{
			id:  0,
			pos: int32(len(l.in)),
			str: "EOF",
		}
	}
	return l.tokens[l.lastPos]
}

// NewAnnotation returns a new annotation index.
func (l *lexer) NewAnnotation() tree.AnnotationIdx {
	l.numAnnotations++
	return l.numAnnotations
}

// SetStmt is called from the parser when the statement is constructed.
func (l *lexer) SetStmt(stmt tree.Statement) {
	l.stmt = stmt
}

// UpdateNumPlaceholders is called from the parser when a placeholder is constructed.
func (l *lexer) UpdateNumPlaceholders(p *tree.Placeholder) {
	if n := int(p.Idx) + 1; l.numPlaceholders < n {
		l.numPlaceholders = n
	}
}

// Unimplemented wraps Error, setting lastUnimplementedError.
func (l *lexer) Unimplemented(feature string) {
	l.lastError = unimp.New(feature, "this syntax")
	l.populateErrorDetails()
}

// UnimplementedWithIssue wraps Error, setting lastUnimplementedError.
func (l *lexer) UnimplementedWithIssue(issue int) {
	l.lastError = unimp.NewWithIssue(issue, "this syntax")
	l.populateErrorDetails()
}

// UnimplementedWithIssueDetail wraps Error, setting lastUnimplementedError.
func (l *lexer) UnimplementedWithIssueDetail(issue int, detail string) {
	l.lastError = unimp.NewWithIssueDetail(issue, detail, "this syntax")
	l.populateErrorDetails()
}

// PurposelyUnimplemented wraps Error, setting lastUnimplementedError.
func (l *lexer) PurposelyUnimplemented(feature string, reason string) {
	// We purposely do not use unimp here, as it appends hints to suggest that
	// the error may be actively tracked as a bug.
	l.lastError = errors.WithHint(
		errors.WithTelemetry(
			pgerror.Newf(pgcode.Syntax, "unimplemented: this syntax"),
			fmt.Sprintf("sql.purposely_unimplemented.%s", feature),
		),
		reason,
	)
	l.populateErrorDetails()
}

// setErr is called from parsing action rules to register an error observed
// while running the action. That error becomes the actual "cause" of the
// syntax error.
func (l *lexer) setErr(err error) {
	err = pgerror.WithCandidateCode(err, pgcode.Syntax)
	l.lastError = err
	l.populateErrorDetails()
}

func (l *lexer) Error(e string) {
	e = strings.TrimPrefix(e, "syntax error: ") // we'll add it again below.
	l.lastError = pgerror.WithCandidateCode(errors.New(e), pgcode.Syntax)
	l.populateErrorDetails()
}

func (l *lexer) populateErrorDetails() {
	lastTok := l.lastToken()

	if lastTok.id == ERROR {
		// This is a tokenizer (lexical) error: the scanner
		// will have stored the error message in the string field.
		err := pgerror.WithCandidateCode(errors.Newf("lexical error: %s", lastTok.str), pgcode.Syntax)
		l.lastError = errors.WithSecondaryError(err, l.lastError)
	} else {
		// This is a contextual error. Print the provided error message
		// and the error context.
		if !strings.Contains(l.lastError.Error(), "syntax error") {
			// "syntax error" is already prepended when the yacc-generated
			// parser encounters a parsing error.
			l.lastError = errors.Wrap(l.lastError, "syntax error")
		}
		l.lastError = errors.Wrapf(l.lastError, "at or near \"%s\"", lastTok.str)
	}

	// Find the end of the line containing the last token.
	i := strings.IndexByte(l.in[lastTok.pos:], '\n')
	if i == -1 {
		i = len(l.in)
	} else {
		i += int(lastTok.pos)
	}
	// Find the beginning of the line containing the last token. Note that
	// LastIndexByte returns -1 if '\n' could not be found.
	j := strings.LastIndexByte(l.in[:lastTok.pos], '\n') + 1
	// Output everything up to and including the line containing the last token.
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "source SQL:\n%s\n", l.in[:i])
	// Output a caret indicating where the last token starts.
	fmt.Fprintf(&buf, "%s^", strings.Repeat(" ", int(lastTok.pos)-j))
	l.lastError = errors.WithDetail(l.lastError, buf.String())
}

// SetHelp marks the "last error" field in the lexer to become a
// help text. This method is invoked in the error action of the
// parser, so the help text is only produced if the last token
// encountered was HELPTOKEN -- other cases are just syntax errors,
// and in that case we do not want the help text to overwrite the
// lastError field, which was set earlier to contain details about the
// syntax error.
func (l *lexer) SetHelp(msg HelpMessage) {
	if l.lastError == nil {
		l.lastError = pgerror.WithCandidateCode(errors.New("help request"), pgcode.Syntax)
	}

	if lastTok := l.lastToken(); lastTok.id == HELPTOKEN {
		l.populateHelpMsg(msg.String())
	} else {
		if msg.Command != "" {
			l.lastError = errors.WithHintf(l.lastError, `try \h %s`, msg.Command)
		} else {
			l.lastError = errors.WithHintf(l.lastError, `try \hf %s`, msg.Function)
		}
	}
}

func (l *lexer) populateHelpMsg(msg string) {
	l.lastError = errors.WithHint(errors.Wrap(l.lastError, "help token in input"), msg)
}

func checkLoopLabels(start, end string) error {
	if start == "" && end != "" {
		return errors.Newf("end label \"%s\" specified for unlabeled block", end)
	}
	if end != "" && start != end {
		return errors.Newf("end label \"%s\" differs from block's label \"%s\"", end, start)
	}
	return nil
}
