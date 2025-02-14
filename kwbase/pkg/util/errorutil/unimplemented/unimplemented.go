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

package unimplemented

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/build"
	"github.com/cockroachdb/errors"
)

// This file re-implements the unimplemented primitives from the
// original pgerror package, using the primitives from the errors
// library instead.

// New constructs an unimplemented feature error.
func New(feature, msg string) error {
	return unimplementedInternal(1 /*depth*/, 0 /*issue*/, feature /*detail*/, false /*format*/, msg)
}

// Newf constructs an unimplemented feature error.
// The message is formatted.
func Newf(feature, format string, args ...interface{}) error {
	return NewWithDepthf(1, feature, format, args...)
}

// NewWithDepthf constructs an implemented feature error,
// tracking the context at the specified depth.
func NewWithDepthf(depth int, feature, format string, args ...interface{}) error {
	return unimplementedInternal(depth+1 /*depth*/, 0 /*issue*/, feature /*detail*/, true /*format*/, format, args...)
}

// NewWithIssue constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func NewWithIssue(issue int, msg string) error {
	return unimplementedInternal(1 /*depth*/, issue, "" /*detail*/, false /*format*/, msg)
}

// NewWithIssuef constructs an error with the formatted message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func NewWithIssuef(issue int, format string, args ...interface{}) error {
	return unimplementedInternal(1 /*depth*/, issue, "" /*detail*/, true /*format*/, format, args...)
}

// NewWithIssueHint constructs an error with the given
// message, hint, and a link to the passed issue. Recorded as "#<issue>"
// in tracking.
func NewWithIssueHint(issue int, msg, hint string) error {
	err := unimplementedInternal(1 /*depth*/, issue, "" /*detail*/, false /*format*/, msg)
	err = errors.WithHint(err, hint)
	return err
}

// NewWithIssueDetail constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>.detail" in tracking.
// This is useful when we need an extra axis of information to drill down into.
func NewWithIssueDetail(issue int, detail, msg string) error {
	return unimplementedInternal(1 /*depth*/, issue, detail, false /*format*/, msg)
}

// NewWithIssueDetailf is like NewWithIssueDetail but the message is formatted.
func NewWithIssueDetailf(issue int, detail, format string, args ...interface{}) error {
	return unimplementedInternal(1 /*depth*/, issue, detail, true /*format*/, format, args...)
}

func unimplementedInternal(
	depth, issue int, detail string, format bool, msg string, args ...interface{},
) error {
	// disable the real issue link to avoid potential legal/abuse risks
	link := errors.IssueLink{Detail: detail, IssueURL: ""}
	// if issue > 0 {
	// 	link.IssueURL = MakeURL(issue)
	// }

	// Instantiate the base error.
	var err error
	if format {
		err = errors.UnimplementedErrorf(link, "unimplemented: "+msg, args...)
		err = errors.WithSafeDetails(err, msg, args...)
	} else {
		err = errors.UnimplementedError(link, "unimplemented: "+msg)
	}
	// Decorate with a stack trace.
	err = errors.WithStackDepth(err, 1+depth)

	if issue > 0 {
		// There is an issue number. Decorate with a telemetry annotation.
		var key string
		if detail == "" {
			key = fmt.Sprintf("#%d", issue)
		} else {
			key = fmt.Sprintf("#%d.%s", issue, detail)
		}
		err = errors.WithTelemetry(err, key)
	} else if detail != "" {
		// No issue but a detail string. It's an indication to also
		// perform telemetry.
		err = errors.WithTelemetry(err, detail)
	}
	return err
}

// MakeURL produces a URL to a CockroachDB issue.
func MakeURL(issue int) string {
	return fmt.Sprintf("https://go.kwdb.dev/issue-v/%d/%s", issue, build.VersionPrefix())
}
