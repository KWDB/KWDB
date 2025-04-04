// Copyright 2016 The Cockroach Authors.
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

package log

import (
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"golang.org/x/net/trace"
)

// ctxEventLogKey is an empty type for the handle associated with the
// ctxEventLog value (see context.Value).
type ctxEventLogKey struct{}

// ctxEventLog is used for contexts to keep track of an EventLog.
type ctxEventLog struct {
	syncutil.Mutex
	eventLog trace.EventLog
}

func (el *ctxEventLog) finish() {
	el.Lock()
	if el.eventLog != nil {
		el.eventLog.Finish()
		el.eventLog = nil
	}
	el.Unlock()
}

func embedCtxEventLog(ctx context.Context, el *ctxEventLog) context.Context {
	return context.WithValue(ctx, ctxEventLogKey{}, el)
}

// withEventLogInternal embeds a trace.EventLog in the context, causing future
// logging and event calls to go to the EventLog. The current context must not
// have an existing open span.
func withEventLogInternal(ctx context.Context, eventLog trace.EventLog) context.Context {
	if opentracing.SpanFromContext(ctx) != nil {
		panic("event log under span")
	}
	return embedCtxEventLog(ctx, &ctxEventLog{eventLog: eventLog})
}

// WithEventLog creates and embeds a trace.EventLog in the context, causing
// future logging and event calls to go to the EventLog. The current context
// must not have an existing open span.
func WithEventLog(ctx context.Context, family, title string) context.Context {
	return withEventLogInternal(ctx, trace.NewEventLog(family, title))
}

var _ = WithEventLog

// WithNoEventLog creates a context which no longer has an embedded event log.
func WithNoEventLog(ctx context.Context) context.Context {
	return withEventLogInternal(ctx, nil)
}

func eventLogFromCtx(ctx context.Context) *ctxEventLog {
	if val := ctx.Value(ctxEventLogKey{}); val != nil {
		return val.(*ctxEventLog)
	}
	return nil
}

// FinishEventLog closes the event log in the context (see WithEventLog).
// Concurrent and subsequent calls to record events are allowed.
func FinishEventLog(ctx context.Context) {
	if el := eventLogFromCtx(ctx); el != nil {
		el.finish()
	}
}

// getSpanOrEventLog returns the current Span. If there is no Span, it returns
// the current ctxEventLog. If neither (or the Span is "black hole"), returns
// false.
func getSpanOrEventLog(ctx context.Context) (opentracing.Span, *ctxEventLog, bool) {
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		if tracing.IsBlackHoleSpan(sp) {
			return nil, nil, false
		}
		return sp, nil, true
	}
	if el := eventLogFromCtx(ctx); el != nil {
		return nil, el, true
	}
	return nil, nil, false
}

// eventInternal is the common code for logging an event. If no args are given,
// the format is treated as a pre-formatted string.
func eventInternal(ctx context.Context, isErr, withTags bool, format string, args ...interface{}) {
	if sp, el, ok := getSpanOrEventLog(ctx); ok {
		var buf strings.Builder
		if withTags {
			withTags = formatTags(ctx, &buf)
		}

		var msg string
		if !withTags && len(args) == 0 {
			// Fast path for pre-formatted messages.
			msg = format
		} else {
			if len(args) == 0 {
				buf.WriteString(format)
			} else {
				fmt.Fprintf(&buf, format, args...)
			}
			msg = buf.String()
		}

		if sp != nil {
			// TODO(radu): pass tags directly to sp.LogKV when LightStep supports
			// that.
			sp.LogFields(otlog.String(tracing.LogMessageField, msg))
			// if isErr {
			// 	// TODO(radu): figure out a way to signal that this is an error. We
			// 	// could use a different "error" key (provided it shows up in
			// 	// LightStep). Things like NetTraceIntegrator would need to be modified
			// 	// to understand the difference. We could also set a special Tag or
			// 	// Baggage on the span. See #8827 for more discussion.
			// }
		} else {
			el.Lock()
			if el.eventLog != nil {
				if isErr {
					el.eventLog.Errorf("%s", msg)
				} else {
					el.eventLog.Printf("%s", msg)
				}
			}
			el.Unlock()
		}
	}
}

// Event looks for an opentracing.Trace in the context and logs the given
// message to it. If no Trace is found, it looks for an EventLog in the context
// and logs the message to it. If neither is found, does nothing.
func Event(ctx context.Context, msg string) {
	eventInternal(ctx, false /* isErr */, true /* withTags */, msg)
}

// Eventf looks for an opentracing.Trace in the context and formats and logs
// the given message to it. If no Trace is found, it looks for an EventLog in
// the context and logs the message to it. If neither is found, does nothing.
func Eventf(ctx context.Context, format string, args ...interface{}) {
	eventInternal(ctx, false /* isErr */, true /* withTags */, format, args...)
}

func vEventf(
	ctx context.Context, isErr bool, depth int, level int32, format string, args ...interface{},
) {
	if VDepth(level, 1+depth) {
		// Log the message (which also logs an event).
		sev := Severity_INFO
		if isErr {
			sev = Severity_ERROR
		}
		logDepth(ctx, 1+depth, sev, format, args)
	} else {
		eventInternal(ctx, isErr, true /* withTags */, format, args...)
	}
}

// VEvent either logs a message to the log files (which also outputs to the
// active trace or event log) or to the trace/event log alone, depending on
// whether the specified verbosity level is active.
func VEvent(ctx context.Context, level int32, msg string) {
	vEventf(ctx, false /* isErr */, 1, level, msg)
}

// VEventf either logs a message to the log files (which also outputs to the
// active trace or event log) or to the trace/event log alone, depending on
// whether the specified verbosity level is active.
func VEventf(ctx context.Context, level int32, format string, args ...interface{}) {
	vEventf(ctx, false /* isErr */, 1, level, format, args...)
}

// VEventfDepth performs the same as VEventf but checks the verbosity level
// at the given depth in the call stack.
func VEventfDepth(ctx context.Context, depth int, level int32, format string, args ...interface{}) {
	vEventf(ctx, false /* isErr */, 1+depth, level, format, args...)
}

// VErrEvent either logs an error message to the log files (which also outputs
// to the active trace or event log) or to the trace/event log alone, depending
// on whether the specified verbosity level is active.
func VErrEvent(ctx context.Context, level int32, msg string) {
	vEventf(ctx, true /* isErr */, 1, level, msg)
}

// VErrEventf either logs an error message to the log files (which also outputs
// to the active trace or event log) or to the trace/event log alone, depending
// on whether the specified verbosity level is active.
func VErrEventf(ctx context.Context, level int32, format string, args ...interface{}) {
	vEventf(ctx, true /* isErr */, 1, level, format, args...)
}

// VErrEventfDepth performs the same as VErrEventf but checks the verbosity
// level at the given depth in the call stack.
func VErrEventfDepth(
	ctx context.Context, depth int, level int32, format string, args ...interface{},
) {
	vEventf(ctx, true /* isErr */, 1+depth, level, format, args...)
}

var _ = VErrEventfDepth // silence unused warning

// HasSpanOrEvent returns true if the context has a span or event that should
// be logged to.
func HasSpanOrEvent(ctx context.Context) bool {
	_, _, ok := getSpanOrEventLog(ctx)
	return ok
}
