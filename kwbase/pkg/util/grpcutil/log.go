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

package grpcutil

import (
	"context"
	"math"
	"regexp"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/petermattis/goid"
	"google.golang.org/grpc/grpclog"
)

func init() {
	SetSeverity(log.Severity_ERROR)
}

// SetSeverity sets the severity level below which GRPC log messages are
// suppressed.
//
// Must be called before GRPC is used.
func SetSeverity(s log.Severity) {
	grpclog.SetLoggerV2((*logger)(&s))
}

// NB: This interface is implemented by a pointer because using a value causes
// a synthetic stack frame to be inserted on calls to the interface methods.
// Specifically, we get a stack frame that appears as "<autogenerated>", which
// is not useful in logs.
//
// Also NB: we pass a depth of 2 here because all logging calls originate from
// the logging adapter file in grpc, which is an additional stack frame away
// from the actual logging site.
var _ grpclog.LoggerV2 = (*logger)(nil)

type logger log.Severity

func (severity *logger) Info(args ...interface{}) {
	if log.Severity(*severity) > log.Severity_INFO {
		return
	}
	log.InfofDepth(context.TODO(), 2, "", args...)
}

func (severity *logger) Infoln(args ...interface{}) {
	if log.Severity(*severity) > log.Severity_INFO {
		return
	}
	log.InfofDepth(context.TODO(), 2, "", args...)
}

func (severity *logger) Infof(format string, args ...interface{}) {
	if log.Severity(*severity) > log.Severity_INFO {
		return
	}
	log.InfofDepth(context.TODO(), 2, format, args...)
}

func (severity *logger) Warning(args ...interface{}) {
	if log.Severity(*severity) > log.Severity_WARNING {
		return
	}
	log.WarningfDepth(context.TODO(), 2, "", args...)
}

func (severity *logger) Warningln(args ...interface{}) {
	if log.Severity(*severity) > log.Severity_WARNING {
		return
	}
	log.WarningfDepth(context.TODO(), 2, "", args...)
}

func (severity *logger) Warningf(format string, args ...interface{}) {
	if log.Severity(*severity) > log.Severity_WARNING {
		return
	}
	if shouldPrint(transportFailedRe, connectionRefusedRe, time.Minute, format, args...) {
		log.WarningfDepth(context.TODO(), 2, format, args...)
	}
}

func (severity *logger) Error(args ...interface{}) {
	if log.Severity(*severity) > log.Severity_ERROR {
		return
	}
	log.ErrorfDepth(context.TODO(), 2, "", args...)
}

func (severity *logger) Errorln(args ...interface{}) {
	if log.Severity(*severity) > log.Severity_ERROR {
		return
	}
	log.ErrorfDepth(context.TODO(), 2, "", args...)
}

func (severity *logger) Errorf(format string, args ...interface{}) {
	if log.Severity(*severity) > log.Severity_ERROR {
		return
	}
	log.ErrorfDepth(context.TODO(), 2, format, args...)
}

func (severity *logger) Fatal(args ...interface{}) {
	if log.Severity(*severity) > log.Severity_NONE {
		return
	}
	log.FatalfDepth(context.TODO(), 2, "", args...)
}

func (severity *logger) Fatalln(args ...interface{}) {
	if log.Severity(*severity) > log.Severity_NONE {
		return
	}
	log.FatalfDepth(context.TODO(), 2, "", args...)
}

func (severity *logger) Fatalf(format string, args ...interface{}) {
	if log.Severity(*severity) > log.Severity_NONE {
		return
	}
	log.FatalfDepth(context.TODO(), 2, format, args...)
}

func (severity *logger) V(i int) bool {
	if i < 0 {
		i = 0
	}
	if i > math.MaxInt32 {
		i = math.MaxInt32
	}
	return log.VDepth(int32(i) /* level */, 1 /* depth */)
}

// https://github.com/grpc/grpc-go/blob/v1.7.0/clientconn.go#L937
var (
	transportFailedRe   = regexp.MustCompile("^" + regexp.QuoteMeta("grpc: addrConn.resetTransport failed to create client transport:"))
	connectionRefusedRe = regexp.MustCompile(
		strings.Join([]string{
			// *nix
			regexp.QuoteMeta("connection refused"),
			// Windows
			regexp.QuoteMeta("No connection could be made because the target machine actively refused it"),
			// Host removed from the network and no longer resolvable:
			// https://github.com/golang/go/blob/go1.8.3/src/net/net.go#L566
			regexp.QuoteMeta("no such host"),
		}, "|"),
	)
)

var spamMu = struct {
	syncutil.Mutex
	gids map[int64]time.Time
}{
	gids: make(map[int64]time.Time),
}

func shouldPrint(
	formatRe, argsRe *regexp.Regexp, freq time.Duration, format string, args ...interface{},
) bool {
	if formatRe.MatchString(format) {
		for _, arg := range args {
			if err, ok := arg.(error); ok {
				if argsRe.MatchString(err.Error()) {
					gid := goid.Get()
					now := timeutil.Now()
					spamMu.Lock()
					t, ok := spamMu.gids[gid]
					doPrint := !(ok && now.Sub(t) < freq)
					if doPrint {
						spamMu.gids[gid] = now
					}
					spamMu.Unlock()
					return doPrint
				}
			}
		}
	}
	return true
}
