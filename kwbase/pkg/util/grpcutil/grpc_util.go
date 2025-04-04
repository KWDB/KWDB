// Copyright 2014 The Cockroach Authors.
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
	"fmt"
	"io"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/util/netutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

// ErrCannotReuseClientConn is returned when a failed connection is
// being reused. We require that new connections be created with
// pkg/rpc.GRPCDial instead.
var ErrCannotReuseClientConn = errors.New("cannot reuse client connection")

type localRequestKey struct{}

// NewLocalRequestContext returns a Context that can be used for local (in-process) requests.
func NewLocalRequestContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, localRequestKey{}, struct{}{})
}

// IsLocalRequestContext returns true if this context is marked for local (in-process) use.
func IsLocalRequestContext(ctx context.Context) bool {
	return ctx.Value(localRequestKey{}) != nil
}

// IsTimeout returns true if err's Cause is a gRPC timeout, or the request
// was canceled by a context timeout.
func IsTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	err = errors.Cause(err)
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.DeadlineExceeded
	}
	return false
}

// IsClosedConnection returns true if err's Cause is an error produced by gRPC
// on closed connections.
func IsClosedConnection(err error) bool {
	err = errors.Cause(err)
	if err == ErrCannotReuseClientConn {
		return true
	}
	if s, ok := status.FromError(err); ok {
		if s.Code() == codes.Canceled ||
			s.Code() == codes.Unavailable {
			return true
		}
	}
	if err == context.Canceled ||
		strings.Contains(err.Error(), "is closing") ||
		strings.Contains(err.Error(), "tls: use of closed connection") ||
		strings.Contains(err.Error(), "use of closed network connection") ||
		strings.Contains(err.Error(), io.ErrClosedPipe.Error()) ||
		strings.Contains(err.Error(), io.EOF.Error()) ||
		strings.Contains(err.Error(), "node unavailable") {
		return true
	}
	return netutil.IsClosedConnection(err)
}

// RequestDidNotStart returns true if the given error from gRPC
// means that the request definitely could not have started on the
// remote server.
//
// This method currently depends on implementation details, matching
// on the text of an error message that is known to only be used
// in this case in the version of gRPC that we use today. We will
// need to watch for changes here in future versions of gRPC.
// TODO(bdarnell): Replace this with a cleaner mechanism when/if
// https://github.com/grpc/grpc-go/issues/1443 is resolved.
func RequestDidNotStart(err error) bool {
	if errors.HasType(err, connectionNotReadyError{}) ||
		errors.HasType(err, (*netutil.InitialHeartbeatFailedError)(nil)) {
		return true
	}
	s, ok := status.FromError(errors.Cause(err))
	if !ok {
		// This is a non-gRPC error; assume nothing.
		return false
	}
	// TODO(bdarnell): In gRPC 1.7, we have no good way to distinguish
	// ambiguous from unambiguous failures, so we must assume all gRPC
	// errors are ambiguous.
	// https://gitee.com/kwbasedb/kwbase/issues/19708#issuecomment-343891640
	if false && s.Code() == codes.Unavailable && s.Message() == "grpc: the connection is unavailable" {
		return true
	}
	return false
}

// ConnectionReady returns nil if the given connection is ready to
// send a request, or an error (which will pass RequestDidNotStart) if
// not.
//
// This is a workaround for the fact that gRPC 1.7 fails to
// distinguish between ambiguous and unambiguous errors.
//
// This is designed for use with connections prepared by
// pkg/rpc.Connection.Connect (which performs an initial heartbeat and
// thereby ensures that we will never see a connection in the
// first-time Connecting state).
func ConnectionReady(conn *grpc.ClientConn) error {
	if s := conn.GetState(); s == connectivity.TransientFailure {
		return connectionNotReadyError{s}
	}
	return nil
}

type connectionNotReadyError struct {
	state connectivity.State
}

func (e connectionNotReadyError) Error() string {
	return fmt.Sprintf("connection not ready: %s", e.state)
}
