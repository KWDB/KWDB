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

package ctpb

import "context"

// InboundClient is an interface that narrows ClosedTimestamp_GetServer down to what's
// actually required.
type InboundClient interface {
	Send(*Entry) error
	Recv() (*Reaction, error)
	Context() context.Context
}

// Server is the interface implemented by types that want to serve incoming
// closed timestamp update streams.
type Server interface {
	Get(InboundClient) error
}

// ServerShim is a wrapper around Server that provides the wider interface that
// gRPC expects.
type ServerShim struct{ Server }

var _ ClosedTimestampServer = (*ServerShim)(nil)

// Get implements ClosedTimestampServer by passing through to the wrapped Server.
func (s ServerShim) Get(client ClosedTimestamp_GetServer) error {
	return s.Server.Get(client)
}

var _ InboundClient = ClosedTimestamp_GetServer(nil)
