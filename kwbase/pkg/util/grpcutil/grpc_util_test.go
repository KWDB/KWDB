// Copyright 2017 The Cockroach Authors.
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

package grpcutil_test

import (
	"context"
	"net"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/grpcutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Implement the grpc health check interface (just because it's the
// simplest predefined RPC service I could find that seems unlikely to
// change out from under us) with an implementation that shuts itself
// down whenever anything calls it. This lets us distinguish errors
// caused by server shutdowns during the request from those when the
// server was already down.
type healthServer struct {
	grpcServer *grpc.Server
}

func (hs healthServer) Check(
	ctx context.Context, req *healthpb.HealthCheckRequest,
) (*healthpb.HealthCheckResponse, error) {
	hs.grpcServer.Stop()

	// Wait for the shutdown to happen before returning from this
	// method.
	<-ctx.Done()
	return nil, errors.New("no one should see this")
}

func (hs healthServer) Watch(*healthpb.HealthCheckRequest, healthpb.Health_WatchServer) error {
	panic("not implemented")
}

func TestRequestDidNotStart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("https://gitee.com/kwbasedb/kwbase/issues/19708")

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = lis.Close()
	}()

	server := grpc.NewServer()
	hs := healthServer{server}
	healthpb.RegisterHealthServer(server, hs)
	go func() {
		_ = server.Serve(lis)
	}()

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = conn.Close()
	}()
	client := healthpb.NewHealthClient(conn)

	// The first time, the request will start and we'll get a
	// "connection is closing" message.
	_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err == nil {
		t.Fatal("did not get expected error")
	} else if grpcutil.RequestDidNotStart(err) {
		t.Fatalf("request should have started, but got %s", err)
	} else if !strings.Contains(err.Error(), "is closing") {
		// This assertion is not essential to this test, but since this
		// logic is sensitive to grpc error handling details it's safer to
		// make the test fail when anything changes. This error could be
		// either "transport is closing" or "connection is closing"
		t.Fatalf("expected 'is closing' error but got %s", err)
	}

	// Afterwards, the request will fail immediately without being sent.
	// But if we try too soon, there's a chance the transport hasn't
	// been put in the "transient failure" state yet and we get a
	// different error.
	testutils.SucceedsSoon(t, func() error {
		_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{})
		if err == nil {
			return errors.New("did not get expected error")
		} else if !grpcutil.RequestDidNotStart(err) {
			return errors.Errorf("request should not have started, but got %s", err)
		}
		return nil
	})

	// Once the transport is in the "transient failure" state it should
	// stay that way, and every subsequent request will fail
	// immediately.
	_, err = client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err == nil {
		t.Fatal("did not get expected error")
	} else if !grpcutil.RequestDidNotStart(err) {
		t.Fatalf("request should not have started, but got %s", err)
	}
}
