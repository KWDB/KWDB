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
//
// This file provides generic interfaces that allow tests to set up test servers
// without importing the server package (avoiding circular dependencies).
// To be used, the binary needs to call
// InitTestServerFactory(server.TestServerFactory), generally from a TestMain()
// in an "foo_test" package (which can import server and is linked together with
// the other tests in package "foo").

package serverutils

import (
	"context"
	gosql "database/sql"
	"net/http"
	"net/url"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/server"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/httputil"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

// TestServerInterface defines test server functionality that tests need; it is
// implemented by server.TestServer.
type TestServerInterface interface {
	Stopper() *stop.Stopper

	Start(params base.TestServerArgs) error

	// NodeID returns the ID of this node within its cluster.
	NodeID() roachpb.NodeID

	// ServingRPCAddr returns the server's advertised address.
	ServingRPCAddr() string

	// ServingSQLAddr returns the server's advertised SQL address.
	ServingSQLAddr() string

	// HTTPAddr returns the server's http address.
	HTTPAddr() string

	// RPCAddr returns the server's RPC address.
	// Note: use ServingRPCAddr() instead unless specific reason not to.
	RPCAddr() string

	// SQLAddr returns the server's SQL address.
	// Note: use ServingSQLAddr() instead unless specific reason not to.
	SQLAddr() string

	// Addr returns the server's address.
	Addr() string

	// DB returns a *client.DB instance for talking to this KV server.
	DB() *kv.DB

	// RPCContext returns the rpc context used by the test server.
	RPCContext() *rpc.Context

	// LeaseManager() returns the *sql.LeaseManager as an interface{}.
	LeaseManager() interface{}

	// InternalExecutor returns a *sql.InternalExecutor as an interface{} (which
	// also implements sqlutil.InternalExecutor if the test cannot depend on sql).
	InternalExecutor() interface{}

	// ExecutorConfig returns a copy of the server's ExecutorConfig.
	// The real return type is sql.ExecutorConfig.
	ExecutorConfig() interface{}

	// GossipI returns the gossip used by the TestServer.
	// The real return type is *gossip.Gossip.
	GossipI() interface{}

	// Clock returns the clock used by the TestServer.
	Clock() *hlc.Clock

	// DistSenderI returns the DistSender used by the TestServer.
	// The real return type is *kv.DistSender.
	DistSenderI() interface{}

	// SQLServer returns the *sql.Server as an interface{}.
	SQLServer() interface{}

	// DistSQLServer returns the *distsql.ServerImpl as an interface{}.
	DistSQLServer() interface{}

	// JobRegistry returns the *jobs.Registry as an interface{}.
	JobRegistry() interface{}

	// MigrationManager returns the *jobs.Registry as an interface{}.
	MigrationManager() interface{}

	// HeartbeatNodeLiveness heartbeats the server's NodeLiveness record.
	HeartbeatNodeLiveness() error

	// SetDistSQLSpanResolver changes the SpanResolver used for DistSQL inside the
	// server's executor. The argument must be a physicalplan.SpanResolver
	// instance.
	//
	// This method exists because we cannot pass the fake span resolver with the
	// server or cluster params: the fake span resolver needs the node IDs and
	// addresses of the servers in a cluster, which are not available before we
	// start the servers.
	//
	// It is the caller's responsibility to make sure no queries are being run
	// with DistSQL at the same time.
	SetDistSQLSpanResolver(spanResolver interface{})

	// AdminURL returns the URL for the admin UI.
	AdminURL() string
	// GetHTTPClient returns an http client configured with the client TLS
	// config required by the TestServer's configuration.
	GetHTTPClient() (http.Client, error)
	// GetAdminAuthenticatedHTTPClient returns an http client which has been
	// authenticated to access Admin API methods (via a cookie).
	// The user has admin privileges.
	GetAdminAuthenticatedHTTPClient() (http.Client, error)
	// GetAuthenticatedHTTPClient returns an http client which has been
	// authenticated to access Admin API methods (via a cookie).
	GetAuthenticatedHTTPClient(isAdmin bool) (http.Client, error)

	// MustGetSQLCounter returns the value of a counter metric from the server's
	// SQL Executor. Runs in O(# of metrics) time, which is fine for test code.
	MustGetSQLCounter(name string) int64
	// MustGetSQLNetworkCounter returns the value of a counter metric from the
	// server's SQL server. Runs in O(# of metrics) time, which is fine for test
	// code.
	MustGetSQLNetworkCounter(name string) int64
	// WriteSummaries records summaries of time-series data, which is required for
	// any tests that query server stats.
	WriteSummaries() error

	// GetFirstStoreID is a utility function returning the StoreID of the first
	// store on this node.
	GetFirstStoreID() roachpb.StoreID

	// GetStores returns the collection of stores from this TestServer's node.
	// The return value is of type *storage.Stores.
	GetStores() interface{}

	// ClusterSettings returns the ClusterSettings shared by all components of
	// this server.
	ClusterSettings() *cluster.Settings

	// SplitRange splits the range containing splitKey.
	SplitRange(
		splitKey roachpb.Key,
	) (left roachpb.RangeDescriptor, right roachpb.RangeDescriptor, err error)

	// MergeRanges merges the range containing leftKey with the following adjacent
	// range.
	MergeRanges(leftKey roachpb.Key) (merged roachpb.RangeDescriptor, err error)

	// ExpectedInitialRangeCount returns the expected number of ranges that should
	// be on the server after initial (asynchronous) splits have been completed,
	// assuming no additional information is added outside of the normal bootstrap
	// process.
	ExpectedInitialRangeCount() (int, error)

	// ForceTableGC sends a GCRequest for the ranges corresponding to a table.
	//
	// An error will be returned if the same table name exists in multiple schemas
	// inside the specified database.
	ForceTableGC(ctx context.Context, database, table string, timestamp hlc.Timestamp) error

	// AuditServer return audit server used by log audit
	AuditServer() *server.AuditServer
}

// TestServerFactory encompasses the actual implementation of the shim
// service.
type TestServerFactory interface {
	// New instantiates a test server.
	New(params base.TestServerArgs) interface{}
}

var srvFactoryImpl TestServerFactory

// InitTestServerFactory should be called once to provide the implementation
// of the service. It will be called from a xx_test package that can import the
// server package.
func InitTestServerFactory(impl TestServerFactory) {
	srvFactoryImpl = impl
}

// StartServer creates a test server and sets up a gosql DB connection.
// The server should be stopped by calling server.Stopper().Stop().
func StartServer(
	t testing.TB, params base.TestServerArgs,
) (TestServerInterface, *gosql.DB, *kv.DB) {
	server, err := StartServerRaw(params)
	if err != nil {
		t.Fatal(err)
	}

	pgURL, cleanupGoDB := sqlutils.PGUrl(
		t, server.ServingSQLAddr(), "StartServer" /* prefix */, url.User(security.RootUser))
	pgURL.Path = params.UseDatabase
	if params.Insecure {
		pgURL.RawQuery = "sslmode=disable"
	}
	goDB, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	server.Stopper().AddCloser(
		stop.CloserFn(func() {
			_ = goDB.Close()
			cleanupGoDB()
		}))
	return server, goDB, server.DB()
}

// ServerConn  sets up gosql DB connections.
func ServerConn(
	t testing.TB, params base.TestServerArgs, server TestServerInterface, user string,
) (*gosql.DB, *kv.DB) {

	pgURL, cleanupGoDB := sqlutils.PGUrlWithNoneCer(
		t, server.ServingSQLAddr(), "StartServer" /* prefix */, url.User(user))
	pgURL.Path = params.UseDatabase

	if params.Insecure {
		pgURL.RawQuery = "sslmode=disable"
	}

	goDB, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}

	server.Stopper().AddCloser(
		stop.CloserFn(func() {
			_ = goDB.Close()
			cleanupGoDB()
		}))
	return goDB, server.DB()
}

// StartServerRaw creates and starts a TestServer.
// Generally StartServer() should be used. However this function can be used
// directly when opening a connection to the server is not desired.
func StartServerRaw(args base.TestServerArgs) (TestServerInterface, error) {
	if srvFactoryImpl == nil {
		panic("TestServerFactory not initialized. One needs to be injected " +
			"from the package's TestMain()")
	}
	if args.ThreadPoolSize == "" {
		args.InitTest()
	}
	server := srvFactoryImpl.New(args).(TestServerInterface)
	if err := server.Start(args); err != nil {
		return nil, err
	}
	return server, nil
}

// GetJSONProto uses the supplied client to GET the URL specified by the parameters
// and unmarshals the result into response.
func GetJSONProto(ts TestServerInterface, path string, response protoutil.Message) error {
	return GetJSONProtoWithAdminOption(ts, path, response, true)
}

// GetJSONProtoWithAdminOption is like GetJSONProto but the caller can customize
// whether the request is performed with admin privilege
func GetJSONProtoWithAdminOption(
	ts TestServerInterface, path string, response protoutil.Message, isAdmin bool,
) error {
	httpClient, err := ts.GetAuthenticatedHTTPClient(isAdmin)
	if err != nil {
		return err
	}
	return httputil.GetJSON(httpClient, ts.AdminURL()+path, response)
}

// PostJSONProto uses the supplied client to POST the URL specified by the parameters
// and unmarshals the result into response.
func PostJSONProto(ts TestServerInterface, path string, request, response protoutil.Message) error {
	return PostJSONProtoWithAdminOption(ts, path, request, response, true)
}

// PostJSONProtoWithAdminOption is like PostJSONProto but the caller
// can customize whether the request is performed with admin
// privilege.
func PostJSONProtoWithAdminOption(
	ts TestServerInterface, path string, request, response protoutil.Message, isAdmin bool,
) error {
	httpClient, err := ts.GetAuthenticatedHTTPClient(isAdmin)
	if err != nil {
		return err
	}
	return httputil.PostJSON(httpClient, ts.AdminURL()+path, request, response)
}
