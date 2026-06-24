// Copyright 2020 The Cockroach Authors.
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

package cli

import (
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/cli/cliflags"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/status"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/log/logflags"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

type transientCluster struct {
	connURL    string
	demoDir    string
	useSockets bool
	stopper    *stop.Stopper
	s          *server.TestServer
	servers    []*server.TestServer
}

func setupTransientCluster(
	ctx context.Context, cmd *cobra.Command,
) (c transientCluster, err error) {
	// useSockets is true on unix, false on windows.
	c.useSockets = useUnixSocketsInDemo()

	// The user specified some localities for their nodes.
	if len(demoCtx.localities) != 0 {
		// Error out of localities don't line up with requested node
		// count before doing any sort of setup.
		if len(demoCtx.localities) != demoCtx.nodes {
			return c, errors.Errorf("number of localities specified must equal number of nodes")
		}
	} else {
		demoCtx.localities = make([]roachpb.Locality, demoCtx.nodes)
		for i := 0; i < demoCtx.nodes; i++ {
			demoCtx.localities[i] = defaultLocalities[i%len(defaultLocalities)]
		}
	}

	// Set up logging. For demo/transient server we use non-standard
	// behavior where we avoid file creation if possible.
	fl := flagSetForCmd(cmd)
	df := fl.Lookup(cliflags.LogDir.Name)
	sf := fl.Lookup(logflags.LogToStderrName)
	if !df.Changed && !sf.Changed {
		// User did not request logging flags; shut down all logging.
		// Otherwise, the demo command would cause a kwbase-data
		// directory to appear in the current directory just for logs.
		_ = df.Value.Set("")
		df.Changed = true
		_ = sf.Value.Set(log.Severity_NONE.String())
		sf.Changed = true
	}
	c.stopper, err = setupAndInitializeLoggingAndProfiling(ctx, cmd)
	if err != nil {
		return c, err
	}
	maybeWarnMemSize(ctx)

	// Create a temporary directory for certificates (if secure) and
	// the unix sockets.
	// The directory is removed in the cleanup() method.
	if c.demoDir, err = ioutil.TempDir("", "demo"); err != nil {
		return c, err
	}

	if !demoCtx.insecure {
		if err := generateCerts(c.demoDir); err != nil {
			return c, err
		}
	}

	serverFactory := server.TestServerFactory
	var servers []*server.TestServer

	// latencyMapWaitCh is used to block test servers after RPC address computation until the artificial
	// latency map has been constructed.
	latencyMapWaitCh := make(chan struct{})

	// errCh is used to catch all errors when initializing servers.
	// Sending a nil on this channel indicates success.
	errCh := make(chan error, demoCtx.nodes)

	for i := 0; i < demoCtx.nodes; i++ {
		// All the nodes connect to the address of the first server created.
		var joinAddr string
		if c.s != nil {
			joinAddr = c.s.ServingRPCAddr()
		}
		nodeID := roachpb.NodeID(i + 1)
		args := testServerArgsForTransientCluster(c.sockForServer(nodeID), nodeID, joinAddr, c.demoDir)

		// servRPCReadyCh is used if latency simulation is requested to notify that a test server has
		// successfully computed its RPC address.
		servRPCReadyCh := make(chan struct{})

		if demoCtx.simulateLatency {
			args.Knobs = base.TestingKnobs{
				Server: &server.TestingKnobs{
					PauseAfterGettingRPCAddress:  latencyMapWaitCh,
					SignalAfterGettingRPCAddress: servRPCReadyCh,
					ContextTestingKnobs: rpc.ContextTestingKnobs{
						ArtificialLatencyMap: make(map[string]int),
					},
				},
			}
		}

		serv := serverFactory.New(args).(*server.TestServer)

		if i == 0 {
			c.s = serv
		}
		servers = append(servers, serv)

		// We force a wait for all servers until they are ready.
		servReadyFnCh := make(chan struct{})
		serv.Cfg.ReadyFn = func(_ bool) {
			close(servReadyFnCh)
		}

		// If latency simulation is requested, start the servers in a background thread. We do this because
		// the start routine needs to wait for the latency map construction after their RPC address has been computed.
		if demoCtx.simulateLatency {
			go func(i int) {
				if err := serv.Start(args); err != nil {
					errCh <- err
				} else {
					// Block until the ReadyFn has been called before continuing.
					<-servReadyFnCh
					errCh <- nil
				}
			}(i)
			<-servRPCReadyCh
		} else {
			if err := serv.Start(args); err != nil {
				return c, err
			}
			// Block until the ReadyFn has been called before continuing.
			<-servReadyFnCh
			errCh <- nil
		}

		c.stopper.AddCloser(stop.CloserFn(serv.Stop))
		// Ensure we close all sticky stores we've created.
		for _, store := range args.StoreSpecs {
			if store.StickyInMemoryEngineID != "" {
				engineID := store.StickyInMemoryEngineID
				c.stopper.AddCloser(stop.CloserFn(func() {
					if err := server.CloseStickyInMemEngine(engineID); err != nil {
						// Something else may have already closed the sticky store.
						// Since we are closer, it doesn't really matter.
						log.Warningf(
							ctx,
							"could not close sticky in-memory store %s: %+v",
							engineID,
							err,
						)
					}
				}))
			}
		}
	}

	c.servers = servers

	if demoCtx.simulateLatency {
		// Now, all servers have been started enough to know their own RPC serving
		// addresses, but nothing else. Assemble the artificial latency map.
		for i, src := range servers {
			latencyMap := src.Cfg.TestingKnobs.Server.(*server.TestingKnobs).ContextTestingKnobs.ArtificialLatencyMap
			srcLocality, ok := src.Cfg.Locality.Find("region")
			if !ok {
				continue
			}
			srcLocalityMap, ok := regionToRegionToLatency[srcLocality]
			if !ok {
				continue
			}
			for j, dst := range servers {
				if i == j {
					continue
				}
				dstLocality, ok := dst.Cfg.Locality.Find("region")
				if !ok {
					continue
				}
				latency := srcLocalityMap[dstLocality]
				latencyMap[dst.ServingRPCAddr()] = latency
			}
		}
	}

	// We've assembled our latency maps and are ready for all servers to proceed
	// through bootstrapping.
	close(latencyMapWaitCh)

	// Wait for all servers to respond.
	{
		timeRemaining := maxNodeInitTime
		lastUpdateTime := timeutil.Now()
		var err error
		for i := 0; i < demoCtx.nodes; i++ {
			select {
			case e := <-errCh:
				err = errors.CombineErrors(err, e)
			case <-time.After(timeRemaining):
				return c, errors.New("failed to setup transientCluster in time")
			}
			updateTime := timeutil.Now()
			timeRemaining -= updateTime.Sub(lastUpdateTime)
			lastUpdateTime = updateTime
		}
		if err != nil {
			return c, err
		}
	}

	// Create the root password if running in secure mode. We'll
	// need that for the URL.
	if !demoCtx.insecure {
		if err := c.setupUserAuth(ctx); err != nil {
			return c, err
		}
	}

	if demoCtx.nodes < 3 {
		// Set up the default zone configuration. We are using an in-memory store
		// so we really want to disable replication.
		if err := cliDisableReplication(ctx, c.s.Server); err != nil {
			return c, err
		}
	}

	// Prepare the URL for use by the SQL shell.
	c.connURL, err = c.getNetworkURLForServer(0, true /* includeAppName */)
	if err != nil {
		return c, err
	}

	// Start up the update check loop.
	// We don't do this in (*server.Server).Start() because we don't want it
	// in tests.
	if !demoCtx.disableTelemetry {
		c.s.PeriodicallyCheckForUpdates(ctx)
	}
	return c, nil
}

// testServerArgsForTransientCluster creates the test arguments for
// a necessary server in the demo cluster.
func testServerArgsForTransientCluster(
	sock unixSocketDetails, nodeID roachpb.NodeID, joinAddr string, demoDir string,
) base.TestServerArgs {
	// Assign a path to the store spec, to be saved.
	storeSpec := base.DefaultTestStoreSpec
	storeSpec.StickyInMemoryEngineID = fmt.Sprintf("demo-node%d", nodeID)

	args := base.TestServerArgs{
		SocketFile:        sock.filename(),
		PartOfCluster:     true,
		Stopper:           stop.NewStopper(),
		JoinAddr:          joinAddr,
		DisableTLSForHTTP: true,
		StoreSpecs:        []base.StoreSpec{storeSpec},
		SQLMemoryPoolSize: demoCtx.sqlPoolMemorySize,
		CacheSize:         demoCtx.cacheSize,
	}

	if demoCtx.localities != nil {
		args.Locality = demoCtx.localities[int(nodeID-1)]
	}
	if demoCtx.insecure {
		args.Insecure = true
	} else {
		args.Insecure = false
		args.SSLCertsDir = demoDir
	}

	return args
}

func (c *transientCluster) cleanup(ctx context.Context) {
	if c.stopper != nil {
		c.stopper.Stop(ctx)
	}
	if c.demoDir != "" {
		if err := checkAndMaybeShout(os.RemoveAll(c.demoDir)); err != nil {
			// There's nothing to do here anymore if err != nil.
			_ = err
		}
	}
}

// DrainNode will gracefully attempt to drain a node in the cluster.
func (c *transientCluster) DrainNode(nodeID roachpb.NodeID) error {
	nodeIndex := int(nodeID - 1)

	if nodeIndex < 0 || nodeIndex >= len(c.servers) {
		return errors.Errorf("node %d does not exist", nodeID)
	}
	// This is possible if we re-assign c.s and make the other nodes to the new
	// base node.
	if nodeIndex == 0 {
		return errors.Errorf("cannot shutdown node %d", nodeID)
	}
	if c.servers[nodeIndex] == nil {
		return errors.Errorf("node %d is already shut down", nodeID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adminClient, finish, err := getAdminClient(ctx, *(c.servers[nodeIndex].Cfg))
	if err != nil {
		return err
	}
	defer finish()

	if err := drainAndShutdown(ctx, adminClient); err != nil {
		return err
	}
	c.servers[nodeIndex] = nil
	return nil
}

// CallDecommission calls the Decommission RPC on a node.
func (c *transientCluster) CallDecommission(nodeID roachpb.NodeID, decommissioning bool) error {
	nodeIndex := int(nodeID - 1)

	if nodeIndex < 0 || nodeIndex >= len(c.servers) {
		return errors.Errorf("node %d does not exist", nodeID)
	}

	req := &serverpb.DecommissionRequest{
		NodeIDs:         []roachpb.NodeID{nodeID},
		Decommissioning: decommissioning,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adminClient, finish, err := getAdminClient(ctx, *(c.s.Cfg))
	if err != nil {
		return err
	}

	defer finish()
	_, err = adminClient.Decommission(ctx, req)
	if err != nil {
		return errors.Wrap(err, "while trying to mark as decommissioning")
	}
	return nil
}

// RestartNode will bring back a node in the cluster.
// The node must have been shut down beforehand.
// The node will restart, connecting to the same in memory node.
func (c *transientCluster) RestartNode(nodeID roachpb.NodeID) error {
	nodeIndex := int(nodeID - 1)

	if nodeIndex < 0 || nodeIndex >= len(c.servers) {
		return errors.Errorf("node %d does not exist", nodeID)
	}
	if c.servers[nodeIndex] != nil {
		return errors.Errorf("node %d is already running", nodeID)
	}

	// TODO(#42243): re-compute the latency mapping.
	args := testServerArgsForTransientCluster(c.sockForServer(nodeID), nodeID, c.s.ServingRPCAddr(), c.demoDir)
	serv := server.TestServerFactory.New(args).(*server.TestServer)

	// We want to only return after the server is ready.
	readyCh := make(chan struct{})
	serv.Cfg.ReadyFn = func(_ bool) {
		close(readyCh)
	}

	if err := serv.Start(args); err != nil {
		return err
	}

	// Wait until the server is ready to action.
	select {
	case <-readyCh:
	case <-time.After(maxNodeInitTime):
		return errors.Newf("could not initialize node %d in time", nodeID)
	}

	c.stopper.AddCloser(stop.CloserFn(serv.Stop))
	c.servers[nodeIndex] = serv
	return nil
}

func maybeWarnMemSize(ctx context.Context) {
	if maxMemory, err := status.GetTotalMemory(ctx); err == nil {
		requestedMem := (demoCtx.cacheSize + demoCtx.sqlPoolMemorySize) * int64(demoCtx.nodes)
		maxRecommendedMem := int64(.75 * float64(maxMemory))
		if requestedMem > maxRecommendedMem {
			log.Shout(
				ctx,
				log.Severity_WARNING,
				fmt.Sprintf(`HIGH MEMORY USAGE
The sum of --max-sql-memory (%s) and --cache (%s) multiplied by the
number of nodes (%d) results in potentially high memory usage on your
device.
This server is running at increased risk of memory-related failures.`,
					demoNodeSQLMemSizeValue,
					demoNodeCacheSizeValue,
					demoCtx.nodes,
				),
			)
		}
	}
}

// generateCerts generates some temporary certificates for kwbase demo.
func generateCerts(certsDir string) (err error) {
	caKeyPath := filepath.Join(certsDir, security.EmbeddedCAKey)
	// Create a CA-Key.
	if err := security.CreateCAPair(
		certsDir,
		caKeyPath,
		defaultKeySize,
		defaultCALifetime,
		false, /* allowKeyReuse */
		false, /*overwrite */
	); err != nil {
		return err
	}
	// Generate a certificate for the demo nodes.
	if err := security.CreateNodePair(
		certsDir,
		caKeyPath,
		defaultKeySize,
		defaultCertLifetime,
		false, /* overwrite */
		[]string{"127.0.0.1"},
	); err != nil {
		return err
	}
	// Create a certificate for the root user.
	return security.CreateClientPair(
		certsDir,
		caKeyPath,
		defaultKeySize,
		defaultCertLifetime,
		false, /* overwrite */
		security.RootUser,
		false, /* generatePKCS8Key */
	)
}

func (c *transientCluster) getNetworkURLForServer(
	serverIdx int, includeAppName bool,
) (string, error) {
	options := url.Values{}
	if includeAppName {
		options.Add("application_name", sqlbase.ReportableAppNamePrefix+"kwbase demo")
	}
	sqlURL := url.URL{
		Scheme: "postgres",
		Host:   c.servers[serverIdx].ServingSQLAddr(),
	}
	// For a demo cluster we don't use client TLS certs and instead use
	// password-based authentication with the password pre-filled in the
	// URL.
	if demoCtx.insecure {
		sqlURL.User = url.User(security.RootUser)
		options.Add("sslmode", "disable")
	} else {
		sqlURL.User = url.UserPassword(security.RootUser, defaultRootPassword)
		options.Add("sslmode", "require")
	}
	sqlURL.RawQuery = options.Encode()
	return sqlURL.String(), nil
}

func (c *transientCluster) setupUserAuth(ctx context.Context) error {
	ie := c.s.InternalExecutor().(*sql.InternalExecutor)
	_, err := ie.Exec(ctx, "set-root-password", nil, /* txn*/
		`ALTER USER $1 WITH PASSWORD $2`,
		security.RootUser,
		defaultRootPassword,
	)
	return err
}

// acquireDemoLicense begins an asynchronous process to obtain a
// temporary demo license from the Kw Labs website. It returns
// a channel that can be waited on if it is needed to wait on the
// license acquisition.
func (c *transientCluster) acquireDemoLicense(ctx context.Context) (chan error, error) {
	// Communicate information about license acquisition to services
	// that depend on it.
	licenseDone := make(chan error)
	if demoCtx.disableLicenseAcquisition {
		// If we are not supposed to acquire a license, close the channel
		// immediately so that future waiters don't hang.
		close(licenseDone)
	} else {
		// If we allow telemetry, then also try and get an enterprise license for the demo.
		// GetAndApplyLicense will be nil in the pure OSS/BSL build of kwbase.
		db, err := gosql.Open("postgres", c.connURL)
		if err != nil {
			return nil, err
		}
		go func() {
			defer db.Close()

			success, err := GetAndApplyLicense(db, c.s.ClusterID(), demoOrg)
			if err != nil {
				licenseDone <- err
				return
			}
			_ = success
			close(licenseDone)
		}()
	}

	return licenseDone, nil
}

// sockForServer generates the metadata for a unix socket for the given node.
// For example, node 1 gets socket /tmpdemodir/.s.PGSQL.26267,
// node 2 gets socket /tmpdemodir/.s.PGSQL.26268, etc.
func (c *transientCluster) sockForServer(nodeID roachpb.NodeID) unixSocketDetails {
	if !c.useSockets {
		return unixSocketDetails{}
	}
	defaultPort, _ := strconv.Atoi(base.DefaultPort)
	return unixSocketDetails{
		socketDir:  c.demoDir,
		portNumber: defaultPort + int(nodeID) - 1,
	}
}

type unixSocketDetails struct {
	socketDir  string
	portNumber int
}

func (s unixSocketDetails) exists() bool {
	return s.socketDir != ""
}

func (s unixSocketDetails) filename() string {
	if !s.exists() {
		// No socket configured.
		return ""
	}
	return filepath.Join(s.socketDir, fmt.Sprintf(".s.PGSQL.%d", s.portNumber))
}

func (s unixSocketDetails) String() string {
	options := url.Values{}
	options.Add("host", s.socketDir)
	options.Add("port", strconv.Itoa(s.portNumber))

	// Node: in the generated unix socket URL, a password is always
	// included even in insecure mode This is OK because in insecure
	// mode the password is not checked on the server.
	sqlURL := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(security.RootUser, defaultRootPassword),
		RawQuery: options.Encode(),
	}
	return sqlURL.String()
}

func (c *transientCluster) listDemoNodes(w io.Writer, justOne bool) {
	numNodesLive := 0
	for i, s := range c.servers {
		if s == nil {
			continue
		}
		numNodesLive++
		if numNodesLive > 1 && justOne {
			// Demo introduction: we just want conn parameters for one node.
			continue
		}

		nodeID := s.NodeID()
		if !justOne {
			// We skip the node ID if we're in the top level introduction of
			// the demo.
			fmt.Fprintf(w, "node %d:\n", nodeID)
		}
		// Print node ID and admin UI URL.
		fmt.Fprintf(w, "  (console) %s\n", s.AdminURL())
		// Print unix socket if defined.
		if c.useSockets {
			sock := c.sockForServer(nodeID)
			fmt.Fprintln(w, "  (sql)    ", sock)
		}
		// Print network URL if defined.
		netURL, err := c.getNetworkURLForServer(i, false /*includeAppName*/)
		if err != nil {
			fmt.Fprintln(stderr, errors.Wrap(err, "retrieving network URL"))
		} else {
			fmt.Fprintln(w, "  (sql/tcp)", netURL)
		}
		fmt.Fprintln(w)
	}
	if numNodesLive == 0 {
		fmt.Fprintln(w, "no demo nodes currently running")
	}
	if justOne && numNodesLive > 1 {
		fmt.Fprintln(w, `To display connection parameters for other nodes, use \demo ls.`)
	}
}
