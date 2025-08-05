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

package cli

import (
	"context"
	"net/url"
	"os"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// serverCfg is used as the client-side copy of default server
// parameters for CLI utilities.
//
// NB: `kwbase start` further annotates serverCfg for the newly created
// server.
var serverCfg = func() server.Config {
	st := cluster.MakeClusterSettings()
	settings.SetCanonicalValuesContainer(&st.SV)

	s := server.MakeConfig(context.Background(), st)
	s.SQLAuditLogDirName = &sqlAuditLogDir
	return s
}()

var sqlAuditLogDir log.DirName

var baseCfg = serverCfg.Config

// initCLIDefaults serves as the single point of truth for
// configuration defaults. It is suitable for calling between tests of
// the CLI utilities inside a single testing process.
func initCLIDefaults() {
	// We don't reset the pointers (because they are tied into the
	// flags), but instead overwrite the existing structs' values.
	baseCfg.InitDefaults()

	// isInteractive is only set to `true` by `kwbase sql` -- all
	// other client commands are non-interactive, regardless of whether
	// the standard input is a terminal.
	cliCtx.isInteractive = false
	// See also setCLIDefaultForTests() in cli_test.go.
	cliCtx.terminalOutput = isatty.IsTerminal(os.Stdout.Fd())
	cliCtx.tableDisplayFormat = tableDisplayTSV
	if cliCtx.terminalOutput {
		// See also setCLIDefaultForTests() in cli_test.go.
		cliCtx.tableDisplayFormat = tableDisplayTable
	}
	cliCtx.cmdTimeout = 0 // no timeout
	cliCtx.clientConnHost = ""
	cliCtx.clientConnPort = base.DefaultPort
	cliCtx.certPrincipalMap = nil
	cliCtx.sqlConnURL = ""
	cliCtx.sqlConnUser = ""
	cliCtx.sqlConnPasswd = ""
	cliCtx.sqlConnDBName = ""
	cliCtx.extraConnURLOptions = nil

	sqlCtx.setStmts = nil
	sqlCtx.execStmts = nil
	sqlCtx.repeatDelay = 0
	sqlCtx.safeUpdates = false
	sqlCtx.showTimes = false
	sqlCtx.debugMode = false
	sqlCtx.echo = false

	zipCtx.nodes = nodeSelection{}

	debugCtx.startKey = storage.NilKey
	debugCtx.endKey = storage.MVCCKeyMax
	debugCtx.values = false
	debugCtx.sizes = false
	debugCtx.replicated = false
	debugCtx.inputFile = ""
	debugCtx.printSystemConfig = false
	debugCtx.maxResults = 1000
	debugCtx.ballastSize = base.SizeSpec{InBytes: 1000000000}

	serverCfg.GoroutineDumpDirName = ""
	serverCfg.HeapProfileDirName = ""
	serverCfg.ReadyFn = nil
	serverCfg.DelayedBootstrapFn = nil
	serverCfg.SocketFile = ""
	serverCfg.JoinList = nil
	serverCfg.JoinPreferSRVRecords = false
	serverCfg.DefaultZoneConfig = zonepb.DefaultZoneConfig()
	// default ts_merge.days = 10d
	serverCfg.DefaultZoneConfig.TimeSeriesMergeDuration = time.Hour * 24 * 10
	serverCfg.DefaultSystemZoneConfig = zonepb.DefaultSystemZoneConfig()
	// Attempt to default serverCfg.SQLMemoryPoolSize to 25% if possible.
	if bytes, _ := memoryPercentResolver(25); bytes != 0 {
		serverCfg.SQLMemoryPoolSize = bytes
	}

	startCtx.serverInsecure = baseCfg.Insecure
	startCtx.serverSSLCertsDir = base.DefaultCertsDirectory
	startCtx.serverCertPrincipalMap = nil
	startCtx.serverListenAddr = ""
	startCtx.unencryptedLocalhostHTTP = false
	startCtx.tempDir = ""
	startCtx.externalIODir = ""
	startCtx.listeningURLFile = ""
	startCtx.pidFile = ""
	startCtx.inBackground = false
	startCtx.threadPoolSize = base.DefaultThreadPoolSize
	startCtx.taskQueueSize = base.DefaultTaskQueueSize
	startCtx.bufferPoolSize = base.DefaultBufferPoolSize
	startCtx.cgroupUser = base.DefaultCgroupUser
	startCtx.restfulPort = base.DefaultRestfulPort
	startCtx.restfulTimeOut = base.DefaultRestfulTimeOut
	startCtx.upgradeComplete = false

	quitCtx.serverDecommission = false
	quitCtx.drainWait = 10 * time.Minute

	nodeCtx.nodeDecommissionWait = nodeDecommissionWaitAll
	nodeCtx.statusShowRanges = false
	nodeCtx.statusShowStats = false
	nodeCtx.statusShowAll = false
	nodeCtx.statusShowDecommission = false

	cfg := tree.DefaultPrettyCfg()
	sqlfmtCtx.len = cfg.LineWidth
	sqlfmtCtx.useSpaces = !cfg.UseTabs
	sqlfmtCtx.tabWidth = cfg.TabWidth
	sqlfmtCtx.noSimplify = !cfg.Simplify
	sqlfmtCtx.align = (cfg.Align != tree.PrettyNoAlign)
	sqlfmtCtx.execStmts = nil

	systemBenchCtx.concurrency = 1
	systemBenchCtx.duration = 60 * time.Second
	systemBenchCtx.tempDir = "."
	systemBenchCtx.writeSize = 32 << 10
	systemBenchCtx.syncInterval = 512 << 10

	networkBenchCtx.server = true
	networkBenchCtx.port = 8081
	networkBenchCtx.addresses = []string{"localhost:8081"}

	demoCtx.nodes = 1
	demoCtx.sqlPoolMemorySize = 128 << 20 // 128MB, chosen to fit 9 nodes on 2GB machine.
	demoCtx.cacheSize = 64 << 20          // 64MB, chosen to fit 9 nodes on 2GB machine.
	demoCtx.useEmptyDatabase = false
	demoCtx.simulateLatency = false
	demoCtx.runWorkload = false
	demoCtx.localities = nil
	demoCtx.geoPartitionedReplicas = false
	demoCtx.disableTelemetry = false
	demoCtx.disableLicenseAcquisition = false
	demoCtx.transientCluster = nil
	demoCtx.insecure = true

	authCtx.validityPeriod = 1 * time.Hour

	setStmtDiagContextDefaults()

	initPreFlagsDefaults()

	// Clear the "Changed" state of all the registered command-line flags.
	clearFlagChanges(kwbaseCmd)
}

func clearFlagChanges(cmd *cobra.Command) {
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) { f.Changed = false })
	for _, subCmd := range cmd.Commands() {
		clearFlagChanges(subCmd)
	}
}

// cliContext captures the command-line parameters of most CLI commands.
type cliContext struct {
	// Embed the base context.
	*base.Config

	// isInteractive indicates whether the session is interactive, that
	// is, the commands executed are extremely likely to be *input* from
	// a human user: the standard input is a terminal and `-e` was not
	// used (the shell has a prompt).
	isInteractive bool

	// terminalOutput indicates whether output is going to a terminal,
	// that is, it is not going to a file, another program for automated
	// processing, etc.: the standard output is a terminal.
	terminalOutput bool

	// tableDisplayFormat indicates how to format result tables.
	tableDisplayFormat tableDisplayFormat

	// cmdTimeout sets the maximum run time for the command.
	// Commands that wish to use this must use cmdTimeoutContext().
	cmdTimeout time.Duration

	// clientConnHost is the hostname/address to use to connect to a server.
	clientConnHost string

	// clientConnPort is the port name/number to use to connect to a server.
	clientConnPort string

	// certPrincipalMap is the cert-principal:db-principal map.
	certPrincipalMap []string

	// for CLI commands that use the SQL interface, these parameters
	// determine how to connect to the server.
	sqlConnURL, sqlConnUser, sqlConnDBName string

	// The client password to use. This can be set via the --url flag.
	sqlConnPasswd string

	// extraConnURLOptions contains any additional query URL options
	// specified in --url that do not have discrete equivalents.
	extraConnURLOptions url.Values
}

// cliCtx captures the command-line parameters common to most CLI utilities.
// Defaults set by InitCLIDefaults() above.
var cliCtx = cliContext{Config: baseCfg}

// sqlCtx captures the command-line parameters of the `sql` command.
// Defaults set by InitCLIDefaults() above.
var sqlCtx = struct {
	*cliContext

	// setStmts is a list of \set commands to execute before entering the sql shell.
	setStmts statementsValue

	// execStmts is a list of statements to execute.
	execStmts statementsValue

	// repeatDelay indicates that the execStmts should be "watched"
	// at the specified time interval. Zero disables
	// the watch.
	repeatDelay time.Duration

	// safeUpdates indicates whether to set sql_safe_updates in the CLI
	// shell.
	safeUpdates bool

	// showTimes indicates whether to display query times after each result line.
	showTimes bool

	// echo, when set, requests that SQL queries sent to the server are
	// also printed out on the client.
	echo bool

	// debugMode, when set, overrides the defaults to disable as much
	// "intelligent behavior" in the SQL shell as possible and become
	// more verbose (sets echo).
	debugMode bool

	// interactEOF, when set, output after each statement finished.
	interactEOF string
}{cliContext: &cliCtx}

// zipCtx captures the command-line parameters of the `zip` command.
// Defaults set by InitCLIDefaults() above.
var zipCtx struct {
	nodes nodeSelection
}

// authCtx captures the command-line parameters of the `auth-session`
// command.
var authCtx struct {
	onlyCookie     bool
	validityPeriod time.Duration
}

// debugCtx captures the command-line parameters of the `debug` command.
// Defaults set by InitCLIDefaults() above.
var debugCtx struct {
	startKey, endKey  storage.MVCCKey
	values            bool
	sizes             bool
	replicated        bool
	inputFile         string
	ballastSize       base.SizeSpec
	printSystemConfig bool
	maxResults        int64
}

// startCtx captures the command-line arguments for the `start` command.
// Defaults set by InitCLIDefaults() above.
var startCtx struct {
	// server-specific values of some flags.
	serverInsecure         bool
	serverSSLCertsDir      string
	serverCertPrincipalMap []string
	serverListenAddr       string

	// if specified, this forces the HTTP listen addr to localhost
	// and disables TLS on the HTTP listener.
	unencryptedLocalhostHTTP bool

	// temporary directory to use to spill computation results to disk.
	tempDir string

	// directory to use for remotely-initiated operations that can
	// specify node-local I/O paths, like BACKUP/RESTORE/IMPORT.
	externalIODir string

	// inBackground is set to true when restarting in the
	// background after --background was processed.
	inBackground bool

	// listeningURLFile indicates the file to which the server writes
	// its listening URL when it is ready.
	listeningURLFile string

	// pidFile indicates the file to which the server writes its PID
	// when it is ready.
	pidFile string

	// logging settings specific to file logging.
	logDir log.DirName

	// ExtraStartupItem is used to set whether startups ME and/or AE.
	extraStartupItem string

	// AE executor thread pool size.
	threadPoolSize string

	// AE executor task queue size
	taskQueueSize string

	// AE executor buffer pool size
	bufferPoolSize string

	// Control groups user
	cgroupUser string

	// Restful API port
	restfulPort string

	// Restful API timeout
	restfulTimeOut string

	// upgrade complete
	upgradeComplete bool
}

// quitCtx captures the command-line parameters of the `quit` and
// `node drain` commands.
// Defaults set by InitCLIDefaults() above.
var quitCtx struct {
	// serverDecommission indicates the server should be decommissioned
	// before it is drained.
	serverDecommission bool
	// drainWait is the amount of time to wait for the server
	// to drain. Set to 0 to disable a timeout (let the server decide).
	drainWait time.Duration
}

// nodeCtx captures the command-line parameters of the `node` command.
// Defaults set by InitCLIDefaults() above.
var nodeCtx struct {
	nodeDecommissionWait   nodeDecommissionWaitType
	statusShowRanges       bool
	statusShowStats        bool
	statusShowDecommission bool
	statusShowAll          bool
}

// systemBenchCtx captures the command-line parameters of the `systembench` command.
// Defaults set by InitCLIDefaults() above.
var systemBenchCtx struct {
	concurrency  int
	duration     time.Duration
	tempDir      string
	writeSize    int64
	syncInterval int64
}

var networkBenchCtx struct {
	server    bool
	port      int
	addresses []string
	latency   bool
}

// sqlfmtCtx captures the command-line parameters of the `sqlfmt` command.
// Defaults set by InitCLIDefaults() above.
var sqlfmtCtx struct {
	len        int
	useSpaces  bool
	tabWidth   int
	noSimplify bool
	align      bool
	execStmts  statementsValue
}

// demoCtx captures the command-line parameters of the `demo` command.
// Defaults set by InitCLIDefaults() above.
var demoCtx struct {
	nodes                     int
	sqlPoolMemorySize         int64
	cacheSize                 int64
	disableTelemetry          bool
	disableLicenseAcquisition bool
	useEmptyDatabase          bool
	runWorkload               bool
	localities                demoLocalityList
	geoPartitionedReplicas    bool
	simulateLatency           bool
	transientCluster          *transientCluster
	insecure                  bool
}

// stmtDiagCtx captures the command-line parameters of the 'statement-diag'
// command.
var stmtDiagCtx struct {
	all bool
}

func setStmtDiagContextDefaults() {
	stmtDiagCtx.all = false
}
