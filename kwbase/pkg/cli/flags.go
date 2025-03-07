// Copyright 2015 The Cockroach Authors.
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
	"flag"
	"net"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/cli/cliflags"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/log/logflags"
	"gitee.com/kwbasedb/kwbase/pkg/util/netutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// special global variables used by flag variable definitions below.
// These do not correspond directly to the configuration parameters
// used as input by the CLI commands (these are defined in context
// structs in context.go). Instead, they are used at the *end* of
// command-line parsing to override the defaults in the context
// structs.
//
// Corollaries:
//   - it would be a programming error to access these variables directly
//     outside of this file (flags.go)
//   - the underlying context parameters must receive defaults in
//     initCLIDefaults() even when they are otherwise overridden by the
//     flags logic, because some tests to not use the flag logic at all.
var serverListenPort, serverSocketDir string
var serverAdvertiseAddr, serverAdvertisePort string
var serverSQLAddr, serverSQLPort string
var serverSQLAdvertiseAddr, serverSQLAdvertisePort string
var serverHTTPAddr, serverHTTPPort string
var localityAdvertiseHosts localityList

// initPreFlagsDefaults initializes the values of the global variables
// defined above.
func initPreFlagsDefaults() {
	serverListenPort = base.DefaultPort
	serverSocketDir = ""
	serverAdvertiseAddr = ""
	serverAdvertisePort = ""

	serverSQLAddr = ""
	serverSQLPort = ""
	serverSQLAdvertiseAddr = ""
	serverSQLAdvertisePort = ""

	serverHTTPAddr = ""
	serverHTTPPort = base.DefaultHTTPPort

	localityAdvertiseHosts = localityList{}
}

// AddPersistentPreRunE add 'fn' as a persistent pre-run function to 'cmd'.
// If the command has an existing pre-run function, it is saved and will be called
// at the beginning of 'fn'.
// This allows an arbitrary number of pre-run functions with ordering based
// on the order in which AddPersistentPreRunE is called (usually package init order).
func AddPersistentPreRunE(cmd *cobra.Command, fn func(*cobra.Command, []string) error) {
	// Save any existing hooks.
	wrapped := cmd.PersistentPreRunE

	cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// Run the previous hook if it exists.
		if wrapped != nil {
			if err := wrapped(cmd, args); err != nil {
				return err
			}
		}

		// Now we can call the new function.
		return fn(cmd, args)
	}
}

// StringFlag creates a string flag and registers it with the FlagSet.
func StringFlag(f *pflag.FlagSet, valPtr *string, flagInfo cliflags.FlagInfo, defaultVal string) {
	f.StringVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, flagInfo.Usage())

	registerEnvVarDefault(f, flagInfo)
}

// IntFlag creates an int flag and registers it with the FlagSet.
func IntFlag(f *pflag.FlagSet, valPtr *int, flagInfo cliflags.FlagInfo, defaultVal int) {
	f.IntVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, flagInfo.Usage())

	registerEnvVarDefault(f, flagInfo)
}

// BoolFlag creates a bool flag and registers it with the FlagSet.
func BoolFlag(f *pflag.FlagSet, valPtr *bool, flagInfo cliflags.FlagInfo, defaultVal bool) {
	f.BoolVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, flagInfo.Usage())

	registerEnvVarDefault(f, flagInfo)
}

// DurationFlag creates a duration flag and registers it with the FlagSet.
func DurationFlag(
	f *pflag.FlagSet, valPtr *time.Duration, flagInfo cliflags.FlagInfo, defaultVal time.Duration,
) {
	f.DurationVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, flagInfo.Usage())

	registerEnvVarDefault(f, flagInfo)
}

// VarFlag creates a custom-variable flag and registers it with the FlagSet.
func VarFlag(f *pflag.FlagSet, value pflag.Value, flagInfo cliflags.FlagInfo) {
	f.VarP(value, flagInfo.Name, flagInfo.Shorthand, flagInfo.Usage())

	registerEnvVarDefault(f, flagInfo)
}

// StringSlice creates a string slice flag and registers it with the FlagSet.
func StringSlice(
	f *pflag.FlagSet, valPtr *[]string, flagInfo cliflags.FlagInfo, defaultVal []string,
) {
	f.StringSliceVar(valPtr, flagInfo.Name, defaultVal, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo)
}

// aliasStrVar wraps a string configuration option and is meant
// to be used in addition to / next to another flag that targets the
// same option. It does not implement "default values" so that the
// main flag can perform the default logic.
type aliasStrVar struct{ p *string }

// String implements the pflag.Value interface.
func (a aliasStrVar) String() string { return "" }

// Set implements the pflag.Value interface.
func (a aliasStrVar) Set(v string) error {
	if v != "" {
		*a.p = v
	}
	return nil
}

// Type implements the pflag.Value interface.
func (a aliasStrVar) Type() string { return "string" }

// addrSetter wraps a address/port configuration option pair and
// enables setting them both with a single command-line flag.
type addrSetter struct {
	addr *string
	port *string
}

// String implements the pflag.Value interface.
func (a addrSetter) String() string {
	return net.JoinHostPort(*a.addr, *a.port)
}

// Type implements the pflag.Value interface.
func (a addrSetter) Type() string { return "<addr/host>[:<port>]" }

// Set implements the pflag.Value interface.
func (a addrSetter) Set(v string) error {
	addr, port, err := netutil.SplitHostPort(v, *a.port)
	if err != nil {
		return err
	}
	*a.addr = addr
	*a.port = port
	return nil
}

// clusterNameSetter wraps the cluster name variable
// and verifies its format during configuration.
type clusterNameSetter struct {
	clusterName *string
}

// String implements the pflag.Value interface.
func (a clusterNameSetter) String() string { return *a.clusterName }

// Type implements the pflag.Value interface.
func (a clusterNameSetter) Type() string { return "<identifier>" }

// Set implements the pflag.Value interface.
func (a clusterNameSetter) Set(v string) error {
	if v == "" {
		return errors.New("cluster name cannot be empty")
	}
	if len(v) > maxClusterNameLength {
		return errors.Newf(`cluster name can contain at most %d characters`, maxClusterNameLength)
	}
	if !clusterNameRe.MatchString(v) {
		return errClusterNameInvalidFormat
	}
	*a.clusterName = v
	return nil
}

var errClusterNameInvalidFormat = errors.New(`cluster name must contain only letters, numbers or the "-" and "." characters`)

// clusterNameRe matches valid cluster names.
// For example, "a", "a123" and "a-b" are OK,
// but "0123", "a-" and "123a" are not OK.
var clusterNameRe = regexp.MustCompile(`^[a-zA-Z](?:[-a-zA-Z0-9]*[a-zA-Z0-9]|)$`)

const maxClusterNameLength = 256

const backgroundEnvVar = "KWBASE_BACKGROUND_RESTART"

// flagSetForCmd is a replacement for cmd.Flag() that properly merges
// persistent and local flags, until the upstream bug
// https://github.com/spf13/cobra/issues/961 has been fixed.
func flagSetForCmd(cmd *cobra.Command) *pflag.FlagSet {
	_ = cmd.LocalFlags() // force merge persistent+local flags
	return cmd.Flags()
}

func init() {
	initCLIDefaults()
	defer func() {
		if err := processEnvVarDefaults(); err != nil {
			panic(err)
		}
	}()

	// Every command but start will inherit the following setting.
	AddPersistentPreRunE(kwbaseCmd, func(cmd *cobra.Command, _ []string) error {
		if err := extraClientFlagInit(); err != nil {
			return err
		}
		return setDefaultStderrVerbosity(cmd, log.Severity_WARNING)
	})

	// Add a pre-run command for `start` and `start-single-node`.
	for _, cmd := range StartCmds {
		AddPersistentPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
			// Finalize the configuration of network and logging settings.
			if err := extraServerFlagInit(cmd); err != nil {
				return err
			}
			return setDefaultStderrVerbosity(cmd, log.Severity_INFO)
		})
	}

	// Map any flags registered in the standard "flag" package into the
	// top-level kwbase command.
	pf := kwbaseCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		flag := pflag.PFlagFromGoFlag(f)
		// TODO(peter): Decide if we want to make the lightstep flags visible.
		if strings.HasPrefix(flag.Name, "lightstep_") {
			flag.Hidden = true
		}
		if strings.HasPrefix(flag.Name, "httptest.") {
			// If we test the cli commands in tests, we may end up transitively
			// importing httptest, for example via `testify/assert`. Make sure
			// it doesn't show up in the output or it will confuse tests.
			flag.Hidden = true
		}
		switch flag.Name {
		case logflags.NoRedirectStderrName:
			flag.Hidden = true
		case logflags.ShowLogsName:
			flag.Hidden = true
		case logflags.LogToStderrName:
			// The actual default value for --logtostderr is overridden in
			// cli.Main. We don't override it here as doing so would affect all of
			// the cli tests and any package which depends on cli. The following line
			// is only overriding the default value for the pflag package (and what
			// is visible in help text), not the stdlib flag value.
			flag.DefValue = "NONE"
		case logflags.LogDirName,
			logflags.LogFileMaxSizeName,
			logflags.LogFilesCombinedMaxSizeName,
			logflags.LogFileVerbosityThresholdName:
			// The --log-dir* and --log-file* flags are specified only for the
			// `start` and `demo` commands.
			return
		}
		pf.AddFlag(flag)
	})

	// When a flag is specified but without a value, pflag assigns its
	// NoOptDefVal to it via Set(). This is also the value used to
	// generate the implicit assigned value in the usage text
	// (e.g. "--logtostderr[=XXXXX]"). We can't populate a real default
	// unfortunately, because the default depends on which command is
	// run (`start` vs. the rest), and pflag does not support
	// per-command NoOptDefVals. So we need some sentinel value here
	// that we can recognize when setDefaultStderrVerbosity() is called
	// after argument parsing. We could use UNKNOWN, but to ensure that
	// the usage text is somewhat less confusing to the user, we use the
	// special severity value DEFAULT instead.
	pf.Lookup(logflags.LogToStderrName).NoOptDefVal = log.Severity_DEFAULT.String()

	// Remember we are starting in the background as the `start` command will
	// avoid printing some messages to standard output in that case.
	_, startCtx.inBackground = envutil.EnvString(backgroundEnvVar, 1)

	for _, cmd := range StartCmds {
		f := cmd.Flags()

		// Server flags.
		VarFlag(f, addrSetter{&startCtx.serverListenAddr, &serverListenPort}, cliflags.ListenAddr)
		VarFlag(f, addrSetter{&serverAdvertiseAddr, &serverAdvertisePort}, cliflags.AdvertiseAddr)
		VarFlag(f, addrSetter{&serverSQLAddr, &serverSQLPort}, cliflags.ListenSQLAddr)
		VarFlag(f, addrSetter{&serverSQLAdvertiseAddr, &serverSQLAdvertisePort}, cliflags.SQLAdvertiseAddr)
		VarFlag(f, addrSetter{&serverHTTPAddr, &serverHTTPPort}, cliflags.ListenHTTPAddr)
		StringFlag(f, &serverSocketDir, cliflags.SocketDir, serverSocketDir)
		// --socket is deprecated as of 20.1.
		// TODO(knz): remove in 20.2.
		StringFlag(f, &serverCfg.SocketFile, cliflags.Socket, serverCfg.SocketFile)
		_ = f.MarkDeprecated(cliflags.Socket.Name, "use the --socket-dir and --listen-addr flags instead")
		BoolFlag(f, &startCtx.unencryptedLocalhostHTTP, cliflags.UnencryptedLocalhostHTTP, startCtx.unencryptedLocalhostHTTP)

		// Backward-compatibility flags.

		// These are deprecated but until we have qualitatively new
		// functionality in the flags above, there is no need to nudge the
		// user away from them with a deprecation warning. So we keep
		// them, but hidden from docs so that they don't appear as
		// redundant with the main flags.
		VarFlag(f, aliasStrVar{&startCtx.serverListenAddr}, cliflags.ServerHost)
		_ = f.MarkHidden(cliflags.ServerHost.Name)
		VarFlag(f, aliasStrVar{&serverListenPort}, cliflags.ServerPort)
		_ = f.MarkHidden(cliflags.ServerPort.Name)

		VarFlag(f, aliasStrVar{&serverAdvertiseAddr}, cliflags.AdvertiseHost)
		_ = f.MarkHidden(cliflags.AdvertiseHost.Name)
		VarFlag(f, aliasStrVar{&serverAdvertisePort}, cliflags.AdvertisePort)
		_ = f.MarkHidden(cliflags.AdvertisePort.Name)

		VarFlag(f, aliasStrVar{&serverHTTPAddr}, cliflags.ListenHTTPAddrAlias)
		_ = f.MarkHidden(cliflags.ListenHTTPAddrAlias.Name)
		VarFlag(f, aliasStrVar{&serverHTTPPort}, cliflags.ListenHTTPPort)
		_ = f.MarkHidden(cliflags.ListenHTTPPort.Name)

		// More server flags.

		VarFlag(f, &localityAdvertiseHosts, cliflags.LocalityAdvertiseAddr)

		StringFlag(f, &serverCfg.Attrs, cliflags.Attrs, serverCfg.Attrs)
		VarFlag(f, &serverCfg.Locality, cliflags.Locality)

		VarFlag(f, &serverCfg.Stores, cliflags.Store)
		VarFlag(f, &serverCfg.TsStores, cliflags.TsStore)
		VarFlag(f, &serverCfg.StorageEngine, cliflags.StorageEngine)
		VarFlag(f, &serverCfg.MaxOffset, cliflags.MaxOffset)
		StringFlag(f, &serverCfg.ClockDevicePath, cliflags.ClockDevice, "")

		StringFlag(f, &startCtx.listeningURLFile, cliflags.ListeningURLFile, startCtx.listeningURLFile)

		StringFlag(f, &startCtx.pidFile, cliflags.PIDFile, startCtx.pidFile)

		// The flag is used to set whether startups ME and/or AE.
		StringFlag(f, &startCtx.extraStartupItem, cliflags.ExtraStartupItem, startCtx.extraStartupItem)

		// Use a separate variable to store the value of ServerInsecure.
		// We share the default with the ClientInsecure flag.
		BoolFlag(f, &startCtx.serverInsecure, cliflags.ServerInsecure, startCtx.serverInsecure)
		// If start with flag --import-path, accept a path of import file
		StringFlag(f, &serverCfg.ImportPath, cliflags.ImportPath, "")
		// If start with flag --import-password, accept a password of decompress file
		StringFlag(f, &serverCfg.ImportPassword, cliflags.ImportPassword, "")
		// If start with flag --import-filename, accept a filename of decompress file
		StringFlag(f, &serverCfg.ImportFileName, cliflags.ImportFileName, "")

		// set thread pool size with --thread-pool-size
		StringFlag(f, &startCtx.threadPoolSize, cliflags.ThreadPoolSize, startCtx.threadPoolSize)

		// set thread queue size with --task-queue-size
		StringFlag(f, &startCtx.taskQueueSize, cliflags.TaskQueueSize, startCtx.taskQueueSize)

		// set buffer pool size with --buffer-pool-size
		StringFlag(f, &startCtx.bufferPoolSize, cliflags.BufferPoolSize, startCtx.bufferPoolSize)

		// set cgroup user with --cgroup-user
		StringFlag(f, &startCtx.cgroupUser, cliflags.CgroupUser, startCtx.cgroupUser)

		// set restful api port with --restful-port
		StringFlag(f, &startCtx.restfulPort, cliflags.RestfulPort, startCtx.restfulPort)

		// set restful api port with --restful-port
		StringFlag(f, &startCtx.restfulTimeOut, cliflags.RestfulTimeOut, startCtx.restfulTimeOut)

		// set upgradeComplete with --upgrade-complete
		BoolFlag(f, &startCtx.upgradeComplete, cliflags.UpgradeComplete, startCtx.upgradeComplete)

		// Enable/disable various external storage endpoints.
		serverCfg.ExternalIOConfig = base.ExternalIOConfig{}
		BoolFlag(f, &serverCfg.ExternalIOConfig.DisableHTTP,
			cliflags.ExternalIODisableHTTP, false)
		BoolFlag(f, &serverCfg.ExternalIOConfig.DisableImplicitCredentials,
			cliflags.ExtenralIODisableImplicitCredentials, false)

		// Certificates directory. Use a server-specific flag and value to ignore environment
		// variables, but share the same default.
		StringFlag(f, &startCtx.serverSSLCertsDir, cliflags.ServerCertsDir, startCtx.serverSSLCertsDir)

		// Certificate principal map.
		StringSlice(f, &startCtx.serverCertPrincipalMap,
			cliflags.CertPrincipalMap, startCtx.serverCertPrincipalMap)

		// Cluster joining flags. We need to enable this both for 'start'
		// and 'start-single-node' although the latter does not support
		// --join, because it delegates its logic to that of 'start', and
		// 'start' will check that the flag is properly defined.
		VarFlag(f, &serverCfg.JoinList, cliflags.Join)
		BoolFlag(f, &serverCfg.JoinPreferSRVRecords, cliflags.JoinPreferSRVRecords, serverCfg.JoinPreferSRVRecords)
		VarFlag(f, clusterNameSetter{&baseCfg.ClusterName}, cliflags.ClusterName)
		BoolFlag(f, &baseCfg.DisableClusterNameVerification,
			cliflags.DisableClusterNameVerification, baseCfg.DisableClusterNameVerification)
		if cmd == startSingleNodeCmd {
			// Even though all server flags are supported for
			// 'start-single-node', we intend that command to be used by
			// beginners / developers running on a single machine. To
			// enhance the UX, we hide the flags since they are not directly
			// relevant when running a single node.
			_ = f.MarkHidden(cliflags.Join.Name)
			_ = f.MarkHidden(cliflags.ClusterName.Name)
			_ = f.MarkHidden(cliflags.DisableClusterNameVerification.Name)
			_ = f.MarkHidden(cliflags.MaxOffset.Name)
			_ = f.MarkHidden(cliflags.LocalityAdvertiseAddr.Name)
			_ = f.MarkHidden(cliflags.AdvertiseAddr.Name)
			_ = f.MarkHidden(cliflags.SQLAdvertiseAddr.Name)
		}

		// Engine flags.
		VarFlag(f, cacheSizeValue, cliflags.Cache)
		VarFlag(f, sqlSizeValue, cliflags.SQLMem)
		// N.B. diskTempStorageSizeValue.ResolvePercentage() will be called after
		// the stores flag has been parsed and the storage device that a percentage
		// refers to becomes known.
		VarFlag(f, diskTempStorageSizeValue, cliflags.SQLTempStorage)
		StringFlag(f, &startCtx.tempDir, cliflags.TempDir, startCtx.tempDir)
		StringFlag(f, &startCtx.externalIODir, cliflags.ExternalIODir, startCtx.externalIODir)

		VarFlag(f, serverCfg.SQLAuditLogDirName, cliflags.SQLAuditLogDirName)
	}

	// Flags that apply to commands that start servers.
	serverCmds := append(StartCmds, demoCmd)
	serverCmds = append(serverCmds, demoCmd.Commands()...)
	for _, cmd := range serverCmds {
		f := cmd.Flags()
		VarFlag(f, &startCtx.logDir, cliflags.LogDir)
		VarFlag(f,
			pflag.PFlagFromGoFlag(flag.Lookup(logflags.LogFilesCombinedMaxSizeName)).Value,
			cliflags.LogDirMaxSize)
		VarFlag(f,
			pflag.PFlagFromGoFlag(flag.Lookup(logflags.LogFileMaxSizeName)).Value,
			cliflags.LogFileMaxSize)
		VarFlag(f,
			pflag.PFlagFromGoFlag(flag.Lookup(logflags.LogFileVerbosityThresholdName)).Value,
			cliflags.LogFileVerbosity)

		// Report flag usage for server commands in telemetry. We do this
		// only for server commands, as there is no point in accumulating
		// telemetry if there's no telemetry reporting loop being started.
		AddPersistentPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
			prefix := "cli." + cmd.Name()
			// Count flag usage.
			cmd.Flags().Visit(func(fl *pflag.Flag) {
				telemetry.Count(prefix + ".explicitflags." + fl.Name)
			})
			// Also report use of the command on its own. This is necessary
			// so we can compute flag usage as a % of total command invocations.
			telemetry.Count(prefix + ".runs")
			return nil
		})
	}

	for _, cmd := range certCmds {
		f := cmd.Flags()
		// All certs commands need the certificate directory.
		StringFlag(f, &baseCfg.SSLCertsDir, cliflags.CertsDir, baseCfg.SSLCertsDir)
		// All certs commands get the certificate principal map.
		StringSlice(f, &cliCtx.certPrincipalMap,
			cliflags.CertPrincipalMap, cliCtx.certPrincipalMap)
	}

	for _, cmd := range []*cobra.Command{createCACertCmd, createClientCACertCmd} {
		f := cmd.Flags()
		// CA certificates have a longer expiration time.
		DurationFlag(f, &caCertificateLifetime, cliflags.CertificateLifetime, defaultCALifetime)
		// The CA key can be re-used if it exists.
		BoolFlag(f, &allowCAKeyReuse, cliflags.AllowCAKeyReuse, false)
	}

	for _, cmd := range []*cobra.Command{createNodeCertCmd, createClientCertCmd} {
		f := cmd.Flags()
		DurationFlag(f, &certificateLifetime, cliflags.CertificateLifetime, defaultCertLifetime)
	}

	// The remaining flags are shared between all cert-generating functions.
	for _, cmd := range []*cobra.Command{createCACertCmd, createClientCACertCmd, createNodeCertCmd, createClientCertCmd} {
		f := cmd.Flags()
		StringFlag(f, &baseCfg.SSLCAKey, cliflags.CAKey, baseCfg.SSLCAKey)
		IntFlag(f, &keySize, cliflags.KeySize, defaultKeySize)
		BoolFlag(f, &overwriteFiles, cliflags.OverwriteFiles, false)
	}
	// PKCS8 key format is only available for the client cert command.
	BoolFlag(createClientCertCmd.Flags(), &generatePKCS8Key, cliflags.GeneratePKCS8Key, false)

	clientCmds := []*cobra.Command{
		debugGossipValuesCmd,
		debugTimeSeriesDumpCmd,
		debugZipCmd,
		genHAProxyCmd,
		initCmd,
		quitCmd,
		sqlShellCmd,
		/* StartCmds are covered above */
	}
	clientCmds = append(clientCmds, authCmds...)
	clientCmds = append(clientCmds, nodeCmds...)
	clientCmds = append(clientCmds, systemBenchCmds...)
	clientCmds = append(clientCmds, nodeLocalCmds...)
	clientCmds = append(clientCmds, stmtDiagCmds...)
	for _, cmd := range clientCmds {
		f := cmd.PersistentFlags()
		VarFlag(f, addrSetter{&cliCtx.clientConnHost, &cliCtx.clientConnPort}, cliflags.ClientHost)
		StringFlag(f, &cliCtx.clientConnPort, cliflags.ClientPort, cliCtx.clientConnPort)
		_ = f.MarkHidden(cliflags.ClientPort.Name)

		BoolFlag(f, &baseCfg.Insecure, cliflags.ClientInsecure, baseCfg.Insecure)

		// Certificate flags.
		StringFlag(f, &baseCfg.SSLCertsDir, cliflags.CertsDir, baseCfg.SSLCertsDir)
		// Certificate principal map.
		StringSlice(f, &cliCtx.certPrincipalMap,
			cliflags.CertPrincipalMap, cliCtx.certPrincipalMap)
	}

	// Auth commands.
	{
		f := loginCmd.Flags()
		DurationFlag(f, &authCtx.validityPeriod, cliflags.AuthTokenValidityPeriod, authCtx.validityPeriod)
		BoolFlag(f, &authCtx.onlyCookie, cliflags.OnlyCookie, authCtx.onlyCookie)
	}

	timeoutCmds := []*cobra.Command{
		statusNodeCmd,
		lsNodesCmd,
		debugZipCmd,
		// If you add something here, make sure the actual implementation
		// of the command uses `cmdTimeoutContext(.)` or it will ignore
		// the timeout.
	}

	for _, cmd := range timeoutCmds {
		DurationFlag(cmd.Flags(), &cliCtx.cmdTimeout, cliflags.Timeout, cliCtx.cmdTimeout)
	}

	// Node Status command.
	{
		f := statusNodeCmd.Flags()
		BoolFlag(f, &nodeCtx.statusShowRanges, cliflags.NodeRanges, nodeCtx.statusShowRanges)
		BoolFlag(f, &nodeCtx.statusShowStats, cliflags.NodeStats, nodeCtx.statusShowStats)
		BoolFlag(f, &nodeCtx.statusShowAll, cliflags.NodeAll, nodeCtx.statusShowAll)
		BoolFlag(f, &nodeCtx.statusShowDecommission, cliflags.NodeDecommission, nodeCtx.statusShowDecommission)
	}

	// HDD Bench command.
	{
		f := seqWriteBench.Flags()
		VarFlag(f, humanizeutil.NewBytesValue(&systemBenchCtx.writeSize), cliflags.WriteSize)
		VarFlag(f, humanizeutil.NewBytesValue(&systemBenchCtx.syncInterval), cliflags.SyncInterval)
	}

	// Network Bench command.
	{
		f := networkBench.Flags()
		BoolFlag(f, &networkBenchCtx.server, cliflags.BenchServer, networkBenchCtx.server)
		IntFlag(f, &networkBenchCtx.port, cliflags.BenchPort, networkBenchCtx.port)
		StringSlice(f, &networkBenchCtx.addresses, cliflags.BenchAddresses, networkBenchCtx.addresses)
		BoolFlag(f, &networkBenchCtx.latency, cliflags.BenchLatency, networkBenchCtx.latency)
	}

	// Bench command.
	{
		for _, cmd := range systemBenchCmds {
			f := cmd.Flags()
			IntFlag(f, &systemBenchCtx.concurrency, cliflags.BenchConcurrency, systemBenchCtx.concurrency)
			DurationFlag(f, &systemBenchCtx.duration, cliflags.BenchDuration, systemBenchCtx.duration)
			StringFlag(f, &systemBenchCtx.tempDir, cliflags.TempDir, systemBenchCtx.tempDir)
		}
	}

	// Zip command.
	{
		f := debugZipCmd.Flags()
		VarFlag(f, &zipCtx.nodes.inclusive, cliflags.ZipNodes)
		VarFlag(f, &zipCtx.nodes.exclusive, cliflags.ZipExcludeNodes)
	}

	// Upgrading command.
	{
		f := upgradeNodeCmd.Flags()
		VarFlag(f, &nodeCtx.nodeDecommissionWait, cliflags.Wait)
	}

	// Decommission command.
	VarFlag(decommissionNodeCmd.Flags(), &nodeCtx.nodeDecommissionWait, cliflags.Wait)

	// Quit command.
	{
		f := quitCmd.Flags()
		// The --decommission flag for quit is now deprecated.
		// Users should use `node decommission` and then `quit` after
		// decommission completes.
		// TODO(knz): Remove in 20.2.
		BoolFlag(f, &quitCtx.serverDecommission, cliflags.Decommission, quitCtx.serverDecommission)
		_ = f.MarkDeprecated(cliflags.Decommission.Name, `use 'kwbase node decommission' then 'kwbase quit' instead`)
	}

	// Quit and node drain.
	for _, cmd := range []*cobra.Command{quitCmd, drainNodeCmd} {
		f := cmd.Flags()
		DurationFlag(f, &quitCtx.drainWait, cliflags.DrainWait, quitCtx.drainWait)
	}

	// SQL and demo commands.
	for _, cmd := range append([]*cobra.Command{sqlShellCmd, demoCmd}, demoCmd.Commands()...) {
		f := cmd.Flags()
		VarFlag(f, &sqlCtx.setStmts, cliflags.Set)
		VarFlag(f, &sqlCtx.execStmts, cliflags.Execute)
		DurationFlag(f, &sqlCtx.repeatDelay, cliflags.Watch, sqlCtx.repeatDelay)
		BoolFlag(f, &sqlCtx.safeUpdates, cliflags.SafeUpdates, sqlCtx.safeUpdates)
		BoolFlag(f, &sqlCtx.debugMode, cliflags.CliDebugMode, sqlCtx.debugMode)
	}

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{sqlShellCmd, demoCmd}
	sqlCmds = append(sqlCmds, authCmds...)
	sqlCmds = append(sqlCmds, demoCmd.Commands()...)
	sqlCmds = append(sqlCmds, stmtDiagCmds...)
	sqlCmds = append(sqlCmds, nodeLocalCmds...)
	for _, cmd := range sqlCmds {
		f := cmd.Flags()
		BoolFlag(f, &sqlCtx.echo, cliflags.EchoSQL, sqlCtx.echo)

		if cmd != demoCmd {
			VarFlag(f, urlParser{cmd, &cliCtx, false /* strictSSL */}, cliflags.URL)
			StringFlag(f, &cliCtx.sqlConnUser, cliflags.User, cliCtx.sqlConnUser)

			// Even though SQL commands take their connection parameters via
			// --url / --user (see above), the urlParser{} struct internally
			// needs the ClientHost and ClientPort flags to be defined -
			// even if they are invisible - due to the way initialization from
			// env vars is implemented.
			//
			// TODO(knz): if/when env var option initialization is deferred
			// to parse time, this can be removed.
			VarFlag(f, addrSetter{&cliCtx.clientConnHost, &cliCtx.clientConnPort}, cliflags.ClientHost)
			_ = f.MarkHidden(cliflags.ClientHost.Name)
			StringFlag(f, &cliCtx.clientConnPort, cliflags.ClientPort, cliCtx.clientConnPort)
			_ = f.MarkHidden(cliflags.ClientPort.Name)

		}

		if cmd == sqlShellCmd {
			StringFlag(f, &cliCtx.sqlConnDBName, cliflags.Database, cliCtx.sqlConnDBName)
		}
	}

	// Make the non-SQL client commands also recognize --url in strict SSL mode
	// and ensure they can connect to clusters that use a cluster-name.
	for _, cmd := range clientCmds {
		if f := flagSetForCmd(cmd).Lookup(cliflags.URL.Name); f != nil {
			// --url already registered above, nothing to do.
			continue
		}
		f := cmd.PersistentFlags()
		VarFlag(f, urlParser{cmd, &cliCtx, true /* strictSSL */}, cliflags.URL)
		VarFlag(f, clusterNameSetter{&baseCfg.ClusterName}, cliflags.ClusterName)
		BoolFlag(f, &baseCfg.DisableClusterNameVerification,
			cliflags.DisableClusterNameVerification, baseCfg.DisableClusterNameVerification)
	}

	// Commands that print tables.
	tableOutputCommands := append(
		[]*cobra.Command{sqlShellCmd, genSettingsListCmd, demoCmd},
		demoCmd.Commands()...)
	tableOutputCommands = append(tableOutputCommands, nodeCmds...)
	tableOutputCommands = append(tableOutputCommands, authCmds...)

	// By default, these commands print their output as pretty-formatted
	// tables on terminals, and TSV when redirected to a file. The user
	// can override with --format.
	// By default, query times are not displayed. The default is overridden
	// in the CLI shell.
	for _, cmd := range tableOutputCommands {
		f := cmd.PersistentFlags()
		VarFlag(f, &cliCtx.tableDisplayFormat, cliflags.TableDisplayFormat)
	}

	// demo command.
	demoFlags := demoCmd.PersistentFlags()
	// We add this command as a persistent flag so you can do stuff like
	// ./kwbase demo movr --nodes=3.
	IntFlag(demoFlags, &demoCtx.nodes, cliflags.DemoNodes, demoCtx.nodes)
	BoolFlag(demoFlags, &demoCtx.runWorkload, cliflags.RunDemoWorkload, demoCtx.runWorkload)
	VarFlag(demoFlags, &demoCtx.localities, cliflags.DemoNodeLocality)
	BoolFlag(demoFlags, &demoCtx.geoPartitionedReplicas, cliflags.DemoGeoPartitionedReplicas, demoCtx.geoPartitionedReplicas)
	VarFlag(demoFlags, demoNodeSQLMemSizeValue, cliflags.DemoNodeSQLMemSize)
	VarFlag(demoFlags, demoNodeCacheSizeValue, cliflags.DemoNodeCacheSize)
	BoolFlag(demoFlags, &demoCtx.insecure, cliflags.ClientInsecure, demoCtx.insecure)
	BoolFlag(demoFlags, &demoCtx.disableLicenseAcquisition, cliflags.DemoNoLicense, demoCtx.disableLicenseAcquisition)
	// Mark the --global flag as hidden until we investigate it more.
	BoolFlag(demoFlags, &demoCtx.simulateLatency, cliflags.Global, demoCtx.simulateLatency)
	_ = demoFlags.MarkHidden(cliflags.Global.Name)
	// The --empty flag is only valid for the top level demo command,
	// so we use the regular flag set.
	BoolFlag(demoCmd.Flags(), &demoCtx.useEmptyDatabase, cliflags.UseEmptyDatabase, demoCtx.useEmptyDatabase)

	// statement-diag command.
	{
		BoolFlag(stmtDiagDeleteCmd.Flags(), &stmtDiagCtx.all, cliflags.StmtDiagDeleteAll, false)
		BoolFlag(stmtDiagCancelCmd.Flags(), &stmtDiagCtx.all, cliflags.StmtDiagCancelAll, false)
	}

	// sqlfmt command.
	fmtFlags := sqlfmtCmd.Flags()
	VarFlag(fmtFlags, &sqlfmtCtx.execStmts, cliflags.Execute)
	cfg := tree.DefaultPrettyCfg()
	IntFlag(fmtFlags, &sqlfmtCtx.len, cliflags.SQLFmtLen, cfg.LineWidth)
	BoolFlag(fmtFlags, &sqlfmtCtx.useSpaces, cliflags.SQLFmtSpaces, !cfg.UseTabs)
	IntFlag(fmtFlags, &sqlfmtCtx.tabWidth, cliflags.SQLFmtTabWidth, cfg.TabWidth)
	BoolFlag(fmtFlags, &sqlfmtCtx.noSimplify, cliflags.SQLFmtNoSimplify, !cfg.Simplify)
	BoolFlag(fmtFlags, &sqlfmtCtx.align, cliflags.SQLFmtAlign, (cfg.Align != tree.PrettyNoAlign))

	// Debug commands.
	{
		f := debugKeysCmd.Flags()
		VarFlag(f, (*mvccKey)(&debugCtx.startKey), cliflags.From)
		VarFlag(f, (*mvccKey)(&debugCtx.endKey), cliflags.To)
		BoolFlag(f, &debugCtx.values, cliflags.Values, debugCtx.values)
		BoolFlag(f, &debugCtx.sizes, cliflags.Sizes, debugCtx.sizes)
	}
	{
		f := debugRangeDataCmd.Flags()
		BoolFlag(f, &debugCtx.replicated, cliflags.Replicated, debugCtx.replicated)
	}
	{
		f := debugGossipValuesCmd.Flags()
		StringFlag(f, &debugCtx.inputFile, cliflags.GossipInputFile, debugCtx.inputFile)
		BoolFlag(f, &debugCtx.printSystemConfig, cliflags.PrintSystemConfig, debugCtx.printSystemConfig)
	}
	{
		f := debugBallastCmd.Flags()
		VarFlag(f, &debugCtx.ballastSize, cliflags.Size)
	}
}

// processEnvVarDefaults injects the current value of flag-related
// environment variables into the initial value of the settings linked
// to the flags, during initialization and before the command line is
// actually parsed. For example, it will inject the value of
// $KWBASE_URL into the urlParser object linked to the --url flag.
func processEnvVarDefaults() error {
	for _, d := range envVarDefaults {
		f := d.flagSet.Lookup(d.flagName)
		if f == nil {
			panic(errors.AssertionFailedf("unknown flag: %s", d.flagName))
		}
		var err error
		if url, ok := f.Value.(urlParser); ok {
			// URLs are a special case: they can emit a warning if there's
			// excess configuration for certain commands.
			// Since the env-var initialization is ran for all commands
			// all the time, regardless of which particular command is
			// currently active, we want to silence this warning here.
			//
			// TODO(knz): rework this code to only pull env var values
			// for the current command.
			err = url.setInternal(d.envValue, false /* warn */)
		} else {
			err = d.flagSet.Set(d.flagName, d.envValue)
		}
		if err != nil {
			return errors.Wrapf(err, "setting --%s from %s", d.flagName, d.envVar)
		}
	}
	return nil
}

// envVarDefault describes a delayed default initialization of the
// setting covered by a flag from the value of an environment
// variable.
type envVarDefault struct {
	envVar   string
	envValue string
	flagName string
	flagSet  *pflag.FlagSet
}

// envVarDefaults records the initializations from environment variables
// for processing at the end of initialization, before flag parsing.
var envVarDefaults []envVarDefault

// registerEnvVarDefault registers a deferred initialization of a flag
// from an environment variable.
func registerEnvVarDefault(f *pflag.FlagSet, flagInfo cliflags.FlagInfo) {
	if flagInfo.EnvVar == "" {
		return
	}
	value, set := envutil.EnvString(flagInfo.EnvVar, 2)
	if !set {
		// Env var not set. Nothing to do.
		return
	}
	envVarDefaults = append(envVarDefaults, envVarDefault{
		envVar:   flagInfo.EnvVar,
		envValue: value,
		flagName: flagInfo.Name,
		flagSet:  f,
	})
}

// extraServerFlagInit configures the server.Config based on the command-line flags.
// It is only called when the command being ran is one of the start commands.
func extraServerFlagInit(cmd *cobra.Command) error {
	if err := security.SetCertPrincipalMap(startCtx.serverCertPrincipalMap); err != nil {
		return err
	}
	serverCfg.User = security.NodeUser
	serverCfg.Insecure = startCtx.serverInsecure
	serverCfg.SSLCertsDir = startCtx.serverSSLCertsDir

	// Construct the main RPC listen address.
	serverCfg.Addr = net.JoinHostPort(startCtx.serverListenAddr, serverListenPort)

	fs := flagSetForCmd(cmd)

	// Construct the socket name, if requested.
	if !fs.Lookup(cliflags.Socket.Name).Changed && fs.Lookup(cliflags.SocketDir.Name).Changed {
		// If --socket (DEPRECATED) was set, then serverCfg.SocketFile is
		// already set and we don't want to change it.
		// However, if --socket-dir is set, then we'll use that.
		// There are two cases:
		// --socket-dir is set and is empty; in this case the user is telling us "disable the socket".
		// is set and non-empty. Then it should be used as specified.
		if serverSocketDir == "" {
			serverCfg.SocketFile = ""
		} else {
			serverCfg.SocketFile = filepath.Join(serverSocketDir, ".s.PGSQL."+serverListenPort)
		}
	}

	// Fill in the defaults for --advertise-addr.
	if serverAdvertiseAddr == "" {
		serverAdvertiseAddr = startCtx.serverListenAddr
	}
	if serverAdvertisePort == "" {
		serverAdvertisePort = serverListenPort
	}
	serverCfg.AdvertiseAddr = net.JoinHostPort(serverAdvertiseAddr, serverAdvertisePort)

	// Fill in the defaults for --sql-addr.
	if serverSQLAddr == "" {
		serverSQLAddr = startCtx.serverListenAddr
	}
	if serverSQLPort == "" {
		serverSQLPort = serverListenPort
	}
	serverCfg.SQLAddr = net.JoinHostPort(serverSQLAddr, serverSQLPort)
	serverCfg.SplitListenSQL = fs.Lookup(cliflags.ListenSQLAddr.Name).Changed

	// Fill in the defaults for --advertise-sql-addr.
	advSpecified := fs.Lookup(cliflags.AdvertiseAddr.Name).Changed ||
		fs.Lookup(cliflags.AdvertiseHost.Name).Changed
	if serverSQLAdvertiseAddr == "" {
		if advSpecified {
			serverSQLAdvertiseAddr = serverAdvertiseAddr
		} else {
			serverSQLAdvertiseAddr = serverSQLAddr
		}
	}
	if serverSQLAdvertisePort == "" {
		if advSpecified && !serverCfg.SplitListenSQL {
			serverSQLAdvertisePort = serverAdvertisePort
		} else {
			serverSQLAdvertisePort = serverSQLPort
		}
	}
	serverCfg.SQLAdvertiseAddr = net.JoinHostPort(serverSQLAdvertiseAddr, serverSQLAdvertisePort)

	// Fill in the defaults for --http-addr.
	if serverHTTPAddr == "" {
		serverHTTPAddr = startCtx.serverListenAddr
	}
	if startCtx.unencryptedLocalhostHTTP {
		// If --unencrypted-localhost-http was specified, we want to
		// override whatever was specified or derived from other flags for
		// the host part of --http-addr.
		//
		// Before we do so, we'll check whether the user explicitly
		// specified something contradictory, and tell them that's no
		// good.
		if (fs.Lookup(cliflags.ListenHTTPAddr.Name).Changed ||
			fs.Lookup(cliflags.ListenHTTPAddrAlias.Name).Changed) &&
			(serverHTTPAddr != "" && serverHTTPAddr != "localhost") {
			return errors.WithHintf(
				errors.Newf("--unencrypted-localhost-http is incompatible with --http-addr=%s:%s",
					serverHTTPAddr, serverHTTPPort),
				`When --unencrypted-localhost-http is specified, use --http-addr=:%s or omit --http-addr entirely.`, serverHTTPPort)
		}

		// Now do the override proper.
		serverHTTPAddr = "localhost"
		// We then also tell the server to disable TLS for the HTTP
		// listener.
		serverCfg.DisableTLSForHTTP = true
	}
	serverCfg.HTTPAddr = net.JoinHostPort(serverHTTPAddr, serverHTTPPort)

	// Fill the advertise port into the locality advertise addresses.
	for i, addr := range localityAdvertiseHosts {
		host, port, err := netutil.SplitHostPort(addr.Address.AddressField, serverAdvertisePort)
		if err != nil {
			return err
		}
		localityAdvertiseHosts[i].Address.AddressField = net.JoinHostPort(host, port)
	}
	serverCfg.LocalityAddresses = localityAdvertiseHosts

	return nil
}

func extraClientFlagInit() error {
	if err := security.SetCertPrincipalMap(cliCtx.certPrincipalMap); err != nil {
		return err
	}
	serverCfg.Addr = net.JoinHostPort(cliCtx.clientConnHost, cliCtx.clientConnPort)
	serverCfg.AdvertiseAddr = serverCfg.Addr
	serverCfg.SQLAddr = net.JoinHostPort(cliCtx.clientConnHost, cliCtx.clientConnPort)
	serverCfg.SQLAdvertiseAddr = serverCfg.SQLAddr
	if serverHTTPAddr == "" {
		serverHTTPAddr = startCtx.serverListenAddr
	}
	serverCfg.HTTPAddr = net.JoinHostPort(serverHTTPAddr, serverHTTPPort)

	// If CLI/SQL debug mode is requested, override the echo mode here,
	// so that the initial client/server handshake reveals the SQL being
	// sent.
	if sqlCtx.debugMode {
		sqlCtx.echo = true
	}
	return nil
}

func setDefaultStderrVerbosity(cmd *cobra.Command, defaultSeverity log.Severity) error {
	vf := flagSetForCmd(cmd).Lookup(logflags.LogToStderrName)

	// if `--logtostderr` was not specified and no log directory was
	// set, or `--logtostderr` was specified but without explicit level,
	// then set stderr logging to the level considered default by the
	// specific command.
	if (!vf.Changed && !log.DirSet()) ||
		(vf.Changed && vf.Value.String() == log.Severity_DEFAULT.String()) {
		if err := vf.Value.Set(defaultSeverity.String()); err != nil {
			return err
		}
	}

	return nil
}
