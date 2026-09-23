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
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"text/tabwriter"

	"gitee.com/kwbasedb/kwbase/pkg/build"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/log/logflags"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	_ "github.com/benesch/cgosymbolizer" // calls runtime.SetCgoTraceback on import
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// Main is the entry point for the cli, with a single line calling it intended
// to be the body of an action package main `main` func elsewhere. It is
// abstracted for reuse by duplicated `main` funcs in different distributions.
func Main() {
	// Seed the math/rand RNG from crypto/rand.
	rand.Seed(randutil.NewPseudoSeed())

	if len(os.Args) == 1 {
		os.Args = append(os.Args, "help")
	}

	// Change the logging defaults for the main kwbase binary.
	// The value is overridden after command-line parsing.
	if err := flag.Lookup(logflags.LogToStderrName).Value.Set("NONE"); err != nil {
		panic(err)
	}

	cmdName := commandName(os.Args[1:])

	if cmdName == startMppNodeCmd.Use {
		serverCfg.ModeFlag = server.SetMppModeFlag(serverCfg.ModeFlag)
	}
	serverCfg.StartMode = cmdName

	if cmdName == startSingleNodeCmd.Use {
		serverCfg.ModeFlag = server.SetMppModeFlag(serverCfg.ModeFlag)
		serverCfg.ModeFlag = server.SetSingleNodeModeFlag(serverCfg.ModeFlag)
	}

	log.SetupCrashReporter(
		context.Background(),
		cmdName,
	)

	defer log.RecoverAndReportPanic(context.Background(), &serverCfg.Settings.SV)

	err := Run(os.Args[1:])

	errCode := 0
	if err != nil {
		// Display the error and its details/hints.
		cliOutputError(stderr, err, true /*showSeverity*/, false /*verbose*/)

		// Remind the user of which command was being run.
		fmt.Fprintf(stderr, "Failed running %q\n", cmdName)

		// Finally, extract the error code, as optionally specified
		// by the sub-command.
		errCode = 1
		var cliErr *cliError
		if errors.As(err, &cliErr) {
			errCode = cliErr.exitCode
		}
	}

	os.Exit(errCode)
}

// commandName computes the name of the command that args would invoke. For
// example, the full name of "kwbase debug zip" is "debug zip". If args
// specify a nonexistent command, commandName returns "kwbase".
func commandName(args []string) string {
	rootName := kwbaseCmd.CommandPath()
	// Ask Cobra to find the command so that flags and their arguments are
	// ignored. The name of "kwbase --log-dir foo start" is "start", not
	// "--log-dir" or "foo".
	if cmd, _, _ := kwbaseCmd.Find(os.Args[1:]); cmd != nil {
		return strings.TrimPrefix(cmd.CommandPath(), rootName+" ")
	}
	return rootName
}

type cliError struct {
	exitCode int
	severity log.Severity
	cause    error
}

func (e *cliError) Error() string { return e.cause.Error() }

// Cause implements causer.
func (e *cliError) Cause() error { return e.cause }

// Format implements fmt.Formatter.
func (e *cliError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// FormatError implements errors.Formatter.
func (e *cliError) FormatError(p errors.Printer) error {
	if p.Detail() {
		p.Printf("error with exit code: %d", e.exitCode)
	}
	return e.cause
}

// stderr aliases log.OrigStderr; we use an alias here so that tests
// in this package can redirect the output of CLI commands to stdout
// to be captured.
var stderr = log.OrigStderr

// stdin aliases os.Stdin; we use an alias here so that tests in this
// package can redirect the input of the CLI shell.
var stdin = os.Stdin

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "output version information",
	Long: `
Output build version information.
`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		info := build.GetInfo()
		tw := tabwriter.NewWriter(os.Stdout, 2, 1, 2, ' ', 0)
		fmt.Fprintf(tw, "KaiwuDB Version:  %s\n", info.Tag)
		fmt.Fprintf(tw, "Build Time:       %s\n", info.Time)
		fmt.Fprintf(tw, "Distribution:     %s\n", info.Distribution)
		fmt.Fprintf(tw, "Platform:         %s", info.Platform)
		if info.CgoTargetTriple != "" {
			fmt.Fprintf(tw, " (%s)", info.CgoTargetTriple)
		}
		fmt.Fprintln(tw)
		fmt.Fprintf(tw, "Go Version:       %s\n", info.GoVersion)
		fmt.Fprintf(tw, "C Compiler:       %s\n", info.CgoCompiler)
		fmt.Fprintf(tw, "Build SHA-1:      %s\n", info.Revision)
		return tw.Flush()
	},
}

var kwbaseCmd = &cobra.Command{
	Use:   "kwbase [command] (flags)",
	Short: "KwDB command-line interface and server",
	// TODO(cdo): Add a pointer to the docs in Long.
	Long: `KwDB command-line interface and server.`,
	// Disable automatic printing of usage information whenever an error
	// occurs. Many errors are not the result of a bad command invocation,
	// e.g. attempting to start a node on an in-use port, and printing the
	// usage information in these cases obscures the cause of the error.
	// Commands should manually print usage information when the error is,
	// in fact, a result of a bad invocation, e.g. too many arguments.
	SilenceUsage: true,
	// Disable automatic printing of the error. We want to also print
	// details and hints, which cobra does not do for us. Instead
	// we do the printing in Main().
	SilenceErrors: true,
}

func init() {
	cobra.EnableCommandSorting = false

	// Set an error function for flag parsing which prints the usage message.
	kwbaseCmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		if err := c.Usage(); err != nil {
			return err
		}
		fmt.Fprintln(c.OutOrStderr()) // provide a line break between usage and error
		return err
	})

	kwbaseCmd.AddCommand(
		startCmd,
		startMppNodeCmd,
		startSingleNodeCmd,
		initCmd,
		certCmd,
		quitCmd,

		sqlShellCmd,
		stmtDiagCmd,
		authCmd,
		nodeCmd,
		nodeLocalCmd,

		// Miscellaneous commands.
		// TODO(pmattis): stats
		demoCmd,
		genCmd,
		versionCmd,
		DebugCmd,
		sqlfmtCmd,
	)
}

// AddCmd adds a command to the cli.
func AddCmd(c *cobra.Command) {
	kwbaseCmd.AddCommand(c)
}

// Run ...
func Run(args []string) error {
	kwbaseCmd.SetArgs(args)
	return kwbaseCmd.Execute()
}

// usageAndErr informs the user about the usage of the command
// and returns an error. This ensures that the top-level command
// has a suitable exit status.
func usageAndErr(cmd *cobra.Command, args []string) error {
	if err := cmd.Usage(); err != nil {
		return err
	}
	return fmt.Errorf("unknown sub-command: %q", strings.Join(args, " "))
}
