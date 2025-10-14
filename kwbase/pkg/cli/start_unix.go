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

//go:build !windows
// +build !windows

package cli

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/cli/cliflags"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/sdnotify"
	"gitee.com/kwbasedb/kwbase/pkg/util/sysutil"
	"golang.org/x/sys/unix"
)

// drainSignals are the signals that will cause the server to drain and exit.
//
// If two drain signals are seen, the second drain signal will be reraised
// without a signal handler. The default action of any signal listed here thus
// must terminate the process.
var drainSignals = []os.Signal{unix.SIGINT, unix.SIGTERM, unix.SIGQUIT}

// quitSignal is the signal to recognize to dump Go stacks.
var quitSignal os.Signal = unix.SIGQUIT

func handleSignalDuringShutdown(sig os.Signal) {
	// On Unix, a signal that was not handled gracefully by the application
	// should be reraised so it is visible in the exit code.

	// Reset signal to its original disposition.
	signal.Reset(sig)

	// Reraise the signal. os.Signal is always sysutil.Signal.
	if err := unix.Kill(unix.Getpid(), sig.(sysutil.Signal)); err != nil {
		// Sending a valid signal to ourselves should never fail.
		//
		// Unfortunately it appears (#34354) that some users
		// run KwDB in containers that only support
		// a subset of all syscalls. If this ever happens, we
		// still need to quit immediately.
		log.Fatalf(context.Background(), "unable to forward signal %v: %v", sig, err)
	}

	// Block while we wait for the signal to be delivered.
	select {}
}

var startBackground bool

func init() {
	for _, cmd := range StartCmds {
		BoolFlag(cmd.Flags(), &startBackground, cliflags.Background, false)
	}
}

func maybeRerunBackground() (bool, error) {
	if startBackground {
		args := make([]string, 0, len(os.Args))
		foundBackground := false
		for _, arg := range os.Args {
			if arg == "--background" || strings.HasPrefix(arg, "--background=") {
				foundBackground = true
				continue
			}
			args = append(args, arg)
		}
		if !foundBackground {
			args = append(args, "--background=false")
		}
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = stderr

		// Notify to ourselves that we're restarting.
		_ = os.Setenv(backgroundEnvVar, "1")

		return true, sdnotify.Exec(cmd)
	}
	return false, nil
}

func disableOtherPermissionBits() {
	mask := unix.Umask(0000)
	mask |= 00007
	_ = unix.Umask(mask)
}

func useUnixSocketsInDemo() bool {
	return true
}
