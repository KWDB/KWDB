// Copyright 2017 The Cockroach Authors.
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

// Package sdnotify implements both sides of the systemd readiness
// protocol. Servers can use sdnotify.Ready() to signal that they are
// ready to receive traffic, and process managers can use
// sdnotify.Exec() to run processes that implement this protocol.
package sdnotify

import "os/exec"

// Ready sends a readiness signal using the systemd notification
// protocol. It should be called (once) by a server after it has
// completed its initialization (including but not necessarily limited
// to binding ports) and is ready to receive traffic.
func Ready() error {
	return ready()
}

// Exec the given command in the background using the systemd
// notification protocol. This function returns once the command has
// either exited or signaled that it is ready. If the command exits
// with a non-zero status before signaling readiness, returns an
// exec.ExitError.
func Exec(cmd *exec.Cmd) error {
	return bgExec(cmd)
}
