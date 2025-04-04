// Copyright 2018 The Cockroach Authors.
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

package install

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/cmd/roachprod/config"
	"github.com/cockroachdb/errors"
)

type session interface {
	CombinedOutput(cmd string) ([]byte, error)
	Run(cmd string) error
	SetStdin(r io.Reader)
	SetStdout(w io.Writer)
	SetStderr(w io.Writer)
	Start(cmd string) error
	StdinPipe() (io.WriteCloser, error)
	StdoutPipe() (io.Reader, error)
	StderrPipe() (io.Reader, error)
	RequestPty() error
	Wait() error
	Close()
}

type remoteSession struct {
	*exec.Cmd
	cancel  func()
	logfile string // captures ssh -vvv
}

func newRemoteSession(user, host string, logdir string) (*remoteSession, error) {
	// TODO(tbg): this is disabled at the time of writing. It was difficult
	// to assign the logfiles to the roachtest and as a bonus our CI harness
	// never actually managed to collect the files since they had wrong
	// permissions; instead they clogged up the roachprod dir.
	// logfile := filepath.Join(
	//	logdir,
	// 	fmt.Sprintf("ssh_%s_%s", host, timeutil.Now().Format(time.RFC3339)),
	// )
	const logfile = ""
	args := []string{
		user + "@" + host,

		// TODO(tbg): see above.
		//"-vvv", "-E", logfile,
		// NB: -q suppresses -E, at least on OSX. Difficult decisions will have
		// to be made if omitting -q leads to annoyance on stdout/stderr.

		"-q",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "StrictHostKeyChecking=no",
		// Send keep alives every minute to prevent connections without activity
		// from dropping. (It is speculative that the absence of this caused
		// problems, though there's some indication that we need them:
		//
		// https://gitee.com/kwbasedb/kwbase/issues/35337
		"-o", "ServerAliveInterval=60",
	}
	args = append(args, sshAuthArgs()...)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "ssh", args...)
	return &remoteSession{cmd, cancel, logfile}, nil
}

func (s *remoteSession) errWithDebug(err error) error {
	if err != nil && s.logfile != "" {
		err = errors.Wrapf(err, "ssh verbose log retained in %s", s.logfile)
		s.logfile = "" // prevent removal on close
	}
	return err
}

func (s *remoteSession) CombinedOutput(cmd string) ([]byte, error) {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	b, err := s.Cmd.CombinedOutput()
	return b, s.errWithDebug(err)
}

func (s *remoteSession) Run(cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.errWithDebug(s.Cmd.Run())
}

func (s *remoteSession) Start(cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.Start()
}

func (s *remoteSession) SetStdin(r io.Reader) {
	s.Stdin = r
}

func (s *remoteSession) SetStdout(w io.Writer) {
	s.Stdout = w
}

func (s *remoteSession) SetStderr(w io.Writer) {
	s.Stderr = w
}

func (s *remoteSession) StdoutPipe() (io.Reader, error) {
	// NB: exec.Cmd.StdoutPipe returns a io.ReadCloser, hence the need for the
	// temporary storage into local variables.
	r, err := s.Cmd.StdoutPipe()
	return r, err
}

func (s *remoteSession) StderrPipe() (io.Reader, error) {
	// NB: exec.Cmd.StderrPipe returns a io.ReadCloser, hence the need for the
	// temporary storage into local variables.
	r, err := s.Cmd.StderrPipe()
	return r, err
}

func (s *remoteSession) RequestPty() error {
	s.Cmd.Args = append(s.Cmd.Args, "-t")
	return nil
}

func (s *remoteSession) Wait() error {
	return s.Cmd.Wait()
}

func (s *remoteSession) Close() {
	s.cancel()
	if s.logfile != "" {
		_ = os.Remove(s.logfile)
	}
}

type localSession struct {
	*exec.Cmd
	cancel func()
}

func newLocalSession() *localSession {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "/bin/bash", "-c")
	return &localSession{cmd, cancel}
}

func (s *localSession) CombinedOutput(cmd string) ([]byte, error) {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.CombinedOutput()
}

func (s *localSession) Run(cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.Run()
}

func (s *localSession) Start(cmd string) error {
	s.Cmd.Args = append(s.Cmd.Args, cmd)
	return s.Cmd.Start()
}

func (s *localSession) SetStdin(r io.Reader) {
	s.Stdin = r
}

func (s *localSession) SetStdout(w io.Writer) {
	s.Stdout = w
}

func (s *localSession) SetStderr(w io.Writer) {
	s.Stderr = w
}

func (s *localSession) StdoutPipe() (io.Reader, error) {
	// NB: exec.Cmd.StdoutPipe returns a io.ReadCloser, hence the need for the
	// temporary storage into local variables.
	r, err := s.Cmd.StdoutPipe()
	return r, err
}

func (s *localSession) StderrPipe() (io.Reader, error) {
	// NB: exec.Cmd.StderrPipe returns a io.ReadCloser, hence the need for the
	// temporary storage into local variables.
	r, err := s.Cmd.StderrPipe()
	return r, err
}

func (s *localSession) RequestPty() error {
	return nil
}

func (s *localSession) Wait() error {
	return s.Cmd.Wait()
}

func (s *localSession) Close() {
	s.cancel()
}

var sshAuthArgsVal []string
var sshAuthArgsOnce sync.Once

func sshAuthArgs() []string {
	sshAuthArgsOnce.Do(func() {
		paths := []string{
			filepath.Join(config.OSUser.HomeDir, ".ssh", "id_rsa"),
			filepath.Join(config.OSUser.HomeDir, ".ssh", "google_compute_engine"),
		}
		for _, p := range paths {
			if _, err := os.Stat(p); err == nil {
				sshAuthArgsVal = append(sshAuthArgsVal, "-i", p)
			}
		}
	})
	return sshAuthArgsVal
}
