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

package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"

	"gitee.com/kwbasedb/kwbase/pkg/util/fileutil"
	"github.com/pkg/errors"
)

// TestLogScope represents the lifetime of a logging output.  It
// ensures that the log files are stored in a directory specific to a
// test, and asserts that logging output is not written to this
// directory beyond the lifetime of the scope.
type TestLogScope struct {
	logDir  string
	cleanup func()
}

// tShim is the part of testing.T used by TestLogScope.
// We can't use testing.T directly because we have
// a linter which forbids its use in public interfaces.
type tShim interface {
	Fatal(...interface{})
	Failed() bool
	Error(...interface{})
	Errorf(fmt string, args ...interface{})
	Name() string
	Log(...interface{})
	Logf(fmt string, args ...interface{})
}

// showLogs is used for testing
var showLogs int64

// Scope creates a TestLogScope which corresponds to the lifetime of a logging
// directory. The logging directory is named after the calling test. It also
// disables logging to stderr.
func Scope(t tShim) *TestLogScope {
	intToSeverityMap := map[int64]Severity{
		1: Severity_INFO,
		2: Severity_WARNING,
		3: Severity_ERROR,
		4: Severity_FATAL,
	}
	if sev, ok := intToSeverityMap[showLogs]; ok {
		logging.stderrThreshold = sev
		return (*TestLogScope)(nil)
	}

	scope := ScopeWithoutShowLogs(t)
	t.Log("use -show-logs to present logs inline")
	return scope
}

// ScopeWithoutShowLogs ignores the -show-logs flag and should be used for tests
// that require the logs go to files.
func ScopeWithoutShowLogs(t tShim) *TestLogScope {
	tempDir, err := ioutil.TempDir("", "log"+fileutil.EscapeFilename(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	if err := dirTestOverride("", tempDir); err != nil {
		t.Fatal(err)
	}
	undo, err := enableLogFileOutput(tempDir, Severity_NONE)
	if err != nil {
		undo()
		t.Fatal(err)
	}
	t.Logf("test logs captured to: %s", tempDir)
	return &TestLogScope{logDir: tempDir, cleanup: undo}
}

// enableLogFileOutput turns on logging using the specified directory.
// For unittesting only.
func enableLogFileOutput(dir string, stderrSeverity Severity) (func(), error) {
	mainLog.mu.Lock()
	defer mainLog.mu.Unlock()
	oldStderrThreshold := logging.stderrThreshold.get()
	oldNoStderrRedirect := mainLog.noStderrRedirect

	undo := func() {
		mainLog.mu.Lock()
		defer mainLog.mu.Unlock()
		logging.stderrThreshold.set(oldStderrThreshold)
		mainLog.noStderrRedirect = oldNoStderrRedirect
	}
	logging.stderrThreshold.set(stderrSeverity)
	mainLog.noStderrRedirect = true
	return undo, mainLog.logDir.Set(dir)
}

// Close cleans up a TestLogScope. The directory and its contents are
// deleted, unless the test has failed and the directory is non-empty.
func (l *TestLogScope) Close(t tShim) {
	// Ensure any remaining logs are written.
	Flush()

	if l == nil {
		// Never initialized.
		return
	}
	defer func() {
		// Check whether there is something to remove.
		emptyDir, err := isDirEmpty(l.logDir)
		if err != nil {
			t.Fatal(err)
		}
		inPanic := calledDuringPanic()
		if (t.Failed() && !emptyDir) || inPanic {
			// If the test failed or there was a panic, we keep the log
			// files for further investigation.
			if inPanic {
				fmt.Fprintln(OrigStderr, "\nERROR: a panic has occurred!\n"+
					"Details cannot be printed yet because we are still unwinding.\n"+
					"Hopefully the test harness prints the panic below, otherwise check the test logs.")
			}
			fmt.Fprintln(OrigStderr, "test logs left over in:", l.logDir)
		} else {
			// Clean up.
			if err := os.RemoveAll(l.logDir); err != nil {
				t.Error(err)
			}
		}
	}()
	defer l.cleanup()

	// Flush/Close the log files.
	if err := dirTestOverride(l.logDir, ""); err != nil {
		t.Fatal(err)
	}
}

// calledDuringPanic returns true if panic() is one of its callers.
func calledDuringPanic() bool {
	var pcs [40]uintptr
	runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:])

	for {
		f, more := frames.Next()
		if f.Function == "runtime.gopanic" {
			return true
		}
		if !more {
			break
		}
	}
	return false
}

// dirTestOverride sets the default value for the logging output directory
// for use in tests.
func dirTestOverride(expected, newDir string) error {
	if err := mainLog.dirTestOverride(expected, newDir); err != nil {
		return err
	}
	// Same with secondary loggers.
	secondaryLogRegistry.mu.Lock()
	defer secondaryLogRegistry.mu.Unlock()
	for _, l := range secondaryLogRegistry.mu.loggers {
		if err := l.logger.dirTestOverride(expected, newDir); err != nil {
			return err
		}
	}
	return nil
}

func (l *loggerT) dirTestOverride(expected, newDir string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.logDir.Lock()
	// The following check is intended to catch concurrent uses of
	// Scope() or TestLogScope.Close(), which would be invalid.
	if l.logDir.name != expected {
		l.logDir.Unlock()
		return errors.Errorf("unexpected logDir setting: set to %q, expected %q",
			l.logDir.name, expected)
	}
	l.logDir.name = newDir
	l.logDir.Unlock()

	// When we change the directory we close the current logging
	// output, so that a rotation to the new directory is forced on
	// the next logging event.
	return l.closeFileLocked()
}

func (l *loggerT) closeFileLocked() error {
	if l.mu.file != nil {
		if sb, ok := l.mu.file.(*syncBuffer); ok {
			if err := sb.file.Close(); err != nil {
				return err
			}
		}
		l.mu.file = nil
	}
	return restoreStderr()
}

func isDirEmpty(dirname string) (bool, error) {
	f, err := os.Open(dirname)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	list, err := f.Readdir(1)
	errClose := f.Close()
	if err != nil && err != io.EOF {
		return false, err
	}
	if errClose != nil {
		return false, errClose
	}
	return len(list) == 0, nil
}
