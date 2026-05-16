// Copyright 2018 The Cockroach Authors.
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

//go:build windows
// +build windows

package sysutil

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// DumpBacktraceSignal is used to dump all thread and goroutine stack backtrace
var DumpBacktraceSignal os.Signal = syscall.Signal(0x1e) // syscall.SIGUSR1

// NotifyThreadDumpSignal is used signal thread to dump stack backtrace
var NotifyThreadDumpSignal os.Signal = syscall.Signal(0x1f) //syscall.SIGUSR2

// AeDiedSignal is used to detect the ae died event
var AeDiedSignal os.Signal = syscall.Signal(0x14) //syscall.SIGCHLD

// ProcessIdentity returns a string describing the user and group that this
// process is running as.
func ProcessIdentity() string {
	u, err := user.Current()
	if err != nil {
		return "<unknown>"
	}
	return fmt.Sprintf("uid %s, gid %s", u.Uid, u.Gid)
}

// StatFS returns an FSInfo describing the named filesystem. It is only
// supported on Unix-like platforms.
func StatFS(path string) (*FSInfo, error) {
	return nil, errors.New("unsupported on Windows")
}

// StatAndLinkCount wraps os.Stat, returning its result and a zero link count.
func StatAndLinkCount(path string) (os.FileInfo, int64, error) {
	stat, err := os.Stat(path)
	return stat, 0, err
}

// IsCrossDeviceLinkErrno checks whether the given error object (as
// extracted from an *os.LinkError) is a cross-device link/rename
// error.
func IsCrossDeviceLinkErrno(errno error) bool {
	// 0x11 is Win32 Error Code ERROR_NOT_SAME_DEVICE
	// See: https://msdn.microsoft.com/en-us/library/cc231199.aspx
	return errno == syscall.Errno(0x11)
}

// GetPid call syscall.Getpid()
func GetPid() int {
	return os.Getpid()
}

// GetDeviceNumber call syscall.Stat_t
func GetDeviceNumber(path string) (int64, error) {
	volumePath := getVolumePath(path)
	if volumePath == "" {
		return 0, fmt.Errorf("failed to get volume path for %s", path)
	}

	return getVolumeSerialNumber(volumePath)
}

// getVolumePath return volume path.
func getVolumePath(path string) string {
	absPath, err := windows.FullPath(path)
	if err != nil {
		return ""
	}

	var volumePath [windows.MAX_PATH]uint16
	ret, _, _ := windows.NewLazyDLL("kernel32.dll").NewProc("GetVolumePathNameW").Call(
		uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(absPath))),
		uintptr(unsafe.Pointer(&volumePath[0])),
		windows.MAX_PATH,
	)

	if ret == 0 {
		return ""
	}

	return windows.UTF16ToString(volumePath[:])
}

// getVolumeSerialNumber return volume serial number.
func getVolumeSerialNumber(volumePath string) (int64, error) {
	var volumeSerialNumber uint32
	var maxComponentLength uint32
	var fileSystemFlags uint32
	var fileSystemNameBuffer [windows.MAX_PATH]uint16

	ret, _, err := windows.NewLazyDLL("kernel32.dll").NewProc("GetVolumeInformationW").Call(
		uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(volumePath))),
		0,
		0,
		uintptr(unsafe.Pointer(&volumeSerialNumber)),
		uintptr(unsafe.Pointer(&maxComponentLength)),
		uintptr(unsafe.Pointer(&fileSystemFlags)),
		uintptr(unsafe.Pointer(&fileSystemNameBuffer[0])),
		windows.MAX_PATH,
	)

	if ret == 0 {
		return 0, err
	}

	return int64(volumeSerialNumber), nil
}
