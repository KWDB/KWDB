// Copyright 2019 The Cockroach Authors.
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

package fileutil

import (
	"os"

	"gitee.com/kwbasedb/kwbase/pkg/util/sysutil"
)

// Move moves a file from a directory to another, while handling
// cross-filesystem moves properly.
// If the target file already exists, it is truncated.
// If the move fails, then the target file may be left in an inconsistent state.
func Move(oldPath, newPath string) error {
	err := os.Rename(oldPath, newPath)
	if !isCrossDeviceLinkError(err) {
		return err
	}

	if err = CopyFile(oldPath, newPath); err != nil {
		return err
	}

	return os.RemoveAll(oldPath)
}

func isCrossDeviceLinkError(err error) bool {
	if err == nil {
		return false
	}
	le, ok := err.(*os.LinkError)
	if !ok {
		return false
	}

	return sysutil.IsCrossDeviceLinkErrno(le.Err)
}
