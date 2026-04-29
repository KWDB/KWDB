// Copyright 2026 The Cockroach Authors.
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

package opt

import (
	"context"
	"strconv"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const windowsPushdownAllDisabledMessage = "Windows does not support sql.all_push_down.enabled=true; distributed TS queries must exchange data through the ME Go layer"

var windowsPushdownAllWarning sync.Once

func pushdownAllDisabledOnWindows(enabled bool, windows bool) bool {
	return windows && enabled
}

func rejectPushdownAllSetting(encodedValue string, windows bool) bool {
	enabled, err := strconv.ParseBool(encodedValue)
	return err == nil && windows && enabled
}

// CheckPushdownAllSetting validates whether sql.all_push_down.enabled can be enabled.
func CheckPushdownAllSetting(encodedValue string, windows bool) error {
	if rejectPushdownAllSetting(encodedValue, windows) {
		return errors.New(windowsPushdownAllDisabledMessage)
	}
	return nil
}

// EffectivePushdownAll returns the effective value of sql.all_push_down.enabled.
func EffectivePushdownAll(sv *settings.Values) bool {
	raw := PushdownAll.Get(sv)
	if !pushdownAllDisabledOnWindows(raw, base.IsWindows()) {
		return raw
	}
	windowsPushdownAllWarning.Do(func() {
		log.Warningf(context.Background(),
			"sql.all_push_down.enabled=true is ignored on Windows; distributed TS queries use the ME Go-layer exchange path")
	})
	return false
}
