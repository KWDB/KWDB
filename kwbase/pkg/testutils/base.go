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

package testutils

import (
	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/security"
)

// NewNodeTestBaseContext creates a base context for testing. This uses
// embedded certs and the default node user. The default node user has both
// server and client certificates.
func NewNodeTestBaseContext() *base.Config {
	return NewTestBaseContext(security.NodeUser)
}

// NewTestBaseContext creates a secure base context for user.
func NewTestBaseContext(user string) *base.Config {
	cfg := &base.Config{
		Insecure: false,
		User:     user,
	}
	FillCerts(cfg)
	return cfg
}

// FillCerts sets the certs on a base.Config.
func FillCerts(cfg *base.Config) {
	cfg.SSLCertsDir = security.EmbeddedCertsDir
}
