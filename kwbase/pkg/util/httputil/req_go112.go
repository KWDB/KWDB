// Copyright 2019 The Cockroach Authors.
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

// +build !go1.13

package httputil

import (
	"context"
	"io"
	"net/http"
)

// NewRequestWithContext provides the same interface as the Go 1.13 function
// in http but ignores the context argument.
//
// This is transition code until the repository is upgraded to use Go
// 1.13. In the meantime, the callers rely on requests eventually
// succeeding or failing with a timeout if the remote server does not
// provide a response on time.
//
// TODO(knz): remove this when the repo does not use 1.12 any more.
func NewRequestWithContext(
	ctx context.Context, method, url string, body io.Reader,
) (*http.Request, error) {
	return http.NewRequest(method, url, body)
}
