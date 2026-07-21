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
	"bytes"
	"crypto/tls"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/httputil"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/pkg/errors"
)

// consoleJSONUnmarshaler is used specifically for parsing JSON responses from gRPC gateway.
// It has AllowUnknownFields set to true to handle compatibility issues with RangeDesc.
// This is a deliberate exception to the lint rule requiring protoutil.Unmarshal.
var consoleJSONUnmarshaler = jsonpb.Unmarshaler{AllowUnknownFields: true}

type consoleClient struct {
	baseURL string
	client  http.Client
}

type rangeReportData struct {
	Range     serverpb.RangeResponse
	Allocator serverpb.AllocatorRangeResponse
	RangeLog  serverpb.RangeLogResponse
}

func normalizeConsoleURL(raw string, insecure bool) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("console URL is required")
	}
	if idx := strings.Index(raw, "#"); idx >= 0 {
		raw = raw[:idx]
	}
	raw = strings.TrimRight(raw, "/")
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" {
		if insecure {
			u.Scheme = "http"
		} else {
			u.Scheme = "https"
		}
	}
	if u.Host == "" {
		return "", errors.Errorf("invalid console URL: %q", raw)
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/"), nil
}

func newConsoleClient(baseURL string) (*consoleClient, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	return &consoleClient{
		baseURL: baseURL,
		client: http.Client{
			Jar:     jar,
			Timeout: 2 * time.Minute,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
			},
		},
	}, nil
}

func (c *consoleClient) login(username, password string) error {
	req := &serverpb.UserLoginRequest{
		Username: username,
		Password: password,
	}
	var resp serverpb.UserLoginResponse
	if _, err := httputil.PostJSONWithRequest(c.client, c.baseURL+"/login", req, &resp); err != nil {
		return errors.Wrap(err, "console login failed")
	}
	return nil
}

func (c *consoleClient) getJSON(path string, response protoutil.Message) error {
	req, err := http.NewRequest("GET", c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set(httputil.AcceptHeader, httputil.JSONContentType)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return errors.Errorf("status: %s, body: %s (Console login required; omit --insecure and provide --user/--password)", resp.Status, string(body))
		}
		return errors.Errorf("status: %s, body: %s", resp.Status, body)
	}
	if err := consoleJSONUnmarshaler.Unmarshal(bytes.NewReader(body), response); err != nil {
		return err
	}
	return nil
}

func (c *consoleClient) fetchRangeReport(rangeID int64) (*rangeReportData, error) {
	data := &rangeReportData{}
	id := strconv.FormatInt(rangeID, 10)
	if err := c.getJSON("/_status/range/"+id, &data.Range); err != nil {
		return nil, errors.Wrap(err, "fetch range status")
	}
	if err := c.getJSON("/_status/allocator/range/"+id, &data.Allocator); err != nil {
		return nil, errors.Wrap(err, "fetch allocator dry run")
	}
	if err := c.getJSON("/_admin/v1/rangelog/"+id+"?limit=100", &data.RangeLog); err != nil {
		return nil, errors.Wrap(err, "fetch range log")
	}
	return data, nil
}
