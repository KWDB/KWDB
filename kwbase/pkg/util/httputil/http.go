// Copyright 2014 The Cockroach Authors.
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

package httputil

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/pkg/errors"
)

const (
	// AcceptHeader is the canonical header name for accept.
	AcceptHeader = "Accept"
	// AcceptEncodingHeader is the canonical header name for accept encoding.
	AcceptEncodingHeader = "Accept-Encoding"
	// ContentEncodingHeader is the canonical header name for content type.
	ContentEncodingHeader = "Content-Encoding"
	// ContentTypeHeader is the canonical header name for content type.
	ContentTypeHeader = "Content-Type"
	// JSONContentType is the JSON content type.
	JSONContentType = "application/json"
	// AltJSONContentType is the alternate JSON content type.
	AltJSONContentType = "application/x-json"
	// ProtoContentType is the protobuf content type.
	ProtoContentType = "application/x-protobuf"
	// AltProtoContentType is the alternate protobuf content type.
	AltProtoContentType = "application/x-google-protobuf"
	// PlaintextContentType is the plaintext content type.
	PlaintextContentType = "text/plain"
	// GzipEncoding is the gzip encoding.
	GzipEncoding = "gzip"
)

// GetJSON uses the supplied client to GET the URL specified by the parameters
// and unmarshals the result into response.
// TODO(someone): make this context-aware, see client.go.
func GetJSON(httpClient http.Client, path string, response protoutil.Message) error {
	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return err
	}
	_, err = doJSONRequest(httpClient, req, response)
	return err
}

// PostJSON uses the supplied client to POST request to the URL specified by
// the parameters and unmarshals the result into response.
// TODO(someone): make this context-aware, see client.go.
func PostJSON(httpClient http.Client, path string, request, response protoutil.Message) error {
	// Hack to avoid upsetting TestProtoMarshal().
	marshalFn := (&jsonpb.Marshaler{}).Marshal

	var buf bytes.Buffer
	if err := marshalFn(&buf, request); err != nil {
		return err
	}
	req, err := http.NewRequest("POST", path, &buf)
	if err != nil {
		return err
	}
	_, err = doJSONRequest(httpClient, req, response)
	return err
}

// PostJSONWithRequest uses the supplied client to POST request to the URL
// specified by the parameters and unmarshals the result into response.
//
// The response is returned to the caller, though its body will have been
// closed.
// TODO(someone): make this context-aware, see client.go.
func PostJSONWithRequest(
	httpClient http.Client, path string, request, response protoutil.Message,
) (*http.Response, error) {
	// Hack to avoid upsetting TestProtoMarshal().
	marshalFn := (&jsonpb.Marshaler{}).Marshal

	var buf bytes.Buffer
	if err := marshalFn(&buf, request); err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", path, &buf)
	if err != nil {
		return nil, err
	}

	return doJSONRequest(httpClient, req, response)
}

func doJSONRequest(
	httpClient http.Client, req *http.Request, response protoutil.Message,
) (*http.Response, error) {
	if timeout := httpClient.Timeout; timeout > 0 {
		req.Header.Set("Grpc-Timeout", strconv.FormatInt(timeout.Nanoseconds(), 10)+"n")
	}
	req.Header.Set(AcceptHeader, JSONContentType)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if contentType := resp.Header.Get(ContentTypeHeader); !(resp.StatusCode == http.StatusOK && contentType == JSONContentType) {
		b, err := ioutil.ReadAll(resp.Body)
		return resp, errors.Errorf(
			"status: %s, content-type: %s, body: %s, error: %v", resp.Status, contentType, b, err,
		)
	}
	return resp, jsonpb.Unmarshal(resp.Body, response)
}
