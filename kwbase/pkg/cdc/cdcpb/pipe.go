//
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Shanghai Yunxi Technology Co, Ltd makes no representations or warranties about the suitability
// of the software, either express or implied, including but not limited to the implied warranties
// of merchantability, fitness for a particular purpose, or non-infringement. Shanghai Yunxi
// Technology Co, Ltd shall not be liable for any damages suffered by licensee as a result
// of using, modifying or distributing this software or its derivatives.
//

package cdcpb

import (
	"encoding/json"

	kjson "gitee.com/kwbasedb/kwbase/pkg/util/json"
)

// PipeParameters stores pipe parameters.
type PipeParameters struct {
	Tables      []CDCTableInfo `json:"tables"`
	TableIDs    []uint64       `json:"table_ids,omitempty"`
	PipeOptions PipeOptions    `json:"options"`
	DatabaseID  uint64         `json:"database_id,omitempty"`
}

// PipeOptions stores pipe options.
type PipeOptions struct {
	Enable        string `json:"enable"`
	Sink          string `json:"sink"`
	MessageFormat string `json:"message_format"`
	IgnoreHistory string `json:"ignore_history"`
	BufferSize    int    `json:"buffer_size"`
	CheckTag      string `json:"retrieve_tags"`
	Publish       string `json:"publish"`
}

// MarshalPipeParameters marshals pipe parameters to json.
func MarshalPipeParameters(para PipeParameters) (kjson.JSON, error) {
	pipePara, err := json.Marshal(para)
	if err != nil {
		return nil, err
	}
	return kjson.FromString(string(pipePara)), nil
}

// UnmarshalPipeParameters unmarshal json to pipe parameters.
func UnmarshalPipeParameters(parameters kjson.JSON) (PipeParameters, error) {
	var para PipeParameters
	str, err := parameters.AsText()
	if err != nil {
		return para, err
	}
	if err := json.Unmarshal([]byte(*str), &para); err != nil {
		return para, err
	}
	return para, nil
}

// CheckSink checks if the sink URL is valid.
var CheckSink func(sinkURL string, enabled bool) error

// MockData used to test pipe.
var MockData map[string][][]byte
