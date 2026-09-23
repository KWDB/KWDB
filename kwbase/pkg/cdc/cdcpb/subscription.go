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

// SubParameters stores parameters of subscription.
type SubParameters struct {
	TableList   []CDCTableInfo `json:"table_list"`
	SubOptions  SubOptions     `json:"options"`
	Connection  string         `json:"connection"`
	Publication string         `json:"publication"`
	DatabaseID  uint64         `json:"database_id,omitempty"`
}

// SubOptions stores options of subscription.
type SubOptions struct {
	Enable         string            `json:"enable"`
	Retry          int               `json:"retry"`
	Target         map[string]string `json:"target,omitempty"`
	IgnoreHistory  string            `json:"ignore_history"`
	DisableOnError string            `json:"disable_on_error"`
}

// NotIgnoreHistory checks whether the ignore_history is off.
// It returns true if ignore_history is off, and will send history data.
func (s *SubOptions) NotIgnoreHistory() bool {
	return s.IgnoreHistory == "off"
}

// MarshalSubParameters marshals parameters of subscription to json.
func MarshalSubParameters(para SubParameters) (kjson.JSON, error) {
	SubPara, err := json.Marshal(para)
	if err != nil {
		return nil, err
	}

	return kjson.FromString(string(SubPara)), nil
}

// UnmarshalSubParameters unmarshal json to parameters of subscription.
func UnmarshalSubParameters(parameters kjson.JSON) (SubParameters, error) {
	var para SubParameters
	str, err := parameters.AsText()
	if err != nil {
		return para, err
	}

	if err = json.Unmarshal([]byte(*str), &para); err != nil {
		return para, err
	}

	return para, nil
}

// SubRequest stores request of subscription.
type SubRequest struct {
	ClusterID string         `json:"cluster_id"`
	TableList []CDCTableInfo `json:"table_list"`
}
