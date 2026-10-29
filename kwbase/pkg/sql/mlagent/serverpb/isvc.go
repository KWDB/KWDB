// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

/*
Package serverpb defines the serverpb features.
*/
package serverpb

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	// Unknown is unknown status
	Unknown = "N/A"

	// StatusIsvcReady is ready status.
	StatusIsvcReady = "Ready"
	// StatusIsvcInitializing is initializing status.
	StatusIsvcInitializing = "Initializing"
	// StatusIsvcError is error status.
	StatusIsvcError = "Error"

	// ServicePrefix is inference service pod prefix.
	ServicePrefix = ""

	// MaxServiceNameLength is inference service name length limit.
	MaxServiceNameLength = 28

	// ResourceTypeModel define resource type model.
	ResourceTypeModel = "model"

	// ResourceTypeJob define resource type job.
	ResourceTypeJob = "job"
)

// PredictParam define predict param struct.
type PredictParam struct {
	Instances [][]interface{} `json:"instances,omitempty"`
	Header    []string        `json:"header,omitempty"`
}

// NewReadyStatus create a ready status.
func NewReadyStatus() *IsvcStatus {
	return &IsvcStatus{Status: StatusIsvcReady}
}

// NewUnknownStatus create a unknown status.
func NewUnknownStatus(message string) *IsvcStatus {
	return &IsvcStatus{Status: Unknown, Message: message}
}

// NewInitializeStatus create a initializing status.
func NewInitializeStatus(message string) *IsvcStatus {
	return &IsvcStatus{Status: StatusIsvcInitializing, Message: message}
}

// NewErrorStatus create a error status.
func NewErrorStatus(message string) *IsvcStatus {
	return &IsvcStatus{Status: StatusIsvcError, Message: message}
}

// AddPodsStatus add a pod status.
func (x *IsvcStatus) AddPodsStatus(podStatus ...*PodStatus) {
	x.PodStatus = append(x.PodStatus, podStatus...)
}

// IsReady check status is ready.
func (x *IsvcStatus) IsReady() bool {
	return x.Status == StatusIsvcReady
}

// IsError check status is error.
func (x *IsvcStatus) IsError() bool {
	return x.Status == StatusIsvcError
}

// IsPending check status is pending.
func (x *IsvcStatus) IsPending() bool {
	if x.IsReady() || x.IsError() {
		return false
	}

	for _, podStatus := range x.PodStatus {
		if podStatus.Status != "Pending" {
			return false
		}
	}

	return true
}

// CheckError check status have a error.
func (x *IsvcStatus) CheckError() bool {
	if x.IsReady() {
		return false
	}

	for _, podStatus := range x.PodStatus {
		if podStatus.Status == "Error" {
			x.Status = StatusIsvcError
			x.Message = fmt.Sprintf("%s have a error", podStatus.Name)

			return true
		}
	}

	return false
}

// JSONString marshal json to string.
func (x *IsvcStatus) JSONString() string {
	JSON, err := json.Marshal(x)
	if err != nil {
		return fmt.Sprintf(`{"Status":"%s","Message":"%s"}`, StatusIsvcError, err.Error())
	}

	return string(JSON)
}

// GenIsvcName generate inference service name.
func (m *ModelSpec) GenIsvcName() string {
	name := strings.ReplaceAll(m.ModelName, "_", "-")

	return fmt.Sprintf("%s%s-%s", ServicePrefix, name, m.ModelVersion)
}
