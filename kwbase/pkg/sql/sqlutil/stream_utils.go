// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sqlutil

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	kjson "gitee.com/kwbasedb/kwbase/pkg/util/json"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
)

// Constants for the stream options.
const (
	InvalidWaterMark = int64(math.MinInt64)
	TimeoutFactor    = 5

	OptEnable                 = "enable"
	OptMaxDelay               = "max_delay"
	OptSyncTime               = "sync_time"
	OptProcessHistory         = "process_history"
	OptIgnoreExpired          = "ignore_expired"
	OptIgnoreUpdate           = "ignore_update"
	OptMaxRetries             = "max_retries"
	OptCheckpointInterval     = "checkpoint_interval"
	OptHeartbeatInterval      = "heartbeat_interval"
	OptRecalculateDelayRounds = "recalculate_delay_rounds"
	OptBufferSize             = "buffer_size"

	StreamStatusEnable  = "Enable"
	StreamStatusDisable = "Disable"

	StreamOptOn  = "on"
	StreamOptOff = "off"

	StreamMaxRunInfo = 5
)

// DefaultOptions stores default stream options.
var DefaultOptions = map[string]string{
	OptEnable:                 "on",
	OptMaxDelay:               "24h",
	OptSyncTime:               "1m",
	OptProcessHistory:         "off",
	OptIgnoreExpired:          "on",
	OptIgnoreUpdate:           "on",
	OptMaxRetries:             "5",
	OptCheckpointInterval:     "10s",
	OptHeartbeatInterval:      "2s",
	OptRecalculateDelayRounds: "10",
	OptBufferSize:             "2GiB",
}

// StreamParameters stores stream parameters.
type StreamParameters struct {
	SourceTableID      uint64             `json:"sourceTableID,omitempty"`
	SourceTable        cdcpb.CDCTableInfo `json:"sourceTable,omitempty"`
	TargetTableID      uint64             `json:"targetTableID,omitempty"`
	TargetTable        cdcpb.CDCTableInfo `json:"targetTable,omitempty"`
	TargetStartColName string             `json:"targetStartColName,omitempty"`
	TargetEndColName   string             `json:"targetEndColName,omitempty"`
	Options            StreamOptions      `json:"options,omitempty"`
	StreamSink         StreamSink         `json:"streamSink,omitempty"`
}

// StreamOptions stores stream options.
type StreamOptions struct {
	Enable                 string `json:"enable,omitempty"`
	MaxDelay               string `json:"max_delay,omitempty"`
	SyncTime               string `json:"sync_time,omitempty"`
	ProcessHistory         string `json:"process_history,omitempty"`
	IgnoreExpired          string `json:"ignore_expired,omitempty"`
	IgnoreUpdate           string `json:"ignore_update,omitempty"`
	MaxRetries             string `json:"max_retries,omitempty"`
	CheckpointInterval     string `json:"checkpoint_interval,omitempty"`
	HeartbeatInterval      string `json:"heartbeat_interval,omitempty"`
	RecalculateDelayRounds string `json:"recalculate_delay_rounds,omitempty"`
	BufferSize             string `json:"buffer_size,omitempty"`
}

// ParsedStreamOptions stores parsed stream options.
type ParsedStreamOptions struct {
	Enable                 bool
	MaxDelay               time.Duration
	SyncTime               time.Duration
	ProcessHistory         bool
	IgnoreExpired          bool
	IgnoreUpdate           bool
	MaxRetries             int
	CheckpointInterval     time.Duration
	HeartbeatInterval      time.Duration
	RecalculateDelayRounds int
	BufferSize             uint64
}

// constructStreamOpts converts map to StreamOptions.
func constructStreamOpts(input map[string]string) *StreamOptions {
	return &StreamOptions{
		Enable:                 input[OptEnable],
		MaxDelay:               input[OptMaxDelay],
		SyncTime:               input[OptSyncTime],
		ProcessHistory:         input[OptProcessHistory],
		IgnoreExpired:          input[OptIgnoreExpired],
		IgnoreUpdate:           input[OptIgnoreUpdate],
		MaxRetries:             input[OptMaxRetries],
		CheckpointInterval:     input[OptCheckpointInterval],
		HeartbeatInterval:      input[OptHeartbeatInterval],
		RecalculateDelayRounds: input[OptRecalculateDelayRounds],
		BufferSize:             input[OptBufferSize],
	}
}

// ConvertStreamOptsToMap converts StreamOptions to map.
func ConvertStreamOptsToMap(opts *StreamOptions) map[string]string {
	return map[string]string{
		OptEnable:                 opts.Enable,
		OptMaxDelay:               opts.MaxDelay,
		OptSyncTime:               opts.SyncTime,
		OptProcessHistory:         opts.ProcessHistory,
		OptIgnoreExpired:          opts.IgnoreExpired,
		OptIgnoreUpdate:           opts.IgnoreUpdate,
		OptMaxRetries:             opts.MaxRetries,
		OptCheckpointInterval:     opts.CheckpointInterval,
		OptHeartbeatInterval:      opts.HeartbeatInterval,
		OptRecalculateDelayRounds: opts.RecalculateDelayRounds,
		OptBufferSize:             opts.BufferSize,
	}
}

// RunInfo records information of running stream.
type RunInfo struct {
	JobID        int64  `json:"job_id,omitempty"`
	StartTime    string `json:"start_time,omitempty"`
	EndTime      string `json:"end_time,omitempty"`
	ErrorMessage string `json:"message,omitempty"`
}

// StreamSink records information of running stream query.
type StreamSink struct {
	SQL    string `json:"sql,omitempty"`
	HasAgg bool   `json:"has_agg,omitempty"`
}

// MarshalStreamParameters marshals stream parameters to json.
func MarshalStreamParameters(para StreamParameters) (kjson.JSON, error) {
	streamPara, err := json.Marshal(para)
	if err != nil {
		return nil, err
	}
	return kjson.FromString(string(streamPara)), nil
}

// MarshalStreamOptions marshals stream options to json.
func MarshalStreamOptions(para *StreamOptions) (string, error) {
	streamPara, err := json.Marshal(para)
	if err != nil {
		return "", err
	}
	return string(streamPara), nil
}

// MarshalStreamRunInfo marshals stream job info to json.
func MarshalStreamRunInfo(runInfo []RunInfo) (kjson.JSON, error) {
	info, err := json.Marshal(runInfo)
	if err != nil {
		return nil, err
	}
	return kjson.FromString(string(info)), nil
}

// UnmarshalStreamParameters unmarshal json to stream parameters.
func UnmarshalStreamParameters(parameters kjson.JSON) (StreamParameters, error) {
	var para StreamParameters
	str, err := parameters.AsText()
	if err != nil {
		return para, err
	}
	if err := json.Unmarshal([]byte(*str), &para); err != nil {
		return para, err
	}
	return para, nil
}

// UnmarshalStreamRunInfo unmarshal json to stream job info.
func UnmarshalStreamRunInfo(runInfo kjson.JSON) ([]RunInfo, error) {
	var para []RunInfo
	str, err := runInfo.AsText()
	if err != nil {
		return para, err
	}
	if err := json.Unmarshal([]byte(*str), &para); err != nil {
		return para, err
	}
	return para, nil
}

// MakeStreamOptions build stream options.
func MakeStreamOptions(
	streamOpts map[string]string, originOpts map[string]string,
) (*StreamOptions, error) {
	fixedOpts := make(map[string]string)
	for key := range DefaultOptions {
		opt, err := makeStreamOpt(streamOpts, originOpts, key)
		if err != nil {
			return nil, err
		}
		fixedOpts[key] = opt
	}
	return constructStreamOpts(fixedOpts), nil
}

// CheckStreamOptions checks stream options
func CheckStreamOptions(opts *StreamOptions, isTsTable bool) error {
	if !isTsTable {
		if opts.ProcessHistory == StreamOptOn {
			return errors.Errorf(" cannot use 'process_history'='on' on non-ts target table")
		}
		if opts.IgnoreExpired == StreamOptOff {
			return errors.Errorf("cannot use 'ignore_expired'='off' on non-ts target table")
		}

		if opts.IgnoreUpdate == StreamOptOff {
			return errors.Errorf("cannot use 'ignore_update'='off' on non-ts target table")
		}
	}

	parsedStreamOpts, err := ParseStreamOpts(opts)
	if err != nil {
		return err
	}

	if parsedStreamOpts.CheckpointInterval <= parsedStreamOpts.HeartbeatInterval {
		return errors.Errorf("the checkpoint interval should be larger than the heartbeat interval")
	}

	if parsedStreamOpts.SyncTime <= parsedStreamOpts.CheckpointInterval {
		return errors.Errorf("too small sync_time: %s", opts.SyncTime)
	}

	if parsedStreamOpts.MaxDelay <= parsedStreamOpts.SyncTime {
		return errors.Errorf("too small max_delay: %s", opts.MaxDelay)
	}

	return nil
}

// CheckStreamOptions builds the stream option.
func makeStreamOpt(
	streamOpts map[string]string, originOpts map[string]string, optName string,
) (string, error) {
	var optValue string
	if value, ok := streamOpts[optName]; ok {
		lowerValue := strings.ToLower(value)
		switch optName {
		case OptEnable, OptProcessHistory, OptIgnoreExpired, OptIgnoreUpdate:
			switch lowerValue {
			case StreamOptOn, StreamOptOff:
			default:
				return "", pgerror.Newf(
					pgcode.InvalidParameterValue,
					"unexpected value %q for stream option %q (expected 'on/off')",
					value, optName,
				)

			}
		case OptMaxDelay:
			// use parseInterval to support time unit 'day', for example '1d'.
			val, err := parseInterval(lowerValue)
			if err != nil {
				return "", pgerror.Newf(
					pgcode.InvalidParameterValue,
					"unexpected value %q for stream option %q (expected 'Time Duration', for example, '20s')",
					value, optName,
				)
			}

			if val < time.Second {
				return "", pgerror.Newf(
					pgcode.InvalidParameterValue,
					"unexpected value %q for stream option %q, the minimal value is 1 Second",
					value, optName,
				)
			}

		case OptSyncTime, OptCheckpointInterval, OptHeartbeatInterval:
			// the sync_time, checkpoint_interval and heartbeat_interval is smaller than one time.Hour,
			// so use time.ParseDuration to parse them.
			val, err := time.ParseDuration(lowerValue)
			if err != nil {
				return "", pgerror.Newf(
					pgcode.InvalidParameterValue,
					"unexpected value %q for stream option %q (expected 'Time Duration', for example, '20s')",
					value, optName,
				)
			}

			if val < time.Second {
				return "", pgerror.Newf(
					pgcode.InvalidParameterValue,
					"unexpected value %q for stream option %q, the minimal value is 1 Second",
					value, optName,
				)
			}

			if val > time.Hour {
				return "", pgerror.Newf(
					pgcode.InvalidParameterValue,
					"unexpected value %q for stream option %q, the maximum value is 1 Hour",
					value, optName,
				)
			}

		case OptMaxRetries, OptRecalculateDelayRounds:
			val, err := strconv.Atoi(value)
			if val < 0 || err != nil {
				return "", pgerror.Newf(
					pgcode.InvalidParameterValue,
					"unexpected value %q for stream option %q (expected nonnegative 'Integer', for example, '10')",
					value, optName,
				)
			}
		case OptBufferSize:
			val, err := humanize.ParseBytes(value)
			if err != nil {
				return "", pgerror.Newf(
					pgcode.InvalidParameterValue,
					"unexpected value %q for stream option %q (expected 'Bytes Size', for example, '16Mb')",
					value, optName,
				)
			}

			if val < humanize.MiByte {
				return "", pgerror.Newf(
					pgcode.InvalidParameterValue,
					"unexpected value %q for stream option %q, the minimal value is 1MiByte",
					value, optName,
				)
			}
		default:
			return "", pgerror.Newf(pgcode.Internal, "invalid stream option %q", optName)
		}
		optValue = lowerValue
	} else {
		if originOpts != nil {
			optValue = originOpts[optName]
		} else {
			optValue = DefaultOptions[optName]
		}
	}
	return optValue, nil
}

// ParseStreamParameters parses stream parameters.
func ParseStreamParameters(para string) (*StreamParameters, error) {
	parseJSON, err := kjson.ParseJSON(para)
	if err != nil {
		return nil, err
	}

	marshaledStreamOpts, err := UnmarshalStreamParameters(parseJSON)
	if err != nil {
		return nil, err
	}
	return &marshaledStreamOpts, nil
}

// ParseStreamOpts parses stream options.
func ParseStreamOpts(opts *StreamOptions) (*ParsedStreamOptions, error) {
	var parsedOpts ParsedStreamOptions
	var err error

	if opts.Enable == "on" {
		parsedOpts.Enable = true
	}

	parsedOpts.MaxDelay, err = parseInterval(opts.MaxDelay)
	if err != nil {
		return nil, err
	}

	parsedOpts.SyncTime, err = time.ParseDuration(opts.SyncTime)
	if err != nil {
		return nil, err
	}

	if opts.ProcessHistory == "on" {
		parsedOpts.ProcessHistory = true
	}

	if opts.IgnoreExpired == "on" {
		parsedOpts.IgnoreExpired = true
	}

	if opts.IgnoreUpdate == "on" {
		parsedOpts.IgnoreUpdate = true
	}

	parsedOpts.MaxRetries, err = strconv.Atoi(opts.MaxRetries)
	if err != nil {
		return nil, err
	}

	parsedOpts.RecalculateDelayRounds, err = strconv.Atoi(opts.RecalculateDelayRounds)
	if err != nil {
		return nil, err
	}

	parsedOpts.CheckpointInterval, err = time.ParseDuration(opts.CheckpointInterval)
	if err != nil {
		return nil, err
	}

	parsedOpts.HeartbeatInterval, err = time.ParseDuration(opts.HeartbeatInterval)
	if err != nil {
		return nil, err
	}

	parsedOpts.BufferSize, err = humanize.ParseBytes(opts.BufferSize)
	if err != nil {
		return nil, err
	}

	return &parsedOpts, nil
}

// parseInterval parses and returns the *DInterval Datum value represented by the provided string.
// It supports ms, s, m, h, d, w.
func parseInterval(intervalStr string) (time.Duration, error) {
	dInterval, err := tree.ParseDInterval(intervalStr)
	if err != nil {
		return 0, err
	}
	var newTime int64
	if dInterval.Months != 0 {
		return 0, errors.Errorf("unit 'Month' in duration %q is not supported", intervalStr)
	}

	if dInterval.Days != 0 {
		newTime += dInterval.Days * int64(time.Hour*24)
	}
	if dInterval.Nanos() != 0 {
		newTime += dInterval.Nanos()
	}

	return time.Duration(newTime), nil
}

// ShouldLogError returns if the error is canceled.
func ShouldLogError(err error) bool {
	// error message 'context canceled' means user stops the job,
	// consider it as a normal situation.
	return !strings.Contains(err.Error(), "context canceled")
}
