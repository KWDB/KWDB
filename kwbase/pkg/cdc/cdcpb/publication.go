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

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	kjson "gitee.com/kwbasedb/kwbase/pkg/util/json"
)

// GetPublicationsHook returns the instance interface Publications
var GetPublicationsHook func() Publications

// Publications maintains Publications like metadata
type Publications interface {
	// GetPublicationInfo returns current Publication info.
	GetPublicationInfo(evalCtx *tree.EvalContext, pubName string) ([]byte, error)
	// SubscriptRealtime reads real time data from CDC.
	SubscriptRealtime(evalCtx *tree.EvalContext, pubName string, param []byte) (tree.ValueGenerator, error)
	// SubscriptHistory reads history data.
	SubscriptHistory(evalCtx *tree.EvalContext, pubName string, param []byte) (tree.ValueGenerator, error)
}

// PubSubFuncType defines the tuples of returned names and types in publication and subscription.
var PubSubFuncType = types.MakeLabeledTuple(
	[]types.T{*types.String, *types.String, *types.Bytes, *types.Int, *types.Int, *types.Int, *types.String},
	[]string{"table_name", "data_type", "data", "format", "row_number", "watermark", "cluster_id"},
)

// PubParameters stores parameters of publication.
type PubParameters struct {
	TableList  []CDCTableInfo `json:"table_list"`
	PubOptions PubOptions     `json:"options"`
	DatabaseID uint64         `json:"database_id,omitempty"`
}

// PubOptions stores options of publication.
type PubOptions struct {
	Publish    string `json:"publish"`
	BufferSize int    `json:"buffer_size"`
	CheckTag   string `json:"retrieve_tags"`
	SubTimeout int    `json:"sub_timeout"`
}

// MarshalPubParameters marshals parameters of publication to json.
func MarshalPubParameters(para PubParameters) (kjson.JSON, error) {
	PubPara, err := json.Marshal(para)
	if err != nil {
		return nil, err
	}
	return kjson.FromString(string(PubPara)), nil
}

// UnmarshalPubParameters unmarshal json to parameters of publication.
func UnmarshalPubParameters(parameters kjson.JSON) (PubParameters, error) {
	var para PubParameters
	str, err := parameters.AsText()
	if err != nil {
		return para, err
	}
	if err := json.Unmarshal([]byte(*str), &para); err != nil {
		return para, err
	}
	return para, nil
}
