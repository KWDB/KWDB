// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package sql

import (
	"gitee.com/kwbasedb/kwbase/pkg/settings"
)

// MLAgentAddress is the address of the KWDB ML agent (gRPC endpoint).
//
//	SET CLUSTER SETTING ml.agent.addr = 'host:port';
//
// The ML management code (package mlmgt) reads this setting through
// ExecutorConfig.MLAgent().
var MLAgentAddress = settings.RegisterPublicStringSetting(
	"ml.agent.addr",
	"the address of KWDB ML cluster",
	"",
)
