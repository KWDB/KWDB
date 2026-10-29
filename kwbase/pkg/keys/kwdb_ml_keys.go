// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package keys

import "gitee.com/kwbasedb/kwbase/pkg/roachpb"

// systemNamedKey returns the system-range key for the given name. It is used
// by the ML model-version generator.
func systemNamedKey(name string) roachpb.Key {
	return roachpb.Key(makeKey(SystemPrefix, roachpb.RKey(name)))
}

// ModelVersionPrefix is the sequenced numeric version for each ML model
// version. The suffix of the key is the model UUID.
var ModelVersionPrefix = systemNamedKey("modelversion-idgen/")
