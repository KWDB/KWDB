// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package pgwire

import (
	"encoding/json"
	"hash/fnv"
	"net"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

type session struct {
	Username        string
	PortalName      string
	CurrentTime     time.Time
	Expire          time.Time
	ServerHost      string
	RemoteAddr      net.Addr
	ApplicationName string
}

func generateToken(c *conn, addr string) (int64, error) {
	portalName, _ := c.sessionArgs.SessionDefaults.Get("portal")
	applicationName, _ := c.sessionArgs.SessionDefaults.Get("application_name")
	s := session{
		Username:        c.sessionArgs.User,
		PortalName:      portalName,
		CurrentTime:     timeutil.Now(),
		Expire:          time.Time{},
		ServerHost:      addr,
		RemoteAddr:      c.sessionArgs.RemoteAddr,
		ApplicationName: applicationName,
	}

	bytes, err := json.Marshal(s)
	if err != nil {
		return 0, err
	}

	h1 := fnv.New64()
	h1.Write(bytes)
	token := int64(h1.Sum64())

	if token == 0 {
		return generateToken(c, addr)
	}

	return token, nil
}
