// Copyright 2017 The Cockroach Authors.
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

// Package tc contains utility methods for using the Linux tc (traffic control)
// command to mess with the network links between kwbase nodes running on
// the local machine.
//
// Requires passwordless sudo in order to run tc.
//
// Does not work on OS X due to the lack of the tc command (and even an
// alternative wouldn't work for the current use case of this code, which also
// requires being able to bind to multiple localhost addresses).
package tc

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const (
	rootHandle   = 1
	defaultClass = 1
)

// Controller provides a way to add artificial latency to local traffic.
type Controller struct {
	interfaces []string
	nextClass  int
}

// NewController creates and returns a controller that will modify the traffic
// routing for the provided interfaces.
func NewController(interfaces ...string) *Controller {
	return &Controller{
		interfaces: interfaces,
		nextClass:  defaultClass + 1,
	}
}

// Init prepares the local network interfaces so that we can later add per-node
// traffic shaping rules.
func (c *Controller) Init() error {
	for _, ifce := range c.interfaces {
		_, _ = exec.Command("sudo", strings.Split(fmt.Sprintf("tc qdisc del dev %s root", ifce), " ")...).Output()
		out, err := exec.Command("sudo", strings.Split(fmt.Sprintf("tc qdisc add dev %s root handle %d: htb default %d",
			ifce, rootHandle, defaultClass), " ")...).Output()
		if err != nil {
			return errors.Wrapf(err, "failed to create root tc qdisc for %q: %s", ifce, out)
		}
		// The 100mbit limitation is because classes of type htb (hierarchy token
		// bucket) need a bandwidth limit, and we want an arbitrarily high one. Feel
		// free to bump it up here and below if you think it's limiting you.
		out, err = exec.Command("sudo", strings.Split(fmt.Sprintf("tc class add dev %s parent %d: classid %d:%d htb rate 100mbit",
			ifce, rootHandle, rootHandle, defaultClass), " ")...).Output()
		if err != nil {
			return errors.Wrapf(err, "failed to create root tc class for %q: %s", ifce, out)
		}
	}
	return nil
}

// AddLatency adds artificial latency between the specified source and dest
// addresses.
func (c *Controller) AddLatency(srcIP, dstIP string, latency time.Duration) error {
	class := c.nextClass
	handle := class * 10
	c.nextClass++
	for _, ifce := range c.interfaces {
		out, err := exec.Command("sudo", strings.Split(fmt.Sprintf("tc class add dev %s parent %d: classid %d:%d htb rate 100mbit",
			ifce, rootHandle, rootHandle, class), " ")...).Output()
		if err != nil {
			return errors.Wrapf(err, "failed to add tc class %d: %s", class, out)
		}
		out, err = exec.Command("sudo", strings.Split(fmt.Sprintf("tc qdisc add dev %s parent %d:%d handle %d: netem delay %v",
			ifce, rootHandle, class, handle, latency), " ")...).Output()
		if err != nil {
			return errors.Wrapf(err, "failed to add tc netem delay of %v: %s", latency, out)
		}
		out, err = exec.Command("sudo", strings.Split(fmt.Sprintf("tc filter add dev %s parent %d: protocol ip u32 match ip src %s/32 match ip dst %s/32 flowid %d:%d",
			ifce, rootHandle, srcIP, dstIP, rootHandle, class), " ")...).Output()
		if err != nil {
			return errors.Wrapf(err, "failed to add tc filter rule between %s and %s: %s", srcIP, dstIP, out)
		}
	}
	return nil
}

// CleanUp resets all interfaces back to their default tc policies.
func (c *Controller) CleanUp() error {
	var errs []string
	for _, ifce := range c.interfaces {
		out, err := exec.Command("sudo", strings.Split(fmt.Sprintf("tc qdisc del dev %s root", ifce), " ")...).Output()
		if err != nil {
			errs = append(errs, errors.Wrapf(
				err, "failed to remove tc rules for %q -- you may have to remove them manually: %s", ifce, out).Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}
