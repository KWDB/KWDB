// Copyright 2018 The Cockroach Authors.
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

package install

import (
	"bufio"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"os"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cmd/roachprod/ssh"
)

// Cassandra TODO(peter): document
type Cassandra struct{}

// Start implements the ClusterImpl.NodeDir interface.
func (Cassandra) Start(c *SyncedCluster, extraArgs []string) {
	yamlPath, err := makeCassandraYAML(c)
	if err != nil {
		log.Fatal(err)
	}
	c.Put(yamlPath, "./cassandra.yaml")
	_ = os.Remove(yamlPath)

	display := fmt.Sprintf("%s: starting cassandra (be patient)", c.Name)
	nodes := c.ServerNodes()
	c.Parallel(display, len(nodes), 1, func(i int) ([]byte, error) {
		host := c.host(nodes[i])
		user := c.user(nodes[i])

		if err := func() error {
			session, err := ssh.NewSSHSession(user, host)
			if err != nil {
				return err
			}
			defer func() {
				_ = session.Close()
			}()

			cmd := `env ` + c.Env + ` ROACHPROD=true cassandra` +
				` -Dcassandra.config=file://${PWD}/cassandra.yaml` +
				` -Dcassandra.ring_delay_ms=3000` +
				` > cassandra.stdout 2> cassandra.stderr`
			_, err = session.CombinedOutput(cmd)
			return err
		}(); err != nil {
			return nil, err
		}

		for {
			up, err := func() (bool, error) {
				session, err := ssh.NewSSHSession(user, host)
				if err != nil {
					return false, err
				}
				defer func() {
					_ = session.Close()
				}()

				cmd := `nc -z $(hostname) 9042`
				if _, err := session.CombinedOutput(cmd); err != nil {
					// The common case here is going to be "exit status 1" until the
					// cassandra process starts listening on the port. Logging would
					// just generate noise.
					return false, nil //nolint:returnerrcheck
				}
				return true, nil
			}()
			if err != nil {
				return nil, err
			}
			if up {
				break
			}
			time.Sleep(time.Second)
		}
		return nil, nil
	})
}

// NodeDir implements the ClusterImpl.NodeDir interface.
func (Cassandra) NodeDir(c *SyncedCluster, index int) string {
	if c.IsLocal() {
		// TODO(peter): This will require a bit of work to adjust paths in
		// cassandra.yaml.
		panic("Cassandra.NodeDir unimplemented")
	}
	return "/mnt/data1/cassandra"
}

// LogDir implements the ClusterImpl.NodeDir interface.
func (Cassandra) LogDir(c *SyncedCluster, index int) string {
	panic("Cassandra.LogDir unimplemented")
}

// CertsDir implements the ClusterImpl.NodeDir interface.
func (Cassandra) CertsDir(c *SyncedCluster, index int) string {
	panic("Cassandra.CertsDir unimplemented")
}

// NodeURL implements the ClusterImpl.NodeDir interface.
func (Cassandra) NodeURL(_ *SyncedCluster, host string, port int) string {
	return fmt.Sprintf("'cassandra://%s:%d'", host, port)
}

// NodePort implements the ClusterImpl.NodeDir interface.
func (Cassandra) NodePort(c *SyncedCluster, index int) int {
	// TODO(peter): This will require a bit of work to adjust ports in
	// cassandra.yaml.
	// if c.IsLocal() {
	// }
	return 9042
}

// NodeUIPort implements the ClusterImpl.NodeDir interface.
func (Cassandra) NodeUIPort(c *SyncedCluster, index int) int {
	return 0 // unimplemented
}

func makeCassandraYAML(c *SyncedCluster) (string, error) {
	ip, err := c.GetInternalIP(c.ServerNodes()[0])
	if err != nil {
		return "", err
	}

	f, err := ioutil.TempFile("", "cassandra.yaml")
	if err != nil {
		return "", err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	if _, err := w.WriteString(cassandraDefaultYAML); err != nil {
		return "", err
	}
	defer w.Flush()

	t, err := template.New("cassandra.yaml").Parse(cassandraDiffYAML)
	if err != nil {
		log.Fatal(err)
	}
	m := map[string]interface{}{
		"Seeds": ip,
	}
	if err := t.Execute(w, m); err != nil {
		log.Fatal(err)
	}
	return f.Name(), nil
}
