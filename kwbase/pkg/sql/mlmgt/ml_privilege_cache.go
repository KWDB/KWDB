// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

const mlMLPrivilegesCacheCapacity = 1000

// MLPrivilegesCache is ml privileges cache.
type MLPrivilegesCache struct {
	lock       syncutil.Mutex
	gossip     *gossip.Gossip
	size       int
	capacity   int
	cache      map[string]*mlDLinkedNode
	head, tail *mlDLinkedNode
}

type mlDLinkedNode struct {
	key        string
	prev, next *mlDLinkedNode
}

// NewMLPrivilegesCache create a new MLPrivilegesCache.
func NewMLPrivilegesCache(g *gossip.Gossip) *MLPrivilegesCache {
	cache := &MLPrivilegesCache{
		cache:    map[string]*mlDLinkedNode{},
		gossip:   g,
		capacity: mlMLPrivilegesCacheCapacity,
	}

	cache.head = cache.initDLinkedNode("head")
	cache.tail = cache.initDLinkedNode("tail")
	cache.head.next = cache.tail
	cache.tail.prev = cache.head

	if g != nil {
		g.RegisterCallback(gossip.KeyGossipMLCache, cache.release, gossip.Redundant)
	}

	return cache
}

func (c *MLPrivilegesCache) initDLinkedNode(key string) *mlDLinkedNode {
	return &mlDLinkedNode{
		key: key,
	}
}

func (c *MLPrivilegesCache) genKey(grantee, schema, name string) string {
	return fmt.Sprintf("%s-%s-%s", schema, name, grantee)
}

// Get cache.
func (c *MLPrivilegesCache) Get(grantee, schema, name string) (bool, string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	key := c.genKey(grantee, schema, name)

	node, ok := c.cache[key]
	if !ok {
		return false, key
	}

	c.moveToHead(node)

	return true, key
}

func (c *MLPrivilegesCache) notifying() {
	if err := c.gossip.AddInfo(gossip.KeyGossipMLCache, nil, 0); err != nil {
		log.Warningf(context.Background(), "error notifying of ml privileges cache: %s", err)
	}
}

func (c *MLPrivilegesCache) release(key string, _ roachpb.Value) {
	if key != gossip.KeyGossipMLCache {
		return
	}

	c.ResetAll()
}

// ResetAll cache.
func (c *MLPrivilegesCache) ResetAll() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.cache = map[string]*mlDLinkedNode{}
	c.head = c.initDLinkedNode("head")
	c.tail = c.initDLinkedNode("tail")
	c.head.next = c.tail
	c.tail.prev = c.head
}

// Put cache.
func (c *MLPrivilegesCache) Put(grantee, schema, name string) {
	key := c.genKey(grantee, schema, name)
	c.PutKey(key)
}

// PutKey cache.
func (c *MLPrivilegesCache) PutKey(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	node := c.initDLinkedNode(key)
	c.cache[key] = node
	c.addToHead(node)
	c.size++

	if c.size > c.capacity {
		removed := c.removeTail()
		delete(c.cache, removed.key)
		c.size--
	}
}

// Claim cache.
func (c *MLPrivilegesCache) Claim(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if node, ok := c.cache[key]; ok {
		c.moveToHead(node)
	}
}

// Reset cache.
func (c *MLPrivilegesCache) Reset(grantee, schema, name string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	key := c.genKey(grantee, schema, name)

	if node, ok := c.cache[key]; ok {
		c.removeNode(node)
		delete(c.cache, key)
		c.size--
	}

	c.notifying()
}

// BatchReset remove the cache of a model or training pipeline.
func (c *MLPrivilegesCache) BatchReset(schema, name string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	prefixKey := c.genKey("", schema, name)

	for key := range c.cache {
		if strings.HasPrefix(key, prefixKey) {
			delete(c.cache, key)
			c.size--
		}
	}

	c.notifying()
}

func (c *MLPrivilegesCache) String() string {
	var buf bytes.Buffer

	for k := range c.cache {
		_, _ = fmt.Fprintf(&buf, "%s \n", k)
	}

	return buf.String()
}

func (c *MLPrivilegesCache) addToHead(node *mlDLinkedNode) {
	node.prev = c.head
	node.next = c.head.next
	c.head.next.prev = node
	c.head.next = node
}

func (c *MLPrivilegesCache) removeNode(node *mlDLinkedNode) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (c *MLPrivilegesCache) moveToHead(node *mlDLinkedNode) {
	c.removeNode(node)
	c.addToHead(node)
}

func (c *MLPrivilegesCache) removeTail() *mlDLinkedNode {
	node := c.tail.prev
	c.removeNode(node)

	return node
}
