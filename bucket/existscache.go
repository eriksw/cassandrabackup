// Copyright 2020 RetailNext, Inc.
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

package bucket

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/retailnext/cassandrabackup/cache"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

const objectLockSafetyMargin = 12 * time.Hour

type ExistsCache struct {
	Storage                  *cache.Storage
	UseDeprecatedCommonFiles bool
	lock                     sync.RWMutex
	caches                   map[string]*cache.Cache
}

func (e *ExistsCache) getCache(node manifests.NodeIdentity) *cache.Cache {
	if e.Storage == nil {
		return nil
	}
	cacheName := "bucket_exists"
	if !e.UseDeprecatedCommonFiles {
		cacheName = "bucket_exists/" + node.Cluster + "/" + node.Hostname
	}
	e.lock.RLock()
	nodeCache, ok := e.caches[cacheName]
	e.lock.RUnlock()
	if ok {
		return nodeCache
	}

	e.lock.Lock()
	defer e.lock.Unlock()
	if e.caches == nil {
		e.caches = make(map[string]*cache.Cache)
	}
	nodeCache, ok = e.caches[cacheName]
	if ok {
		return nodeCache
	}

	nodeCache = e.Storage.Cache(cacheName)
	e.caches[cacheName] = nodeCache
	return nodeCache
}

func (e *ExistsCache) Get(node manifests.NodeIdentity, restore digest.ForRestore) bool {
	c := e.getCache(node)
	var exists bool
	key, err := restore.MarshalBinary()
	if err != nil {
		panic(err)
	}
	err = c.Get(key, func(value []byte) error {
		var lockedUntil unixtime.Seconds
		if err := lockedUntil.UnmarshalBinary(value); err != nil {
			return err
		}
		if time.Now().Add(objectLockSafetyMargin).Unix() < int64(lockedUntil) {
			exists = true
			return nil
		} else {
			existsCacheLockTimeMisses.Inc()
		}
		return cache.DoNotPromote
	})
	if err != nil {
		switch err {
		case cache.NotFound, cache.DoNotPromote:
		default:
			zap.S().Warnw("blob_exists_cache_get_error", "key", restore, "err", err)
		}
	}
	return exists
}

func (e *ExistsCache) Put(node manifests.NodeIdentity, restore digest.ForRestore, lockedUntil time.Time) {
	c := e.getCache(node)
	key, err := restore.MarshalBinary()
	if err != nil {
		panic(err)
	}
	seconds := unixtime.Seconds(lockedUntil.Unix())
	value, err := seconds.MarshalBinary()
	if err != nil {
		panic(err)
	}
	err = c.Put(key, value)
	if err != nil {
		zap.S().Warnw("blob_exists_cache_put_error", "key", restore, "err", err)
	}
}

var existsCacheLockTimeMisses = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "cassandrabackup",
	Subsystem: "bucket_exists_cache",
	Name:      "lock_time_misses_total",
	Help:      "Number of exists cache misses due to expired/future lock time.",
})

func init() {
	prometheus.MustRegister(existsCacheLockTimeMisses)
}
