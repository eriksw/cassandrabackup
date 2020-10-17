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
	"context"

	"github.com/retailnext/cassandrabackup/manifests"
	"go.uber.org/zap"
)

func (c *Client) ListHostNames(ctx context.Context, cluster string) ([]manifests.NodeIdentity, error) {
	listPrefix := c.layout.absoluteKeyPrefixForClusterHosts(cluster)
	prefixes, err := c.storageClient.ListPrefixes(ctx, listPrefix)
	if err != nil {
		return nil, err
	}
	nodes, bonus := c.layout.decodeClusterHosts(prefixes)
	if len(bonus) > 0 {
		zap.S().Warnw("unexpected_prefixes_in_bucket_listing_hosts", "cluster", cluster, "hosts", nodes, "bonus", bonus)
	}
	return nodes, nil
}

func (c *Client) ListClusters(ctx context.Context) ([]string, error) {
	listPrefix := c.layout.absoluteKeyPrefixForClusters()
	prefixes, err := c.storageClient.ListPrefixes(ctx, listPrefix)
	if err != nil {
		return nil, err
	}
	clusters, bonus := c.layout.decodeClusters(prefixes)
	if len(bonus) > 0 {
		zap.S().Warnw("unexpected_prefixes_in_bucket_listing_clusters", "clusters", clusters, "bonus", bonus)
	}
	return clusters, nil
}
