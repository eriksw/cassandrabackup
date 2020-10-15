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
	"path/filepath"

	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

func (c *Client) ListManifests(ctx context.Context, identity manifests.NodeIdentity, startAfter, notAfter unixtime.Seconds) (manifests.ManifestKeys, error) {
	lgr := zap.S()
	prefix := c.layout.absoluteKeyPrefixForManifests(identity)
	startOffset := c.layout.absoluteKeyForManifestTimeRange(identity, startAfter)
	// TODO this is probably wrong
	endOffset := ""
	if notAfter > 0 {
		endOffset = c.layout.absoluteKeyForManifestTimeRange(identity, notAfter+1)
	}

	keys, err := c.storageClient.ListObjects(ctx, prefix, startOffset, endOffset)
	if err != nil {
		return nil, err
	}

	var result manifests.ManifestKeys
	for _, key := range keys {
		name := filepath.Base(key)
		var manifestKey manifests.ManifestKey
		if err := manifestKey.PopulateFromFileName(name); err != nil {
			lgr.Warnw("list_manifests_ignoring_bad_filename", "name", name, "err", err)
		} else {
			result = append(result, manifestKey)
		}
	}
	return result, nil
}

func (c *Client) PutManifest(ctx context.Context, identity manifests.NodeIdentity, manifest manifests.Manifest) error {
	if manifest.ManifestType == manifests.ManifestTypeInvalid {
		panic("invalid manifest type")
	}
	absoluteKey := c.layout.absoluteKeyForManifest(identity, manifest.Key())
	return c.putDocument(ctx, absoluteKey, manifest)
}

func (c *Client) GetManifests(ctx context.Context, identity manifests.NodeIdentity, keys manifests.ManifestKeys) ([]manifests.Manifest, error) {
	var results []manifests.Manifest
	for _, manifestKey := range keys {
		absoluteKey := c.layout.absoluteKeyForManifest(identity, manifestKey)
		var m manifests.Manifest
		if err := c.getDocument(ctx, absoluteKey, &m); err != nil {
			zap.S().Errorw("get_manifest_error", "key", absoluteKey, "err", err)
			return nil, err
		}
		results = append(results, m)
	}
	return results, nil
}
