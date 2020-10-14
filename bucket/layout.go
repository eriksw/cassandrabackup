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
//

package bucket

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/unixtime"
)

type Layout struct {
	Prefix                   string
	UseDeprecatedCommonFiles bool
}

func (l Layout) keyWithPrefix(key string) string {
	if l.Prefix == "" {
		return key
	}
	return l.Prefix + "/" + key
}

func (l Layout) AbsoluteKeyForBlob(node manifests.NodeIdentity, digests digest.ForRestore) string {
	encoded := digests.URLSafe()
	if l.UseDeprecatedCommonFiles {
		return l.keyWithPrefix("files/blake2b/" + encoded[0:1] + "/" + encoded[1:2] + "/" + encoded[2:])
	}
	urlCluster, urlHostname := encodeNodeIdentity(node)
	return l.keyWithPrefix("files/" + urlCluster + "/" + urlHostname + "/blake2b/" + encoded[0:1] + "/" + encoded[1:2] + "/" + encoded[2:])
}

func encodeNodeIdentity(node manifests.NodeIdentity) (urlCluster, urlHostname string) {
	urlCluster = base64.URLEncoding.EncodeToString([]byte(node.Cluster))
	urlHostname = base64.URLEncoding.EncodeToString([]byte(node.Hostname))
	return
}

func (l Layout) absoluteKeyPrefixForClusters() string {
	return l.keyWithPrefix("manifests/")
}

func (l Layout) absoluteKeyPrefixForClusterHosts(cluster string) string {
	if cluster == "" {
		panic("empty cluster")
	}
	urlCluster := base64.URLEncoding.EncodeToString([]byte(cluster))
	clustersPrefix := l.absoluteKeyPrefixForClusters()
	return fmt.Sprintf("%s%s/", clustersPrefix, urlCluster)
}

func (l Layout) absoluteKeyPrefixForManifests(identity manifests.NodeIdentity) string {
	if identity.Hostname == "" {
		panic("empty Hostname")
	}
	clusterPrefix := l.absoluteKeyPrefixForClusterHosts(identity.Cluster)
	urlHostname := base64.URLEncoding.EncodeToString([]byte(identity.Hostname))
	return fmt.Sprintf("%s%s/", clusterPrefix, urlHostname)
}

func (l Layout) absoluteKeyForManifestTimeRange(identity manifests.NodeIdentity, boundary unixtime.Seconds) string {
	return l.absoluteKeyPrefixForManifests(identity) + boundary.Decimal()
}

func (l Layout) absoluteKeyForManifest(identity manifests.NodeIdentity, manifestKey manifests.ManifestKey) string {
	return l.absoluteKeyPrefixForManifests(identity) + manifestKey.FileName()
}

func (l Layout) decodeCluster(key string) (string, error) {
	clustersPrefix := l.absoluteKeyPrefixForClusters()
	urlCluster := strings.TrimSuffix(strings.TrimPrefix(key, clustersPrefix), "/")
	cluster, err := base64.URLEncoding.DecodeString(urlCluster)
	return string(cluster), err
}

func (l Layout) decodeClusterHosts(prefixes []*s3.CommonPrefix) ([]manifests.NodeIdentity, []string) {
	result := make([]manifests.NodeIdentity, 0, len(prefixes))
	var bonus []string
	skip := len(l.absoluteKeyPrefixForClusters())
	for _, obj := range prefixes {
		raw := *obj.Prefix
		trimmed := raw[skip:]
		parts := strings.Split(trimmed, "/")
		if len(parts) != 3 {
			bonus = append(bonus, raw)
			continue
		}
		cluster, err := base64.URLEncoding.DecodeString(parts[0])
		if err != nil {
			bonus = append(bonus, raw)
			continue
		}
		hostname, err := base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			bonus = append(bonus, raw)
			continue
		}
		ni := manifests.NodeIdentity{
			Cluster:  string(cluster),
			Hostname: string(hostname),
		}
		result = append(result, ni)
	}
	return result, bonus
}
