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

package prospect

import (
	"path/filepath"
	"strings"
)

type PathProcessor func(relativePath string) string

type IncrementalPathProcessor struct {
	ignoreKeyspaces map[string]struct{}
}

func (p IncrementalPathProcessor) ManifestPath(dataRelPath string) string {
	parts := strings.Split(dataRelPath, string(filepath.Separator))
	if len(parts) < 4 || parts[2] != "backups" {
		return ""
	}

	// This is just for development
	if _, ignore := p.ignoreKeyspaces[parts[0]]; ignore {
		return ""
	}

	restoreParts := make([]string, 0, len(parts)-1)
	restoreParts = append(restoreParts, parts[0:2]...)
	restoreParts = append(restoreParts, parts[3:]...)
	return strings.Join(restoreParts, string(filepath.Separator))
}

type SnapshotPathProcessor struct {
	name string
}

func (p SnapshotPathProcessor) ManifestPath(dataRelPath string) string {
	parts := strings.Split(dataRelPath, string(filepath.Separator))
	if len(parts) < 5 || parts[2] != "snapshots" || parts[3] != p.name {
		return ""
	}
	restoreParts := make([]string, 0, len(parts)-2)
	restoreParts = append(restoreParts, parts[0:2]...)
	restoreParts = append(restoreParts, parts[4:]...)
	return strings.Join(restoreParts, string(filepath.Separator))
}

func UnsafeLivePathProcessor(dataRelPath string) string {
	if strings.HasSuffix(dataRelPath, ".tmp") {
		// Ignore files that are in the process of being written.
		return ""
	}
	parts := strings.Split(dataRelPath, string(filepath.Separator))
	switch len(parts) {
	case 3:
		switch parts[2] {
		case "manifest.json", "schema.cql":
			return ""
		}
		return dataRelPath
	case 4:
		switch parts[2] {
		case "backups", "snapshots":
			return ""
		}
		switch parts[3] {
		case "manifest.json", "schema.cql":
			return ""
		}
		return dataRelPath
	}
	return ""
}
