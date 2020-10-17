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
	"os"
	"path/filepath"
	"strings"
)

type WalkErrorFilter func(relativePath string, err error) error

func IgnoreLiveErrors(relativePath string, err error) error {
	parts := strings.Split(relativePath, string(filepath.Separator))
	switch len(parts) {
	case 3:
		switch filepath.Ext(parts[2]) {
		case "txt", "db", "crc32", "sha1":
			// This may be a live sstable (not in backups or snapshots) that went away mid-scan
			if os.IsNotExist(err) {
				return nil
			}
		}
	case 4:
		switch parts[2] {
		case "backups", "snapshots":
			// This is in backups or snapshots, we cannot ignore this.
			return err
		}
		if strings.HasSuffix(parts[2], "_index") {
			// This is "live" like above, but is an index in 3.x
			switch filepath.Ext(parts[3]) {
			case "txt", "db", "crc32", "sha1":
				// This may be a live sstable of an index that went away mid-scan
				if os.IsNotExist(err) {
					return nil
				}
			}
		}
	}
	return err
}

func IgnoreSnapshotAndBackupErrors(relativePath string, err error) error {
	parts := strings.Split(relativePath, string(filepath.Separator))
	switch len(parts) {
	case 4:
		switch parts[2] {
		case "backups", "snapshots":
			// This is in backups or snapshots, we want to ignore these.
			return nil
		}
	}
	return err
}
