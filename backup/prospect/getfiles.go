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

	"github.com/retailnext/cassandrabackup/paranoid"
)

func GetFiles(root string, pathProcessor PathProcessor, errorFilter WalkErrorFilter) (map[string]paranoid.File, error) {
	result := make(map[string]paranoid.File)

	walkErr := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			// We always want to crash out if there's an error regarding a directory.
			// (If there isn't an error, that's fine too.)
			return err
		}

		relPath, relPathErr := filepath.Rel(root, path)
		if relPathErr != nil {
			panic(relPathErr)
		}
		if err != nil {
			err = errorFilter(relPath, err)
		}
		if err != nil {
			return err
		}
		if resultPath := pathProcessor(relPath); resultPath != "" {
			result[resultPath] = paranoid.NewFileFromInfo(path, info)
		}
		return nil
	})

	if walkErr != nil {
		return nil, walkErr
	}

	return result, nil
}
