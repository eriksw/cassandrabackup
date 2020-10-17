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

package special

import (
	"context"

	"github.com/retailnext/cassandrabackup/backup/prospect"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/paranoid"
	"go.uber.org/zap"
)

type ToUpload struct {
	File    paranoid.File
	Digests digest.ForUpload
}

type ToUploadByManifestPath map[string]ToUpload

func FindLiveFiles(ctx context.Context) (ToUploadByManifestPath, error) {
	lgr := zap.S()
	attempt := 0
	for {
		result, err := findLiveFiles(ctx, lgr.With("attempt", attempt))
		if err == nil {
			return result, nil
		}
		if attempt > 4 {
			return nil, err
		}
		attempt++
	}
}

func findLiveFiles(ctx context.Context, lgr *zap.SugaredLogger) (ToUploadByManifestPath, error) {
	root := "/var/lib/cassandra/data"

	files, err := prospect.GetFiles(root, prospect.UnsafeLivePathProcessor, prospect.IgnoreLiveErrors)
	if err != nil {
		lgr.Errorw("prospect_get_files_error", "err", err)
		return nil, err
	}
	return getDigests(ctx, lgr, files)
}

func getDigests(ctx context.Context, lgr *zap.SugaredLogger, files map[string]paranoid.File) (ToUploadByManifestPath, error) {
	digestCache := digest.OpenShared()

	result := make(ToUploadByManifestPath)
	for manifestPath, file := range files {
		digests, err := digestCache.Get(ctx, file)
		if err != nil {
			lgr.Errorw("digest_get_error", "file", file, "err", err)
			return nil, err
		}
		result[manifestPath] = ToUpload{
			File:    file,
			Digests: digests,
		}
	}

	return result, nil
}
