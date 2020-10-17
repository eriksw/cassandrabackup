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

package restore

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/multierr"

	"github.com/retailnext/cassandrabackup/paranoid"

	"go.uber.org/zap"

	"github.com/retailnext/cassandrabackup/manifests"

	"github.com/retailnext/cassandrabackup/bucket"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/writefile"
)

type worker2 struct {
	staging   writefile.Config
	target    writefile.Config
	graveyard writefile.Config

	ctx context.Context

	digestCache  *digest.Cache
	bucketClient *bucket.Client

	downloadLimiter chan struct{}
	verifyLimiter   chan struct{}

	wg sync.WaitGroup

	linkReadyWait sync.WaitGroup
	linkReady     chan struct{}

	fileStatus     map[string]FileStatus
	fileStatusLock sync.Mutex

	noDownloadToStaging   bool
	noLinkToTarget        bool
	removeInvalidAtTarget bool
}

type FileStatus struct {
	InStaging      bool
	InPlace        bool
	InvalidInPlace bool
	Error          error
}

func (w *worker2) addFileStatus(updates map[string]FileStatus) {
	w.fileStatusLock.Lock()
	defer w.fileStatusLock.Unlock()
	if w.fileStatus == nil {
		w.fileStatus = make(map[string]FileStatus)
	}
	for name, status := range updates {
		w.fileStatus[name] = status
	}
}

type statusByName map[string]FileStatus

func (sbn statusByName) isDone() bool {
	for _, status := range sbn {
		if !status.InPlace {
			return false
		}
	}
	return true
}

func (w *worker2) processFile(names []string, forRestore digest.ForRestore, nodes []manifests.NodeIdentity) {
	lgr := zap.S().With("digest", forRestore)

	statusUpdates := make(statusByName, len(names))
	for _, name := range names {
		statusUpdates[name] = FileStatus{}
	}

	defer func() {
		w.addFileStatus(statusUpdates)
		w.wg.Done()
	}()

	doneCh := w.ctx.Done()

	var inStaging bool
	select {
	case <-doneCh:
		return
	case w.verifyLimiter <- struct{}{}:
		inStaging = w.checkStaging(forRestore)
	}
	if inStaging {
		for _, name := range names {
			status := statusUpdates[name]
			status.InStaging = true
			statusUpdates[name] = status
		}
	}

	for _, name := range names {
		select {
		case <-doneCh:
			return
		case w.verifyLimiter <- struct{}{}:
			targetOk, bogonPresent, err := w.checkExisting(name, forRestore)
			status := statusUpdates[name]
			status.InPlace = targetOk
			status.InvalidInPlace = bogonPresent
			status.Error = multierr.Append(status.Error, err)
		}
	}

	if statusUpdates.isDone() {
		w.linkReadyWait.Done()
		lgr.Infow("nothing_to_do", "paths", names)
		return
	}

	if !inStaging {
		if w.noDownloadToStaging {
			lgr.Infow("would_download", "source_nodes", nodes)
			return
		}

		select {
		case <-doneCh:
			return
		case w.downloadLimiter <- struct{}{}:
			ok, err := w.downloadToStagingFromAny(forRestore, nodes)
			for _, name := range names {
				status := statusUpdates[name]
				status.Error = multierr.Append(status.Error, err)
				status.InStaging = ok
				statusUpdates[name] = status
			}
			inStaging = ok
		}
	}

	if !inStaging {
		lgr.Errorw("cannot_proceed", "status", statusUpdates)
		return
	}

	w.linkReadyWait.Done()
	// Wait for LinkReady or context close.
	select {
	case <-doneCh:
		return
	case <-w.linkReady:
	}
}

func (w *worker2) checkStaging(forRestore digest.ForRestore) bool {
	defer func() {
		<-w.verifyLimiter
	}()
	lgr := zap.S().With("digest", forRestore)

	targetPath := w.stagingPath(forRestore)
	file, err := paranoid.NewFile(targetPath)
	if err != nil {
		if !os.IsNotExist(err) {
			lgr.Errorw("error_checking_staging", "err", err)
		}
		return false
	}
	forUpload, err := w.digestCache.Get(w.ctx, file)
	if err != nil {
		if err != context.Canceled {
			lgr.Errorw("staging_get_digest_error", "err", err)
		}
		return false
	}
	if forUpload.ForRestore() != forRestore {
		bogonTargetPath := w.stagingPath(forUpload.ForRestore())
		err = os.Rename(targetPath, bogonTargetPath)
		if err == nil {
			lgr.Warnw("staging_invalid_file_remove_ok", "path", targetPath, "moved_to", bogonTargetPath)
		} else {
			lgr.Panicw("staging_invalid_file_remove_error", "path", targetPath, "err", err)
		}
		return false
	}
	return true
}

func (w *worker2) stagingPath(forRestore digest.ForRestore) string {
	return filepath.Join(w.staging.Directory, forRestore.URLSafe())
}

func (w *worker2) checkExisting(name string, forRestore digest.ForRestore) (targetOk, bogonPresent bool, err error) {
	defer func() {
		<-w.verifyLimiter
	}()

	targetPath := filepath.Join(w.target.Directory, name)
	lgr := zap.S().With("path", targetPath)

	file, err := paranoid.NewFile(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, false, nil
		}
		lgr.Errorw("check_existing_file_error", "err", err)
		return false, false, err
	}
	forUpload, err := w.digestCache.Get(w.ctx, file)
	if err != nil {
		if os.IsNotExist(err) || err == context.Canceled {
			return false, false, nil
		}
		lgr.Errorw("check_existing_file_digest_error", "err", err)
		return false, false, err
	}

	if forUpload.ForRestore() != forRestore {
		if w.removeInvalidAtTarget {
			relPath := filepath.Join(targetPath, forUpload.URLSafe())
			encodedRelPath := base64.URLEncoding.EncodeToString([]byte(relPath))
			graveyardPath := filepath.Join(w.graveyard.Directory, encodedRelPath)
			err = w.graveyard.EnsureDirectory()
			if err != nil {
				err = os.Rename(targetPath, graveyardPath)
			}
			if os.IsNotExist(err) {
				err = nil
			}
			if err == nil {
				lgr.Infow("remove_invalid_target_ok")
				return false, false, nil
			}
			lgr.Errorw("remove_invalid_target_failed", "err", err)
			return false, true, err
		}
	}
	return true, false, nil
}

func (w *worker2) downloadToStagingFromAny(forRestore digest.ForRestore, nodes []manifests.NodeIdentity) (bool, error) {
	doneCh := w.ctx.Done()

	var err error
	for _, node := range nodes {
		select {
		case <-doneCh:
			return false, nil
		case w.downloadLimiter <- struct{}{}:
			ok, thisErr := w.downloadToStaging(forRestore, node)
			if ok {
				return true, nil
			}
			err = multierr.Append(err, thisErr)
		}
	}

	zap.S().Errorw("download_failed_all_hosts", "digest", forRestore, "nodes", nodes, "err", err)

	return false, err
}

func (w *worker2) downloadToStaging(forRestore digest.ForRestore, node manifests.NodeIdentity) (bool, error) {
	lgr := zap.S().With("digest", forRestore, "source_node", node)
	var alreadyReleasedDownloadLimiter bool
	defer func() {
		if !alreadyReleasedDownloadLimiter {
			<-w.downloadLimiter
		}
	}()

	name := forRestore.URLSafe()
	doneCh := w.ctx.Done()

	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			waitCh := time.After(time.Duration(attempt) * time.Second)
			select {
			case <-doneCh:
				return false, nil
			case <-waitCh:
			}
		}
		attemptErr := w.staging.WriteFile(name, func(file *os.File) error {
			return w.bucketClient.DownloadBlobNoVerify(w.ctx, node, forRestore, file)
		})
		if attemptErr == nil {
			err = nil
			break
		}
		lgr.Errorw("download_blob_error", "attempt", attempt, "err", attemptErr)
		err = multierr.Append(err, attemptErr)
	}
	<-w.downloadLimiter
	alreadyReleasedDownloadLimiter = true

	select {
	case <-doneCh:
		return false, nil
	case w.verifyLimiter <- struct{}{}:
		return w.checkStaging(forRestore), nil
	}
}
