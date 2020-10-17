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
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/retailnext/cassandrabackup/checkpoint"

	"go.uber.org/multierr"

	"github.com/retailnext/cassandrabackup/paranoid"

	"go.uber.org/zap"

	"github.com/retailnext/cassandrabackup/manifests"

	"github.com/retailnext/cassandrabackup/bucket"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/writefile"
)

type WorkerOptions struct {
	TargetDirectory       string
	StagingDirectory      string
	GraveyardDirectory    string
	EnsureOwnership       bool
	ConcurrentDownload    int
	ConcurrentVerify      int
	NoDownloadToStaging   bool
	NoLinkToTarget        bool
	RemoveInvalidAtTarget bool
}

func makeWritefileConfig(dir string) writefile.Config {
	c := writefile.Config{
		Directory:                dir,
		DirectoryMode:            0755,
		EnsureDirectoryOwnership: true,
		FileMode:                 0644,
		EnsureFileOwnership:      true,
	}

	lgr := zap.S()
	userName := "cassandra"
	osUser, err := user.Lookup(userName)
	if err != nil {
		lgr.Panicw("user_lookup_error", "user", userName, "err", err)
	}

	uid, err := strconv.Atoi(osUser.Uid)
	if err != nil {
		lgr.Panicw("user_id_lookup_error", "uid", osUser.Uid, "err", err)
	}
	c.DirectoryUID = uid
	c.FileUID = uid

	gid, err := strconv.Atoi(osUser.Gid)
	if err != nil {
		lgr.Panicw("group_id_lookup_error", "gid", osUser.Gid, "err", err)
	}
	c.DirectoryGID = gid
	c.FileGID = gid
	return c
}

type DownloadableFile struct {
	Digest digest.ForRestore
	Nodes  manifests.NodeIdentities
}

type WorkerPlan map[string]DownloadableFile

func Restore(ctx context.Context, wp WorkerPlan, options WorkerOptions) error {
	tasks := make(map[digest.ForRestore]downloadTask)
	for name, df := range wp {
		task := tasks[df.Digest]
		task.digest = df.Digest
		task.nodes = append(task.nodes, df.Nodes...)
		sort.Sort(task.nodes)
		filteredNodes := task.nodes[:0]
		for i := range task.nodes {
			if i == 0 || task.nodes[i] != filteredNodes[len(filteredNodes)-1] {
				filteredNodes = append(filteredNodes, task.nodes[i])
			}
		}
		task.nodes = filteredNodes
		task.paths = append(task.paths, name)
		tasks[df.Digest] = task
	}

	w2 := worker2{
		staging:               makeWritefileConfig(options.StagingDirectory),
		target:                makeWritefileConfig(options.TargetDirectory),
		graveyard:             makeWritefileConfig(options.GraveyardDirectory),
		ctx:                   ctx,
		digestCache:           digest.OpenShared(),
		bucketClient:          bucket.OpenShared(),
		downloadLimiter:       make(chan struct{}, options.ConcurrentDownload),
		verifyLimiter:         make(chan struct{}, options.ConcurrentVerify),
		noDownloadToStaging:   options.NoDownloadToStaging,
		noLinkToTarget:        options.NoLinkToTarget,
		removeInvalidAtTarget: options.RemoveInvalidAtTarget,
	}

	w2.wg.Add(len(tasks))
	w2.linkReady.Add(len(tasks))
	for forRestore, task := range tasks {
		go w2.processFile(task.paths, forRestore, task.nodes)
	}

	w2.wg.Wait()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	w2.fileStatusLock.Lock()
	defer w2.fileStatusLock.Unlock()

	result := make(FileErrors)

	var inPlace, readyInStaging, errored, other int

	for name, status := range w2.fileStatus {
		if status.InPlace {
			inPlace++
			continue
		} else if status.InStaging {
			readyInStaging++
		} else if status.Error != nil {
			errored++
			result[name] = status.Error
		} else {
			other++
		}
	}
	zap.S().Infow("restore_done", "in_place", inPlace, "ready_in_staging", readyInStaging, "errored", errored, "other", other)

	if len(result) == 0 {
		return nil
	}

	return result
}

type downloadTask struct {
	paths  []string
	digest digest.ForRestore
	nodes  manifests.NodeIdentities
}

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

	linkReady checkpoint.Barrier

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
	lgr := zap.S().With("digest", forRestore, "paths", names)
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
		w.linkReady.Abort()
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
			w.linkReady.Abort()
			return
		case w.verifyLimiter <- struct{}{}:
			targetOk, bogonPresent, err := w.checkExisting(name, forRestore)
			status := statusUpdates[name]
			status.InPlace = targetOk
			status.InvalidInPlace = bogonPresent
			status.Error = multierr.Append(status.Error, err)
			statusUpdates[name] = status
		}
	}

	if statusUpdates.isDone() {
		// lgr.Infow("nothing_to_do", "paths", names)
		w.linkReady.Done()
		return
	}

	if !inStaging {
		if w.noDownloadToStaging {
			lgr.Infow("would_download", "source_nodes", nodes)
			w.linkReady.Abort()
			return
		}

		select {
		case <-doneCh:
			return
		default:
		}

		ok, err := w.downloadToStagingFromAny(forRestore, nodes)
		for _, name := range names {
			status := statusUpdates[name]
			status.Error = multierr.Append(status.Error, err)
			status.InStaging = ok
			statusUpdates[name] = status
		}
		inStaging = ok

	}

	if !inStaging {
		lgr.Errorw("cannot_proceed", "status", statusUpdates)
		w.linkReady.Abort()
		return
	}

	w.linkReady.Done()
	linkReadyAbortCh, linkReadyProceedCh := w.linkReady.Wait()
	// Wait for LinkReady or context close.
	select {
	case <-doneCh:
		return
	case <-linkReadyAbortCh:
		lgr.Debugw("lr aborted")
		return
	case <-linkReadyProceedCh:
	}
	lgr.Debugw("lr ready")

	var namesToLink []string
	for _, name := range names {
		status := statusUpdates[name]
		if status.InPlace {
			continue
		}
		namesToLink = append(namesToLink, name)
	}

	if w.noLinkToTarget {
		lgr.Infow("would_link", "to_link", namesToLink)
		return
	}

	for _, name := range namesToLink {
		status := statusUpdates[name]
		err := w.linkFromStaging(name, forRestore)
		if err == nil {
			status.InPlace = true
			status.Error = nil
		} else {
			status.Error = multierr.Append(status.Error, err)
		}
		statusUpdates[name] = status
	}
}

func (w *worker2) linkFromStaging(name string, forRestore digest.ForRestore) error {
	lgr := zap.S().With("path", name, "digest", forRestore)
	targetPath := filepath.Join(w.target.Directory, name)

	targetPathDir := w.target
	targetPathDir.Directory = filepath.Dir(targetPath)

	err := os.Link(w.stagingPath(forRestore), targetPath)
	if err == nil {
		lgr.Debugw("link_ok")
		return nil
	}
	lgr.Debugw("link_error", "err", err)
	if os.IsNotExist(err) {
		// try ensuring the target directory
		targetPathDir := w.target
		targetPathDir.Directory = filepath.Dir(targetPath)
		ensureDirectoryErr := targetPathDir.EnsureDirectory()
		if ensureDirectoryErr != nil {
			lgr.Warnw("failed_to_ensure_directory", "err", ensureDirectoryErr)
			return ensureDirectoryErr
		}
		return os.Link(w.stagingPath(forRestore), targetPath)
	}
	return err
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
			lgr.Infow("download_start", "attempt", attempt)
			return w.bucketClient.DownloadBlobNoVerify(w.ctx, node, forRestore, file)
		})
		if attemptErr == nil {
			err = nil
			break
		}
		lgr.Errorw("download_blob_error", "attempt", attempt, "err", attemptErr)
		err = multierr.Append(err, attemptErr)
	}
	lgr.Debugw("download_complete")
	<-w.downloadLimiter
	alreadyReleasedDownloadLimiter = true

	select {
	case <-doneCh:
		return false, nil
	case w.verifyLimiter <- struct{}{}:
		return w.checkStaging(forRestore), nil
	}
}
