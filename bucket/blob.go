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
	"errors"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/paranoid"
	"github.com/retailnext/cassandrabackup/unixtime"
)

var UploadSkipped = errors.New("upload skipped")

func (c *Client) PutBlob(ctx context.Context, node manifests.NodeIdentity, file paranoid.File, digests digest.ForUpload) error {
	if exists, err := c.blobExists(ctx, node, digests.ForRestore()); err != nil {
		uploadErrors.Inc()
		return err
	} else if exists {
		skippedFiles.Inc()
		skippedBytes.Add(float64(file.Len()))
		return UploadSkipped
	}

	key := c.layout.AbsoluteKeyForBlob(node, digests.ForRestore())
	eventHold, lockedUntil, err := c.storageClient.PutFile(ctx, key, file, digests)
	if err != nil {
		uploadErrors.Inc()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	c.updateExistsCache(node, digests.ForRestore(), eventHold, lockedUntil)
	uploadedFiles.Inc()
	uploadedBytes.Add(float64(file.Len()))
	return nil
}

func (c *Client) DownloadBlob(ctx context.Context, node manifests.NodeIdentity, restore digest.ForRestore, file *os.File) error {
	key := c.layout.AbsoluteKeyForBlob(node, restore)
	err := c.storageClient.GetFile(ctx, file, key)
	if err != nil {
		return err
	}
	return restore.Verify(ctx, file)
}

func (c *Client) updateExistsCache(node manifests.NodeIdentity, restore digest.ForRestore, eventHold bool, lockedUntil unixtime.Seconds) {
	if eventHold {
		lockedUntil = unixtime.Now().Add(c.storageClient.LockDuration())
	}
	if lockedUntil > 0 {
		c.existsCache.Put(node, restore, lockedUntil)
	}
}

func (c *Client) blobExists(ctx context.Context, node manifests.NodeIdentity, restore digest.ForRestore) (bool, error) {
	if c.existsCache.Get(node, restore) {
		return true, nil
	}

	key := c.layout.AbsoluteKeyForBlob(node, restore)
	eventHold, lockedUntil, err := c.storageClient.HeadObject(ctx, key)
	if err != nil {
		if c.storageClient.IsNotFoundOrDeleted(err) {
			return false, nil
		}
		return false, err
	}

	c.updateExistsCache(node, restore, eventHold, lockedUntil)
	return true, nil
}

var (
	skippedBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "bucket",
		Name:      "skipped_bytes_total",
		Help:      "Total bytes not uploaded due to them already existing in the bucket.",
	})
	skippedFiles = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "bucket",
		Name:      "skipped_files_total",
		Help:      "Number of files not uploaded due to them already existing in the bucket.",
	})
	uploadedBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "bucket",
		Name:      "upload_bytes_total",
		Help:      "Total bytes uploaded to the bucket.",
	})
	uploadedFiles = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "bucket",
		Name:      "upload_files_total",
		Help:      "Number of files uploaded to the bucket.",
	})
	uploadErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "bucket",
		Name:      "upload_errors_total",
		Help:      "Number of failed file uploads.",
	})
)

func init() {
	prometheus.MustRegister(skippedBytes)
	prometheus.MustRegister(skippedFiles)
	prometheus.MustRegister(uploadedBytes)
	prometheus.MustRegister(uploadedFiles)
	prometheus.MustRegister(uploadErrors)
}
