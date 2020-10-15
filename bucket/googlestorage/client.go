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

package googlestorage

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/storage"
	"go.uber.org/zap"
)

type Client struct {
	bucket       *storage.BucketHandle
	lockDuration time.Duration
}

func NewClient(ctx context.Context, bucket string) (*Client, error) {
	if bucket == "" {
		return nil, errors.New("bucket cannot be empty")
	}

	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	cl := Client{
		bucket: storageClient.Bucket(bucket),
	}

	bktAttrs, err := cl.bucket.Attrs(ctx)
	if err != nil {
		return nil, err
	}
	if bktAttrs.DefaultEventBasedHold && bktAttrs.RetentionPolicy != nil {
		cl.lockDuration = bktAttrs.RetentionPolicy.RetentionPeriod
	}
	zap.S().Debugw("storage_client_ok", "bucket", bucket, "lock_duration", cl.lockDuration)

	return &cl, nil
}

func (c *Client) LockDuration() time.Duration {
	return c.lockDuration
}
