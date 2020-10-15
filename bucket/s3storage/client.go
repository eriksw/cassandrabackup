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

package s3storage

import (
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
)

type Client struct {
	s3                   s3iface.S3API
	bucket               string
	serverSideEncryption *string
	storageClass         *string
	downloader           s3manageriface.DownloaderAPI
	objectLockDuration   time.Duration
}

func NewClient(s3 s3iface.S3API, bucket string, serverSideEncryption, storageClass *string) *Client {
	return &Client{
		s3:                   s3,
		bucket:               bucket,
		serverSideEncryption: serverSideEncryption,
		storageClass:         storageClass,
		downloader: s3manager.NewDownloaderWithClient(s3, func(d *s3manager.Downloader) {
			d.PartSize = 64 * 1024 * 1024 // 64MB per part
		}),
		// TODO get this by inspecting the bucket.
		objectLockDuration: 140 * 24 * time.Hour,
	}
}

func (c *Client) LockDuration() time.Duration {
	return c.objectLockDuration
}
