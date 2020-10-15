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
	"sync"
	"time"

	"github.com/retailnext/cassandrabackup/bucket/googlestorage"
	"github.com/retailnext/cassandrabackup/cache"
	"go.uber.org/zap"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	putJsonRetriesLimit       = 3
	getJsonRetriesLimit       = 3
	getBlobRetriesLimit       = 3
	listManifestsRetriesLimit = 3
	retrySleepPerAttempt      = time.Second
)

type Client struct {
	existsCache   ExistsCache
	storageClient StorageClient
	layout        Layout
}

/*
	s3BucketName             = kingpin.Flag("s3-bucket", "S3 bucket name.").String()
	s3BucketRegion           = kingpin.Flag("s3-region", "S3 bucket region.").Envar("AWS_REGION").String()
	s3BucketKeyPrefix        = kingpin.Flag("s3-key-prefix", "Set the prefix for files in the S3 bucket").Default("/").String()
	s3BucketBlobStorageClass = kingpin.Flag("s3-storage-class", "Set the storage class for files in S3").Default(s3.StorageClassStandardIa).String()
*/

var gcsBucketName = kingpin.Flag("gcs-bucket", "GCS bucket name.").String()

var (
	Shared *Client
	once   sync.Once
)

func OpenShared() *Client {
	once.Do(func() {
		Shared = newGoogleClient()
	})
	return Shared
}

func newGoogleClient() *Client {
	cache.OpenShared()

	ctx := context.TODO()

	cl := Client{
		layout: Layout{
			Prefix:                   "",
			UseDeprecatedCommonFiles: false,
		},
		existsCache: ExistsCache{
			Storage:                  cache.Shared,
			UseDeprecatedCommonFiles: true,
		},
	}

	if sc, err := googlestorage.NewClient(ctx, *gcsBucketName); err == nil {
		cl.storageClient = sc
	} else {
		zap.S().Fatalw("storage_client_error", "err", err)
	}

	return &cl
}

/*

func newClient() *Client {
	cache.OpenShared()

	awsConf := aws.NewConfig().WithRegion(*s3BucketRegion)
	awsSession, err := session.NewSession(awsConf)
	if err != nil {
		zap.S().Fatalw("aws_new_session_error", "err", err)
	}

	s3Svc := s3.New(awsSession)
	c := &Client{
		s3Svc: s3Svc,
		uploader: &safeuploader.SafeUploader{
			S3:                   s3Svc,
			Bucket:               *s3BucketName,
			ServerSideEncryption: aws.String(s3.ServerSideEncryptionAes256),
			StorageClass:         s3BucketBlobStorageClass,
		},
		downloader: s3manager.NewDownloaderWithClient(s3Svc, func(d *s3manager.Downloader) {
			d.PartSize = 64 * 1024 * 1024 // 64MB per part
		}),
		existsCache: &ExistsCache{
			Storage:                  cache.Shared,
			UseDeprecatedCommonFiles: true,
		},
		layout: Layout{
			Prefix:                   strings.Trim(*s3BucketKeyPrefix, "/"),
			UseDeprecatedCommonFiles: true,
		},
		bucket:               *s3BucketName,
		serverSideEncryption: aws.String(s3.ServerSideEncryptionAes256),
	}
	c.validateEncryptionConfiguration()
	return c
}

func (c *Client) validateEncryptionConfiguration() {
	input := &s3.GetBucketEncryptionInput{
		Bucket: &c.bucket,
	}
	output, err := c.s3Svc.GetBucketEncryption(input)
	if err != nil {
		zap.S().Fatalw("failed_to_validate_bucket_encryption", "err", err)
	}
	for _, rule := range output.ServerSideEncryptionConfiguration.Rules {
		if rule.ApplyServerSideEncryptionByDefault != nil {
			if rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm != nil {
				return
			}
		}
	}
	zap.S().Fatalw("bucket_not_configured_with_sse_algorithm", "bucket", c.bucket)
}
*/
