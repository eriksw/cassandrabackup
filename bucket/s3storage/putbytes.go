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
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/retailnext/cassandrabackup/unixtime"
)

func (c *Client) PutBytes(ctx context.Context, key, contentType, contentEncoding string, contents []byte) (bool, unixtime.Seconds, error) {
	t0 := unixtime.Now()
	putObjectInput := s3.PutObjectInput{
		Bucket:               &c.bucket,
		Key:                  &key,
		ContentType:          &contentType,
		ContentEncoding:      &contentEncoding,
		ServerSideEncryption: c.serverSideEncryption,
		Body:                 bytes.NewReader(contents),
	}
	_, err := c.s3.PutObjectWithContext(ctx, &putObjectInput)

	var lockedUntil unixtime.Seconds
	if err == nil && c.objectLockDuration > 0 {
		lockedUntil = t0.Add(c.objectLockDuration)
	}
	return false, lockedUntil, err
}
