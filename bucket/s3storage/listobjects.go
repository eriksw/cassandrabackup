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
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (c *Client) ListObjects(ctx context.Context, prefix, startOffsetKey, endOffsetKey string) ([]string, error) {
	var result []string
	input := s3.ListObjectsV2Input{
		Bucket:    &c.bucket,
		Delimiter: aws.String("/"),
		Prefix:    &prefix,
	}
	// TODO fix offset semantics to match google
	// startOffset is >= but startAfter is >

	if startOffsetKey != "" {
		input.StartAfter = &startOffsetKey
	}
	err := c.s3.ListObjectsV2PagesWithContext(ctx, &input, func(output *s3.ListObjectsV2Output, b bool) bool {
		var done bool
		for _, obj := range output.Contents {
			key := *obj.Key
			if endOffsetKey != "" && key > endOffsetKey {
				done = true
			} else {
				result = append(result, key)
			}
		}
		return !done
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
