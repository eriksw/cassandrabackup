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
	"io/ioutil"

	"github.com/aws/aws-sdk-go/service/s3"
)

func (c *Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	getObjectInput := s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
	}
	getObjectOutput, err := c.s3.GetObjectWithContext(ctx, &getObjectInput)
	var contents []byte
	if err == nil {
		contents, err = ioutil.ReadAll(getObjectOutput.Body)
		if closeErr := getObjectOutput.Body.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}
	if err != nil {
		return nil, err
	}
	return contents, nil
}