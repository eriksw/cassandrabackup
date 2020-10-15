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
	"io"
	"os"
)

func (c *Client) GetFile(ctx context.Context, file *os.File, key string) error {
	sourceReader, err := c.bucket.Object(key).NewReader(ctx)
	if err != nil {
		return err
	}
	_, err = io.Copy(file, sourceReader)
	if closeErr := sourceReader.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	return err
}
