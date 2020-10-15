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
	"bytes"
	"compress/gzip"
	"context"

	"github.com/mailru/easyjson"
)

func (c *Client) putDocument(ctx context.Context, absoluteKey string, v easyjson.Marshaler) error {
	var encodeBuffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&encodeBuffer)
	if _, err := easyjson.MarshalToWriter(v, gzipWriter); err != nil {
		panic(err)
	}
	if err := gzipWriter.Close(); err != nil {
		panic(err)
	}

	_, _, err := c.storageClient.PutBytes(ctx, absoluteKey, "application/json", "gzip", encodeBuffer.Bytes())
	return err
}

func (c *Client) getDocument(ctx context.Context, absoluteKey string, v easyjson.Unmarshaler) error {
	contents, err := c.storageClient.GetBytes(ctx, absoluteKey)
	if err != nil {
		return err
	}
	return easyjson.Unmarshal(contents, v)
}
