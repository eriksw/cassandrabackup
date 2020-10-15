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
	"crypto/md5"
	"io"

	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

func (c *Client) PutBytes(ctx context.Context, key, contentType, contentEncoding string, contents []byte) (bool, unixtime.Seconds, error) {
	lgr := zap.S().With("key", key)

	obj := c.bucket.Object(key)
	putCtx, cancelPutCtx := context.WithCancel(ctx)
	// Make sure that no matter what happens, we clean up the child context.
	defer cancelPutCtx()

	targetWriter := obj.NewWriter(putCtx)
	targetWriter.ObjectAttrs.ContentType = contentType
	targetWriter.ObjectAttrs.ContentEncoding = contentEncoding
	targetWriter.EventBasedHold = true

	// Calculate and set the MD5 because we need to be extra sure an incomplete upload can't wind up completed.
	h := md5.New()
	h.Write(contents)
	targetWriter.ObjectAttrs.MD5 = h.Sum(nil)

	n, err := targetWriter.Write(contents)
	if err != nil || n != len(contents) {
		lgr.Errorw("put_write_error", "copied", n, "err", err)
		cancelPutCtx()
		if err == nil {
			err = io.ErrShortWrite
		}
		// It doesn't matter that we're not closing the writer.
		// It will be cleaned up when the putCtx is closed per defer above.
		return false, 0, err
	}

	closeErr := targetWriter.Close()
	if closeErr != nil {
		lgr.Errorw("put_writer_close_error", "copied", n, "err", closeErr)
		return false, 0, closeErr
	}

	var eventHold bool
	var lockedUntil unixtime.Seconds
	attrs := targetWriter.Attrs()
	eventHold = attrs.EventBasedHold
	if !attrs.RetentionExpirationTime.IsZero() {
		lockedUntil = unixtime.Seconds(attrs.RetentionExpirationTime.Unix())
	}

	return eventHold, lockedUntil, nil
}
