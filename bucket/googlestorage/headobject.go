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

	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

func (c *Client) HeadObject(ctx context.Context, key string) (eventHold bool, lockedUntil unixtime.Seconds, err error) {
	obj := c.bucket.Object(key)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return false, 0, err
	}

	zap.S().Debugw("head_object_attrs", "key", key, "event_based_hold", attrs.EventBasedHold, "retention_expiration_time", attrs.RetentionExpirationTime)
	eventHold = attrs.EventBasedHold
	if !attrs.RetentionExpirationTime.IsZero() {
		lockedUntil = unixtime.Seconds(attrs.RetentionExpirationTime.Unix())
	}
	return
}
