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

	"cloud.google.com/go/storage"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

func (c *Client) ListPrefixes(ctx context.Context, prefix string) ([]string, error) {
	query := storage.Query{
		Delimiter: "/",
		Prefix:    prefix,
	}

	objects := c.bucket.Objects(ctx, &query)
	var result []string
	var err error
	for {
		attrs, iterErr := objects.Next()
		if iterErr != nil {
			if iterErr != iterator.Done {
				err = iterErr
			}
			break
		}
		if attrs.Prefix != "" {
			result = append(result, attrs.Prefix)
		} else {
			zap.S().Infow("unexpected_object_while_listing_prefixes", "key", attrs.Name)
		}
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}
