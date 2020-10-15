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
	"os"
	"time"

	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/paranoid"
	"github.com/retailnext/cassandrabackup/unixtime"
)

type StorageClient interface {
	GetBytes(ctx context.Context, key string) ([]byte, error)
	GetFile(ctx context.Context, file *os.File, key string) error
	HeadObject(ctx context.Context, key string) (eventHold bool, lockedUntil unixtime.Seconds, err error)
	IsNotFoundOrDeleted(err error) bool
	ListObjects(ctx context.Context, prefix, startOffsetKey, endOffsetKey string) ([]string, error)
	ListPrefixes(ctx context.Context, prefix string) ([]string, error)
	LockDuration() time.Duration
	PutBytes(ctx context.Context, key, contentType, contentEncoding string, contents []byte) (eventHold bool, lockedUntil unixtime.Seconds, err error)
	PutFile(ctx context.Context, key string, file paranoid.File, digests digest.ForUpload) (eventHold bool, lockedUntil unixtime.Seconds, err error)
}
