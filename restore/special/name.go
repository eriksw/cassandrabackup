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

package special

import (
	"strconv"
	"strings"
)

type FileName struct {
	Keyspace   string
	Table      string
	Index      string
	Version    string
	Generation int
	Component  string
}

func (pn FileName) String() string {
	if pn.Index != "" {
		return pn.Keyspace + "/" + pn.Table + "/" + pn.Index + "/" + pn.Version + "-" + strconv.Itoa(pn.Generation) + "-big-" + pn.Component
	}
	return pn.Keyspace + "/" + pn.Table + "/" + pn.Version + "-" + strconv.Itoa(pn.Generation) + "-big-" + pn.Component
}

func parseName(dataRelativePath string) (FileName, bool) {
	var result FileName
	var raw string
	pathParts := strings.Split(dataRelativePath, "/")
	switch len(pathParts) {
	case 3:
		result.Keyspace = pathParts[0]
		result.Table = pathParts[1]
		raw = pathParts[2]
	case 4:
		result.Keyspace = pathParts[0]
		result.Table = pathParts[1]
		result.Index = pathParts[2]
		raw = pathParts[3]
	default:
		return FileName{}, false
	}

	nameParts := strings.Split(raw, "-")
	if len(nameParts) != 4 || nameParts[2] != "big" {
		return FileName{}, false
	}

	if generation, err := strconv.Atoi(nameParts[1]); err == nil {
		result.Generation = generation
	} else {
		return FileName{}, false
	}

	result.Version = nameParts[0]
	result.Component = nameParts[3]
	return result, true
}
