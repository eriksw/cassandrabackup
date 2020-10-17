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
	"io"
)

type descriptionItem struct {
	text       string
	isDownload bool
}

type descriptionItems []descriptionItem

func (dis descriptionItems) Len() int {
	return len(dis)
}

func (dis descriptionItems) Swap(i, j int) {
	dis[i], dis[j] = dis[j], dis[i]
}

func (dis descriptionItems) Less(i, j int) bool {
	return dis[i].text < dis[j].text
}

func (di descriptionItem) writeTo(w io.Writer) (int, error) {
	var s string
	switch {
	case di.isDownload:
		s = "+\t\t" + di.text + "\n"
	default:
		s = "\t\t" + di.text + "\n"
	}
	return w.Write([]byte(s))
}
