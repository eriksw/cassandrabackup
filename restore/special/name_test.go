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

import "testing"

func TestFileName(t *testing.T) {
	cases := []string{
		"luneta/ap-a485d1f5cbdd35ff9c7e652c2fe8d20d/.ap_id_index/md-53-big-CompressionInfo.db",
		"luneta/ap-a485d1f5cbdd35ff9c7e652c2fe8d20d/.ap_id_index/md-53-big-Data.db",
		"luneta/ap-a485d1f5cbdd35ff9c7e652c2fe8d20d/.ap_id_index/md-53-big-Digest.crc32",
		"luneta/ap-a485d1f5cbdd35ff9c7e652c2fe8d20d/.ap_id_index/md-53-big-Filter.db",
		"luneta/ap-a485d1f5cbdd35ff9c7e652c2fe8d20d/.ap_id_index/md-53-big-Index.db",
		"luneta/ap-a485d1f5cbdd35ff9c7e652c2fe8d20d/.ap_id_index/md-53-big-Statistics.db",
		"luneta/ap-a485d1f5cbdd35ff9c7e652c2fe8d20d/.ap_id_index/md-53-big-Summary.db",
		"luneta/ap-a485d1f5cbdd35ff9c7e652c2fe8d20d/.ap_id_index/md-53-big-TOC.txt",
		"system_distributed/repair_history-759fffad624b318180eefa9a52d1f627/md-1864-big-CompressionInfo.db",
		"system_distributed/repair_history-759fffad624b318180eefa9a52d1f627/md-1864-big-Data.db",
		"system_distributed/repair_history-759fffad624b318180eefa9a52d1f627/md-1864-big-Digest.crc32",
		"system_distributed/repair_history-759fffad624b318180eefa9a52d1f627/md-1864-big-Filter.db",
		"system_distributed/repair_history-759fffad624b318180eefa9a52d1f627/md-1864-big-Index.db",
		"system_distributed/repair_history-759fffad624b318180eefa9a52d1f627/md-1864-big-Statistics.db",
		"system_distributed/repair_history-759fffad624b318180eefa9a52d1f627/md-1864-big-Summary.db",
		"system_distributed/repair_history-759fffad624b318180eefa9a52d1f627/md-1864-big-TOC.txt",
	}

	for n, name := range cases {
		parsed, ok := parseName(name)
		if !ok {
			t.Errorf("case %d: parseName failed", n)
			continue
		}
		result := parsed.String()
		if result != name {
			t.Errorf("case %d: unexpected round-trip result %q", n, result)
		}
	}
}
