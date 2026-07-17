// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package malloc

import (
	"fmt"
	"sort"

	"github.com/google/pprof/profile"
)

type HeapProfileInuseSummary struct {
	InuseBytes    int64    `json:"inuse_bytes"`
	InuseObjects  int64    `json:"inuse_objects"`
	AllocBytes    int64    `json:"alloc_bytes"`
	AllocObjects  int64    `json:"alloc_objects"`
	TopFunction   string   `json:"top_function"`
	TopSourceLine string   `json:"top_source_line"`
	Stack         []string `json:"stack"`
}

// GlobalHeapProfileInuseTop returns the largest currently-live allocation
// stacks recorded by the global heap profiler.  It is intended for lightweight
// operational diagnostics; for full fidelity use /debug/malloc or WriteProfileData.
func GlobalHeapProfileInuseTop(limit int, minInuseBytes int64, stackDepth int) []HeapProfileInuseSummary {
	if limit <= 0 {
		return nil
	}
	if stackDepth <= 0 {
		stackDepth = 1
	}

	summaries := make([]HeapProfileInuseSummary, 0, limit)
	globalProfiler.locationsToSample.Range(func(_, v any) bool {
		info := v.(*SampleInfo[*HeapSampleValues])
		values := info.Values.Values()
		inuseBytes := values[3]
		if inuseBytes < minInuseBytes {
			return true
		}
		summary := HeapProfileInuseSummary{
			AllocObjects: values[0],
			AllocBytes:   values[1],
			InuseObjects: values[2],
			InuseBytes:   inuseBytes,
			Stack:        profileLocationsToStrings(info.Locations, stackDepth),
		}
		if len(info.Locations) > 0 && len(info.Locations[0].Line) > 0 {
			line := info.Locations[0].Line[0]
			if line.Function != nil {
				summary.TopFunction = line.Function.Name
				summary.TopSourceLine = fmt.Sprintf("%s:%d", line.Function.Filename, line.Line)
			}
		}
		summaries = append(summaries, summary)
		return true
	})

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].InuseBytes > summaries[j].InuseBytes
	})
	if len(summaries) > limit {
		summaries = summaries[:limit]
	}
	return summaries
}

func profileLocationsToStrings(locations []*profile.Location, stackDepth int) []string {
	if len(locations) == 0 {
		return nil
	}
	if len(locations) > stackDepth {
		locations = locations[:stackDepth]
	}
	stack := make([]string, 0, len(locations))
	for _, location := range locations {
		if len(location.Line) == 0 {
			continue
		}
		line := location.Line[0]
		if line.Function == nil {
			continue
		}
		stack = append(stack, fmt.Sprintf("%s %s:%d", line.Function.Name, line.Function.Filename, line.Line))
	}
	return stack
}
