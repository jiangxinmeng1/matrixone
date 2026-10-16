// Copyright 2021 Matrix Origin
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

package test

import (
	"testing"
	"time"
)

// These defaults keep the original 128-PK test behavior.  The fast variant
// below temporarily shortens both maintenance intervals to increase the
// number of flush/merge transitions seen by the commit queue.
var (
	concurrentUpdateFlushInterval = 10 * time.Millisecond
	concurrentUpdateMergeInterval = 250 * time.Millisecond
)

func TestConcurrentUpdateByPreviousRowIDWithFastFlushMerge10Min(t *testing.T) {
	oldFlushInterval := concurrentUpdateFlushInterval
	oldMergeInterval := concurrentUpdateMergeInterval
	concurrentUpdateFlushInterval = time.Millisecond
	concurrentUpdateMergeInterval = 25 * time.Millisecond
	defer func() {
		concurrentUpdateFlushInterval = oldFlushInterval
		concurrentUpdateMergeInterval = oldMergeInterval
	}()

	testConcurrentUpdateByPreviousRowIDWithFlushMergeDuration(
		t, 128, 128, 0, 10*time.Minute,
	)
}
