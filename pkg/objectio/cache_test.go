// Copyright 2026 Matrix Origin
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

package objectio

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDedupLoadConvertsPanicAndDeletesInflight(t *testing.T) {
	ctx := context.Background()
	key := mataCacheKey{1}
	defer metaCache.Delete(ctx, key)
	defer metaLoadGroup.Delete(key)

	_, err := dedupLoad(ctx, key, func() ([]byte, error) {
		panic("boom")
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "panic boom")

	val, err := dedupLoad(ctx, key, func() ([]byte, error) {
		return []byte("ok"), nil
	})
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), val)
}
