// Copyright 2022 Matrix Origin
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

package docfilter

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCbitmapFilter(t *testing.T) {
	mp := mpool.MustNewZero()
	present := []int64{1, 2, 100, 4096, 7}
	v := buildIntVec(t, mp, types.T_int64.ToType(), present, nil)
	defer v.Free(mp)

	payload, ok, err := BuildCbitmapBytes(v)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, payload)

	f, err := NewCbitmapFilter(payload)
	require.NoError(t, err)
	require.True(t, f.Valid())

	for i := range present {
		require.True(t, f.Test(v.GetRawBytesAt(i)), "value %d present", present[i])
	}
	absent := buildIntVec(t, mp, types.T_int64.ToType(), []int64{3, 4, 999, 1 << 20}, nil)
	defer absent.Free(mp)
	for i := 0; i < absent.Length(); i++ {
		require.False(t, f.Test(absent.GetRawBytesAt(i)))
	}

	probe := buildIntVec(t, mp, types.T_int64.ToType(), []int64{1, 3, 100, 0}, map[int]bool{3: true})
	defer probe.Free(mp)
	require.Equal(t, []uint8{1, 0, 1, 0}, f.TestVector(probe, nil))

	// share independence
	a := f.Share()
	a.Free()
	require.True(t, f.Valid())
	require.True(t, f.Test(v.GetRawBytesAt(0)))

	// over-budget id SPAN -> ok=false (caller falls back to CRoaring). With the
	// base offset on (default), feasibility is span-based, so a lone huge value
	// (span 0) is feasible — it's a set whose SPAN exceeds the cap that falls
	// back. {0, MaxCbitmapBits+10} spans past the cap either way (base 0).
	big := buildIntVec(t, mp, types.T_int64.ToType(), []int64{0, int64(MaxCbitmapBits) + 10}, nil)
	defer big.Free(mp)
	_, ok, err = BuildCbitmapBytes(big)
	require.NoError(t, err)
	require.False(t, ok)
}

// TestCbitmapNegativeInt32 verifies negative int32 values — which zero-extend to
// large uint64 (int32 -1 -> 0xFFFFFFFF = 4294967295, NOT the int64 -1 pattern) —
// are handled consistently across build (C decode), Test (Go rawIntToUint64),
// and TestVector (C decode). It also shows the offset layout makes a clustered
// all-negative set feasible, where the value-indexed layout cannot (max ~4.3B).
func TestCbitmapNegativeInt32(t *testing.T) {
	mp := mpool.MustNewZero()
	orig := CbitmapUseOffset
	defer func() { CbitmapUseOffset = orig }()

	// All-negative, clustered: uint64(int32(-1)) ~= 4.29B (max), uint64(int32(
	// -1000)) (min) -> span ~999.
	present := []int32{-1000, -100, -50, -1}
	v := buildIntVec(t, mp, types.T_int32.ToType(), present, nil)
	defer v.Free(mp)

	// Without offset: max ~4.29B >> MaxCbitmapBits -> infeasible -> CRoaring.
	CbitmapUseOffset = false
	_, ok, err := BuildCbitmapBytes(v)
	require.NoError(t, err)
	require.False(t, ok, "negative int32 max (~4.3B) exceeds the cap without offset")

	// With offset: span ~999 -> feasible, membership exact.
	CbitmapUseOffset = true
	payload, ok, err := BuildCbitmapBytes(v)
	require.NoError(t, err)
	require.True(t, ok, "offset makes the narrow negative span feasible")

	f, err := NewCbitmapFilter(payload)
	require.NoError(t, err)
	defer f.Free()

	for i := range present {
		require.True(t, f.Test(v.GetRawBytesAt(i)), "value %d present", present[i])
	}
	// absent: negatives inside the span but unset, plus positives (below base).
	absent := buildIntVec(t, mp, types.T_int32.ToType(), []int32{-2, -999, -101, 0, 5}, nil)
	defer absent.Free(mp)
	for i := 0; i < absent.Length(); i++ {
		require.False(t, f.Test(absent.GetRawBytesAt(i)), "row %d should be absent", i)
	}
	// TestVector (C decode) must agree with Test (Go decode) on the same data.
	require.Equal(t, []uint8{1, 1, 1, 1}, f.TestVector(v, nil))
}

func TestCbitmapConstVector(t *testing.T) {
	runConstVectorFilterTest(t, func(t *testing.T, v *vector.Vector) constProbeFilter {
		data, ok, err := BuildCbitmapBytes(v)
		require.NoError(t, err)
		require.True(t, ok, "small ints should fit a dense cbitmap")
		f, err := NewCbitmapFilter(data)
		require.NoError(t, err)
		return f
	})
}

// TestBuildIntegerFilterStaleNullBits is a regression test for the class of bugs
// where vector.SetLength preserves stale null-bits beyond the logical length and
// a downstream build path could mis-count non-null values, causing an out-of-
// bounds panic or a semantically wrong filter. The current build path uses
// vecFixedArgs which passes nitem = v.Length() to C, and the C code iterates
// [0, nitem), so stale bits at positions >= nitem are never read. This test pins
// that correctness assertion.
//
// Specifically: a length-10 int64 vector with ten non-null values and a stale
// null bit at index 18 must produce a correct filter with all ten values present.
// Nulls.Count() still reports the stale bit — the fix is that the build path
// does not rely on Nulls.Count() for sizing.
func TestBuildIntegerFilterStaleNullBits(t *testing.T) {
	mp := mpool.MustNewZero()

	vals := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	v := buildIntVec(t, mp, types.T_int64.ToType(), vals, nil)
	defer v.Free(mp)

	// Assert the pre-condition: 10 logical rows, none null inside the logical
	// length. Then inject a stale null bit at index 18 and verify the null
	// count is misleading — the build path must ignore it.
	require.Equal(t, 10, v.Length())
	require.Equal(t, 0, v.GetNulls().Count(), "no nulls inside logical length")

	v.SetNull(18)
	require.Equal(t, 1, v.GetNulls().Count(), "Nulls.Count() includes the stale bit at 18 — caller must not rely on it")
	require.Equal(t, 10, v.Length(), "logical length unchanged")

	// Build via the production entry point (BuildCbitmapBytes for a dense set,
	// falling back to BuildCRoaringBytes for a sparse one — both paths must be
	// safe). All ten logical values must be present.
	payload, ok, err := BuildCbitmapBytes(v)
	require.NoError(t, err)
	if ok {
		// Dense cbitmap path.
		f, err := NewCbitmapFilter(payload)
		require.NoError(t, err)
		defer f.Free()
		res := f.TestVector(v, nil)
		require.Len(t, res, 10)
		for i := 0; i < 10; i++ {
			require.Equal(t, uint8(1), res[i], "cbitmap: row %d must be present", i)
		}
	} else {
		// CRoaring fallback path.
		payload, err := BuildCRoaringBytes(v)
		require.NoError(t, err)
		f, err := NewCRoaringFilter(payload)
		require.NoError(t, err)
		defer f.Free()
		res := f.TestVector(v, nil)
		require.Len(t, res, 10)
		for i := 0; i < 10; i++ {
			require.Equal(t, uint8(1), res[i], "croaring: row %d must be present", i)
		}
	}

	// Also test the top-level Build + New round-trip (tag routing).
	payload, err := Build(v)
	require.NoError(t, err)
	require.NotEmpty(t, payload)
	f, err := New(payload)
	require.NoError(t, err)
	defer f.Free()
	res := f.TestVector(v, nil)
	require.Len(t, res, 10)
	for i := 0; i < 10; i++ {
		require.Equal(t, uint8(1), res[i], "Build/New round-trip: row %d must be present", i)
	}
}
