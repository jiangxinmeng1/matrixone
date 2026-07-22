// Copyright 2022 Matrix Origin
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

package plan

import (
	"fmt"
	"math"
	"math/bits"
	"strings"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	threshHoldForShuffleGroup       = 64000
	threshHoldForRightJoinShuffle   = 8192
	threshHoldForShuffleJoin        = 120000
	threshHoldForHybirdShuffle      = 4000000
	threshHoldForHashShuffle        = 2000000
	ShuffleThreshHoldOfNDV          = 50000
	ShuffleTypeThreshHoldLowerLimit = 16
	ShuffleTypeThreshHoldUpperLimit = 1024

	overlapThreshold = 0.95
	uniformThreshold = 0.3
)

const (
	ShuffleToRegIndex        int32 = 0
	ShuffleToLocalMatchedReg int32 = 1
	ShuffleToMultiMatchedReg int32 = 2
)

// convert first 8 bytes to uint64, slice might be less than 8 bytes
func ByteSliceToUint64(bytes []byte) uint64 {
	var result uint64 = 0
	i := 0
	length := len(bytes)
	for ; i < 8; i++ {
		result = result * 256
		if i < length {
			result += uint64(bytes[i])
		}
	}
	return result
}

// convert first 8 bytes to uint64. vec.area must be nil
// if varlena length less than 8 bytes, should have filled zero in varlena
func VarlenaToUint64Inline(v *types.Varlena) uint64 {
	return bits.ReverseBytes64(*(*uint64)(unsafe.Add(unsafe.Pointer(&v[0]), 1)))
}

// convert first 8 bytes to uint64
func VarlenaToUint64(v *types.Varlena, area []byte) uint64 {
	svlen := (*v)[0]
	if svlen <= types.VarlenaInlineSize {
		return VarlenaToUint64Inline(v)
	} else {
		voff, _ := v.OffsetLen()
		return bits.ReverseBytes64(*(*uint64)(unsafe.Pointer(&area[voff])))
	}
}

func SimpleCharHashToRange(bytes []byte, upperLimit uint64) uint64 {
	lenBytes := len(bytes)
	if lenBytes == 0 {
		// always hash empty string to first bucket
		return 0
	}
	if lenBytes == 1 {
		return uint64(bytes[0]) % upperLimit
	}
	//sample 7 bytes
	h := ((uint64(bytes[0])+1)*(uint64(bytes[lenBytes/4])+uint64(bytes[lenBytes/2])+uint64(bytes[lenBytes*3/4])+1) +
		(uint64(bytes[lenBytes-1])+1)*(uint64(bytes[1])+uint64(bytes[lenBytes-2])+1))
	return hashtable.Int64HashWithFixedSeed(h) % upperLimit
}

func SimpleInt64HashToRange(i uint64, upperLimit uint64) uint64 {
	return hashtable.Int64HashWithFixedSeed(i) % upperLimit
}

func shuffleByZonemap(rsp *engine.RangesShuffleParam, zm objectio.ZoneMap, bucketNum int) uint64 {
	if !rsp.Init {
		rsp.Init = true
		switch zm.GetType() {
		case types.T_int64, types.T_int32, types.T_int16:
			rsp.ShuffleRangeInt64 = ShuffleRangeReEvalSigned(rsp.Node.Stats.HashmapStats.Ranges, bucketNum, rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
		case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
			rsp.ShuffleRangeUint64 = ShuffleRangeReEvalUnsigned(rsp.Node.Stats.HashmapStats.Ranges, bucketNum, rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
		}
	}

	var shuffleIDX uint64
	if len(rsp.ShuffleRangeUint64) > 0 {
		shuffleIDX = GetRangeShuffleIndexForZMUnsignedSlice(rsp.ShuffleRangeUint64, zm)
	} else if len(rsp.ShuffleRangeInt64) > 0 {
		shuffleIDX = GetRangeShuffleIndexForZMSignedSlice(rsp.ShuffleRangeInt64, zm)
	} else {
		shuffleIDX = GetRangeShuffleIndexForZM(rsp.Node.Stats.HashmapStats.ShuffleColMin, rsp.Node.Stats.HashmapStats.ShuffleColMax, zm, uint64(bucketNum))
	}
	return shuffleIDX
}

func shuffleByValueExtractedFromZonemap(rsp *engine.RangesShuffleParam, zm objectio.ZoneMap, bucketNum int) uint64 {
	t := types.T(rsp.Node.Stats.HashmapStats.ShuffleColIdx) // actually this is specially used for sort key column type
	if !rsp.Init {
		rsp.Init = true
		switch t {
		case types.T_int64, types.T_int32, types.T_int16:
			rsp.ShuffleRangeInt64 = ShuffleRangeReEvalSigned(rsp.Node.Stats.HashmapStats.Ranges, bucketNum, rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
		case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text, types.T_bit, types.T_datalink:
			rsp.ShuffleRangeUint64 = ShuffleRangeReEvalUnsigned(rsp.Node.Stats.HashmapStats.Ranges, bucketNum, rsp.Node.Stats.HashmapStats.Nullcnt, int64(rsp.Node.Stats.TableCnt))
		}
	}

	var shuffleIDX uint64
	if rsp.ShuffleRangeUint64 != nil {
		shuffleIDX = GetRangeShuffleIndexForValuesExtractedFromZMUnsignedSlice(rsp.ShuffleRangeUint64, zm, t)
	} else if rsp.ShuffleRangeInt64 != nil {
		shuffleIDX = GetRangeShuffleIndexForValuesExtractedFromZMSignedSlice(rsp.ShuffleRangeInt64, zm, t)
	} else {
		shuffleIDX = GetRangeShuffleIndexForExtractedZM(rsp.Node.Stats.HashmapStats.ShuffleColMin, rsp.Node.Stats.HashmapStats.ShuffleColMax, zm, uint64(bucketNum), t)
	}
	return shuffleIDX
}

func CalcRangeShuffleIDXForObj(rsp *engine.RangesShuffleParam, objstats *objectio.ObjectStats, bucketNum int) uint64 {
	zm := objstats.SortKeyZoneMap()
	if !zm.IsInited() {
		// an object with all null will send to shuffleIDX 0
		return 0
	}
	if len(rsp.Node.TableDef.Pkey.Names) == 1 {
		return shuffleByZonemap(rsp, zm, bucketNum)
	} else {
		return shuffleByValueExtractedFromZonemap(rsp, zm, bucketNum)
	}
}

func ShouldSkipObjByShuffle(rsp *engine.RangesShuffleParam, objstats *objectio.ObjectStats) bool {
	if rsp == nil || rsp.CNCNT <= 1 || rsp.Node == nil {
		return false
	}
	if objstats.GetAppendable() {
		//aobj always shuffle to local CN
		return !rsp.IsLocalCN
	}
	if rsp.Node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Range {
		//shuffle by range
		return CalcRangeShuffleIDXForObj(rsp, objstats, int(rsp.CNCNT)) != uint64(rsp.CNIDX)
	}
	//shuffle by hash
	objID := objstats.ObjectLocation().ObjectId()
	return SimpleCharHashToRange(objID[:], uint64(rsp.CNCNT)) != uint64(rsp.CNIDX)
}

func GetCenterValueForZMSigned(zm objectio.ZoneMap) int64 {
	switch zm.GetType() {
	case types.T_int64:
		return types.DecodeInt64(zm.GetMinBuf())/2 + types.DecodeInt64(zm.GetMaxBuf())/2
	case types.T_int32:
		return int64(types.DecodeInt32(zm.GetMinBuf()))/2 + int64(types.DecodeInt32(zm.GetMaxBuf()))/2
	case types.T_int16:
		return int64(types.DecodeInt16(zm.GetMinBuf()))/2 + int64(types.DecodeInt16(zm.GetMaxBuf()))/2
	default:
		panic("wrong type!")
	}
}

func GetCenterValueExtractFromZMSigned(zm objectio.ZoneMap, t types.T) int64 {
	idx := 0 //for now, it's always 0
	minelms, _ := types.Unpack(zm.GetMinBuf())
	maxelms, _ := types.Unpack(zm.GetMaxBuf())
	minval := minelms[idx]
	maxval := maxelms[idx]
	switch t {
	case types.T_int64:
		return minval.(int64)/2 + maxval.(int64)/2
	case types.T_int32:
		return int64(minval.(int32)/2 + maxval.(int32)/2)
	case types.T_int16:
		return int64(minval.(int16)/2 + maxval.(int16)/2)
	default:
		panic("wrong type!")
	}
}

func GetCenterValueForZMUnsigned(zm objectio.ZoneMap) uint64 {
	switch zm.GetType() {
	case types.T_uint64:
		return types.DecodeUint64(zm.GetMinBuf())/2 + types.DecodeUint64(zm.GetMaxBuf())/2
	case types.T_uint32:
		return uint64(types.DecodeUint32(zm.GetMinBuf()))/2 + uint64(types.DecodeUint32(zm.GetMaxBuf()))/2
	case types.T_uint16:
		return uint64(types.DecodeUint16(zm.GetMinBuf()))/2 + uint64(types.DecodeUint16(zm.GetMaxBuf()))/2
	case types.T_varchar, types.T_char, types.T_text:
		return ByteSliceToUint64(zm.GetMinBuf())/2 + ByteSliceToUint64(zm.GetMaxBuf())/2
	default:
		panic("wrong type!")
	}
}

func GetCenterValueExtractFromZMUnsigned(zm objectio.ZoneMap, t types.T) uint64 {
	idx := 0 //for now, it's always 0
	minelms, _ := types.Unpack(zm.GetMinBuf())
	maxelms, _ := types.Unpack(zm.GetMaxBuf())
	minval := minelms[idx]
	maxval := maxelms[idx]
	switch t {
	case types.T_uint64:
		return minval.(uint64)/2 + maxval.(uint64)/2
	case types.T_uint32:
		return uint64(minval.(uint32)/2 + maxval.(uint32)/2)
	case types.T_uint16:
		return uint64(minval.(uint16)/2 + maxval.(uint16)/2)
	case types.T_varchar, types.T_char, types.T_text:
		return ByteSliceToUint64(minval.([]byte))/2 + ByteSliceToUint64(maxval.([]byte))/2
	default:
		panic("wrong type!")
	}
}

func GetRangeShuffleIndexForZM(minVal, maxVal int64, zm objectio.ZoneMap, upplerLimit uint64) uint64 {
	switch zm.GetType() {
	case types.T_int64, types.T_int32, types.T_int16:
		return GetRangeShuffleIndexSignedMinMax(minVal, maxVal, GetCenterValueForZMSigned(zm), upplerLimit)
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		return GetRangeShuffleIndexUnsignedMinMax(uint64(minVal), uint64(maxVal), GetCenterValueForZMUnsigned(zm), upplerLimit)
	}
	logutil.Infof("unsupported zm type %v", zm.GetType())
	panic("unsupported shuffle type!")
}

func GetRangeShuffleIndexForExtractedZM(minVal, maxVal int64, zm objectio.ZoneMap, upplerLimit uint64, t types.T) uint64 {
	switch t {
	case types.T_int64, types.T_int32, types.T_int16:
		return GetRangeShuffleIndexSignedMinMax(minVal, maxVal, GetCenterValueExtractFromZMSigned(zm, t), upplerLimit)
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		return GetRangeShuffleIndexUnsignedMinMax(uint64(minVal), uint64(maxVal), GetCenterValueExtractFromZMUnsigned(zm, t), upplerLimit)
	}
	panic("unsupported shuffle type!")
}

func GetRangeShuffleIndexForZMSignedSlice(val []int64, zm objectio.ZoneMap) uint64 {
	switch zm.GetType() {
	case types.T_int64, types.T_int32, types.T_int16:
		return GetRangeShuffleIndexSignedSlice(val, GetCenterValueForZMSigned(zm))
	}
	panic("wrong type!")
}

func GetRangeShuffleIndexForValuesExtractedFromZMSignedSlice(val []int64, zm objectio.ZoneMap, t types.T) uint64 {
	switch t {
	case types.T_int64, types.T_int32, types.T_int16:
		return GetRangeShuffleIndexSignedSlice(val, GetCenterValueExtractFromZMSigned(zm, t))
	}
	panic("wrong type!")
}

func GetRangeShuffleIndexForZMUnsignedSlice(val []uint64, zm objectio.ZoneMap) uint64 {
	switch zm.GetType() {
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		return GetRangeShuffleIndexUnsignedSlice(val, GetCenterValueForZMUnsigned(zm))
	}
	panic("wrong type!")
}

func GetRangeShuffleIndexForValuesExtractedFromZMUnsignedSlice(val []uint64, zm objectio.ZoneMap, t types.T) uint64 {
	switch t {
	case types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		return GetRangeShuffleIndexUnsignedSlice(val, GetCenterValueExtractFromZMUnsigned(zm, t))
	}
	panic("wrong type!")
}

func GetRangeShuffleIndexSignedMinMax(minVal, maxVal, currentVal int64, upplerLimit uint64) uint64 {
	if currentVal <= minVal {
		return 0
	} else if currentVal >= maxVal {
		return upplerLimit - 1
	} else {
		step := uint64(maxVal-minVal) / upplerLimit
		ret := uint64(currentVal-minVal) / step
		if ret >= upplerLimit {
			return upplerLimit - 1
		}
		return ret
	}
}

func GetRangeShuffleIndexUnsignedMinMax(minVal, maxVal, currentVal uint64, upplerLimit uint64) uint64 {
	if currentVal <= minVal {
		return 0
	} else if currentVal >= maxVal {
		return upplerLimit - 1
	} else {
		step := (maxVal - minVal) / upplerLimit
		ret := (currentVal - minVal) / step
		if ret >= upplerLimit {
			return upplerLimit - 1
		}
		return ret
	}
}

func GetRangeShuffleIndexSignedSlice(val []int64, currentVal int64) uint64 {
	if currentVal <= val[0] {
		return 0
	}
	left := 0
	right := len(val) - 1
	for left < right {
		mid := (left + right) >> 1
		if currentVal > val[mid] {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if currentVal > val[right] {
		right += 1
	}
	return uint64(right)
}

func GetRangeShuffleIndexUnsignedSlice(val []uint64, currentVal uint64) uint64 {
	if currentVal <= val[0] {
		return 0
	}
	left := 0
	right := len(val) - 1
	for left < right {
		mid := (left + right) >> 1
		if currentVal > val[mid] {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if currentVal > val[right] {
		right += 1
	}
	return uint64(right)
}

func GetHashColumn(expr *plan.Expr) (*plan.ColRef, int32) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		// support shuffle on serial_full/serial function expressions used in secondary index joins
		if exprImpl.F.Func.ObjName == "serial_full" || exprImpl.F.Func.ObjName == "serial" {
			return nil, expr.Typ.Id
		}
		return nil, -1
	case *plan.Expr_Col:
		return exprImpl.Col, expr.Typ.Id
	}
	return nil, -1
}

func maybeSorted(node *plan.Node, builder *QueryBuilder, tag int32) bool {
	// for scan node, primary key and cluster by may be sorted
	if node.NodeType == plan.Node_TABLE_SCAN {
		return node.BindingTags[0] == tag
	}
	// for inner join, if left child may be sorted, then inner join may be sorted
	if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_INNER {
		leftChild := builder.qry.Nodes[node.Children[0]]
		return maybeSorted(leftChild, builder, tag)
	}
	return false
}

func determineShuffleType(col *plan.ColRef, node *plan.Node, builder *QueryBuilder) {
	// hash by default
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash

	if builder == nil {
		return
	}
	tableDef, ok := builder.tag2Table[col.RelPos]

	if !ok {
		child := builder.qry.Nodes[node.Children[0]]
		if child.NodeType == plan.Node_AGG && child.Stats.HashmapStats.Shuffle && col.RelPos == child.BindingTags[0] {
			col = child.GroupBy[col.ColPos].GetCol()
			if col == nil {
				return
			}
			_, ok = builder.tag2Table[col.RelPos]
			if !ok {
				return
			}
			node.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Reuse
			node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
			node.Stats.HashmapStats.HashmapSize = child.Stats.HashmapStats.HashmapSize
			node.Stats.HashmapStats.ShuffleColMin = child.Stats.HashmapStats.ShuffleColMin
			node.Stats.HashmapStats.ShuffleColMax = child.Stats.HashmapStats.ShuffleColMax
			node.Stats.HashmapStats.Ranges = child.Stats.HashmapStats.Ranges
			node.Stats.HashmapStats.Nullcnt = child.Stats.HashmapStats.Nullcnt
		}
		return
	}

	colName := tableDef.Cols[col.ColPos].Name

	// for shuffle join, if left child is not sorted, the cost will be very high
	// should use complex shuffle type
	if node.NodeType == plan.Node_JOIN {
		leftSorted := true
		if GetSortOrder(tableDef, col.ColPos) != 0 {
			leftSorted = false
		}
		if !maybeSorted(builder.qry.Nodes[node.Children[0]], builder, col.RelPos) {
			leftSorted = false
		}
		if !leftSorted {
			leftCost := builder.qry.Nodes[node.Children[0]].Stats.Outcnt
			rightCost := builder.qry.Nodes[node.Children[1]].Stats.Outcnt
			if node.IsRightJoin {
				// its better for right join to go shuffle, but can not go complex shuffle
				if node.JoinType != plan.Node_DEDUP && leftCost > ShuffleTypeThreshHoldUpperLimit*rightCost {
					return
				}
			} else if leftCost > ShuffleTypeThreshHoldLowerLimit*rightCost {
				node.Stats.HashmapStats.ShuffleTypeForMultiCN = plan.ShuffleTypeForMultiCN_Hybrid
			}
		}
	}

	w := builder.getStatsInfoByTableID(tableDef.TblId)
	if w == nil || w.GetStats() == nil {
		return
	}
	s := w.GetStats()
	if node.NodeType == plan.Node_AGG {
		if shouldUseHashShuffle(s.ShuffleRangeMap[colName]) {
			return
		}
	}
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
	node.Stats.HashmapStats.ShuffleColMin = int64(s.MinValMap[colName])
	node.Stats.HashmapStats.ShuffleColMax = int64(s.MaxValMap[colName])
	node.Stats.HashmapStats.Ranges = shouldUseShuffleRanges(s.ShuffleRangeMap[colName], colName)
	node.Stats.HashmapStats.Nullcnt = int64(s.NullCntMap[colName])
}

// to determine if join need to go shuffle
func determineShuffleForJoin(node *plan.Node, builder *QueryBuilder) {
	// do not shuffle by default
	node.Stats.HashmapStats.ShuffleColIdx = -1
	if node.NodeType != plan.Node_JOIN {
		return
	}

	diag := joinShuffleDiag{
		idx:                  -1,
		hashmapSizeThreshold: -1,
		hashmapSizeFactor:    -1,
		hashColType:          -1,
		rightHashColType:     -1,
		highestNDV:           highestNDVForJoinShuffleLog(node),
	}

	switch node.JoinType {
	case plan.Node_DEDUP:
		dedupJoinCtx := node.GetDedupJoinCtx()
		if len(dedupJoinCtx.GetOldColCaptureList()) > 0 {
			logJoinShuffleDecision("old-col-capture-list", node, builder, diag)
			logDedupJoinShuffleDecision("old-col-capture-list", node, builder)
			return
		}
		if (node.OnDuplicateAction == plan.Node_FAIL || node.OnDuplicateAction == plan.Node_IGNORE) && len(dedupJoinCtx.GetOldColList()) > 0 {
			logJoinShuffleDecision("old-col-list", node, builder, diag)
			logDedupJoinShuffleDecision("old-col-list", node, builder)
			return
		}

		if node.IsRightJoin {
			leftChild := builder.qry.Nodes[node.Children[0]]
			if leftChild.Stats.Outcnt <= 200000 {
				logJoinShuffleDecision("right-dedup-left-outcnt-too-small", node, builder, diag)
				logDedupJoinShuffleDecision("right-dedup-left-outcnt-too-small", node, builder)
				return
			}
		} else {
			rightChild := builder.qry.Nodes[node.Children[1]]
			if rightChild.Stats.Outcnt > 320000 {
				//dedup join always go hash shuffle, optimize this in the future
				node.Stats.HashmapStats.Shuffle = true
				node.Stats.HashmapStats.ShuffleColIdx = 0
				node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash
				logJoinShuffleDecision("non-right-dedup-right-outcnt-enable-hash-shuffle", node, builder, diag)
				logDedupJoinShuffleDecision("non-right-dedup-right-outcnt-enable-hash-shuffle", node, builder)
			} else {
				logJoinShuffleDecision("non-right-dedup-right-outcnt-too-small", node, builder, diag)
				logDedupJoinShuffleDecision("non-right-dedup-right-outcnt-too-small", node, builder)
			}

			return
		}

	case plan.Node_INNER, plan.Node_ANTI, plan.Node_SEMI, plan.Node_LEFT, plan.Node_RIGHT, plan.Node_OUTER:

	default:
		logJoinShuffleDecision("unsupported-join-type", node, builder, diag)
		return
	}

	// for now, if join children is merge group or filter, do not allow shuffle
	diag.leftDontShuffle = dontShuffle(builder.qry.Nodes[node.Children[0]], builder)
	diag.rightDontShuffle = dontShuffle(builder.qry.Nodes[node.Children[1]], builder)
	if diag.leftDontShuffle || diag.rightDontShuffle {
		logJoinShuffleDecision("child-dont-shuffle", node, builder, diag)
		if node.JoinType == plan.Node_DEDUP {
			logDedupJoinShuffleDecision("child-dont-shuffle", node, builder)
		}
		return
	}

	idx := 0
	diag.isEquiJoin = builder.IsEquiJoin(node)
	if !diag.isEquiJoin {
		logJoinShuffleDecision("not-equi-join", node, builder, diag)
		if node.JoinType == plan.Node_DEDUP {
			logDedupJoinShuffleDecision("not-equi-join", node, builder)
		}
		return
	}
	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = true
	}
	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = true
	}
	// for now ,only support the first join condition
	for i := range node.OnList {
		if isEquiCond(node.OnList[i], leftTags, rightTags) {
			idx = i
			break
		}
	}
	diag.idx = idx

	if node.IsRightJoin {
		diag.hashmapSizeThreshold = threshHoldForRightJoinShuffle
		if node.Stats.HashmapStats.HashmapSize < threshHoldForRightJoinShuffle {
			logJoinShuffleDecision("right-join-hashmap-size-too-small", node, builder, diag)
			if node.JoinType == plan.Node_DEDUP {
				logDedupJoinShuffleDecision("right-join-hashmap-size-too-small", node, builder)
			}
			return
		}
	} else {
		leftchild := builder.qry.Nodes[node.Children[0]]
		rightchild := builder.qry.Nodes[node.Children[1]]
		factor := math.Pow((leftchild.Stats.Outcnt / rightchild.Stats.Outcnt), 0.4)
		diag.hashmapSizeFactor = factor
		diag.hashmapSizeThreshold = threshHoldForShuffleJoin * factor
		if node.Stats.HashmapStats.HashmapSize < diag.hashmapSizeThreshold {
			logJoinShuffleDecision("hashmap-size-too-small", node, builder, diag)
			if node.JoinType == plan.Node_DEDUP {
				logDedupJoinShuffleDecision("hashmap-size-too-small", node, builder)
			}
			return
		}
	}

	// get the column of left child
	var expr0, expr1 *plan.Expr
	cond := node.OnList[idx]
	switch condImpl := cond.Expr.(type) {
	case *plan.Expr_F:
		expr0 = condImpl.F.Args[0]
		expr1 = condImpl.F.Args[1]
	}

	leftHashCol, typ := GetHashColumn(expr0)
	diag.hashColType = typ
	if leftHashCol == nil && typ == -1 {
		logJoinShuffleDecision("left-hash-column-not-supported", node, builder, diag)
		if node.JoinType == plan.Node_DEDUP {
			logDedupJoinShuffleDecision("left-hash-column-not-supported", node, builder)
		}
		return
	}
	rightHashCol, rightTyp := GetHashColumn(expr1)
	diag.rightHashColType = rightTyp
	if rightHashCol == nil && rightTyp == -1 {
		logJoinShuffleDecision("right-hash-column-not-supported", node, builder, diag)
		if node.JoinType == plan.Node_DEDUP {
			logDedupJoinShuffleDecision("right-hash-column-not-supported", node, builder)
		}
		return
	}

	//for now ,only support integer and string type
	isExprBasedShuffle := leftHashCol == nil || rightHashCol == nil
	diag.exprBasedShuffle = isExprBasedShuffle
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		node.Stats.HashmapStats.ShuffleColIdx = int32(idx)
		node.Stats.HashmapStats.Shuffle = true
		if leftHashCol != nil && !isExprBasedShuffle {
			determineShuffleType(leftHashCol, node, builder)
		}
		// For expression-based shuffle (serial_full/serial in join condition):
		// Force hash shuffle because range shuffle depends on column stats (min/max/ranges)
		// which don't apply to expression results. Hash shuffle works universally.
		if isExprBasedShuffle {
			node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash
			if node.OnList[idx].Ndv < 0 {
				node.OnList[idx].Ndv = node.Stats.HashmapStats.HashmapSize
			}
		}
	default:
		logJoinShuffleDecision("hash-column-type-not-supported", node, builder, diag)
		if node.JoinType == plan.Node_DEDUP {
			logDedupJoinShuffleDecision("hash-column-type-not-supported", node, builder)
		}
	}

	//recheck shuffle plan
	if node.Stats.HashmapStats.Shuffle {
		disabledReason := ""
		if node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Hash && node.Stats.HashmapStats.HashmapSize < threshHoldForHashShuffle {
			node.Stats.HashmapStats.Shuffle = false
			disabledReason = "hash-shuffle-hashmap-size-too-small"
		}

		if node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Range && node.Stats.HashmapStats.Ranges == nil && node.Stats.HashmapStats.ShuffleColMax-node.Stats.HashmapStats.ShuffleColMin < 100000 {
			node.Stats.HashmapStats.Shuffle = false
			if disabledReason == "" {
				disabledReason = "range-shuffle-range-too-small"
			}
		}
		if node.Stats.HashmapStats.ShuffleMethod != plan.ShuffleMethod_Reuse {
			highestNDV := node.OnList[idx].Ndv
			// A negative NDV means that statistics are unavailable.  Do not treat
			// unknown cardinality as low cardinality: for a large build side that
			// would turn a valid shuffle plan back into a broadcast hash build and
			// can concentrate the entire hash table on one CN.
			if highestNDV >= 0 && highestNDV < ShuffleThreshHoldOfNDV {
				node.Stats.HashmapStats.Shuffle = false
				if disabledReason == "" {
					disabledReason = "ndv-too-small"
				}
			}
		}

		if node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Hash && node.JoinType == plan.Node_DEDUP && node.IsRightJoin {
			node.Stats.HashmapStats.Shuffle = false
			if disabledReason == "" {
				disabledReason = "right-dedup-hash-shuffle-disabled"
			}
		}

		if node.JoinType == plan.Node_DEDUP && node.IsRightJoin && node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Range {
			rightChild := builder.qry.Nodes[node.Children[1]]
			rightChild.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
			rightChild.Stats.HashmapStats.ShuffleColIdx = node.Stats.HashmapStats.ShuffleColIdx
			rightChild.Stats.HashmapStats.ShuffleColMin = node.Stats.HashmapStats.ShuffleColMin
			rightChild.Stats.HashmapStats.ShuffleColMax = node.Stats.HashmapStats.ShuffleColMax
			rightChild.Stats.HashmapStats.Ranges = node.Stats.HashmapStats.Ranges
		}
		if node.JoinType == plan.Node_DEDUP && disabledReason != "" {
			logDedupJoinShuffleDecision(disabledReason, node, builder)
		}
		if disabledReason != "" {
			logJoinShuffleDecision(disabledReason, node, builder, diag)
		}
	}
	logJoinShuffleDecision("final", node, builder, diag)
	if node.JoinType == plan.Node_DEDUP {
		logDedupJoinShuffleDecision("final", node, builder)
	}
}

type joinShuffleDiag struct {
	idx                  int
	leftDontShuffle      bool
	rightDontShuffle     bool
	isEquiJoin           bool
	exprBasedShuffle     bool
	hashmapSizeThreshold float64
	hashmapSizeFactor    float64
	hashColType          int32
	rightHashColType     int32
	highestNDV           float64
}

func highestNDVForJoinShuffleLog(node *plan.Node) float64 {
	highestNDV := -1.0
	if node == nil {
		return highestNDV
	}
	for _, expr := range node.OnList {
		if expr != nil && expr.Ndv > highestNDV {
			highestNDV = expr.Ndv
		}
	}
	return highestNDV
}

type FinalJoinShuffleState struct {
	Reason               string
	SelectedOnIdx        int
	HighestNDV           float64
	HashColType          int32
	RightHashColType     int32
	HashmapSizeThreshold float64
	HashmapSizeFactor    float64
}

// ExplainFinalJoinShuffleState derives the final reason a join is not planned as
// shuffle from the finished plan state. This is intentionally independent from
// QueryBuilder so compile-time diagnostics can still explain a false final
// Shuffle flag even when the original planner decision log was not emitted or
// cannot be correlated with the statement.
func ExplainFinalJoinShuffleState(node, left, right *plan.Node) FinalJoinShuffleState {
	state := FinalJoinShuffleState{
		Reason:               "shuffle-enabled",
		SelectedOnIdx:        -1,
		HighestNDV:           highestNDVForJoinShuffleLog(node),
		HashColType:          -1,
		RightHashColType:     -1,
		HashmapSizeThreshold: -1,
		HashmapSizeFactor:    -1,
	}
	if node == nil {
		state.Reason = "node-nil"
		return state
	}
	if node.Stats == nil || node.Stats.HashmapStats == nil {
		state.Reason = "hashmap-stats-missing"
		return state
	}
	if node.Stats.HashmapStats.Shuffle {
		return state
	}

	switch node.JoinType {
	case plan.Node_DEDUP:
		if !node.IsRightJoin {
			if len(node.Children) > 1 && right != nil && right.Stats != nil && right.Stats.Outcnt <= 320000 {
				state.Reason = "non-right-dedup-right-outcnt-too-small"
				return state
			}
		}
	case plan.Node_INNER, plan.Node_ANTI, plan.Node_SEMI, plan.Node_LEFT, plan.Node_RIGHT, plan.Node_OUTER:
	default:
		state.Reason = "unsupported-join-type"
		return state
	}

	hashmapSize := node.Stats.HashmapStats.HashmapSize
	if node.IsRightJoin {
		state.HashmapSizeThreshold = threshHoldForRightJoinShuffle
		if hashmapSize < threshHoldForRightJoinShuffle {
			state.Reason = "right-join-hashmap-size-too-small"
			return state
		}
	} else if left != nil && left.Stats != nil && right != nil && right.Stats != nil && right.Stats.Outcnt > 0 {
		state.HashmapSizeFactor = math.Pow((left.Stats.Outcnt / right.Stats.Outcnt), 0.4)
		state.HashmapSizeThreshold = threshHoldForShuffleJoin * state.HashmapSizeFactor
		if hashmapSize < state.HashmapSizeThreshold {
			state.Reason = "hashmap-size-too-small"
			return state
		}
	}

	selectedOnIdx := int(node.Stats.HashmapStats.ShuffleColIdx)
	if selectedOnIdx < 0 || selectedOnIdx >= len(node.OnList) {
		selectedOnIdx = firstJoinEqualCondIdx(node.OnList)
	}
	state.SelectedOnIdx = selectedOnIdx
	if selectedOnIdx < 0 {
		state.Reason = "not-equi-join"
		return state
	}

	expr0, expr1 := joinCondArgs(node.OnList[selectedOnIdx])
	if expr0 == nil || expr1 == nil {
		state.Reason = "join-condition-not-supported"
		return state
	}
	leftHashCol, typ := GetHashColumn(expr0)
	state.HashColType = typ
	if leftHashCol == nil && typ == -1 {
		state.Reason = "left-hash-column-not-supported"
		return state
	}
	rightHashCol, rightTyp := GetHashColumn(expr1)
	state.RightHashColType = rightTyp
	if rightHashCol == nil && rightTyp == -1 {
		state.Reason = "right-hash-column-not-supported"
		return state
	}

	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
	default:
		state.Reason = "hash-column-type-not-supported"
		return state
	}

	if node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Hash && hashmapSize < threshHoldForHashShuffle {
		state.Reason = "hash-shuffle-hashmap-size-too-small"
		return state
	}
	if node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Range &&
		node.Stats.HashmapStats.Ranges == nil &&
		node.Stats.HashmapStats.ShuffleColMax-node.Stats.HashmapStats.ShuffleColMin < 100000 {
		state.Reason = "range-shuffle-range-too-small"
		return state
	}
	if node.Stats.HashmapStats.ShuffleMethod != plan.ShuffleMethod_Reuse {
		ndv := -1.0
		if node.OnList[selectedOnIdx] != nil {
			ndv = node.OnList[selectedOnIdx].Ndv
		}
		if ndv > state.HighestNDV {
			state.HighestNDV = ndv
		}
		if ndv >= 0 && ndv < ShuffleThreshHoldOfNDV {
			state.Reason = "ndv-too-small"
			return state
		}
	}
	if node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Hash && node.JoinType == plan.Node_DEDUP && node.IsRightJoin {
		state.Reason = "right-dedup-hash-shuffle-disabled"
		return state
	}

	state.Reason = "shuffle-field-default-or-later-disabled"
	return state
}

func firstJoinEqualCondIdx(exprs []*plan.Expr) int {
	for i, expr := range exprs {
		if expr == nil {
			continue
		}
		if e, ok := expr.GetExpr().(*plan.Expr_F); ok && e.F != nil && e.F.Func != nil && len(e.F.Args) >= 2 && IsEqualFunc(e.F.Func.GetObj()) {
			lpos, rpos := HasColExpr(e.F.Args[0], -1), HasColExpr(e.F.Args[1], -1)
			if lpos != -1 && rpos != -1 && lpos != rpos {
				return i
			}
		}
	}
	return -1
}

func joinCondArgs(expr *plan.Expr) (*plan.Expr, *plan.Expr) {
	if expr == nil {
		return nil, nil
	}
	if e, ok := expr.GetExpr().(*plan.Expr_F); ok && e.F != nil && len(e.F.Args) >= 2 {
		return e.F.Args[0], e.F.Args[1]
	}
	return nil, nil
}

func logJoinShuffleDecision(reason string, node *plan.Node, builder *QueryBuilder, diag joinShuffleDiag) {
	if builder == nil || builder.qry == nil || node == nil {
		return
	}
	nodeID := findPlanNodeID(builder, node)
	leftNodeID := int32(-1)
	rightNodeID := int32(-1)
	leftOutcnt := -1.0
	rightOutcnt := -1.0
	leftTableCnt := -1.0
	rightTableCnt := -1.0
	leftHashmapSize := -1.0
	rightHashmapSize := -1.0
	if len(node.Children) > 0 {
		leftNodeID = node.Children[0]
		if int(leftNodeID) < len(builder.qry.Nodes) {
			leftChild := builder.qry.Nodes[leftNodeID]
			if leftChild != nil && leftChild.Stats != nil {
				leftOutcnt = leftChild.Stats.Outcnt
				leftTableCnt = leftChild.Stats.TableCnt
				if leftChild.Stats.HashmapStats != nil {
					leftHashmapSize = leftChild.Stats.HashmapStats.HashmapSize
				}
			}
		}
	}
	if len(node.Children) > 1 {
		rightNodeID = node.Children[1]
		if int(rightNodeID) < len(builder.qry.Nodes) {
			rightChild := builder.qry.Nodes[rightNodeID]
			if rightChild != nil && rightChild.Stats != nil {
				rightOutcnt = rightChild.Stats.Outcnt
				rightTableCnt = rightChild.Stats.TableCnt
				if rightChild.Stats.HashmapStats != nil {
					rightHashmapSize = rightChild.Stats.HashmapStats.HashmapSize
				}
			}
		}
	}

	oldColListLen := 0
	oldColCaptureListLen := 0
	if node.DedupJoinCtx != nil {
		oldColListLen = len(node.DedupJoinCtx.OldColList)
		oldColCaptureListLen = len(node.DedupJoinCtx.OldColCaptureList)
	}
	hashmapSize := -1.0
	shuffle := false
	shuffleColIdx := int32(-1)
	shuffleType := plan.ShuffleType_Hash
	shuffleMethod := plan.ShuffleMethod_Normal
	if node.Stats != nil && node.Stats.HashmapStats != nil {
		hashmapSize = node.Stats.HashmapStats.HashmapSize
		shuffle = node.Stats.HashmapStats.Shuffle
		shuffleColIdx = node.Stats.HashmapStats.ShuffleColIdx
		shuffleType = node.Stats.HashmapStats.ShuffleType
		shuffleMethod = node.Stats.HashmapStats.ShuffleMethod
	}
	highestNDV := diag.highestNDV
	if currentNDV := highestNDVForJoinShuffleLog(node); currentNDV > highestNDV {
		highestNDV = currentNDV
	}
	sessionID, statementID, txnID, queryID, sqlPrefix := joinShuffleDiagIDs(builder)

	logutil.Infof(
		"join shuffle decision: reason=%s, node-id=%d, node-type=%v, join-type=%v, shuffle=%v, is-right-join=%v, on-duplicate-action=%v, old-col-list-len=%d, old-col-capture-list-len=%d, left-node-id=%d, right-node-id=%d, left-outcnt=%f, right-outcnt=%f, left-tablecnt=%f, right-tablecnt=%f, hashmap-size=%f, left-hashmap-size=%f, right-hashmap-size=%f, hashmap-size-threshold=%f, hashmap-size-factor=%f, highest-ndv=%f, is-equi-join=%v, left-dont-shuffle=%v, right-dont-shuffle=%v, expr-based-shuffle=%v, hash-col-type=%d, right-hash-col-type=%d, selected-on-idx=%d, shuffle-col-idx=%d, shuffle-type=%v, shuffle-method=%v, load-tag=%v, stmt-type=%v, session-id=%q, statement-id=%q, txn-id=%q, query-id=%q, sql-prefix=%q, left-child=%s, right-child=%s, left-load-source=%s, right-load-source=%s",
		reason,
		nodeID,
		node.NodeType,
		node.JoinType,
		shuffle,
		node.IsRightJoin,
		node.OnDuplicateAction,
		oldColListLen,
		oldColCaptureListLen,
		leftNodeID,
		rightNodeID,
		leftOutcnt,
		rightOutcnt,
		leftTableCnt,
		rightTableCnt,
		hashmapSize,
		leftHashmapSize,
		rightHashmapSize,
		diag.hashmapSizeThreshold,
		diag.hashmapSizeFactor,
		highestNDV,
		diag.isEquiJoin,
		diag.leftDontShuffle,
		diag.rightDontShuffle,
		diag.exprBasedShuffle,
		diag.hashColType,
		diag.rightHashColType,
		diag.idx,
		shuffleColIdx,
		shuffleType,
		shuffleMethod,
		builder.qry.LoadTag,
		builder.qry.StmtType,
		sessionID,
		statementID,
		txnID,
		queryID,
		sqlPrefix,
		formatPlanNodeForDedupShuffleLog(builder, leftNodeID),
		formatPlanNodeForDedupShuffleLog(builder, rightNodeID),
		traceLoadExternalScanForDedupShuffleLog(builder, leftNodeID),
		traceLoadExternalScanForDedupShuffleLog(builder, rightNodeID),
	)
}

func logDedupJoinShuffleDecision(reason string, node *plan.Node, builder *QueryBuilder) {
	nodeID := findPlanNodeID(builder, node)
	leftOutcnt := -1.0
	rightOutcnt := -1.0
	leftHashmapSize := -1.0
	rightHashmapSize := -1.0
	leftNodeID := int32(-1)
	rightNodeID := int32(-1)
	if len(node.Children) > 0 {
		leftNodeID = node.Children[0]
		leftChild := builder.qry.Nodes[node.Children[0]]
		if leftChild != nil && leftChild.Stats != nil {
			leftOutcnt = leftChild.Stats.Outcnt
			if leftChild.Stats.HashmapStats != nil {
				leftHashmapSize = leftChild.Stats.HashmapStats.HashmapSize
			}
		}
	}
	if len(node.Children) > 1 {
		rightNodeID = node.Children[1]
		rightChild := builder.qry.Nodes[node.Children[1]]
		if rightChild != nil && rightChild.Stats != nil {
			rightOutcnt = rightChild.Stats.Outcnt
			if rightChild.Stats.HashmapStats != nil {
				rightHashmapSize = rightChild.Stats.HashmapStats.HashmapSize
			}
		}
	}

	oldColListLen := 0
	oldColCaptureListLen := 0
	if node.DedupJoinCtx != nil {
		oldColListLen = len(node.DedupJoinCtx.OldColList)
		oldColCaptureListLen = len(node.DedupJoinCtx.OldColCaptureList)
	}
	hashmapSize := -1.0
	shuffle := false
	shuffleColIdx := int32(-1)
	shuffleType := plan.ShuffleType_Hash
	shuffleMethod := plan.ShuffleMethod_Normal
	if node.Stats != nil && node.Stats.HashmapStats != nil {
		hashmapSize = node.Stats.HashmapStats.HashmapSize
		shuffle = node.Stats.HashmapStats.Shuffle
		shuffleColIdx = node.Stats.HashmapStats.ShuffleColIdx
		shuffleType = node.Stats.HashmapStats.ShuffleType
		shuffleMethod = node.Stats.HashmapStats.ShuffleMethod
	}
	highestNDV := -1.0
	for _, expr := range node.OnList {
		if expr != nil && expr.Ndv > highestNDV {
			highestNDV = expr.Ndv
		}
	}
	sessionID, statementID, txnID, queryID, sqlPrefix := joinShuffleDiagIDs(builder)

	logutil.Infof(
		"dedup join shuffle decision: reason=%s, node-id=%d, shuffle=%v, is-right-join=%v, on-duplicate-action=%v, old-col-list-len=%d, old-col-capture-list-len=%d, left-node-id=%d, right-node-id=%d, left-outcnt=%f, right-outcnt=%f, hashmap-size=%f, left-hashmap-size=%f, right-hashmap-size=%f, highest-ndv=%f, shuffle-col-idx=%d, shuffle-type=%v, shuffle-method=%v, load-tag=%v, stmt-type=%v, session-id=%q, statement-id=%q, txn-id=%q, query-id=%q, sql-prefix=%q, left-child=%s, right-child=%s, left-load-source=%s, right-load-source=%s",
		reason,
		nodeID,
		shuffle,
		node.IsRightJoin,
		node.OnDuplicateAction,
		oldColListLen,
		oldColCaptureListLen,
		leftNodeID,
		rightNodeID,
		leftOutcnt,
		rightOutcnt,
		hashmapSize,
		leftHashmapSize,
		rightHashmapSize,
		highestNDV,
		shuffleColIdx,
		shuffleType,
		shuffleMethod,
		builder.qry.LoadTag,
		builder.qry.StmtType,
		sessionID,
		statementID,
		txnID,
		queryID,
		sqlPrefix,
		formatPlanNodeForDedupShuffleLog(builder, leftNodeID),
		formatPlanNodeForDedupShuffleLog(builder, rightNodeID),
		traceLoadExternalScanForDedupShuffleLog(builder, leftNodeID),
		traceLoadExternalScanForDedupShuffleLog(builder, rightNodeID),
	)
}

func joinShuffleDiagIDs(builder *QueryBuilder) (sessionID, statementID, txnID, queryID, sqlPrefix string) {
	if builder == nil || builder.compCtx == nil {
		return "", "", "", "", ""
	}
	sqlPrefix = redactShuffleDiagSQL(builder.compCtx.GetRootSql())
	if len(sqlPrefix) > 512 {
		sqlPrefix = sqlPrefix[:512] + "..."
	}
	proc := builder.compCtx.GetProcess()
	if proc == nil {
		return sessionID, statementID, txnID, queryID, sqlPrefix
	}
	if si := proc.GetSessionInfo(); si != nil {
		sessionID = si.SessionId.String()
		if len(si.QueryId) > 0 {
			queryID = si.QueryId[len(si.QueryId)-1]
		}
	}
	if sp := proc.GetStmtProfile(); sp != nil {
		statementID = sp.GetStmtId().String()
		txnID = sp.GetTxnId().String()
	}
	return sessionID, statementID, txnID, queryID, sqlPrefix
}

func redactShuffleDiagSQL(sql string) string {
	const marker = "s3option{"
	lower := strings.ToLower(sql)
	start := strings.Index(lower, marker)
	if start < 0 {
		return sql
	}
	depth := 0
	for i := start + len("s3option"); i < len(sql); i++ {
		switch sql[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return sql[:start] + "s3option{...redacted...}" + sql[i+1:]
			}
		}
	}
	return sql[:start] + "s3option{...redacted...}"
}

func findPlanNodeID(builder *QueryBuilder, target *plan.Node) int32 {
	if builder == nil || builder.qry == nil || target == nil {
		return -1
	}
	for i, node := range builder.qry.Nodes {
		if node == target {
			return int32(i)
		}
	}
	return -1
}

func formatPlanNodeForDedupShuffleLog(builder *QueryBuilder, nodeID int32) string {
	return formatPlanNodeForDedupShuffleLogWithDepth(builder, nodeID, 1)
}

func formatPlanNodeForDedupShuffleLogWithDepth(builder *QueryBuilder, nodeID int32, sourceDepth int) string {
	if builder == nil || builder.qry == nil || nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return "invalid"
	}
	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return "nil"
	}

	stats := "stats=nil"
	if node.Stats != nil {
		hashmapSize := -1.0
		shuffle := false
		if node.Stats.HashmapStats != nil {
			hashmapSize = node.Stats.HashmapStats.HashmapSize
			shuffle = node.Stats.HashmapStats.Shuffle
		}
		stats = fmt.Sprintf("outcnt=%f tablecnt=%f cost=%f blocknum=%d hashmap-size=%f shuffle=%v",
			node.Stats.Outcnt, node.Stats.TableCnt, node.Stats.Cost, node.Stats.BlockNum, hashmapSize, shuffle)
	}

	tableName := ""
	if node.TableDef != nil {
		tableName = node.TableDef.Name
	}
	objName := ""
	if node.ObjRef != nil {
		objName = node.ObjRef.ObjName
	}

	sourceStep := int32(-1)
	sourceRoot := "none"
	if len(node.GetSourceStep()) > 0 {
		sourceStep = node.GetSourceStep()[0]
		if sourceStep >= 0 && int(sourceStep) < len(builder.qry.Steps) {
			sourceRootID := builder.qry.Steps[sourceStep]
			if sourceDepth > 0 {
				sourceRoot = formatPlanNodeForDedupShuffleLogWithDepth(builder, sourceRootID, sourceDepth-1)
			} else {
				sourceRoot = fmt.Sprintf("{id=%d depth-limit}", sourceRootID)
			}
		}
	}

	return fmt.Sprintf("{id=%d type=%s join-type=%s table=%q obj=%q children=%v source-step=%d stats={%s} source-root=%s}",
		nodeID, node.NodeType.String(), node.JoinType.String(), tableName, objName, node.Children, sourceStep, stats, sourceRoot)
}

func traceLoadExternalScanForDedupShuffleLog(builder *QueryBuilder, rootID int32) string {
	if builder == nil || builder.qry == nil || rootID < 0 || int(rootID) >= len(builder.qry.Nodes) {
		return "invalid"
	}

	const maxVisit = 64
	type item struct {
		id   int32
		path string
	}
	queue := []item{{id: rootID, path: fmt.Sprintf("%d", rootID)}}
	visited := make(map[int32]bool)
	for len(queue) > 0 && len(visited) < maxVisit {
		cur := queue[0]
		queue = queue[1:]
		if cur.id < 0 || int(cur.id) >= len(builder.qry.Nodes) || visited[cur.id] {
			continue
		}
		visited[cur.id] = true
		node := builder.qry.Nodes[cur.id]
		if node == nil {
			continue
		}
		if node.NodeType == plan.Node_EXTERNAL_SCAN && node.ExternScan != nil && plan.ExternType(node.ExternScan.Type) == plan.ExternType_LOAD {
			stats := "stats=nil"
			if node.Stats != nil {
				stats = fmt.Sprintf("outcnt=%f tablecnt=%f cost=%f blocknum=%d rowsize=%f",
					node.Stats.Outcnt, node.Stats.TableCnt, node.Stats.Cost, node.Stats.BlockNum, node.Stats.Rowsize)
			}
			tableName := ""
			if node.TableDef != nil {
				tableName = node.TableDef.Name
			}
			return fmt.Sprintf("{found=true external-scan-id=%d path=%s table=%q %s}", cur.id, cur.path, tableName, stats)
		}

		for _, childID := range node.Children {
			queue = append(queue, item{id: childID, path: cur.path + "->" + fmt.Sprintf("%d", childID)})
		}
		for _, sourceStep := range node.GetSourceStep() {
			if sourceStep >= 0 && int(sourceStep) < len(builder.qry.Steps) {
				sourceRootID := builder.qry.Steps[sourceStep]
				queue = append(queue, item{id: sourceRootID, path: cur.path + fmt.Sprintf("->step[%d]->%d", sourceStep, sourceRootID)})
			}
		}
	}

	visitedTypes := make([]string, 0, len(visited))
	for id := range visited {
		if id >= 0 && int(id) < len(builder.qry.Nodes) && builder.qry.Nodes[id] != nil {
			visitedTypes = append(visitedTypes, fmt.Sprintf("%d:%s", id, builder.qry.Nodes[id].NodeType.String()))
		}
	}
	return fmt.Sprintf("{found=false visited=%s}", strings.Join(visitedTypes, ","))
}

// find mergegroup or mergegroup->filter node
func dontShuffle(node *plan.Node, builder *QueryBuilder) bool {
	if node.NodeType == plan.Node_AGG && !node.Stats.HashmapStats.Shuffle {
		return true
	}
	if node.NodeType == plan.Node_FILTER {
		if builder.qry.Nodes[node.Children[0]].NodeType == plan.Node_AGG && !builder.qry.Nodes[node.Children[0]].Stats.HashmapStats.Shuffle {
			return true
		}
	}
	return false
}

// to determine if groupby need to go shuffle
func determineShuffleForGroupBy(node *plan.Node, builder *QueryBuilder) {
	// do not shuffle by default
	node.Stats.HashmapStats.ShuffleColIdx = -1

	if node.NodeType != plan.Node_AGG {
		return
	}
	if len(node.GroupBy) == 0 {
		return
	}

	child := builder.qry.Nodes[node.Children[0]]

	// for now, if agg children is agg or filter, do not allow shuffle
	if dontShuffle(child, builder) {
		return
	}

	factor := 1 / math.Pow((node.Stats.Outcnt/node.Stats.Selectivity/child.Stats.Outcnt), 0.8)
	if node.Stats.HashmapStats.HashmapSize < threshHoldForShuffleGroup*factor {
		return
	}

	//find the highest ndv
	highestNDV := node.GroupBy[0].Ndv
	idx := 0
	for i := range node.GroupBy {
		if node.GroupBy[i].Ndv > highestNDV {
			highestNDV = node.GroupBy[i].Ndv
			idx = i
		}
	}
	if highestNDV < ShuffleThreshHoldOfNDV {
		return
	}

	hashCol, typ := GetHashColumn(node.GroupBy[idx])
	if hashCol == nil {
		return
	}
	//for now ,only support integer and string type
	switch types.T(typ) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64, types.T_uint32, types.T_uint16, types.T_varchar, types.T_char, types.T_text:
		node.Stats.HashmapStats.ShuffleColIdx = int32(idx)
		node.Stats.HashmapStats.Shuffle = true
		determineShuffleType(hashCol, node, builder)
		if node.Stats.HashmapStats.ShuffleType == plan.ShuffleType_Hash && node.Stats.HashmapStats.HashmapSize < threshHoldForHashShuffle {
			node.Stats.HashmapStats.Shuffle = false
		}
	}

	//shuffle join-> shuffle group ,if they use the same hask key, the group can reuse the shuffle method
	if child.NodeType == plan.Node_JOIN {
		if node.Stats.HashmapStats.Shuffle && child.Stats.HashmapStats.Shuffle {
			// shuffle group can reuse shuffle join
			if node.Stats.HashmapStats.ShuffleType == child.Stats.HashmapStats.ShuffleType && node.Stats.HashmapStats.ShuffleTypeForMultiCN == child.Stats.HashmapStats.ShuffleTypeForMultiCN {
				groupHashCol, _ := GetHashColumn(node.GroupBy[node.Stats.HashmapStats.ShuffleColIdx])
				switch exprImpl := child.OnList[child.Stats.HashmapStats.ShuffleColIdx].Expr.(type) {
				case *plan.Expr_F:
					for _, arg := range exprImpl.F.Args {
						joinHashCol, _ := GetHashColumn(arg)
						if joinHashCol != nil && groupHashCol != nil && groupHashCol.RelPos == joinHashCol.RelPos && groupHashCol.ColPos == joinHashCol.ColPos {
							node.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Reuse
							return
						}
					}
				}
			}
		}
	}

}

// default shuffle type for scan is hash
// for table with primary key, and ndv of first column in primary key is high enough, use range shuffle
// only support integer type
func determineShuffleForScan(node *plan.Node, builder *QueryBuilder) {
	node.Stats.HashmapStats.Shuffle = true
	node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Hash
	if builder.optimizerHints != nil && builder.optimizerHints.determineShuffle == 2 { // always go hashshuffle for scan
		return
	}
	w := builder.getStatsInfoByTableID(node.TableDef.TblId)
	if w == nil || w.GetStats() == nil {
		return
	}

	var firstSortColName string
	if node.TableDef.ClusterBy != nil {
		firstSortColName = util.GetClusterByFirstColumn(node.TableDef.ClusterBy.Name)
	} else if node.TableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return
	} else {
		firstSortColName = node.TableDef.Pkey.Names[0]
	}

	s := w.GetStats()
	if s.NdvMap[firstSortColName] < ShuffleThreshHoldOfNDV {
		return
	}
	firstSortColID, ok := node.TableDef.Name2ColIndex[firstSortColName]
	if !ok {
		return
	}
	switch types.T(node.TableDef.Cols[firstSortColID].Typ.Id) {
	case types.T_int64, types.T_int32, types.T_int16, types.T_uint64,
		types.T_uint32, types.T_uint16, types.T_char, types.T_varchar, types.T_text:
		node.Stats.HashmapStats.ShuffleType = plan.ShuffleType_Range
		node.Stats.HashmapStats.ShuffleColIdx = node.TableDef.Cols[firstSortColID].Typ.Id // actually this is specially used for sort key column type
		node.Stats.HashmapStats.ShuffleColMin = int64(s.MinValMap[firstSortColName])
		node.Stats.HashmapStats.ShuffleColMax = int64(s.MaxValMap[firstSortColName])
		node.Stats.HashmapStats.Ranges = shouldUseShuffleRanges(s.ShuffleRangeMap[firstSortColName], firstSortColName)
		node.Stats.HashmapStats.Nullcnt = int64(s.NullCntMap[firstSortColName])
	}
}

func determineShuffleMethod(nodeID int32, builder *QueryBuilder) {
	if builder.optimizerHints != nil && builder.optimizerHints.determineShuffle == 1 {
		return
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			determineShuffleMethod(child, builder)
		}
	}
	switch node.NodeType {
	case plan.Node_AGG:
		determineShuffleForGroupBy(node, builder)
	case plan.Node_TABLE_SCAN:
		determineShuffleForScan(node, builder)
	case plan.Node_JOIN:
		determineShuffleForJoin(node, builder)
	default:
	}
}

// second pass of determine shuffle
func determineShuffleMethod2(nodeID, parentID int32, builder *QueryBuilder) {
	if builder.optimizerHints != nil && builder.optimizerHints.determineShuffle == 1 {
		return
	}
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) > 0 {
		for _, child := range node.Children {
			determineShuffleMethod2(child, nodeID, builder)
		}
	}
	if parentID == -1 {
		return
	}
	parent := builder.qry.Nodes[parentID]

	if node.NodeType == plan.Node_JOIN && node.Stats.HashmapStats.ShuffleTypeForMultiCN == plan.ShuffleTypeForMultiCN_Hybrid {
		if parent.NodeType == plan.Node_AGG && parent.Stats.HashmapStats.ShuffleMethod == plan.ShuffleMethod_Reuse {
			return
		}
		if node.Stats.HashmapStats.HashmapSize <= threshHoldForHybirdShuffle {
			node.Stats.HashmapStats.Shuffle = false
			if parent.NodeType == plan.Node_AGG {
				parent.Stats.HashmapStats.ShuffleMethod = plan.ShuffleMethod_Normal
			}
		}
	}
}

func shouldUseHashShuffle(s *pb.ShuffleRange) bool {
	if s == nil || math.IsNaN(s.Overlap) {
		return true
	}
	if s.Overlap > overlapThreshold && s.Result == nil {
		return true
	}
	return false
}

func shouldUseShuffleRanges(s *pb.ShuffleRange, colname string) []float64 {
	if s == nil || math.IsNaN(s.Uniform) || s.Result == nil {
		return nil
	}
	if s.Uniform < uniformThreshold {
		return s.Result
	}
	return nil
}
