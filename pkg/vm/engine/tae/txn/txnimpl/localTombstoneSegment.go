// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

type localTombstoneSegment struct {
	*localSegment
}

func (seg *localTombstoneSegment) RangeDelete(
	id *common.ID,
	start,
	end uint32,
	pk containers.Vector,
	dt handle.DeleteType) (err error) {
	bat, err := seg.makeWorkspaceDeleteBatch(id, start, end, pk)
	if err != nil {
		return
	}
	err = seg.Append(bat)
	return
}

// schema: rowid, pk
// pk is from rpc.req
func (seg *localTombstoneSegment) makeWorkspaceDeleteBatch(
	id *common.ID,
	start,
	end uint32,
	pk containers.Vector) (bat *containers.Batch, err error) {
	bat = containers.NewBatch()
	rowIDVec := containers.MakeVector(types.T_Rowid.ToType())
	bat.AddVector(catalog.AttrRowID, rowIDVec)
	for i := start; i <= end; i++ {
		rowID := objectio.NewRowid(&id.BlockID, i)
		rowIDVec.Append(rowID, false)
	}
	bat.AddVector("pk", pk)
	return
}
