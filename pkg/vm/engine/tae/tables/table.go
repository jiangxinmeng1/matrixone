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

package tables

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type dataTable struct {
	meta       *catalog.TableEntry
	aObj       *aobject
	aTombstone *aobject
	shared     *sharedAppender
	sharedTomb *sharedAppender
	rt         *dbutils.Runtime
}

func newTable(meta *catalog.TableEntry, rt *dbutils.Runtime) *dataTable {
	t := &dataTable{
		meta: meta,
		rt:   rt,
	}
	t.shared = &sharedAppender{table: t}
	t.sharedTomb = &sharedAppender{table: t, isTombstone: true}
	return t
}

func (table *dataTable) GetHandle(isTombstone bool) data.TableHandle {
	if isTombstone {
		return newHandle(table, table.aTombstone, isTombstone)
	} else {
		return newHandle(table, table.aObj, isTombstone)
	}
}

func (table *dataTable) ApplyHandle(h data.TableHandle, isTombstone bool) {
	handle := h.(*tableHandle)
	if isTombstone {
		table.aTombstone = handle.object
	} else {
		table.aObj = handle.object
	}

}

func (table *dataTable) PrepareSharedAppend(
	isTombstone bool,
	schema any,
	txn txnif.AsyncTxn,
	rows uint32,
	isMergeCompact bool,
) (data.ObjectAppender, txnif.AppendNode, uint32, uint32, bool, error) {
	if isTombstone {
		return table.sharedTomb.PrepareAppend(schema.(*catalog.Schema), txn, rows, isMergeCompact)
	}
	return table.shared.PrepareAppend(schema.(*catalog.Schema), txn, rows, isMergeCompact)
}

type sharedAppender struct {
	sync.Mutex
	table       *dataTable
	isTombstone bool
	current     *aobject
	nextRow     uint32
}
