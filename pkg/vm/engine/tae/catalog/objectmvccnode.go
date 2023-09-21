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

package catalog

import (
	"fmt"
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type ObjectMVCCNode struct {
	MetaLoc  objectio.Location
}

func NewEmptyObjectMVCCNode() *ObjectMVCCNode {
	return &ObjectMVCCNode{}
}

func (e *ObjectMVCCNode) CloneAll() *ObjectMVCCNode {
	node := &ObjectMVCCNode{
		MetaLoc:  e.MetaLoc,
	}
	return node
}

func (e *ObjectMVCCNode) CloneData() *ObjectMVCCNode {
	return &ObjectMVCCNode{
		MetaLoc:  e.MetaLoc,
	}
}

func (e *ObjectMVCCNode) String() string {

	return fmt.Sprintf("[MetaLoc=\"%s\"]",
		e.MetaLoc.String())
}

// for create drop in one txn
func (e *ObjectMVCCNode) Update(un *ObjectMVCCNode) {
	if !un.MetaLoc.IsEmpty() {
		e.MetaLoc = un.MetaLoc
	}
}

func (e *ObjectMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	var sn int64
	if sn, err = objectio.WriteBytes(e.MetaLoc, w); err != nil {
		return
	}
	n += sn
	return
}

func (e *ObjectMVCCNode) ReadFromWithVersion(r io.Reader, ver uint16) (n int64, err error) {
	var sn int64
	if e.MetaLoc, sn, err = objectio.ReadBytes(r); err != nil {
		return
	}
	n += sn
	return
}

const (
	ObjectNodeSize int64 = int64(unsafe.Sizeof(ObjectNode{}))
)
type ObjectNode struct {
	state EntryState
}

func EncodeObjectNode(node *ObjectNode) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(node)), BlockNodeSize)
}

func (node *ObjectNode) ReadFrom(r io.Reader) (n int64, err error) {
	if _, err = r.Read(EncodeObjectNode(node)); err != nil {
		return
	}
	n += BlockNodeSize
	return
}

func (node *ObjectNode) WriteTo(w io.Writer) (n int64, err error) {
	if _, err = w.Write(EncodeObjectNode(node)); err != nil {
		return
	}
	n += BlockNodeSize
	return
}
