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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

const (
	CmdUpdateDatabase = int16(256) + iota
	CmdUpdateTable
	CmdUpdateSegment
	CmdUpdateBlock
)

var cmdNames = map[int16]string{
	CmdUpdateDatabase: "UDB",
	CmdUpdateTable:    "UTBL",
	CmdUpdateSegment:  "USEG",
	CmdUpdateBlock:    "UBLK",
}

func CmdName(t int16) string {
	return cmdNames[t]
}

func init() {
	txnif.RegisterCmdFactory(CmdUpdateDatabase, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdUpdateTable, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdUpdateSegment, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdUpdateBlock, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
}

type EntryCommand struct {
	*txnbase.BaseCustomizedCmd
	cmdType   int16
	entry     BaseEntry
	DBID      uint64
	TableID   uint64
	SegmentID uint64
	DB        *DBEntry
	Table     *TableEntry
	Segment   *SegmentEntry
	Block     *BlockEntry
}

func newEmptyEntryCmd(cmdType int16) *EntryCommand {
	impl := &EntryCommand{
		DB:      nil,
		cmdType: cmdType,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(0, impl)
	return impl
}

func newBlockCmd(id uint32, cmdType int16, entry *BlockEntry) *EntryCommand {
	impl := &EntryCommand{
		DB:      entry.GetSegment().GetTable().GetDB(),
		Table:   entry.GetSegment().GetTable(),
		Segment: entry.GetSegment(),
		Block:   entry,
		cmdType: cmdType,
		entry:   entry.MetaBaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newSegmentCmd(id uint32, cmdType int16, entry *SegmentEntry) *EntryCommand {
	impl := &EntryCommand{
		DB:      entry.GetTable().GetDB(),
		Table:   entry.GetTable(),
		Segment: entry,
		cmdType: cmdType,
		entry:   entry.MetaBaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newTableCmd(id uint32, cmdType int16, entry *TableEntry) *EntryCommand {
	impl := &EntryCommand{
		DB:      entry.GetDB(),
		Table:   entry,
		cmdType: cmdType,
		entry:   entry.TableBaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newDBCmd(id uint32, cmdType int16, entry *DBEntry) *EntryCommand {
	impl := &EntryCommand{
		DB:      entry,
		cmdType: cmdType,
	}
	if entry != nil {
		impl.entry = entry.DBBaseEntry
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func (cmd *EntryCommand) Desc() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%s;CSN=%d", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs().ToString(), cmd.ID)
	return s
}

func (cmd *EntryCommand) GetLogIndex() *wal.Index {
	if cmd.entry == nil {
		return nil
	}
	return cmd.entry.GetLatestNodeLocked().GetLogIndex()
}
func (cmd *EntryCommand) SetReplayTxn(txn txnif.AsyncTxn) {
	switch cmd.cmdType {
	case CmdUpdateBlock:
	case CmdUpdateSegment:
	case CmdUpdateTable:
	case CmdUpdateDatabase:
		// cmd.entry.GetLatestNodeLocked().(*DBMVCCNode).Txn = txn
	default:
		panic(fmt.Sprintf("invalid command type %d", cmd.cmdType))
	}
}
func (cmd *EntryCommand) ApplyCommit() {
	switch cmd.cmdType {
	case CmdUpdateSegment, CmdUpdateBlock, CmdUpdateTable:
	case CmdUpdateDatabase:
		// node := cmd.entry.GetLatestNodeLocked()
		// if node.Is1PC() {
		// 	return
		// }
		// if err := node.ApplyCommit(nil); err != nil {
		// 	panic(err)
		// }
	default:
		panic(fmt.Sprintf("invalid command type %d", cmd.cmdType))
	}
}

// TODO
func (cmd *EntryCommand) ApplyRollback() {
	switch cmd.cmdType {
	case CmdUpdateBlock, CmdUpdateSegment, CmdUpdateTable, CmdUpdateDatabase:
		// node := cmd.entry.GetLatestNodeLocked().(*MetadataMVCCNode)
		// if node.Is1PC() {
		// 	return
		// }
		// node.ApplyRollback(nil)
	default:
		panic(fmt.Sprintf("invalid command type %d", cmd.cmdType))
	}
}
func (cmd *EntryCommand) GetTs() types.TS {
	return types.TS{}
	switch cmd.cmdType {
	case CmdUpdateBlock, CmdUpdateSegment:
		return types.TS{}
	}
	ts := cmd.entry.GetLatestNodeLocked().GetPrepare()
	return ts
}
func (cmd *EntryCommand) IDString() string {
	s := ""
	dbid, id := cmd.GetID()
	switch cmd.cmdType {
	case CmdUpdateDatabase:
		s = fmt.Sprintf("%sDB=%d", s, dbid)
	case CmdUpdateTable:
		s = fmt.Sprintf("%sDB=%d;CommonID=%s", s, dbid, id.TableString())
	case CmdUpdateSegment:
		s = fmt.Sprintf("%sDB=%d;CommonID=%s", s, dbid, id.SegmentString())
	case CmdUpdateBlock:
		s = fmt.Sprintf("%sDB=%d;CommonID=%s", s, dbid, id.BlockString())
	}
	return s
}
func (cmd *EntryCommand) GetID() (uint64, *common.ID) {
	id := &common.ID{}
	dbid := uint64(0)
	switch cmd.cmdType {
	case CmdUpdateDatabase:
		// dbid = cmd.entry.GetID()
	case CmdUpdateTable:
		// if cmd.DBID != 0 {
		// 	dbid = cmd.DBID
		// 	id.TableID = cmd.Table.ID
		// } else {
		// 	dbid = cmd.Table.db.ID
		// 	id.TableID = cmd.entry.GetID()
		// }
	case CmdUpdateSegment:
		// if cmd.DBID != 0 {
		// 	dbid = cmd.DBID
		// 	id.TableID = cmd.TableID
		// 	id.SegmentID = cmd.entry.GetID()
		// } else {
		// 	dbid = cmd.DB.ID
		// 	id.TableID = cmd.Table.ID
		// 	id.SegmentID = cmd.Segment.ID
		// }
	case CmdUpdateBlock:
		// if cmd.DBID != 0 {
		// 	dbid = cmd.DBID
		// 	id.TableID = cmd.TableID
		// 	id.SegmentID = cmd.SegmentID
		// 	id.BlockID = cmd.entry.GetID()
		// } else {
		// 	dbid = cmd.DB.ID
		// 	id.TableID = cmd.Table.ID
		// 	id.SegmentID = cmd.Segment.ID
		// 	id.BlockID = cmd.entry.GetID()
		// }
	}
	return dbid, id
}

func (cmd *EntryCommand) String() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%s;CSN=%d;BaseEntry=%s", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs().ToString(), cmd.ID, cmd.entry.String())
	return s
}

func (cmd *EntryCommand) VerboseString() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%s;CSN=%d;BaseEntry=%s", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs().ToString(), cmd.ID, cmd.entry.String())
	switch cmd.cmdType {
	case CmdUpdateTable:
		s = fmt.Sprintf("%s;Schema=%v", s, cmd.Table.schema.String())
	}
	return s
}
func (cmd *EntryCommand) GetType() int16 { return cmd.cmdType }

func (cmd *EntryCommand) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, cmd.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.ID); err != nil {
		return
	}
	n = 4 + 2

	if err = binary.Write(w, binary.BigEndian, cmd.entry.GetID()); err != nil {
		return
	}
	n += 4
	switch cmd.GetType() {
	case CmdUpdateDatabase:
		entries := cmd.DB.MakeLogtailEntries()
		if err = binary.Write(w, binary.BigEndian, uint64(len(entries))); err != nil {
			return
		}
		n += 8
		for _, entry := range entries {
			buf := new(bytes.Buffer)
			if err := gob.NewEncoder(buf).Encode(entry); err != nil {
				return n, err
			}
			length := uint64(buf.Len())
			if err := binary.Write(w, binary.BigEndian, length); err != nil {
				panic(err)
			}
			n += 8
			sn, err := w.Write(buf.Bytes())
			if err != nil {
				panic(err)
			}
			n += int64(sn)
		}
	case CmdUpdateTable:
		entries := cmd.Table.MakeLogtailEntries()
		if err = binary.Write(w, binary.BigEndian, uint64(len(entries))); err != nil {
			return
		}
		n += 8
		for _, entry := range entries {
			buf := new(bytes.Buffer)
			if err := gob.NewEncoder(buf).Encode(entry); err != nil {
				return n, err
			}
			length := uint64(buf.Len())
			if err := binary.Write(w, binary.BigEndian, length); err != nil {
				panic(err)
			}
			n += 8
			sn, err := w.Write(buf.Bytes())
			if err != nil {
				panic(err)
			}
			n += int64(sn)
		}
	case CmdUpdateSegment:
		entries := cmd.Segment.MakeLogtailEntries()
		if err = binary.Write(w, binary.BigEndian, uint64(len(entries))); err != nil {
			return
		}
		n += 8
		for _, entry := range entries {
			buf := new(bytes.Buffer)
			if err := gob.NewEncoder(buf).Encode(entry); err != nil {
				return n, err
			}
			length := uint64(buf.Len())
			if err := binary.Write(w, binary.BigEndian, length); err != nil {
				panic(err)
			}
			n += 8
			sn, err := w.Write(buf.Bytes())
			if err != nil {
				panic(err)
			}
			n += int64(sn)
		}
	case CmdUpdateBlock:
		entries := cmd.Block.MakeLogtailEntries()
		if err = binary.Write(w, binary.BigEndian, uint64(len(entries))); err != nil {
			return
		}
		n += 8
		for _, entry := range entries {
			buf := new(bytes.Buffer)
			if err := gob.NewEncoder(buf).Encode(entry); err != nil {
				return n, err
			}
			length := uint64(buf.Len())
			if err := binary.Write(w, binary.BigEndian, length); err != nil {
				panic(err)
			}
			n += 8
			sn, err := w.Write(buf.Bytes())
			if err != nil {
				panic(err)
			}
			n += int64(sn)
		}
	}
	return
}
func (cmd *EntryCommand) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *EntryCommand) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &cmd.ID); err != nil {
		return
	}
	n += 4
	switch cmd.GetType() {
	case CmdUpdateDatabase:
		id := uint64(0)
		if err = binary.Read(r, binary.BigEndian, &id); err != nil {
			return
		}
		n += 8
		entriesLength := uint64(0)
		if err = binary.Read(r, binary.BigEndian, &entriesLength); err != nil {
			return
		}
		n += 8
		entries := make([]*api.Entry, entriesLength)
		for i := 0; i < int(entriesLength); i++ {
			length := uint64(0)
			if err = binary.Read(r, binary.BigEndian, &length); err != nil {
				return
			}
			n += 8
			buf := make([]byte, length)
			r.Read(buf)
			r2 := bytes.NewBuffer(buf)
			entry := &api.Entry{}
			gob.NewDecoder(r2).Decode(entry)
			entries[i] = entry
		}
		cmd.DB = &DBEntry{}
		cmd.DB.logentries = entries
	case CmdUpdateTable:
		id := uint64(0)
		if err = binary.Read(r, binary.BigEndian, &id); err != nil {
			return
		}
		n += 8
		entriesLength := uint64(0)
		if err = binary.Read(r, binary.BigEndian, &entriesLength); err != nil {
			return
		}
		n += 8
		entries := make([]*api.Entry, entriesLength)
		for i := 0; i < int(entriesLength); i++ {
			length := uint64(0)
			if err = binary.Read(r, binary.BigEndian, &length); err != nil {
				return
			}
			n += 8
			buf := make([]byte, length)
			r.Read(buf)
			r2 := bytes.NewBuffer(buf)
			entry := &api.Entry{}
			gob.NewDecoder(r2).Decode(entry)
			entries[i] = entry
		}
		cmd.Table = &TableEntry{}
		cmd.Table.logentries = entries
	case CmdUpdateSegment:
		id := uint64(0)
		if err = binary.Read(r, binary.BigEndian, &id); err != nil {
			return
		}
		n += 8
		entriesLength := uint64(0)
		if err = binary.Read(r, binary.BigEndian, &entriesLength); err != nil {
			return
		}
		n += 8
		entries := make([]*api.Entry, entriesLength)
		for i := 0; i < int(entriesLength); i++ {
			length := uint64(0)
			if err = binary.Read(r, binary.BigEndian, &length); err != nil {
				return
			}
			n += 8
			buf := make([]byte, length)
			r.Read(buf)
			r2 := bytes.NewBuffer(buf)
			entry := &api.Entry{}
			gob.NewDecoder(r2).Decode(entry)
			entries[i] = entry
		}
		cmd.Segment = &SegmentEntry{}
		cmd.Segment.logentries = entries
	case CmdUpdateBlock:
		id := uint64(0)
		if err = binary.Read(r, binary.BigEndian, &id); err != nil {
			return
		}
		n += 8
		entriesLength := uint64(0)
		if err = binary.Read(r, binary.BigEndian, &entriesLength); err != nil {
			return
		}
		n += 8
		entries := make([]*api.Entry, entriesLength)
		for i := 0; i < int(entriesLength); i++ {
			length := uint64(0)
			if err = binary.Read(r, binary.BigEndian, &length); err != nil {
				return
			}
			n += 8
			buf := make([]byte, length)
			r.Read(buf)
			r2 := bytes.NewBuffer(buf)
			entry := &api.Entry{}
			gob.NewDecoder(r2).Decode(entry)
			entries[i] = entry
		}
		cmd.Block = &BlockEntry{}
		cmd.Block.entries = entries
	}
	return
}

func (cmd *EntryCommand) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = cmd.ReadFrom(bbuf)
	return
}
