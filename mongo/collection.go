// Copyright 2009,2010 The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"bytes"
	"os"
)


type Collection struct {
	db   *Database
	name string
}

func (self *Collection) Drop() os.Error {
	cmdm := map[string]string{"drop": self.fullName()}
	cmd, err := Marshal(cmdm)
	if err != nil {
		return err
	}

	_, err = self.db.Command(cmd)
	return err
}

func (self *Collection) fullName() string {
	return self.db.name + "." + self.name
}

// *** Client Request Messages
// ***

func (self *Connection) writeOp(m message) os.Error {
	body := m.Bytes()
	h := header(msgHeader{int32(len(body) + _HEADER_SIZE), getRequestID(), 0, m.OpCode()})

	msg := bytes.Add(h, body)
	_, err := self.conn.Write(msg)

	return err
}

func (self *Connection) writeOpQuery(m message, reqID int32) os.Error {
	body := m.Bytes()
	h := header(msgHeader{int32(len(body) + _HEADER_SIZE), reqID, 0, m.OpCode()})

	msg := bytes.Add(h, body)
	_, err := self.conn.Write(msg)

	return err
}

// *** OP_UPDATE

var fUpsert, fUpdateAll, fUpsertAll int32 // flags

// Calculates values of flags
func init() {
	//fUpdate := _ZERO
	setBit32(&fUpsert, f_UPSERT)
	setBit32(&fUpdateAll, f_MULTI_UPDATE)
	setBit32(&fUpsertAll, f_UPSERT, f_MULTI_UPDATE)
}

func (self *Collection) Update(selector, document BSON) os.Error {
	return self.update(&opUpdate{self.fullName(), _ZERO, selector, document})
}

func (self *Collection) Upsert(selector, document BSON) os.Error {
	return self.update(&opUpdate{self.fullName(), fUpsert, selector, document})
}

func (self *Collection) UpdateAll(selector, document BSON) os.Error {
	return self.update(&opUpdate{self.fullName(), fUpdateAll, selector, document})
}

func (self *Collection) UpsertAll(selector, document BSON) os.Error {
	return self.update(&opUpdate{self.fullName(), fUpsertAll, selector, document})
}

func (self *Collection) update(msg *opUpdate) os.Error {
	return self.db.Conn.writeOp(msg)
}

// *** OP_INSERT

func (self *Collection) Insert(doc BSON) os.Error {
	msg := &opInsert{self.fullName(), doc}
	return self.db.Conn.writeOp(msg)
}

// *** OP_QUERY

func (self *Collection) Query(query BSON, skip, limit int32) (*Cursor, os.Error) {
	conn := self.db.Conn
	reqID := getRequestID()
	msg := &opQuery{o_NONE, self.fullName(), skip, limit, query}

	if err := conn.writeOpQuery(msg, reqID); err != nil {
		return nil, err
	}

	reply, err := conn.readReply()
	if err != nil {
		return nil, err
	}
	if reply.responseTo != reqID {
		return nil, os.NewError("wrong responseTo code")
	}

	return &Cursor{self, reply.cursorID, 0, reply.documents}, nil
}

func (self *Collection) FindAll(query BSON) (*Cursor, os.Error) {
	return self.Query(query, 0, 0)
}

func (self *Collection) FindOne(query BSON) (BSON, os.Error) {
	cursor, err := self.Query(query, 0, 1)
	if err != nil {
		return nil, err
	}
	return cursor.GetNext()
}

func (self *Collection) Count(query BSON) (int64, os.Error) {
	cmd := &_Object{
		map[string]BSON{
			"count": &_String{self.name, _Null{}},
			"query": query,
		},
		_Null{},
	}

	reply, err := self.db.Command(cmd)
	if err != nil {
		return -1, err
	}

	return int64(reply.Get("n").Number()), nil
}

// *** OP_DELETE

var fSingleRemove int32 // flags

// Calculates values of flags
func init() {
	setBit32(&fSingleRemove, f_SINGLE_REMOVE)
}

func (self *Collection) Remove(selector BSON) os.Error {
	return self.remove(&opDelete{self.fullName(), _ZERO, selector})
}

func (self *Collection) RemoveFirst(selector BSON) os.Error {
	return self.remove(&opDelete{self.fullName(), fSingleRemove, selector})
}

func (self *Collection) remove(msg *opDelete) os.Error {
	return self.db.Conn.writeOp(msg)
}


// *** Indexes
// ***

type indexDesc struct {
	Name string
	Ns   string
	Key  map[string]int
}

func (self *Collection) EnsureIndex(name string, index map[string]int) os.Error {
	coll := self.db.GetCollection("system.indexes")
	id := &indexDesc{name, self.fullName(), index}

	desc, err := Marshal(id)
	if err != nil {
		return err
	}

	return coll.Insert(desc)
}

/* Deletes all indexes on the specified collection. */
func (self *Collection) DropIndexes() os.Error {
	return self.DropIndex("*")
}

/* Deletes a single index. */
func (self *Collection) DropIndex(name string) os.Error {
	cmdm := map[string]string{
		"deleteIndexes": self.fullName(),
		"index": name,
	}

	cmd, err := Marshal(cmdm)
	if err != nil {
		return err
	}

	_, err = self.db.Command(cmd)
	return err
}

