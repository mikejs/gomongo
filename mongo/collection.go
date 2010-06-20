// Copyright 2009,2010 The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"bytes"
	"os"
)


type indexDesc struct {
	Name string
	Ns   string
	Key  map[string]int
}


type Collection struct {
	db   *Database
	name string
}

func (self *Collection) fullName() string { return self.db.name + "." + self.name }

func (self *Collection) EnsureIndex(name string, index map[string]int) os.Error {
	coll := self.db.GetCollection("system.indexes")
	id := &indexDesc{name, self.fullName(), index}
	desc, err := Marshal(id)
	if err != nil {
		return err
	}
	return coll.Insert(desc)
}

func (self *Collection) DropIndexes() os.Error { return self.DropIndex("*") }

func (self *Collection) DropIndex(name string) os.Error {
	cmdm := map[string]string{"deleteIndexes": self.fullName(), "index": name}
	cmd, err := Marshal(cmdm)
	if err != nil {
		return err
	}

	_, err = self.db.Command(cmd)
	return err
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

func (self *Collection) Insert(doc BSON) os.Error {
	im := &opInsert{self.fullName(), doc}
	return self.db.Conn.writeOp(im)
}

func (self *Collection) Remove(selector BSON) os.Error {
	dm := &opDelete{self.fullName(), selector}
	return self.db.Conn.writeOp(dm)
}

func (self *Collection) Query(query BSON, skip, limit int) (*Cursor, os.Error) {
	reqID := getRequestID()
	conn := self.db.Conn
	qm := &opQuery{0, self.fullName(), int32(skip), int32(limit), query}

	err := conn.writeOpQuery(qm, reqID)
	if err != nil {
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

func (self *Collection) update(um *opUpdate) os.Error {
	conn := self.db.Conn

	return conn.writeOp(um)
}

func (self *Collection) Update(selector, document BSON) os.Error {
	return self.update(&opUpdate{self.fullName(), 0, selector, document})
}

func (self *Collection) Upsert(selector, document BSON) os.Error {
	return self.update(&opUpdate{self.fullName(), 1, selector, document})
}

func (self *Collection) UpdateAll(selector, document BSON) os.Error {
	return self.update(&opUpdate{self.fullName(), 2, selector, document})
}

func (self *Collection) UpsertAll(selector, document BSON) os.Error {
	return self.update(&opUpdate{self.fullName(), 3, selector, document})
}


// *** Utility
// ***

func (self *Connection) writeOp(m message) os.Error {
	body := m.Bytes()
	h := header(msgHeader{int32(len(body) + 16), getRequestID(), 0, m.OpCode()})

	msg := bytes.Add(h, body)
	_, err := self.conn.Write(msg)

	return err
}

func (self *Connection) writeOpQuery(m message, reqID int32) os.Error {
	body := m.Bytes()
	h := header(msgHeader{int32(len(body) + 16), reqID, 0, m.OpCode()})

	msg := bytes.Add(h, body)
	_, err := self.conn.Write(msg)

	return err
}

