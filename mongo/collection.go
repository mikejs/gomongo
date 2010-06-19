// Copyright 2009,2010 The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"os"
	"rand"
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
	im := &opInsert{self.fullName(), doc, rand.Int31()}
	return self.db.Conn.writeMessage(im)
}

func (self *Collection) Remove(selector BSON) os.Error {
	dm := &opDelete{self.fullName(), selector, rand.Int31()}
	return self.db.Conn.writeMessage(dm)
}

func (self *Collection) Query(query BSON, skip, limit int) (*Cursor, os.Error) {
	req_id := rand.Int31()
	conn := self.db.Conn
	qm := &opQuery{0, self.fullName(), int32(skip), int32(limit), query, req_id}

	err := conn.writeMessage(qm)
	if err != nil {
		return nil, err
	}

	reply, err := conn.readReply()
	if err != nil {
		return nil, err
	}
	if reply.responseTo != req_id {
		return nil, os.NewError("wrong responseTo code")
	}

	return &Cursor{self, reply.cursorID, 0, reply.docs}, nil
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
	um.requestID = rand.Int31()
	conn := self.db.Conn

	return conn.writeMessage(um)
}

func (self *Collection) Update(selector, document BSON) os.Error {
	return self.update(&opUpdate{self.fullName(), 0, selector, document, 0})
}

func (self *Collection) Upsert(selector, document BSON) os.Error {
	return self.update(&opUpdate{self.fullName(), 1, selector, document, 0})
}

func (self *Collection) UpdateAll(selector, document BSON) os.Error {
	return self.update(&opUpdate{self.fullName(), 2, selector, document, 0})
}

func (self *Collection) UpsertAll(selector, document BSON) os.Error {
	return self.update(&opUpdate{self.fullName(), 3, selector, document, 0})
}

