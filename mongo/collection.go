// Copyright 2009,2010, the 'gomongo' Authors.  All rights reserved.
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

func (db *Database) GetCollection(name string) *Collection {
	return &Collection{db, name}
}

func (c *Collection) fullName() string { return c.db.name + "." + c.name }

func (c *Collection) EnsureIndex(name string, index map[string]int) os.Error {
	coll := c.db.GetCollection("system.indexes")
	id := &indexDesc{name, c.fullName(), index}
	desc, err := Marshal(id)
	if err != nil {
		return err
	}
	return coll.Insert(desc)
}

func (c *Collection) DropIndexes() os.Error { return c.DropIndex("*") }

func (c *Collection) DropIndex(name string) os.Error {
	cmdm := map[string]string{"deleteIndexes": c.fullName(), "index": name}
	cmd, err := Marshal(cmdm)
	if err != nil {
		return err
	}

	_, err = c.db.Command(cmd)
	return err
}

func (c *Collection) Drop() os.Error {
	cmdm := map[string]string{"drop": c.fullName()}
	cmd, err := Marshal(cmdm)
	if err != nil {
		return err
	}

	_, err = c.db.Command(cmd)
	return err
}

func (c *Collection) Insert(doc BSON) os.Error {
	im := &insertMsg{c.fullName(), doc, rand.Int31()}
	return c.db.Conn.writeMessage(im)
}

func (c *Collection) Remove(selector BSON) os.Error {
	dm := &deleteMsg{c.fullName(), selector, rand.Int31()}
	return c.db.Conn.writeMessage(dm)
}

func (coll *Collection) Query(query BSON, skip, limit int) (*Cursor, os.Error) {
	req_id := rand.Int31()
	conn := coll.db.Conn
	qm := &queryMsg{0, coll.fullName(), int32(skip), int32(limit), query, req_id}

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

	return &Cursor{coll, reply.cursorID, 0, reply.docs}, nil
}

func (coll *Collection) FindAll(query BSON) (*Cursor, os.Error) {
	return coll.Query(query, 0, 0)
}

func (coll *Collection) FindOne(query BSON) (BSON, os.Error) {
	cursor, err := coll.Query(query, 0, 1)
	if err != nil {
		return nil, err
	}
	return cursor.GetNext()
}

func (coll *Collection) Count(query BSON) (int64, os.Error) {
	cmd := &_Object{
		map[string]BSON{
			"count": &_String{coll.name, _Null{}},
			"query": query,
		},
		_Null{},
	}

	reply, err := coll.db.Command(cmd)
	if err != nil {
		return -1, err
	}

	return int64(reply.Get("n").Number()), nil
}

func (coll *Collection) update(um *updateMsg) os.Error {
	um.requestID = rand.Int31()
	conn := coll.db.Conn
	return conn.writeMessage(um)
}

func (coll *Collection) Update(selector, document BSON) os.Error {
	return coll.update(&updateMsg{coll.fullName(), 0, selector, document, 0})
}

func (coll *Collection) Upsert(selector, document BSON) os.Error {
	return coll.update(&updateMsg{coll.fullName(), 1, selector, document, 0})
}

func (coll *Collection) UpdateAll(selector, document BSON) os.Error {
	return coll.update(&updateMsg{coll.fullName(), 2, selector, document, 0})
}

func (coll *Collection) UpsertAll(selector, document BSON) os.Error {
	return coll.update(&updateMsg{coll.fullName(), 3, selector, document, 0})
}

