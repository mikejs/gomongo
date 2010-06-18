// Copyright 2009,2010, the 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"os"
	"rand"
	"container/vector"
)


type Cursor struct {
	collection *Collection
	id         int64
	pos        int
	docs       *vector.Vector
}

func (c *Cursor) HasMore() bool {
	if c.pos < c.docs.Len() {
		return true
	}

	err := c.GetMore()
	if err != nil {
		return false
	}

	return c.pos < c.docs.Len()
}

func (c *Cursor) GetNext() (BSON, os.Error) {
	if c.HasMore() {
		doc := c.docs.At(c.pos).(BSON)
		c.pos = c.pos + 1
		return doc, nil
	}
	return nil, os.NewError("cursor failure")
}

func (c *Cursor) GetMore() os.Error {
	if c.id == 0 {
		return os.NewError("no cursorID")
	}

	gm := &getMoreMsg{c.collection.fullName(), 0, c.id, rand.Int31()}
	conn := c.collection.db.Conn
	err := conn.writeMessage(gm)
	if err != nil {
		return err
	}

	reply, err := conn.readReply()
	if err != nil {
		return err
	}

	c.pos = 0
	c.docs = reply.docs

	return nil
}

func (c *Cursor) Close() os.Error {
	if c.id == 0 {
		// not open on server
		return nil
	}

	req_id := rand.Int31()
	km := &killMsg{1, []int64{c.id}, req_id}
	conn := c.collection.db.Conn
	return conn.writeMessage(km)
}

