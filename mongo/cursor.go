// Copyright 2009,2010 The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"container/vector"
	"os"
)


type Cursor struct {
	collection *Collection
	id         int64
	pos        int
	docs       *vector.Vector
}

func (self *Cursor) GetNext() (BSON, os.Error) {
	if self.HasMore() {
		doc := self.docs.At(self.pos).(BSON)
		self.pos = self.pos + 1
		return doc, nil
	}
	return nil, os.NewError("cursor failure")
}

func (self *Cursor) HasMore() bool {
	if self.pos < self.docs.Len() {
		return true
	}

	err := self.GetMore()
	if err != nil {
		return false
	}

	return self.pos < self.docs.Len()
}


// *** Client Request Messages
// ***

// *** OP_GET_MORE

func (self *Cursor) GetMore() os.Error {
	if self.id == 0 {
		return os.NewError("no cursorID")
	}

	conn := self.collection.db.Conn
	msg := &opGetMore{self.collection.fullName(), 0, self.id}

	if err := conn.writeOp(msg); err != nil {
		return err
	}

	reply, err := conn.readReply()
	if err != nil {
		return err
	}

	self.pos = 0
	self.docs = reply.documents

	return nil
}

// *** OP_KILL_CURSORS

func (self *Cursor) Close() os.Error {
	if self.id == 0 {
		// not open on server
		return nil
	}

	msg := &opKillCursors{1, []int64{self.id}}
	return self.collection.db.Conn.writeOp(msg)
}

