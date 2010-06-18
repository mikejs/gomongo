// Copyright 2009,2010 The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

/* Mongo Wire Protocol

http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol
*/

package mongo

import (
	"bytes"
	"container/vector"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
)


// Request Opcodes
const (
	_OP_REPLY        = 1    // Reply to a client request. responseTo is set
	_OP_MSG          = 1000 // generic msg command followed by a string
	_OP_UPDATE       = 2001 // update document
	_OP_INSERT       = 2002 // insert new document
	_RESERVED        = 2003 // formerly used for _OP_GET_BY_OID
	_OP_QUERY        = 2004 // query a collection
	_OP_GET_MORE     = 2005 // Get more data from a query. See Cursors
	_OP_DELETE       = 2006 // Delete documents
	_OP_KILL_CURSORS = 2007 // Tell database client is done with a cursor
)

var last_req int32


// *** Standard Message Header
// ***

type msgHeader struct {
	messageLength int32 // total message size, including this
	requestID     int32 // identifier for this message
	responseTo    int32 // requestID from the original request (used in reponses from db)
	opCode        int32 // request type - see Request Opcodes
}

func header(h msgHeader) []byte {
	b := make([]byte, 16)

	binary.LittleEndian.PutUint32(b[0:4], uint32(h.messageLength))
	binary.LittleEndian.PutUint32(b[4:8], uint32(h.requestID))
	binary.LittleEndian.PutUint32(b[8:12], uint32(h.responseTo))
	binary.LittleEndian.PutUint32(b[12:16], uint32(h.opCode))

	return b
}


// *** Interface
// ***

type message interface {
	Bytes() []byte
	RequestID() int32
	OpCode() int32
}


// *** Client Request Messages
// ***

type deleteMsg struct {
	fullCollectionName string
	selector           BSON
	requestID          int32
}

func (self *deleteMsg) OpCode() int32    { return _OP_DELETE }
func (self *deleteMsg) RequestID() int32 { return self.requestID }

func (self *deleteMsg) Bytes() []byte {
	zero := make([]byte, 4)
	buf := bytes.NewBuffer(zero)
	buf.WriteString(self.fullCollectionName)
	buf.WriteByte(0)
	buf.Write(zero)
	buf.Write(self.selector.Bytes())
	return buf.Bytes()

}

// ***

type getMoreMsg struct {
	fullCollectionName string
	numberToReturn     int32
	cursorID           int64
	requestID          int32
}

func (self *getMoreMsg) OpCode() int32    { return _OP_GET_MORE }
func (self *getMoreMsg) RequestID() int32 { return self.requestID }

func (self *getMoreMsg) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 4))
	buf.WriteString(self.fullCollectionName)
	buf.WriteByte(0)

	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(self.numberToReturn))
	buf.Write(b)

	b = make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(self.cursorID))
	buf.Write(b)

	return buf.Bytes()
}

// ***

type insertMsg struct {
	fullCollectionName string
	doc                BSON
	requestID          int32
}

func (self *insertMsg) OpCode() int32    { return _OP_INSERT }
func (self *insertMsg) RequestID() int32 { return self.requestID }

func (self *insertMsg) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 4))
	buf.WriteString(self.fullCollectionName)
	buf.WriteByte(0)
	buf.Write(self.doc.Bytes())
	return buf.Bytes()
}

// ***

type killMsg struct {
	numberOfCursorIDs int32
	cursorIDs         []int64
	requestID         int32
}

func (self *killMsg) OpCode() int32    { return _OP_KILL_CURSORS }
func (self *killMsg) RequestID() int32 { return self.requestID }

func (self *killMsg) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 4))

	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(self.numberOfCursorIDs))
	buf.Write(b)

	b = make([]byte, 8)
	for _, id := range self.cursorIDs {
		binary.LittleEndian.PutUint64(b, uint64(id))
		buf.Write(b)
	}

	return buf.Bytes()
}

// ***

type queryMsg struct {
	opts               int32
	fullCollectionName string
	numberToSkip       int32
	numberToReturn     int32
	query              BSON
	requestID          int32
}

func (self *queryMsg) OpCode() int32    { return _OP_QUERY }
func (self *queryMsg) RequestID() int32 { return self.requestID }

func (self *queryMsg) Bytes() []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(self.opts))

	buf := bytes.NewBuffer(b)
	buf.WriteString(self.fullCollectionName)
	buf.WriteByte(0)

	binary.LittleEndian.PutUint32(b, uint32(self.numberToSkip))
	buf.Write(b)

	binary.LittleEndian.PutUint32(b, uint32(self.numberToReturn))
	buf.Write(b)

	buf.Write(self.query.Bytes())
	return buf.Bytes()
}

// ***

type updateMsg struct {
	fullCollectionName string
	flags              int32
	selector, document BSON
	requestID          int32
}

func (self *updateMsg) OpCode() int32    { return _OP_UPDATE }
func (self *updateMsg) RequestID() int32 { return self.requestID }

func (self *updateMsg) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 4))
	buf.WriteString(self.fullCollectionName)
	buf.WriteByte(0)

	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(self.flags))
	buf.Write(b)

	buf.Write(self.selector.Bytes())
	buf.Write(self.document.Bytes())

	return buf.Bytes()
}


// *** Database Response Message
// ***

type replyMsg struct {
	responseTo     int32
	responseFlag   int32
	cursorID       int64
	startingFrom   int32
	numberReturned int32
	docs           *vector.Vector
}

func (self *Connection) readReply() (*replyMsg, os.Error) {
	size_bits, _ := ioutil.ReadAll(io.LimitReader(self.conn, 4))
	size := binary.LittleEndian.Uint32(size_bits)
	rest, _ := ioutil.ReadAll(io.LimitReader(self.conn, int64(size)-4))
	reply := parseReply(rest)
	return reply, nil
}

func parseReply(b []byte) *replyMsg {
	r := new(replyMsg)
	r.responseTo = int32(binary.LittleEndian.Uint32(b[4:8]))
	r.responseFlag = int32(binary.LittleEndian.Uint32(b[12:16]))
	r.cursorID = int64(binary.LittleEndian.Uint64(b[16:24]))
	r.startingFrom = int32(binary.LittleEndian.Uint32(b[24:28]))
	r.numberReturned = int32(binary.LittleEndian.Uint32(b[28:32]))
	r.docs = new(vector.Vector)

	if r.numberReturned > 0 {
		buf := bytes.NewBuffer(b[36:len(b)])
		for i := 0; int32(i) < r.numberReturned; i++ {
			var bson BSON
			bb := new(_BSONBuilder)
			bb.ptr = &bson
			bb.Object()
			Parse(buf, bb)
			r.docs.Push(bson)
			ioutil.ReadAll(io.LimitReader(buf, 4))
		}
	}

	return r
}

