// Copyright 2009-2011 The gomongo Authors.  All rights reserved.
// Use of this source code is governed by the 3-clause BSD License
// that can be found in the LICENSE file.

/* Mongo Wire Protocol

http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol
*/

package mongo

import (
	"bytes"
	"container/vector"
	"io"
	"io/ioutil"
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

const (
	_ZERO        = int32(0)
	_HEADER_SIZE = 16 // 4 (fields) of int32 (4 bytes)
)


// === Standard Message Header
// ===

type msgHeader struct {
	messageLength int32 // total message size, including this
	requestID     int32 // identifier for this message
	responseTo    int32 // requestID from the original request (used in reponses from db)
	opCode        int32 // request type - see Request Opcodes
}

func header(h msgHeader) []byte {
	w := make([]byte, _HEADER_SIZE)

	pack.PutUint32(w[0:4], uint32(h.messageLength))
	pack.PutUint32(w[4:8], uint32(h.requestID))
	pack.PutUint32(w[8:12], uint32(h.responseTo))
	pack.PutUint32(w[12:16], uint32(h.opCode))

	return w
}


// === Messages interface
// ===

type message interface {
	Bytes() []byte
	OpCode() int32
}


// === Client Request Messages
// ===

// === OP_UPDATE

// flags
const (
	// If set, the database will insert the supplied object into the collection
	// if no matching document is found.
	f_UPSERT = 0

	// If set, the database will update all matching objects in the collection.
	// Otherwise only updates first matching doc.
	f_MULTI_UPDATE = 1

	// 2-31 - Reserved - Must be set to 0.
)

type opUpdate struct {
	//header           msgHeader // standard message header
	//ZERO             int32     // 0 - reserved for future use
	fullCollectionName string // "dbname.collectionname"
	flags              int32  // bit vector. See above
	selector           BSON   // the query to select the document
	update             BSON   // specification of the update to perform
}

func (self *opUpdate) OpCode() int32 { return _OP_UPDATE }

func (self *opUpdate) Bytes() []byte {
	w32 := make([]byte, _WORD32)
	buf := bytes.NewBuffer(w32) // ZERO

	buf.WriteString(self.fullCollectionName)
	buf.WriteByte(0)

	pack.PutUint32(w32, uint32(self.flags))
	buf.Write(w32)

	buf.Write(self.selector.Bytes())
	buf.Write(self.update.Bytes())

	return buf.Bytes()
}

// === OP_INSERT

type opInsert struct {
	//header           msgHeader // standard message header
	//ZERO             int32     // 0 - reserved for future use
	fullCollectionName string // "dbname.collectionname"
	documents          BSON   // one or more documents to insert into the collection
}

func (self *opInsert) OpCode() int32 { return _OP_INSERT }

func (self *opInsert) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, _WORD32)) // ZERO

	buf.WriteString(self.fullCollectionName)
	buf.WriteByte(0)

	buf.Write(self.documents.Bytes())

	return buf.Bytes()
}

// === OP_QUERY

// opts
const (
	o_NONE              = 0
	o_TAILABLE_CURSOR   = 2
	o_SLAVE_OK          = 4
	o_NO_CURSOR_TIMEOUT = 16
	//o_LOG_REPLAY        = 8 // drivers should not implement
)

// query
//Possible elements include $query, $orderby, $hint, $explain, and $snapshot

type opQuery struct {
	//header            msgHeader // standard message header
	opts               int32  // query options.  See above
	fullCollectionName string // "dbname.collectionname"
	numberToSkip       int32  // number of documents to skip
	numberToReturn     int32  // number of documents to return in the first OP_REPLY batch
	query              BSON   // query object.  See above
	//returnFieldSelector BSON   // Optional. Selector indicating the fields to return.
}

func (self *opQuery) OpCode() int32 { return _OP_QUERY }

func (self *opQuery) Bytes() []byte {
	var buf bytes.Buffer
	w32 := make([]byte, _WORD32)

	pack.PutUint32(w32, uint32(self.opts))
	buf.Write(w32)

	buf.WriteString(self.fullCollectionName)
	buf.WriteByte(0)

	pack.PutUint32(w32, uint32(self.numberToSkip))
	buf.Write(w32)

	pack.PutUint32(w32, uint32(self.numberToReturn))
	buf.Write(w32)

	buf.Write(self.query.Bytes())

	return buf.Bytes()
}

// === OP_GET_MORE

type opGetMore struct {
	//header           msgHeader // standard message header
	//ZERO             int32     // 0 - reserved for future use
	fullCollectionName string // "dbname.collectionname"
	numberToReturn     int32  // number of documents to return
	cursorID           int64  // cursorID from the OP_REPLY
}

func (self *opGetMore) OpCode() int32 { return _OP_GET_MORE }

func (self *opGetMore) Bytes() []byte {
	w32 := make([]byte, _WORD32)
	w64 := make([]byte, _WORD64)
	buf := bytes.NewBuffer(w32) // ZERO

	buf.WriteString(self.fullCollectionName)
	buf.WriteByte(0)

	pack.PutUint32(w32, uint32(self.numberToReturn))
	buf.Write(w32)

	pack.PutUint64(w64, uint64(self.cursorID))
	buf.Write(w64)

	return buf.Bytes()
}

// === OP_DELETE

// flags
const (
	// If set, the database will remove only the first matching document in the
	// collection. Otherwise all matching documents will be removed.
	f_SINGLE_REMOVE = 0

	// 1-31 - Reserved - Must be set to 0.
)

type opDelete struct {
	//header           msgHeader // standard message header
	//ZERO             int32     // 0 - reserved for future use
	fullCollectionName string // "dbname.collectionname"
	flags              int32  // bit vector - see above
	selector           BSON   // query object.  See above
}

func (self *opDelete) OpCode() int32 { return _OP_DELETE }

func (self *opDelete) Bytes() []byte {
	w32 := make([]byte, _WORD32)
	buf := bytes.NewBuffer(w32) // ZERO

	buf.WriteString(self.fullCollectionName)
	buf.WriteByte(0)

	pack.PutUint32(w32, uint32(self.flags))
	buf.Write(w32)

	buf.Write(self.selector.Bytes())

	return buf.Bytes()
}

// === OP_KILL_CURSORS

type opKillCursors struct {
	//header          msgHeader // standard message header
	//ZERO            int32     // 0 - reserved for future use
	numberOfCursorIDs int32   // number of cursorIDs in message
	cursorIDs         []int64 // sequence of cursorIDs to close
}

func (self *opKillCursors) OpCode() int32 { return _OP_KILL_CURSORS }

func (self *opKillCursors) Bytes() []byte {
	w32 := make([]byte, _WORD32)
	w64 := make([]byte, _WORD64)
	buf := bytes.NewBuffer(w32) // ZERO

	pack.PutUint32(w32, uint32(self.numberOfCursorIDs))
	buf.Write(w32)

	for _, id := range self.cursorIDs {
		pack.PutUint64(w64, uint64(id))
		buf.Write(w64)
	}

	return buf.Bytes()
}


// === Database Response Message
// ===

// === OP_REPLY

type opReply struct {
	//header         msgHeader      // standard message header
	responseTo     int32          // !!! Added !!!
	responseFlag   int32          // normally zero, non-zero on query failure
	cursorID       int64          // cursor id if client needs to do get more's
	startingFrom   int32          // where in the cursor this reply is starting
	numberReturned int32          // number of documents in the reply
	documents      *vector.Vector // documents
}

func parseReply(b []byte) *opReply {
	r := new(opReply)

	r.responseTo = int32(pack.Uint32(b[4:8]))
	r.responseFlag = int32(pack.Uint32(b[12:16]))
	r.cursorID = int64(pack.Uint64(b[16:24]))
	r.startingFrom = int32(pack.Uint32(b[24:28]))
	r.numberReturned = int32(pack.Uint32(b[28:32]))
	r.documents = new(vector.Vector)

	if r.numberReturned > 0 {
		buf := bytes.NewBuffer(b[36:len(b)])

		for i := 0; int32(i) < r.numberReturned; i++ {
			var bson BSON
			bb := new(_BSONBuilder)

			bb.ptr = &bson
			bb.Object()
			Parse(buf, bb)
			r.documents.Push(bson)
			ioutil.ReadAll(io.LimitReader(buf, 4))
		}
	}

	return r
}
