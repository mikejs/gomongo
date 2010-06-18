// Copyright 2009,2010, the 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"bytes"
	"container/vector"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
)


const (
	_OP_REPLY        = 1
	_OP_MSG          = 1000
	_OP_UPDATE       = 2001
	_OP_INSERT       = 2002
	_OP_GET_BY_OID   = 2003
	_OP_QUERY        = 2004
	_OP_GET_MORE     = 2005
	_OP_DELETE       = 2006
	_OP_KILL_CURSORS = 2007
)

var last_req int32


type message interface {
	Bytes() []byte
	RequestID() int32
	OpCode() int32
}

func (c *Connection) writeMessage(m message) os.Error {
	body := m.Bytes()
	hb := header(int32(len(body)+16), m.RequestID(), 0, m.OpCode())
	msg := bytes.Add(hb, body)

	_, err := c.conn.Write(msg)

	last_req = m.RequestID()
	return err
}

func header(length, reqID, respTo, opCode int32) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint32(b[0:4], uint32(length))
	binary.LittleEndian.PutUint32(b[4:8], uint32(reqID))
	binary.LittleEndian.PutUint32(b[8:12], uint32(respTo))
	binary.LittleEndian.PutUint32(b[12:16], uint32(opCode))
	return b
}


type replyMsg struct {
	responseTo     int32
	responseFlag   int32
	cursorID       int64
	startingFrom   int32
	numberReturned int32
	docs           *vector.Vector
}

func (c *Connection) readReply() (*replyMsg, os.Error) {
	size_bits, _ := ioutil.ReadAll(io.LimitReader(c.conn, 4))
	size := binary.LittleEndian.Uint32(size_bits)
	rest, _ := ioutil.ReadAll(io.LimitReader(c.conn, int64(size)-4))
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


type deleteMsg struct {
	fullCollectionName string
	selector           BSON
	requestID          int32
}

func (d *deleteMsg) OpCode() int32    { return _OP_DELETE }
func (d *deleteMsg) RequestID() int32 { return d.requestID }
func (d *deleteMsg) Bytes() []byte {
	zero := make([]byte, 4)
	buf := bytes.NewBuffer(zero)
	buf.WriteString(d.fullCollectionName)
	buf.WriteByte(0)
	buf.Write(zero)
	buf.Write(d.selector.Bytes())
	return buf.Bytes()

}


type getMoreMsg struct {
	fullCollectionName string
	numberToReturn     int32
	cursorID           int64
	requestID          int32
}

func (g *getMoreMsg) OpCode() int32    { return _OP_GET_MORE }
func (g *getMoreMsg) RequestID() int32 { return g.requestID }
func (g *getMoreMsg) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 4))
	buf.WriteString(g.fullCollectionName)
	buf.WriteByte(0)

	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(g.numberToReturn))
	buf.Write(b)

	b = make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(g.cursorID))
	buf.Write(b)

	return buf.Bytes()
}


type insertMsg struct {
	fullCollectionName string
	doc                BSON
	requestID          int32
}

func (i *insertMsg) OpCode() int32    { return _OP_INSERT }
func (i *insertMsg) RequestID() int32 { return i.requestID }
func (i *insertMsg) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 4))
	buf.WriteString(i.fullCollectionName)
	buf.WriteByte(0)
	buf.Write(i.doc.Bytes())
	return buf.Bytes()
}


type killMsg struct {
	numberOfCursorIDs int32
	cursorIDs         []int64
	requestID         int32
}

func (k *killMsg) OpCode() int32    { return _OP_KILL_CURSORS }
func (k *killMsg) RequestID() int32 { return k.requestID }
func (k *killMsg) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 4))

	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(k.numberOfCursorIDs))
	buf.Write(b)

	b = make([]byte, 8)
	for _, id := range k.cursorIDs {
		binary.LittleEndian.PutUint64(b, uint64(id))
		buf.Write(b)
	}

	return buf.Bytes()
}


type queryMsg struct {
	opts               int32
	fullCollectionName string
	numberToSkip       int32
	numberToReturn     int32
	query              BSON
	requestID          int32
}

func (q *queryMsg) OpCode() int32    { return _OP_QUERY }
func (q *queryMsg) RequestID() int32 { return q.requestID }
func (q *queryMsg) Bytes() []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(q.opts))

	buf := bytes.NewBuffer(b)
	buf.WriteString(q.fullCollectionName)
	buf.WriteByte(0)

	binary.LittleEndian.PutUint32(b, uint32(q.numberToSkip))
	buf.Write(b)

	binary.LittleEndian.PutUint32(b, uint32(q.numberToReturn))
	buf.Write(b)

	buf.Write(q.query.Bytes())
	return buf.Bytes()
}


type updateMsg struct {
	fullCollectionName string
	flags              int32
	selector, document BSON
	requestID          int32
}

func (u *updateMsg) OpCode() int32    { return _OP_UPDATE }
func (u *updateMsg) RequestID() int32 { return u.requestID }
func (u *updateMsg) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 4))
	buf.WriteString(u.fullCollectionName)
	buf.WriteByte(0)

	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(u.flags))
	buf.Write(b)

	buf.Write(u.selector.Bytes())
	buf.Write(u.document.Bytes())

	return buf.Bytes()
}

