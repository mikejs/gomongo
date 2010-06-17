// Copyright 2009,2010, the 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"os"
	"io"
	"io/ioutil"
	"net"
	"fmt"
	"rand"
	"bytes"
	"encoding/binary"
	"container/vector"
)

var last_req int32

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

type message interface {
	Bytes() []byte
	RequestID() int32
	OpCode() int32
}

type Connection struct {
	Addr *net.TCPAddr
	conn *net.TCPConn
}

func header(length, reqID, respTo, opCode int32) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint32(b[0:4], uint32(length))
	binary.LittleEndian.PutUint32(b[4:8], uint32(reqID))
	binary.LittleEndian.PutUint32(b[8:12], uint32(respTo))
	binary.LittleEndian.PutUint32(b[12:16], uint32(opCode))
	return b
}

func Connect(host string, port int) (*Connection, os.Error) {
	addr, err := net.ResolveTCPAddr(fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}

	return ConnectByAddr(addr)
}

func ConnectByAddr(addr *net.TCPAddr) (*Connection, os.Error) {
	// Connects from local host (nil)
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}

	return &Connection{addr, conn}, nil
}

/* Closes the conection to the database. */
func (self *Connection) Close() os.Error {
	if err := self.conn.Close(); err != nil {
		return err
	}
	return nil
}

func (c *Connection) writeMessage(m message) os.Error {
	body := m.Bytes()
	hb := header(int32(len(body)+16), m.RequestID(), 0, m.OpCode())
	msg := bytes.Add(hb, body)

	_, err := c.conn.Write(msg)

	last_req = m.RequestID()
	return err
}

func (c *Connection) readReply() (*replyMsg, os.Error) {
	size_bits, _ := ioutil.ReadAll(io.LimitReader(c.conn, 4))
	size := binary.LittleEndian.Uint32(size_bits)
	rest, _ := ioutil.ReadAll(io.LimitReader(c.conn, int64(size)-4))
	reply := parseReply(rest)
	return reply, nil
}

type Database struct {
	conn *Connection
	name string
}

func (c *Connection) GetDB(name string) *Database {
	return &Database{c, name}
}

func (db *Database) Drop() os.Error {
	cmd, err := Marshal(map[string]int{"dropDatabase": 1})
	if err != nil {
		return err
	}

	_, err = db.Command(cmd)
	return err
}

func (db *Database) Repair(preserveClonedFilesOnFailure, backupOriginalFiles bool) os.Error {
	cmd := &_Object{map[string]BSON{"repairDatabase": &_Number{1, _Null{}}, "preserveClonedFilesOnFailure": &_Boolean{preserveClonedFilesOnFailure, _Null{}}, "backupOriginalFiles": &_Boolean{backupOriginalFiles, _Null{}}}, _Null{}}
	_, err := db.Command(cmd)
	return err
}

type Collection struct {
	db   *Database
	name string
}

func (db *Database) GetCollection(name string) *Collection {
	return &Collection{db, name}
}

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
	conn := c.collection.db.conn
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
	conn := c.collection.db.conn
	return conn.writeMessage(km)
}

func (c *Collection) fullName() string { return c.db.name + "." + c.name }

type indexDesc struct {
	Name string
	Ns   string
	Key  map[string]int
}

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
	return c.db.conn.writeMessage(im)
}

func (c *Collection) Remove(selector BSON) os.Error {
	dm := &deleteMsg{c.fullName(), selector, rand.Int31()}
	return c.db.conn.writeMessage(dm)
}

func (coll *Collection) Query(query BSON, skip, limit int) (*Cursor, os.Error) {
	req_id := rand.Int31()
	conn := coll.db.conn
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
	cmd := &_Object{map[string]BSON{"count": &_String{coll.name, _Null{}}, "query": query}, _Null{}}
	reply, err := coll.db.Command(cmd)
	if err != nil {
		return -1, err
	}

	return int64(reply.Get("n").Number()), nil
}

func (coll *Collection) update(um *updateMsg) os.Error {
	um.requestID = rand.Int31()
	conn := coll.db.conn
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

func (db *Database) Command(cmd BSON) (BSON, os.Error) {
	coll := db.GetCollection("$cmd")
	return coll.FindOne(cmd)
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

func (db *Database) GetCollectionNames() *vector.StringVector {
	return new(vector.StringVector)
}

type replyMsg struct {
	responseTo     int32
	responseFlag   int32
	cursorID       int64
	startingFrom   int32
	numberReturned int32
	docs           *vector.Vector
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

