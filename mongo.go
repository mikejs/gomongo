package mongo

import (
	"os";
	"io";
	"net";
	"fmt";
	"rand";
	"bytes";
	"encoding/binary";
	"container/vector";
)

var last_req int32

const (
	OP_REPLY	= 1;
	OP_MSG		= 1000;
	OP_UPDATE	= 2001;
	OP_INSERT	= 2002;
	OP_GET_BY_OID	= 2003;
	OP_QUERY	= 2004;
	OP_GET_MORE	= 2005;
	OP_DELETE	= 2006;
	OP_KILL_CURSORS	= 2007;
)

type message interface {
	Bytes() []byte;
	RequestID() int32;
	OpCode() int32;
}

type Connection struct {
	host	string;
	port	int;
	conn	*net.TCPConn;
}

func Connect(host string, port int) (*Connection, os.Error) {
	laddr, _ := net.ResolveTCPAddr("localhost");
	addr, _ := net.ResolveTCPAddr(fmt.Sprintf("%s:%d", host, port));
	conn, err := net.DialTCP("tcp", laddr, addr);

	if err != nil {
		return nil, err
	}

	return &Connection{host, port, conn}, nil;
}

func header(length, reqID, respTo, opCode int32) []byte {
	b := make([]byte, 16);
	binary.LittleEndian.PutUint32(b[0:4], uint32(length));
	binary.LittleEndian.PutUint32(b[4:8], uint32(reqID));
	binary.LittleEndian.PutUint32(b[8:12], uint32(respTo));
	binary.LittleEndian.PutUint32(b[12:16], uint32(opCode));
	return b;
}

func (c *Connection) writeMessage(m message) os.Error {
	body := m.Bytes();
	hb := header(int32(len(body)+16), m.RequestID(), 0, m.OpCode());
	msg := bytes.Add(hb, body);

	_, err := c.conn.Write(msg);

	last_req = m.RequestID();
	return err;
}

func (c *Connection) readReply() (*replyMsg, os.Error) {
	size_bits, _ := io.ReadAll(io.LimitReader(c.conn, 4));
	size := binary.LittleEndian.Uint32(size_bits);
	rest, _ := io.ReadAll(io.LimitReader(c.conn, int64(size)-4));
	reply := ParseReply(rest);
	return reply, nil;
}

type Database struct {
	conn	*Connection;
	name	string;
}

func (c *Connection) GetDB(name string) *Database {
	return &Database{c, name}
}

type Collection struct {
	db	*Database;
	name	string;
}

func (db *Database) GetCollection(name string) *Collection {
	return &Collection{db, name}
}

type Cursor struct {
	collection	*Collection;
	id		int64;
	pos		int;
	docs		*vector.Vector;
}

func (c *Cursor) HasMore() bool {
	if c.pos < c.docs.Len() {
		return true
	}

	err := c.GetMore();
	if err != nil {
		return false
	}

	return c.pos < c.docs.Len();
}

func (c *Cursor) GetNext() (BSON, os.Error) {
	if c.HasMore() {
		return c.docs.At(c.pos).(BSON), nil
	}
	return Null, os.NewError("cursor failure");
}

func (c *Cursor) GetMore() os.Error {
	if c.id == 0 {
		return os.NewError("no cursorID")
	}

	gm := &getMoreMsg{c.collection.fullName(), 0, c.id, rand.Int31()};
	conn := c.collection.db.conn;
	err := conn.writeMessage(gm);
	if err != nil {
		return err
	}

	reply, err := conn.readReply();
	if err != nil {
		return err
	}

	c.pos = 0;
	c.docs = reply.docs;

	return nil;
}

func (c *Collection) fullName() string	{ return c.db.name + "." + c.name }

func (c *Collection) Insert(doc BSON) os.Error {
	im := &insertMsg{c.fullName(), doc, rand.Int31()};
	return c.db.conn.writeMessage(im);
}

func (coll *Collection) Query(query BSON) (*Cursor, os.Error) {
	req_id := rand.Int31();
	qm := &queryMsg{0, coll.fullName(), 0, 0, query, req_id};

	conn := coll.db.conn;
	err := conn.writeMessage(qm);
	if err != nil {
		return nil, err
	}

	reply, err := conn.readReply();
	if err != nil {
		return nil, err
	}

	return &Cursor{coll, reply.cursorID, 0, reply.docs}, nil;
}

type queryMsg struct {
	opts			int32;
	fullCollectionName	string;
	numberToSkip		int32;
	numberToReturn		int32;
	query			BSON;
	requestID		int32;
}

func (q *queryMsg) OpCode() int32	{ return OP_QUERY }
func (q *queryMsg) RequestID() int32	{ return q.requestID }
func (q *queryMsg) Bytes() []byte {
	b := make([]byte, 4);
	binary.LittleEndian.PutUint32(b, uint32(q.opts));

	buf := bytes.NewBuffer(b);
	buf.WriteString(q.fullCollectionName);
	buf.WriteByte(0);

	binary.LittleEndian.PutUint32(b, uint32(q.numberToSkip));
	buf.Write(b);

	binary.LittleEndian.PutUint32(b, uint32(q.numberToReturn));
	buf.Write(b);

	buf.Write(q.query.Bytes());
	return buf.Bytes();
}

type insertMsg struct {
	fullCollectionName	string;
	doc			BSON;
	requestID		int32;
}

func (i *insertMsg) OpCode() int32	{ return OP_INSERT }
func (i *insertMsg) RequestID() int32	{ return i.requestID }
func (i *insertMsg) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 4));
	buf.WriteString(i.fullCollectionName);
	buf.WriteByte(0);
	buf.Write(i.doc.Bytes());
	return buf.Bytes();
}

type getMoreMsg struct {
	fullCollectionName	string;
	numberToReturn		int32;
	cursorID		int64;
	requestID		int32;
}

func (g *getMoreMsg) OpCode() int32	{ return OP_GET_MORE }
func (g *getMoreMsg) RequestID() int32	{ return g.requestID }
func (g *getMoreMsg) Bytes() []byte {
	buf := bytes.NewBuffer(make([]byte, 4));
	buf.WriteString(g.fullCollectionName);
	buf.WriteByte(0);

	b := make([]byte, 4);
	binary.LittleEndian.PutUint32(b, uint32(g.numberToReturn));
	buf.Write(b);

	b = make([]byte, 8);
	binary.LittleEndian.PutUint64(b, uint64(g.cursorID));
	buf.Write(b);

	return buf.Bytes();
}

func (db *Database) GetCollectionNames() *vector.StringVector {
	return vector.NewStringVector(0)
}

type replyMsg struct {
	responseTo	int32;
	responseFlag	int32;
	cursorID	int64;
	startingFrom	int32;
	numberReturned	int32;
	docs		*vector.Vector;
}

func ParseReply(b []byte) *replyMsg {
	r := new(replyMsg);
	r.responseTo = int32(binary.LittleEndian.Uint32(b[4:8]));
	r.responseFlag = int32(binary.LittleEndian.Uint32(b[12:16]));
	r.cursorID = int64(binary.LittleEndian.Uint64(b[16:24]));
	r.startingFrom = int32(binary.LittleEndian.Uint32(b[24:28]));
	r.numberReturned = int32(binary.LittleEndian.Uint32(b[28:32]));
	r.docs = vector.New(0);

	buf := bytes.NewBuffer(b[36:len(b)]);
	for i := 0; int32(i) < r.numberReturned; i++ {
		var bson BSON;
		bb := new(_BSONBuilder);
		bb.ptr = &bson;
		bb.Object();
		Parse(buf, bb);
		r.docs.Push(bson);
		io.ReadAll(io.LimitReader(buf, 4));
	}

	return r;
}
