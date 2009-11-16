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

func (c *Collection) fullName() string	{ return c.db.name + "." + c.name }

func (c *Collection) Insert(doc BSON) {
	im := &insertMsg{c.fullName(), doc, rand.Int31()};

	err := c.db.conn.writeMessage(im);

	if err != nil {
		fmt.Printf("Error inserting: %v\n", err);
		os.Exit(1);
	}
}

func (coll *Collection) Query(query BSON) (*vector.Vector, os.Error) {
	req_id := rand.Int31();
	qm := &queryMsg{0, coll.fullName(), 0, 0, query, req_id};

	conn := coll.db.conn;
	err := conn.writeMessage(qm);
	if err != nil {
		return nil, err
	}

	size_bits, _ := io.ReadAll(io.LimitReader(conn.conn, 4));
	size := binary.LittleEndian.Uint32(size_bits);

	rest, _ := io.ReadAll(io.LimitReader(conn.conn, int64(size)-4));
	resp_id := int32(binary.LittleEndian.Uint32(rest[4:8]));
	if resp_id != req_id {
		return nil, os.NewError("fail")
	}

	reply := ParseReply(rest[12:len(rest)]);
	if reply.numberReturned == 0 {
		return nil, os.NewError("0 returned")
	}

	return reply.docs, nil;
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

func (db *Database) GetCollectionNames() *vector.StringVector {
	return vector.NewStringVector(0)
}

type replyMsg struct {
	responseFlag	int32;
	cursorID	int64;
	startingFrom	int32;
	numberReturned	int32;
	docs		*vector.Vector;
}

func ParseReply(b []byte) *replyMsg {
	r := new(replyMsg);
	r.responseFlag = int32(binary.LittleEndian.Uint32(b[0:4]));
	r.cursorID = int64(binary.LittleEndian.Uint64(b[4:12]));
	r.startingFrom = int32(binary.LittleEndian.Uint32(b[12:16]));
	r.numberReturned = int32(binary.LittleEndian.Uint32(b[16:20]));
	r.docs = vector.New(0);

	buf := bytes.NewBuffer(b[24:len(b)]);
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
