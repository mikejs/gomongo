package mongo

import (
	"os";
	"net";
	"fmt";
	"bytes";
	"encoding/binary";
	"container/vector";
)

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

	hb := header(int32(buf.Len()+16), q.requestID, 0, OP_QUERY);

	return bytes.Add(hb, buf.Bytes());
}

type message interface {
	Bytes() []byte;
	RequestID() int32;
	OpCode() int32;
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

	hb := header(int32(buf.Len()+16), i.requestID, 0, OP_INSERT);

	return bytes.Add(hb, buf.Bytes());
}

func Query(collection string, query BSON) BSON {
	laddr, _ := net.ResolveTCPAddr("localhost");
	addr, _ := net.ResolveTCPAddr("localhost:27017");
	conn, err := net.DialTCP("tcp", laddr, addr);

	if err != nil {
		fmt.Printf("Failed connecting to database: %v\n", err);
		os.Exit(1);
	}

	qm := queryMsg{0, "test.coll", 0, 0, query, 1};
	b := qm.Bytes();

	n, err := conn.Write(b);

	if err != nil || n != len(b) {
		fmt.Printf("Error writing query: %v\n", err);
		os.Exit(1);
	}

	return query;
}

func (c *Connection) Insert(collection string, doc BSON) {
	im := insertMsg{collection, doc, 529};

	_, err := c.conn.Write(im.Bytes());

	if err != nil {
		fmt.Printf("Error inserting: %v\n", err);
		os.Exit(1);
	}
}

type Database struct {
	conn	*Connection;
	name	string;
}

func (db *Database) GetCollectionNames() *vector.StringVector {
	return vector.NewStringVector(0)
}
