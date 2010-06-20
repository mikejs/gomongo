// Copyright 2009,2010 The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"bytes"
	"container/vector"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
)


type Connection struct {
	Addr *net.TCPAddr
	conn *net.TCPConn
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

/* Reconnects using the same address `Addr`. */
func (self *Connection) Reconnect() (*Connection, os.Error) {
	connection, err := ConnectByAddr(self.Addr)
	if err != nil {
		return nil, err
	}

	return connection, nil
}

/* Disconnects the conection from MongoDB. */
func (self *Connection) Disconnect() os.Error {
	if err := self.conn.Close(); err != nil {
		return err
	}
	return nil
}

func (self *Connection) GetDB(name string) *Database {
	return &Database{self, name}
}


// *** Database Response Message
// ***

// *** OP_REPLY

func (self *Connection) readReply() (*opReply, os.Error) {
	size_bits, _ := ioutil.ReadAll(io.LimitReader(self.conn, 4))
	size := pack.Uint32(size_bits)
	rest, _ := ioutil.ReadAll(io.LimitReader(self.conn, int64(size)-4))
	reply := parseReply(rest)
	return reply, nil
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

