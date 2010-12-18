// Copyright 2009,2010, The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the 3-clause BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
)


// Default Socket Port
//const _PORT = 27017


type Connection struct {
	Addr *net.TCPAddr
	conn *net.TCPConn
}

func Connect(host string, port int) (*Connection, os.Error) {
	return ConnectAt(host, port)
}

/* Creates a new connection to a single MongoDB instance at host:port. */
func ConnectAt(host string, port int) (*Connection, os.Error) {
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

// === OP_REPLY

/* Gets the message of reply from database. */
func (self *Connection) readReply() (*opReply, os.Error) {
	size_bits, _ := ioutil.ReadAll(io.LimitReader(self.conn, 4))
	size := pack.Uint32(size_bits)
	rest, _ := ioutil.ReadAll(io.LimitReader(self.conn, int64(size)-4))
	reply := parseReply(rest)

	return reply, nil
}

