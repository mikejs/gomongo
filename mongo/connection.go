// Copyright 2009,2010, the 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"bytes"
	"fmt"
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

func (c *Connection) writeMessage(m message) os.Error {
	body := m.Bytes()
	hb := header(msgHeader{int32(len(body)+16), m.RequestID(), 0, m.OpCode()})
	msg := bytes.Add(hb, body)

	_, err := c.conn.Write(msg)

	last_req = m.RequestID()
	return err
}

