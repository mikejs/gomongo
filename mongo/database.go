// Copyright 2009,2010, the 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"container/vector"
	"os"
)


type Database struct {
	Conn *Connection
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
	cmd := &_Object{
		map[string]BSON{
			"repairDatabase": &_Number{
				1, _Null{},
			},
			"preserveClonedFilesOnFailure": &_Boolean{
				preserveClonedFilesOnFailure, _Null{},
			},
			"backupOriginalFiles": &_Boolean{
				backupOriginalFiles, _Null{},
			},
		},
		_Null{},
	}

	_, err := db.Command(cmd)
	return err
}

func (db *Database) Command(cmd BSON) (BSON, os.Error) {
	coll := db.GetCollection("$cmd")
	return coll.FindOne(cmd)
}

func (db *Database) GetCollectionNames() *vector.StringVector {
	return new(vector.StringVector)
}

