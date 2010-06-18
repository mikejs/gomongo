// Copyright 2009,2010 The 'gomongo' Authors.  All rights reserved.
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

func (self *Database) GetCollection(name string) *Collection {
	return &Collection{self, name}
}

func (self *Database) Drop() os.Error {
	cmd, err := Marshal(map[string]int{"dropDatabase": 1})
	if err != nil {
		return err
	}

	_, err = self.Command(cmd)
	return err
}

func (self *Database) Repair(preserveClonedFilesOnFailure, backupOriginalFiles bool) os.Error {
	cmd := &_Object{
		map[string]BSON{
			"repairDatabase":               &_Number{1, _Null{}},
			"preserveClonedFilesOnFailure": &_Boolean{preserveClonedFilesOnFailure, _Null{}},
			"backupOriginalFiles":          &_Boolean{backupOriginalFiles, _Null{}},
		},
		_Null{},
	}

	_, err := self.Command(cmd)
	return err
}

func (self *Database) Command(cmd BSON) (BSON, os.Error) {
	coll := self.GetCollection("$cmd")
	return coll.FindOne(cmd)
}

func (self *Database) GetCollectionNames() *vector.StringVector {
	return new(vector.StringVector)
}

