// Copyright 2009 Michael Stephens.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mongo_test

import (
	"mongo";
	"testing";
	"fmt";
)

type KeyStruct struct {
	First int32;
}

type IndexCmd struct {
	Name, Ns	string;
	Key		*KeyStruct;
}

func TestStuff(t *testing.T) {
	obj, err := mongo.BytesToBSON([]byte{92, 0, 0, 0, 1, 115, 101, 99, 111, 110, 100, 0, 0, 0, 0, 0, 0, 0, 0, 64, 3, 102, 105, 102, 116, 104, 0, 23, 0, 0, 0, 2, 118, 0, 2, 0, 0, 0, 101, 0, 2, 102, 0, 2, 0, 0, 0, 105, 0, 0, 3, 102, 111, 117, 114, 116, 104, 0, 5, 0, 0, 0, 0, 2, 116, 104, 105, 114, 100, 0, 6, 0, 0, 0, 116, 104, 114, 101, 101, 0, 16, 102, 105, 114, 115, 116, 0, 1, 0, 0, 0, 0});
	assertTrue(err == nil, "failed parsing BSON obj", t);

	conn, err := mongo.Connect("127.0.0.1", 27017);
	assertTrue(err == nil && conn != nil, fmt.Sprintf("failed connecting to mongo: %v", err), t);

	db := conn.GetDB("go_driver_tests");
	coll := db.GetCollection("coll");
	coll.Drop();

	coll.Insert(obj);

	q, _ := mongo.Marshal(map[string]string{});
	ret, err := coll.FindAll(q);
	assertTrue(err == nil && ret != nil, "query succeeded", t);

	doc, _ := ret.GetNext();
	assertTrue(doc.Kind() == mongo.ObjectKind, "query returned document", t);
	assertTrue(doc.Get("first").Int() == 1, "returned doc has proper 'first' element", t);
	assertTrue(doc.Get("second").Number() == 2, "returned doc has proper 'second' element", t);
	assertTrue(doc.Get("third").String() == "three", "returned doc has proper 'third' element", t);
	assertTrue(doc.Get("fourth").Kind() == mongo.ObjectKind, "returned doc has proper 'fourth' element", t);
	assertTrue(doc.Get("fifth").Get("f").String() == "i" && doc.Get("fifth").Get("v").String() == "e", "returned doc has proper 'fifth' element", t);

	count, err := coll.Count(q);
	assertTrue(count == 1, "count", t);

	newDoc, _ := mongo.Marshal(map[string]string{"first": "one", "second": "two", "third": "three"});
	coll.Update(q, newDoc);
	doc, _ = coll.FindOne(q);
	assertTrue(doc.Get("first").String() == "one", "update", t);

	rem, _ := mongo.Marshal(map[string]string{"third": "three"});
	coll.Remove(rem);
	doc, err = coll.FindOne(rem);
	assertTrue(err != nil, "remove", t);

	coll.Drop();

	statusCmd, _ := mongo.Marshal(map[string]float64{"serverStatus": 1});
	status, _ := db.Command(statusCmd);
	assertTrue(status.Get("uptime").Number() != 0, "valid serverStatus", t);

	db.Drop();
}
