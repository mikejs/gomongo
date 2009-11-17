// Copyright 2009 Michael Stephens.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mongo_test

import (
	"testing";
	"mongo";
	"fmt";
)

func assertTrue(tf bool, msg string, t *testing.T) {
	if !tf {
		t.Error(msg)
	}
}

type EmptyStruct struct{}

type OtherStruct struct {
	F, V string;
}

type ExampleStruct struct {
	First	int32;
	Second	float64;
	Third	string;
	Fourth	EmptyStruct;
	Fifth	OtherStruct;
}

var b []byte = []byte{92, 0, 0, 0, 1, 115, 101, 99, 111, 110, 100, 0, 0, 0, 0, 0, 0, 0, 0, 64, 3, 102, 105, 102, 116, 104, 0, 23, 0, 0, 0, 2, 118, 0, 2, 0, 0, 0, 101, 0, 2, 102, 0, 2, 0, 0, 0, 105, 0, 0, 3, 102, 111, 117, 114, 116, 104, 0, 5, 0, 0, 0, 0, 2, 116, 104, 105, 114, 100, 0, 6, 0, 0, 0, 116, 104, 114, 101, 101, 0, 16, 102, 105, 114, 115, 116, 0, 1, 0, 0, 0, 0}

func TestSerializeAndDeserialize(t *testing.T) {
	obj, ok := mongo.BytesToBSON(b);
	assertTrue(ok, fmt.Sprintf("failed parsing %v", b), t);
	obj2, _ := mongo.BytesToBSON(obj.Bytes());
	assertTrue(mongo.Equal(obj, obj2), fmt.Sprintf("obj != obj2 for %v", b), t);

	assertTrue(obj.Get("first").Int() == 1, "obj['first'] != 1", t);
	assertTrue(obj.Get("second").Number() == 2, "obj['second'] != 2.0", t);
	assertTrue(obj.Get("third").String() == "three", "obj['third'] != 'three'", t);
	assertTrue(obj.Get("fifth").Get("f").String() == "i", "obj['fifth']['f'] != 'i'", t);
	assertTrue(obj.Get("fifth").Get("v").String() == "e", "obj['fifth']['v'] != 'e'", t);
}

func TestUnmarshal(t *testing.T) {
	var es ExampleStruct;
	mongo.Unmarshal(b, &es);
	assertTrue(es.First == 1, "unmarshal int", t);
	assertTrue(es.Second == 2, "unmarshal float64", t);
	assertTrue(es.Third == "three", "unmarshal string", t);
	assertTrue(es.Fifth.F == "i" && es.Fifth.V == "e", "unmarshal struct", t);
}

func TestMarshal(t *testing.T) {
	var es1 ExampleStruct;
	mongo.Unmarshal(b, &es1);
	bs1, _ := mongo.Marshal(&es1);
	bs2, _ := mongo.BytesToBSON(b);
	assertTrue(mongo.Equal(bs1, bs2), "unmarshal->marshal", t);

	m := map[string]string{"f": "i", "v": "e"};
	bs3, _ := mongo.Marshal(&m);
	assertTrue(mongo.Equal(bs3, bs2.Get("fifth")), "marshal map", t);
}
