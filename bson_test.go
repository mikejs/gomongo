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

func TestSerializeAndDeserialize(t *testing.T) {
	b := []byte{92, 0, 0, 0, 1, 115, 101, 99, 111, 110, 100, 0, 0, 0, 0, 0, 0, 0, 0, 64, 3, 102, 105, 102, 116, 104, 0, 23, 0, 0, 0, 2, 118, 0, 2, 0, 0, 0, 101, 0, 2, 102, 0, 2, 0, 0, 0, 105, 0, 0, 3, 102, 111, 117, 114, 116, 104, 0, 5, 0, 0, 0, 0, 2, 116, 104, 105, 114, 100, 0, 6, 0, 0, 0, 116, 104, 114, 101, 101, 0, 16, 102, 105, 114, 115, 116, 0, 1, 0, 0, 0, 0};
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
