package mongo_test

import (
	"mongo";
	"testing";
	"fmt";
)

func TestInsert(t *testing.T) {
	obj, _ := mongo.BytesToBSON([]byte{92, 0, 0, 0, 1, 115, 101, 99, 111, 110, 100, 0, 0, 0, 0, 0, 0, 0, 0, 64, 3, 102, 105, 102, 116, 104, 0, 23, 0, 0, 0, 2, 118, 0, 2, 0, 0, 0, 101, 0, 2, 102, 0, 2, 0, 0, 0, 105, 0, 0, 3, 102, 111, 117, 114, 116, 104, 0, 5, 0, 0, 0, 0, 2, 116, 104, 105, 114, 100, 0, 6, 0, 0, 0, 116, 104, 114, 101, 101, 0, 16, 102, 105, 114, 115, 116, 0, 1, 0, 0, 0, 0});

	conn, _ := mongo.Connect("localhost", 27017);
	coll := conn.GetDB("test").GetCollection("coll");
	coll.Insert(obj);

	q, _ := mongo.BytesToBSON([]byte{5, 0, 0, 0, 0});
	ret, err := coll.Query(q);
	if err != nil || ret == nil {
		fmt.Printf("Failed: %v\n", err)
	}

	doc, err := ret.GetNext();
	fmt.Printf("First doc: %v\n", doc.Kind());
}
