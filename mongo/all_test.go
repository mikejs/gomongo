// Copyright 2009,2010 The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"testing"
	"fmt"
	"time"
)

type KeyStruct struct {
	First int32
}

type IndexCmd struct {
	Name, Ns string
	Key      *KeyStruct
}

func TestStuff(t *testing.T) {
	obj, err := BytesToBSON([]byte{92, 0, 0, 0, 1, 115, 101, 99, 111, 110, 100, 0, 0, 0, 0, 0, 0, 0, 0, 64, 3, 102, 105, 102, 116, 104, 0, 23, 0, 0, 0, 2, 118, 0, 2, 0, 0, 0, 101, 0, 2, 102, 0, 2, 0, 0, 0, 105, 0, 0, 3, 102, 111, 117, 114, 116, 104, 0, 5, 0, 0, 0, 0, 2, 116, 104, 105, 114, 100, 0, 6, 0, 0, 0, 116, 104, 114, 101, 101, 0, 16, 102, 105, 114, 115, 116, 0, 1, 0, 0, 0, 0})
	assertTrue(err == nil, "failed parsing BSON obj", t)

	conn, err := Connect("127.0.0.1")
	assertTrue(err == nil && conn != nil, fmt.Sprintf("failed connecting to mongo: %v", err), t)

	db := conn.GetDB("go_driver_tests")
	coll := db.GetCollection("coll")
	coll.Drop()

	coll.Insert(obj)

	q, _ := Marshal(map[string]string{})
	ret, err := coll.FindAll(q)
	assertTrue(err == nil && ret != nil, "query succeeded", t)

	doc, _ := ret.GetNext()
	assertTrue(doc.Kind() == ObjectKind, "query returned document", t)
	assertTrue(doc.Get("first").Int() == 1, "returned doc has proper 'first' element", t)
	assertTrue(doc.Get("second").Number() == 2, "returned doc has proper 'second' element", t)
	assertTrue(doc.Get("third").String() == "three", "returned doc has proper 'third' element", t)
	assertTrue(doc.Get("fourth").Kind() == ObjectKind, "returned doc has proper 'fourth' element", t)
	assertTrue(doc.Get("fifth").Get("f").String() == "i" && doc.Get("fifth").Get("v").String() == "e", "returned doc has proper 'fifth' element", t)

	count, err := coll.Count(q)
	assertTrue(count == 1, "count", t)

	newDoc, _ := Marshal(map[string]string{"first": "one", "second": "two", "third": "three"})
	coll.Update(q, newDoc)
	doc, _ = coll.FindOne(q)
	assertTrue(doc.Get("first").String() == "one", "update", t)

	rem, _ := Marshal(map[string]string{"third": "three"})
	coll.Remove(rem)
	doc, err = coll.FindOne(rem)
	assertTrue(err != nil, "remove", t)

	coll.Drop()

	statusCmd, _ := Marshal(map[string]float64{"serverStatus": 1})
	status, _ := db.Command(statusCmd)
	assertTrue(status.Get("uptime").Number() != 0, "valid serverStatus", t)

	db.Drop()
}

func TestOtherStuff(t *testing.T) {
	doc, _ := Marshal(map[string]string{"_id": "doc1", "title": "A Mongo document", "content": "Testing, 1. 2. 3."})
	conn, _ := Connect("127.0.0.1")
	collection := conn.GetDB("test").GetCollection("test_collection")
	collection.Insert(doc)

	query, _ := Marshal(map[string]string{"_id": "doc1"})
	got, _ := collection.FindOne(query)
	assertTrue(Equal(doc, got), "equal", t)

}

const (
	PER_TRIAL  = 1000
	BATCH_SIZE = 100
)

func timeIt(s string, f func(*Collection, *testing.T), coll *Collection, t *testing.T) {
	start := time.Nanoseconds()
	f(coll, t)
	end := time.Nanoseconds()
	diff := end - start
	ops := (PER_TRIAL / float64(diff)) * 1000000000.0
	secs := float64(diff) / 1000000000.0
	t.Logf("%v: %v secs, %v OPS", s, secs, ops)
}

func TestBenchmark(t *testing.T) {
	conn, err := Connect("127.0.0.1")
	if err != nil {
		t.Error("failed connecting")
	}

	db := conn.GetDB("perf_test")
	db.Drop()
	db.GetCollection("creation").Insert(EmptyObject)
	db.GetCollection("creation").Count(EmptyObject)

	timeIt("single.small Insert", singleInsertSmall, db.GetCollection("single.small"), t)
	timeIt("single.medium Insert", singleInsertMedium, db.GetCollection("single.medium"), t)
	timeIt("single.large Insert", singleInsertLarge, db.GetCollection("single.large"), t)
	timeIt("single.small FindOne", findOneSmall, db.GetCollection("single.small"), t)
	timeIt("single.medium FindOne", findOneMedium, db.GetCollection("single.medium"), t)
	timeIt("single.large FindOne", findOne, db.GetCollection("single.large"), t)

	t.Log("---")
	db.GetCollection("single.small.indexed").EnsureIndex("my_index1", map[string]int{"x": 1})
	timeIt("single.small.indexed Insert", singleInsertSmall, db.GetCollection("single.small.indexed"), t)

	db.GetCollection("single.medium.indexed").EnsureIndex("my_index2", map[string]int{"x": 1})
	timeIt("single.medium.indexed Insert", singleInsertMedium, db.GetCollection("single.medium.indexed"), t)

	db.GetCollection("single.large.indexed").EnsureIndex("my_index3", map[string]int{"x": 1})
	timeIt("single.large.indexed Insert", singleInsertLarge, db.GetCollection("single.large.indexed"), t)

	timeIt("single.small.indexed FindOne", findOneSmall, db.GetCollection("single.small.indexed"), t)
	timeIt("single.medium.indexed FindOne", findOneMedium, db.GetCollection("single.medium.indexed"), t)
	timeIt("single.large.indexed FindOne", findOne, db.GetCollection("single.large.indexed"), t)
}

type smallStruct struct {
	X int
}

func singleInsertSmall(coll *Collection, t *testing.T) {
	ss := &smallStruct{0}
	for i := 0; i < PER_TRIAL; i++ {
		ss.X = i
		obj, err := Marshal(ss)
		if err != nil {
			t.Errorf("singleInsertSmall Marshal: %v\n", err)
		}

		err = coll.Insert(obj)
		if err != nil {
			t.Errorf("singleInsertSmall Insert: %v\n", err)
		}
	}
}

func findOneSmall(coll *Collection, t *testing.T) {
	ss := &smallStruct{PER_TRIAL / 2}
	obj, err := Marshal(ss)
	if err != nil {
		t.Errorf("findOneSmall Marshal: %v\n", err)
	}

	for i := 0; i < PER_TRIAL; i++ {
		_, err = coll.FindOne(obj)
		if err != nil {
			t.Errorf("findOneSmall FindOne: %v\n", err)
		}
	}
}

type mediumStruct struct {
	Integer int
	Number  float64
	Boolean bool
	Array   []string
	X       int
}

func singleInsertMedium(coll *Collection, t *testing.T) {
	ms := &mediumStruct{5, 5.05, false, []string{"test", "benchmark"}, 0}
	for i := 0; i < PER_TRIAL; i++ {
		ms.X = i
		obj, err := Marshal(ms)
		if err != nil {
			t.Errorf("singleInsertMedium Marshal: %v\n", err)
		}

		err = coll.Insert(obj)
		if err != nil {
			t.Errorf("singleInsertMedium Insert: %v\n", err)
		}
	}
}

func findOneMedium(coll *Collection, t *testing.T) {
	ss := &smallStruct{PER_TRIAL / 2}
	obj, err := Marshal(ss)
	if err != nil {
		t.Errorf("findOneMedium Marshal: %v\n", err)
	}

	for i := 0; i < PER_TRIAL; i++ {
		_, err = coll.FindOne(obj)
		if err != nil {
			t.Errorf("findOneMedium FindOne: %v\n", err)
		}
	}
}

func findOne(coll *Collection, t *testing.T) {
	ss := &smallStruct{PER_TRIAL / 2}
	obj, err := Marshal(ss)
	if err != nil {
		t.Errorf("findOne Marshal: %v\n", err)
	}

	for i := 0; i < PER_TRIAL; i++ {
		_, err = coll.FindOne(obj)
		if err != nil {
			t.Errorf("findOne FindOne: %v\n", err)
		}
	}
}

type largeStruct struct {
	Base_url         string
	Total_word_count int
	Access_time      *time.Time
	Meta_tags        map[string]string
	Page_structure   map[string]int
	Harvested_words  []string
	X                int
}

func singleInsertLarge(coll *Collection, t *testing.T) {
	base_words := []string{"10gen", "web", "open", "source", "application", "paas",
		"platform-as-a-service", "technology", "helps",
		"developers", "focus", "building", "mongodb", "mongo",
	}

	words := make([]string, 280)
	for i := 0; i < 20; i++ {
		for k, word := range base_words {
			words[i+k] = word
		}
	}

	ls := &largeStruct{"http://www.example.com/test-me",
		6743, time.UTC(),
		map[string]string{"description": "i am a long description string",
			"author":                       "Holly Man",
			"dynamically_created_meta_tag": "who know\n what",
		},
		map[string]int{"counted_tags": 3450,
			"no_of_js_attached": 10,
			"no_of_images":      6,
		},
		words, 0,
	}

	for i := 0; i < PER_TRIAL; i++ {
		ls.X = i
		obj, err := Marshal(ls)
		if err != nil {
			t.Errorf("singleInsertLarge Marshal: %v", err)
		}

		err = coll.Insert(obj)
		if err != nil {
			t.Errorf("singleInsertLarge Insert: %v", err)
		}
	}
}

