// Use of this source code is governed by the 3-clause BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"testing"
	"fmt"
	"time"
)

func assertTrue(tf bool, msg string, t *testing.T) {
	if !tf {
		t.Error(msg)
	}
}

type EmptyStruct struct{}

type OtherStruct struct {
	F, V string
}

type ExampleStruct struct {
	First   int32
	Second  float64
	Third   string
	Fourth  EmptyStruct
	Fifth   OtherStruct
}

type ExampleWithId struct {
	Id_   string
	Other string
}

var b []byte = []byte{92, 0, 0, 0, 1, 115, 101, 99, 111, 110, 100, 0, 0, 0, 0, 0, 0, 0, 0, 64, 3, 102, 105, 102, 116, 104, 0, 23, 0, 0, 0, 2, 118, 0, 2, 0, 0, 0, 101, 0, 2, 102, 0, 2, 0, 0, 0, 105, 0, 0, 3, 102, 111, 117, 114, 116, 104, 0, 5, 0, 0, 0, 0, 2, 116, 104, 105, 114, 100, 0, 6, 0, 0, 0, 116, 104, 114, 101, 101, 0, 16, 102, 105, 114, 115, 116, 0, 1, 0, 0, 0, 0}

func TestSerializeAndDeserialize(t *testing.T) {
	obj, err := BytesToBSON(b)
	assertTrue(err == nil, fmt.Sprintf("failed parsing %v", b), t)
	obj2, _ := BytesToBSON(obj.Bytes())
	assertTrue(Equal(obj, obj2), fmt.Sprintf("obj != obj2 for %v", b), t)

	assertTrue(obj.Get("first").Int() == 1, "obj['first'] != 1", t)
	assertTrue(obj.Get("second").Number() == 2, "obj['second'] != 2.0", t)
	assertTrue(obj.Get("third").String() == "three", "obj['third'] != 'three'", t)
	assertTrue(obj.Get("fifth").Get("f").String() == "i", "obj['fifth']['f'] != 'i'", t)
	assertTrue(obj.Get("fifth").Get("v").String() == "e", "obj['fifth']['v'] != 'e'", t)
}

func TestUnmarshal(t *testing.T) {
	var es ExampleStruct
	Unmarshal(b, &es)
	assertTrue(es.First == 1, "unmarshal int", t)
	assertTrue(es.Second == 2, "unmarshal float64", t)
	assertTrue(es.Third == "three", "unmarshal string", t)
	assertTrue(es.Fifth.F == "i" && es.Fifth.V == "e", "unmarshal struct", t)
}

func TestIdHandling(t *testing.T) {
	ei := ExampleWithId{Id_: "fooid", Other: "bar"}
	// verify Id_ gets turned into _id
	parsed, err := Marshal(ei)
	assertTrue(err == nil, "cannot marshal", t)
	assertTrue(parsed.Get("_id").String() == "fooid", "no _id", t)
	// ...and vice-versa
	var back ExampleWithId
	err = Unmarshal(parsed.Bytes(), &back)
	assertTrue(err == nil, "cannot unmarshal", t)
	assertTrue(back.Id_ == "fooid", "no _id back", t)
}

type ExampleStruct2 struct {
	Date *time.Time
}

func TestMarshal(t *testing.T) {
	var es1 ExampleStruct
	Unmarshal(b, &es1)
	bs1, _ := Marshal(&es1)
	bs2, _ := BytesToBSON(b)
	assertTrue(Equal(bs1, bs2), "unmarshal->marshal", t)

	m := map[string]string{"f": "i", "v": "e"}
	bs3, _ := Marshal(&m)
	assertTrue(Equal(bs3, bs2.Get("fifth")), "marshal map", t)

	arr, _ := Marshal([]int{1, 2, 3})
	assertTrue(arr.Elem(0).Long() == 1, "array marshal (0)", t)
	assertTrue(arr.Elem(1).Long() == 2, "array marshal (1)", t)
	assertTrue(arr.Elem(2).Long() == 3, "array marshal (2)", t)

	d := time.UTC()
	es2 := &ExampleStruct2{d}
	bs2, _ = Marshal(es2)
	assertTrue(bs2.Get("date").Date().Seconds() == d.Seconds(), "date marshal", t)
	es2 = new(ExampleStruct2)
	Unmarshal(bs2.Bytes(), es2)
	assertTrue(es2.Date.Seconds() == d.Seconds(), "date unmarshal", t)
}

func TestVariety(t *testing.T) {
	in := map[string]string{"val": "test"}
	encoded := VerifyMarshal2(t, in)
	expected := []byte("\x13\x00\x00\x00\x02val\x00\x05\x00\x00\x00test\x00\x00")
	compare(t, encoded, expected)

	out := make(map[string]interface{})
	err := Unmarshal(encoded, &out)
	if in["val"] != out["val"] {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out)
	}

	var out1 string
	err = Unmarshal(encoded, &out1)
	if out1 != "test" {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out1)
	}

	var out2 interface{}
	err = Unmarshal(encoded, &out2)
	if out2.(map[string]interface{})["val"].(string) != "test" {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out1)
	}

	type mystruct struct {
		Val string
	}
	var out3 mystruct
	err = Unmarshal(encoded, &out3)
	if out3.Val != "test" {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out1)
	}
}

func TestBinary(t *testing.T) {
	in := map[string][]byte{"val": []byte("test")}
	encoded := VerifyMarshal2(t, in)
	expected := []byte("\x13\x00\x00\x00\x05val\x00\x04\x00\x00\x00\x00test\x00")
	compare(t, encoded, expected)

	out := make(map[string]interface{})
	err := Unmarshal(encoded, &out)
	if string(out["val"].([]byte)) != "test" {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out)
	}

	var out1 []byte
	err = Unmarshal(encoded, &out1)
	if string(out1) != "test" {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out1)
	}
}

func TestInt(t *testing.T) {
	in := map[string]int{"val": 20}
	encoded := VerifyMarshal2(t, in)
	expected := []byte("\x12\x00\x00\x00\x12val\x00\x14\x00\x00\x00\x00\x00\x00\x00\x00")
	compare(t, encoded, expected)

	out := make(map[string]interface{})
	err := Unmarshal(encoded, &out)
	if out["val"].(int64) != 20 {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out)
	}

	var out1 int
	err = Unmarshal(encoded, &out1)
	if out1 != 20 {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%vn", err, in, out1)
	}
}

func VerifyMarshal2(t *testing.T, val interface{}) []byte {
	bs, err := Marshal(val)
	if err != nil {
		t.Errorf("marshal error: %s\n", err)
	}
	expected := bs.Bytes()
	encoded, err2 := Marshal2(val)
	if err2 != nil {
		t.Errorf("marshal2 error: %s\n", err2)
	}
	compare(t, encoded, expected)
	return expected
}

func compare(t *testing.T, encoded []byte, expected []byte) {
	if len(encoded) != len(expected) {
		t.Errorf("encoding mismatch:\n%#v\n%#v\n", string(encoded), string(expected))
	} else {
		for i, _ := range encoded {
			if encoded[i] != expected[i] {
				t.Errorf("encoding mismatch:\n%#v\n%#v\n", string(encoded), string(expected))
				break
			}
		}
	}
}
