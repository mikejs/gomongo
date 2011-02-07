// Copyright 2009 The Go Authors.  All rights reserved.
// Copyright 2009,2010, The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the 3-clause BSD License
// that can be found in the LICENSE and LICENSE.GO files.

package mongo

import (
	"os"
	"math"
	"reflect"
	"strings"
	"time"
	"bytes"
	"strconv"
)

type SimpleContainer struct {
	Val interface{}
}

type LenWriter struct {
	buf        *bytes.Buffer
	len_offset int
}

func NewLenWriter(buf *bytes.Buffer) *LenWriter {
	len_offset := len(buf.Bytes())
	w32 := make([]byte, _WORD32)
	buf.Write(w32)
	return &LenWriter{buf, len_offset}
}

func (self *LenWriter) RecordLen() {
	buf := self.buf.Bytes()
	final_len := len(buf)
	w32 := buf[self.len_offset:self.len_offset+_WORD32]
	pack.PutUint32(w32, uint32(final_len-self.len_offset))
}

func Marshal2(val interface{}) (encoded []byte, err os.Error) {
	defer func() {
		if x := recover(); x != nil {
			err = x.(bsonError)
		}
	}()

	if val == nil {
		return nil, os.NewError("Cannot marshal empty object")
	}

	// Dereference pointer types
	switch v := reflect.NewValue(val).(type) {
	case *reflect.PtrValue:
		val = v.Elem().Interface()
	}

	// Wrap simple types in a container
	switch v:= val.(type) {
	case float64, string, bool, int32, int64, int, time.Time, []byte:
		val = SimpleContainer{v}
	}

	buf := bytes.NewBuffer(make([]byte, 0, 32))
	switch fv := reflect.NewValue(val).(type) {
	case *reflect.StructValue:
		EncodeStruct(buf, fv)
	case *reflect.MapValue:
		EncodeMap(buf, fv)
	}
	return buf.Bytes(), err
}

func EncodeField(buf *bytes.Buffer, key string, val interface{}) {
	// MongoDB uses '_id' as the primary key, but this
	// name is private in Go. Use 'Id_' for this purpose
	// instead.
	if key == "id_" {
		key = "_id"
	}
	switch v := val.(type) {
	case float64:
		EncodePrefix(buf, '\x01', key)
		EncodeFloat64(buf, v)
	case string:
		EncodePrefix(buf, '\x02', key)
		EncodeString(buf, v)
	case bool:
		EncodePrefix(buf, '\x08', key)
		EncodeBool(buf, v)
	case int32:
		EncodePrefix(buf, '\x10', key)
		EncodeInt32(buf, v)
	case int64:
		EncodePrefix(buf, '\x12', key)
		EncodeInt64(buf, v)
	case int:
		EncodePrefix(buf, '\x12', key)
		EncodeInt64(buf, int64(v))
	case time.Time:
		EncodePrefix(buf, '\x11', key)
		EncodeTime(buf, v)
	case []byte:
		EncodePrefix(buf, '\x05', key)
		EncodeBinary(buf, v)
	default:
		goto CompositeType
	}
	return

CompositeType:
	switch fv := reflect.NewValue(val).(type) {
	case *reflect.StructValue:
		EncodePrefix(buf, '\x03', key)
		EncodeStruct(buf, fv)
	case *reflect.MapValue:
		EncodePrefix(buf, '\x03', key)
		EncodeMap(buf, fv)
	case *reflect.SliceValue:
		EncodePrefix(buf, '\x04', key)
		EncodeSlice(buf, fv)
	case *reflect.PtrValue:
		EncodeField(buf, key, fv.Elem().Interface())
	default:
		panic(NewBsonError("don't know how to marshal %v\n", reflect.NewValue(val).Type()))
	}
}

func EncodePrefix(buf *bytes.Buffer, etype byte, key string) {
	buf.WriteByte(etype)
	buf.WriteString(key)
	buf.WriteByte(0)
}

func EncodeFloat64(buf *bytes.Buffer, val float64) {
	bits := math.Float64bits(val)
	w64 := make([]byte, _WORD64)
	pack.PutUint64(w64, bits)
	buf.Write(w64)
}

func EncodeString(buf *bytes.Buffer, val string) {
	w32 := make([]byte, _WORD32)
	pack.PutUint32(w32, uint32(len(val)+1))
	buf.Write(w32)
	buf.WriteString(val)
	buf.WriteByte(0)
}

func EncodeBool(buf *bytes.Buffer, val bool) {
	if val {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
}

func EncodeInt32(buf *bytes.Buffer, val int32) {
	w32 := make([]byte, _WORD32)
	pack.PutUint32(w32, uint32(val))
	buf.Write(w32)
}

func EncodeInt64(buf *bytes.Buffer, val int64) {
	w64 := make([]byte, _WORD64)
	pack.PutUint64(w64, uint64(val))
	buf.Write(w64)
}

func EncodeTime(buf *bytes.Buffer, val time.Time) {
	w64 := make([]byte, _WORD64)
	mtime := val.Seconds() * 1000
	pack.PutUint64(w64, uint64(mtime))
	buf.Write(w64)
}

func EncodeBinary(buf *bytes.Buffer, val []byte) {
	w32 := make([]byte, _WORD32)
	pack.PutUint32(w32, uint32(len(val)))
	buf.Write(w32)
	buf.WriteByte(0)
	buf.Write(val)
}

func EncodeStruct(buf *bytes.Buffer, val *reflect.StructValue) {
	lenWriter := NewLenWriter(buf)
	t := val.Type().(*reflect.StructType)
	for i := 0; i < t.NumField(); i++ {
		key := strings.ToLower(t.Field(i).Name)
		EncodeField(buf, key, val.Field(i).Interface())
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func EncodeMap(buf *bytes.Buffer, val *reflect.MapValue) {
	lenWriter := NewLenWriter(buf)
	mt := val.Type().(*reflect.MapType)
	if mt.Key() != reflect.Typeof("") {
		panic(NewBsonError("can't marshall maps with non-string key types"))
	}
	keys := val.Keys()
	for _, k := range keys {
		key := k.(*reflect.StringValue).Get()
		EncodeField(buf, key, val.Elem(k).Interface())
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func EncodeSlice(buf *bytes.Buffer, val *reflect.SliceValue) {
	lenWriter := NewLenWriter(buf)
	for i := 0; i < val.Len(); i++ {
		EncodeField(buf, strconv.Itoa(i), val.Elem(i).Interface())
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}
