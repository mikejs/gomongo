// Based on the Go json package.

// Copyright 2009 The Go Authors.  All rights reserved.
// Copyright 2009,2010 The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE and LICENSE.GO files.

package mongo

import (
	"os"
	"io"
	"io/ioutil"
	"fmt"
	"math"
	"time"
	"bytes"
	"strconv"
	"container/vector"
)

const (
	EOOKind = iota
	NumberKind
	StringKind
	ObjectKind
	ArrayKind
	BinaryKind
	UndefinedKind // deprecated
	OIDKind
	BooleanKind
	DateKind
	NullKind
	RegexKind
	RefKind // deprecated
	CodeKind
	SymbolKind
	CodeWithScope
	IntKind
	TimestampKind
	LongKind
	MinKeyKind
	MaxKeyKind
)

type BSON interface {
	Kind() int
	Number() float64
	String() string
	OID() []byte
	Bool() bool
	Date() *time.Time
	Regex() (string, string)
	Int() int32
	Long() int64

	Get(s string) BSON
	Elem(i int) BSON
	Len() int

	Bytes() []byte
}

type _Null struct{}

var Null BSON = &_Null{}

func (*_Null) Kind() int               { return NullKind }
func (*_Null) Number() float64         { return 0 }
func (*_Null) String() string          { return "null" }
func (*_Null) OID() []byte             { return nil }
func (*_Null) Bool() bool              { return false }
func (*_Null) Date() *time.Time        { return nil }
func (*_Null) Regex() (string, string) { return "", "" }
func (*_Null) Int() int32              { return 0 }
func (*_Null) Long() int64             { return 0 }
func (*_Null) Get(string) BSON         { return Null }
func (*_Null) Elem(int) BSON           { return Null }
func (*_Null) Len() int                { return 0 }
func (*_Null) Bytes() []byte           { return []byte{0} }

type _Number struct {
	value float64
	_Null
}

func (self *_Number) Kind() int       { return NumberKind }
func (self *_Number) Number() float64 { return self.value }
func (self *_Number) Bytes() []byte {
	bits := math.Float64bits(self.value)
	w64 := _WORD64
	pack.PutUint64(w64, bits)
	return w64
}

type _String struct {
	value string
	_Null
}

func (self *_String) Kind() int      { return StringKind }
func (self *_String) String() string { return self.value }
func (self *_String) Bytes() []byte {
	w32 := _WORD32
	l := len(self.value) + 1
	pack.PutUint32(w32, uint32(l))

	buf := bytes.NewBuffer(w32)
	buf.WriteString(self.value)
	buf.WriteByte(0)

	return buf.Bytes()
}

type _Object struct {
	value map[string]BSON
	_Null
}

func (self *_Object) Kind() int { return ObjectKind }
func (self *_Object) Get(s string) BSON {
	if self.value == nil {
		return Null
	}

	b, ok := self.value[s]
	if !ok {
		return Null
	}

	return b
}
func (self *_Object) Len() int { return len(self.value) }
func (self *_Object) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	for k, v := range self.value {
		buf.WriteByte(byte(v.Kind()))
		buf.WriteString(k)
		buf.WriteByte(0)
		buf.Write(v.Bytes())
	}
	buf.WriteByte(0)

	l := buf.Len() + 4
	w32 := _WORD32
	pack.PutUint32(w32, uint32(l))
	return bytes.Add(w32, buf.Bytes())
}

var EmptyObject BSON = &_Object{map[string]BSON{}, _Null{}}

type _Array struct {
	value *vector.Vector
	_Null
}

func (self *_Array) Kind() int { return ArrayKind }
func (self *_Array) Elem(i int) BSON {
	if self.value == nil {
		return Null
	}

	if self.Len() < i {
		return Null
	}

	return self.value.At(i).(BSON)
}
func (self *_Array) Len() int { return self.value.Len() }
func (self *_Array) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})

	for i := 0; i < self.value.Len(); i++ {
		v := self.value.At(i).(BSON)
		buf.WriteByte(byte(v.Kind()))
		buf.WriteString(strconv.Itoa(i))
		buf.WriteByte(0)
		buf.Write(v.Bytes())
	}
	buf.WriteByte(0)

	l := buf.Len() + 4
	w32 := _WORD32
	pack.PutUint32(w32, uint32(l))
	return bytes.Add(w32, buf.Bytes())
}

type _OID struct {
	value []byte
	_Null
}

func (self *_OID) Kind() int     { return OIDKind }
func (self *_OID) OID() []byte   { return self.value }
func (self *_OID) Bytes() []byte { return self.value }

type _Boolean struct {
	value bool
	_Null
}

func (self *_Boolean) Kind() int  { return BooleanKind }
func (self *_Boolean) Bool() bool { return self.value }
func (self *_Boolean) Bytes() []byte {
	if self.value {
		return []byte{1}
	}
	return []byte{0}
}

type _Date struct {
	value *time.Time
	_Null
}

func (self *_Date) Kind() int        { return DateKind }
func (self *_Date) Date() *time.Time { return self.value }
func (self *_Date) Bytes() []byte {
	w64 := _WORD64
	mtime := self.value.Seconds() * 1000
	pack.PutUint64(w64, uint64(mtime))
	return w64
}

type _Regex struct {
	regex, options string
	_Null
}

func (self *_Regex) Kind() int               { return RegexKind }
func (self *_Regex) Regex() (string, string) { return self.regex, self.options }
func (self *_Regex) Bytes() []byte {
	buf := bytes.NewBufferString(self.regex)
	buf.WriteByte(0)
	buf.WriteString(self.options)
	buf.WriteByte(0)
	return buf.Bytes()
}

type _Int struct {
	value int32
	_Null
}

func (self *_Int) Kind() int  { return IntKind }
func (self *_Int) Int() int32 { return self.value }
func (self *_Int) Bytes() []byte {
	w32 := _WORD32
	pack.PutUint32(w32, uint32(self.value))
	return w32
}

type _Long struct {
	value int64
	_Null
}

func (self *_Long) Kind() int   { return LongKind }
func (self *_Long) Long() int64 { return self.value }
func (self *_Long) Bytes() []byte {
	w64 := _WORD64
	pack.PutUint64(w64, uint64(self.value))
	return w64
}

func Equal(a, b BSON) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	case a.Kind() != b.Kind():
		return false
	}

	switch a.Kind() {
	case NumberKind:
		return a.Number() == b.Number()
	case StringKind:
		return a.String() == b.String()
	case ObjectKind:
		obj := a.(*_Object).value
		if len(obj) != len(b.(*_Object).value) {
			return false
		}
		for k, v := range obj {
			if !Equal(v, b.Get(k)) {
				return false
			}
		}
		return true
	case ArrayKind:
		if a.Len() != b.Len() {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if !Equal(a.Elem(i), b.Elem(i)) {
				return false
			}
		}
		return true
	case OIDKind:
		return bytes.Equal(a.OID(), b.OID())
	case BooleanKind:
		return a.Bool() == b.Bool()
	case DateKind:
		return a.Date() == b.Date()
	case RegexKind:
		ar, ao := a.Regex()
		br, bo := b.Regex()
		return ar == br && ao == bo
	case IntKind:
		return a.Int() == b.Int()
	case LongKind:
		return a.Long() == b.Long()
	}
	return true

}

type Builder interface {
	// Set value
	Int64(i int64)
	Int32(i int32)
	Float64(f float64)
	String(s string)
	Bool(b bool)
	Date(self *time.Time)
	OID(o []byte)
	Regex(regex, options string)
	Null()
	Object()
	Array()

	// Create sub-Builders
	Key(s string) Builder
	Elem(i int) Builder

	// Flush changes to parent Builder if necessary.
	Flush()
}

type _BSONBuilder struct {
	ptr *BSON

	arr  *vector.Vector
	elem int

	obj map[string]BSON
	key string
}

func (self *_BSONBuilder) Put(b BSON) {
	switch {
	case self.ptr != nil:
		*self.ptr = b
	case self.arr != nil:
		self.arr.Set(self.elem, b)
	case self.obj != nil:
		self.obj[self.key] = b
	}
}

func (self *_BSONBuilder) Get() BSON {
	switch {
	case self.ptr != nil:
		return *self.ptr
	case self.arr != nil:
		return self.arr.At(self.elem).(BSON)
	case self.obj != nil:
		return self.obj[self.key]
	}
	return nil
}

func (self *_BSONBuilder) Float64(f float64) { self.Put(&_Number{f, _Null{}}) }
func (self *_BSONBuilder) String(s string)   { self.Put(&_String{s, _Null{}}) }
func (self *_BSONBuilder) Object()           { self.Put(&_Object{make(map[string]BSON), _Null{}}) }
func (self *_BSONBuilder) Array()            { self.Put(&_Array{new(vector.Vector), _Null{}}) }
func (self *_BSONBuilder) Bool(b bool)       { self.Put(&_Boolean{b, _Null{}}) }
func (self *_BSONBuilder) Date(t *time.Time) { self.Put(&_Date{t, _Null{}}) }
func (self *_BSONBuilder) Null()             { self.Put(Null) }
func (self *_BSONBuilder) Regex(regex, options string) {
	self.Put(&_Regex{regex, options, _Null{}})
}
func (self *_BSONBuilder) Int32(i int32) { self.Put(&_Int{i, _Null{}}) }
func (self *_BSONBuilder) Int64(i int64) { self.Put(&_Long{i, _Null{}}) }
func (self *_BSONBuilder) OID(o []byte)  { self.Put(&_OID{o, _Null{}}) }

func (self *_BSONBuilder) Key(key string) Builder {
	bb2 := new(_BSONBuilder)

	switch obj := self.Get().(type) {
	case *_Object:
		bb2.obj = obj.value
		bb2.key = key
		bb2.obj[key] = Null
	case *_Array:
		bb2.arr = obj.value
		elem, _ := strconv.Atoi(key)
		bb2.elem = elem
		for elem >= bb2.arr.Len() {
			bb2.arr.Push(Null)
		}
	}
	return bb2
}

func (self *_BSONBuilder) Elem(i int) Builder {
	bb2 := new(_BSONBuilder)
	bb2.arr = self.Get().(*_Array).value
	bb2.elem = i
	for i >= bb2.arr.Len() {
		bb2.arr.Push(Null)
	}
	return bb2
}

func (self *_BSONBuilder) Flush() {}

func BytesToBSON(b []byte) (BSON, os.Error) {
	var bson BSON
	bb := new(_BSONBuilder)
	bb.ptr = &bson
	bb.Object()
	err := Parse(bytes.NewBuffer(b[4:len(b)]), bb)
	return bson, err
}

func readCString(buf *bytes.Buffer) string {
	out := bytes.NewBuffer([]byte{})
	var c byte
	for c, _ = buf.ReadByte(); c != 0; c, _ = buf.ReadByte() {
		out.WriteByte(c)
	}

	return out.String()
}

func Parse(buf *bytes.Buffer, builder Builder) (err os.Error) {
	kind, _ := buf.ReadByte()
	err = nil

	for kind != EOOKind {
		name := readCString(buf)
		b2 := builder.Key(name)

		switch kind {
		case NumberKind:
			lr := io.LimitReader(buf, 8)
			bits, _ := ioutil.ReadAll(lr)
			ui64 := pack.Uint64(bits)
			fl64 := math.Float64frombits(ui64)
			b2.Float64(fl64)
		case StringKind:
			bits, _ := ioutil.ReadAll(io.LimitReader(buf, 4))
			l := pack.Uint32(bits)
			s, _ := ioutil.ReadAll(io.LimitReader(buf, int64(l-1)))
			buf.ReadByte()
			b2.String(string(s))
		case ObjectKind:
			b2.Object()
			ioutil.ReadAll(io.LimitReader(buf, 4))
			err = Parse(buf, b2)
		case ArrayKind:
			b2.Array()
			ioutil.ReadAll(io.LimitReader(buf, 4))
			err = Parse(buf, b2)
		case OIDKind:
			oid, _ := ioutil.ReadAll(io.LimitReader(buf, 12))
			b2.OID(oid)
		case BooleanKind:
			b, _ := buf.ReadByte()
			if b == 1 {
				b2.Bool(true)
			} else {
				b2.Bool(false)
			}
		case DateKind:
			bits, _ := ioutil.ReadAll(io.LimitReader(buf, 8))
			ui64 := pack.Uint64(bits)
			b2.Date(time.SecondsToUTC(int64(ui64) / 1000))
		case RegexKind:
			regex := readCString(buf)
			options := readCString(buf)
			b2.Regex(regex, options)
		case IntKind:
			bits, _ := ioutil.ReadAll(io.LimitReader(buf, 4))
			ui32 := pack.Uint32(bits)
			b2.Int32(int32(ui32))
		case LongKind:
			bits, _ := ioutil.ReadAll(io.LimitReader(buf, 8))
			ui64 := pack.Uint64(bits)
			b2.Int64(int64(ui64))
		default:
			err = os.NewError(fmt.Sprintf("don't know how to handle kind %v yet", kind))
		}

		kind, _ = buf.ReadByte()
	}

	return
}

