// Based on the Go json package.
// Original Copyright 2009 The Go Authors. All rights reserved.
// Modifications Copyright 2009 Michael Stephens.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE and LICENSE.GO files.

package mongo

import (
	"os";
	"io";
	"fmt";
	"math";
	"bytes";
	"strconv";
	"encoding/binary";
	"container/vector";
)

const (
	EOOKind	= iota;
	NumberKind;
	StringKind;
	ObjectKind;
	ArrayKind;
	BinaryKind;
	UndefinedKind;	// deprecated
	OIDKind;
	BooleanKind;
	DateKind;
	NullKind;
	RegexKind;
	RefKind;	// deprecated
	CodeKind;
	SymbolKind;
	CodeWithScope;
	IntKind;
	TimestampKind;
	LongKind;
	MinKeyKind;
	MaxKeyKind;
)

type BSON interface {
	Kind() int;
	Number() float64;
	String() string;
	OID() []byte;
	Bool() bool;
	Date() int64;
	Regex() (string, string);
	Int() int32;
	Long() int64;

	Get(s string) BSON;
	Elem(i int) BSON;
	Len() int;

	Bytes() []byte;
}

type _Null struct{}

var Null BSON = &_Null{}

func (*_Null) Kind() int		{ return NullKind }
func (*_Null) Number() float64		{ return 0 }
func (*_Null) String() string		{ return "null" }
func (*_Null) OID() []byte		{ return nil }
func (*_Null) Bool() bool		{ return false }
func (*_Null) Date() int64		{ return 0 }
func (*_Null) Regex() (string, string)	{ return "", "" }
func (*_Null) Int() int32		{ return 0 }
func (*_Null) Long() int64		{ return 0 }
func (*_Null) Get(string) BSON		{ return Null }
func (*_Null) Elem(int) BSON		{ return Null }
func (*_Null) Len() int			{ return 0 }
func (*_Null) Bytes() []byte		{ return []byte{0} }

type _Number struct {
	value	float64;
	_Null;
}

func (n *_Number) Kind() int		{ return NumberKind }
func (n *_Number) Number() float64	{ return n.value }
func (n *_Number) Bytes() []byte {
	bits := math.Float64bits(n.value);
	b := []byte{0, 0, 0, 0, 0, 0, 0, 0};
	binary.LittleEndian.PutUint64(b, bits);
	return b;
}

type _String struct {
	value	string;
	_Null;
}

func (s *_String) Kind() int		{ return StringKind }
func (s *_String) String() string	{ return s.value }
func (s *_String) Bytes() []byte {
	b := []byte{0, 0, 0, 0};
	l := len(s.value) + 1;
	binary.LittleEndian.PutUint32(b, uint32(l));

	buf := bytes.NewBuffer(b);
	buf.WriteString(s.value);
	buf.WriteByte(0);

	return buf.Bytes();
}

type _Object struct {
	value	map[string]BSON;
	_Null;
}

func (o *_Object) Kind() int	{ return ObjectKind }
func (o *_Object) Get(s string) BSON {
	if o.value == nil {
		return Null
	}

	b, ok := o.value[s];
	if !ok {
		return Null
	}

	return b;
}
func (o *_Object) Len() int	{ return len(o.value) }
func (o *_Object) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{});
	for k, v := range o.value {
		buf.WriteByte(byte(v.Kind()));
		buf.WriteString(k);
		buf.WriteByte(0);
		buf.Write(v.Bytes());
	}
	buf.WriteByte(0);

	l := buf.Len() + 4;
	b := []byte{0, 0, 0, 0};
	binary.LittleEndian.PutUint32(b, uint32(l));
	return bytes.Add(b, buf.Bytes());
}

type _Array struct {
	value	*vector.Vector;
	_Null;
}

func (a *_Array) Kind() int	{ return ArrayKind }
func (a *_Array) Elem(i int) BSON {
	if a.value == nil {
		return Null
	}

	if a.Len() < i {
		return Null
	}

	return a.value.At(i).(BSON);
}
func (a *_Array) Len() int	{ return a.value.Len() }
func (a *_Array) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{});

	for i := 0; i < a.value.Len(); i++ {
		v := a.value.At(i).(BSON);
		buf.WriteByte(byte(v.Kind()));
		buf.WriteString(strconv.Itoa(i));
		buf.WriteByte(0);
		buf.Write(v.Bytes());
	}
	buf.WriteByte(0);

	l := buf.Len() + 4;
	b := []byte{0, 0, 0, 0};
	binary.LittleEndian.PutUint32(b, uint32(l));
	return bytes.Add(b, buf.Bytes());
}

type _OID struct {
	value	[]byte;
	_Null;
}

func (o *_OID) Kind() int	{ return OIDKind }
func (o *_OID) OID() []byte	{ return o.value }
func (o *_OID) Bytes() []byte	{ return o.value }

type _Boolean struct {
	value	bool;
	_Null;
}

func (b *_Boolean) Kind() int	{ return BooleanKind }
func (b *_Boolean) Bool() bool	{ return b.value }
func (b *_Boolean) Bytes() []byte {
	if b.value {
		return []byte{1}
	}
	return []byte{0};
}

type _Date struct {
	value	int64;
	_Null;
}

func (d *_Date) Kind() int	{ return DateKind }
func (d *_Date) Date() int64	{ return d.value }
func (d *_Date) Bytes() []byte {
	b := []byte{0, 0, 0, 0, 0, 0, 0, 0};
	binary.LittleEndian.PutUint64(b, uint64(d.value));
	return b;
}

type _Regex struct {
	regex, options	string;
	_Null;
}

func (r *_Regex) Kind() int			{ return RegexKind }
func (r *_Regex) Regex() (string, string)	{ return r.regex, r.options }
func (r *_Regex) Bytes() []byte {
	buf := bytes.NewBufferString(r.regex);
	buf.WriteByte(0);
	buf.WriteString(r.options);
	buf.WriteByte(0);
	return buf.Bytes();
}

type _Int struct {
	value	int32;
	_Null;
}

func (i *_Int) Kind() int	{ return IntKind }
func (i *_Int) Int() int32	{ return i.value }
func (i *_Int) Bytes() []byte {
	b := []byte{0, 0, 0, 0};
	binary.LittleEndian.PutUint32(b, uint32(i.value));
	return b;
}

type _Long struct {
	value	int64;
	_Null;
}

func (l *_Long) Kind() int	{ return LongKind }
func (l *_Long) Long() int64	{ return l.value }
func (l *_Long) Bytes() []byte {
	b := []byte{0, 0, 0, 0, 0, 0, 0, 0};
	binary.LittleEndian.PutUint64(b, uint64(l.value));
	return b;
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
		obj := a.(*_Object).value;
		if len(obj) != len(b.(*_Object).value) {
			return false
		}
		for k, v := range obj {
			if !Equal(v, b.Get(k)) {
				return false
			}
		}
		return true;
	case ArrayKind:
		if a.Len() != b.Len() {
			return false
		}
		for i := 0; i < a.Len(); i++ {
			if !Equal(a.Elem(i), b.Elem(i)) {
				return false
			}
		}
		return true;
	case OIDKind:
		return bytes.Equal(a.OID(), b.OID())
	case BooleanKind:
		return a.Bool() == b.Bool()
	case DateKind:
		return a.Date() == b.Date()
	case RegexKind:
		ar, ao := a.Regex();
		br, bo := b.Regex();
		return ar == br && ao == bo;
	case IntKind:
		return a.Int() == b.Int()
	case LongKind:
		return a.Long() == b.Long()
	}
	return true;

}

type Builder interface {
	// Set value
	Int64(i int64);
	Int32(i int32);
	Float64(f float64);
	String(s string);
	Bool(b bool);
	Date(d int64);
	OID(o []byte);
	Regex(regex, options string);
	Null();
	Object();
	Array();

	// Create sub-Builders
	Key(s string) Builder;
	Elem(i int) Builder;

	// Flush changes to parent Builder if necessary.
	Flush();
}

type _BSONBuilder struct {
	ptr	*BSON;

	arr	*vector.Vector;
	elem	int;

	obj	map[string]BSON;
	key	string;
}

func (bb *_BSONBuilder) Put(b BSON) {
	switch {
	case bb.ptr != nil:
		*bb.ptr = b
	case bb.arr != nil:
		bb.arr.Set(bb.elem, b)
	case bb.obj != nil:
		bb.obj[bb.key] = b
	}
}

func (bb *_BSONBuilder) Get() BSON {
	switch {
	case bb.ptr != nil:
		return *bb.ptr
	case bb.arr != nil:
		return bb.arr.At(bb.elem).(BSON)
	case bb.obj != nil:
		return bb.obj[bb.key]
	}
	return nil;
}

func (bb *_BSONBuilder) Float64(f float64)	{ bb.Put(&_Number{f, _Null{}}) }
func (bb *_BSONBuilder) String(s string)	{ bb.Put(&_String{s, _Null{}}) }
func (bb *_BSONBuilder) Object()		{ bb.Put(&_Object{make(map[string]BSON), _Null{}}) }
func (bb *_BSONBuilder) Array()			{ bb.Put(&_Array{vector.New(0), _Null{}}) }
func (bb *_BSONBuilder) Bool(b bool)		{ bb.Put(&_Boolean{b, _Null{}}) }
func (bb *_BSONBuilder) Date(d int64)		{ bb.Put(&_Date{d, _Null{}}) }
func (bb *_BSONBuilder) Null()			{ bb.Put(Null) }
func (bb *_BSONBuilder) Regex(regex, options string) {
	bb.Put(&_Regex{regex, options, _Null{}})
}
func (bb *_BSONBuilder) Int32(i int32)	{ bb.Put(&_Int{i, _Null{}}) }
func (bb *_BSONBuilder) Int64(i int64)	{ bb.Put(&_Long{i, _Null{}}) }
func (bb *_BSONBuilder) OID(o []byte)	{ bb.Put(&_OID{o, _Null{}}) }

func (bb *_BSONBuilder) Key(key string) Builder {
	bb2 := new(_BSONBuilder);
	bb2.obj = bb.Get().(*_Object).value;
	bb2.key = key;
	bb2.obj[key] = Null;
	return bb2;
}

func (bb *_BSONBuilder) Elem(i int) Builder {
	bb2 := new(_BSONBuilder);
	bb2.arr = bb.Get().(*_Array).value;
	bb2.elem = i;
	for i >= bb2.arr.Len() {
		bb2.arr.Push(Null)
	}
	return bb2;
}

func (bb *_BSONBuilder) Flush()	{}

func BytesToBSON(b []byte) (BSON, os.Error) {
	var bson BSON;
	bb := new(_BSONBuilder);
	bb.ptr = &bson;
	bb.Object();
	err := Parse(bytes.NewBuffer(b[4:len(b)]), bb);
	return bson, err;
}

func ReadCString(buf *bytes.Buffer) string {
	out := bytes.NewBuffer([]byte{});
	var c byte;
	for c, _ = buf.ReadByte(); c != 0; c, _ = buf.ReadByte() {
		out.WriteByte(c)
	}

	return out.String();
}

func Parse(buf *bytes.Buffer, builder Builder) (err os.Error) {
	kind, _ := buf.ReadByte();
	err = nil;

	for kind != EOOKind {
		name := ReadCString(buf);
		b2 := builder.Key(name);

		switch kind {
		case NumberKind:
			lr := io.LimitReader(buf, 8);
			bits, _ := io.ReadAll(lr);
			ui64 := binary.LittleEndian.Uint64(bits);
			fl64 := math.Float64frombits(ui64);
			b2.Float64(fl64);
		case StringKind:
			bits, _ := io.ReadAll(io.LimitReader(buf, 4));
			l := binary.LittleEndian.Uint32(bits);
			s, _ := io.ReadAll(io.LimitReader(buf, int64(l-1)));
			buf.ReadByte();
			b2.String(string(s));
		case ObjectKind:
			b2.Object();
			io.ReadAll(io.LimitReader(buf, 4));
			err = Parse(buf, b2);
		case ArrayKind:
			b2.Array();
			io.ReadAll(io.LimitReader(buf, 4));
			err = Parse(buf, b2);
		case OIDKind:
			oid, _ := io.ReadAll(io.LimitReader(buf, 12));
			b2.OID(oid);
		case BooleanKind:
			b, _ := buf.ReadByte();
			if b == 1 {
				b2.Bool(true)
			} else {
				b2.Bool(false)
			}
		case DateKind:
			bits, _ := io.ReadAll(io.LimitReader(buf, 4));
			ui64 := binary.LittleEndian.Uint64(bits);
			b2.Date(int64(ui64));
		case RegexKind:
			regex := ReadCString(buf);
			options := ReadCString(buf);
			b2.Regex(regex, options);
		case IntKind:
			bits, _ := io.ReadAll(io.LimitReader(buf, 4));
			ui32 := binary.LittleEndian.Uint32(bits);
			b2.Int32(int32(ui32));
		case LongKind:
			bits, _ := io.ReadAll(io.LimitReader(buf, 8));
			ui64 := binary.LittleEndian.Uint64(bits);
			b2.Int64(int64(ui64));
		default:
			err = os.NewError(fmt.Sprintf("don't know how to handle kind %v yet", kind))
		}

		kind, _ = buf.ReadByte();
	}

	return;
}
