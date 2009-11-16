// Based on the Go json package.
// Original Copyright 2009 The Go Authors. All rights reserved.
// Modifications Copyright 2009 Michael Stephens.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE and LICENSE.GO files.

package mongo

import (
	"reflect";
	"strings";
	"bytes";
)

type structBuilder struct {
	val	reflect.Value;

	// if map_ != nil, write val to map_[key] on each change
	map_	*reflect.MapValue;
	key	reflect.Value;
}

var nobuilder *structBuilder

func isfloat(v reflect.Value) bool {
	switch v.(type) {
	case *reflect.FloatValue, *reflect.Float32Value, *reflect.Float64Value:
		return true
	}
	return false;
}

func setfloat(v reflect.Value, f float64) {
	switch v := v.(type) {
	case *reflect.FloatValue:
		v.Set(float(f))
	case *reflect.Float32Value:
		v.Set(float32(f))
	case *reflect.Float64Value:
		v.Set(float64(f))
	}
}

func setint(v reflect.Value, i int64) {
	switch v := v.(type) {
	case *reflect.IntValue:
		v.Set(int(i))
	case *reflect.Int8Value:
		v.Set(int8(i))
	case *reflect.Int16Value:
		v.Set(int16(i))
	case *reflect.Int32Value:
		v.Set(int32(i))
	case *reflect.Int64Value:
		v.Set(int64(i))
	case *reflect.UintValue:
		v.Set(uint(i))
	case *reflect.Uint8Value:
		v.Set(uint8(i))
	case *reflect.Uint16Value:
		v.Set(uint16(i))
	case *reflect.Uint32Value:
		v.Set(uint32(i))
	case *reflect.Uint64Value:
		v.Set(uint64(i))
	}
}

// If updating b.val is not enough to update the original,
// copy a changed b.val out to the original.
func (b *structBuilder) Flush() {
	if b == nil {
		return
	}
	if b.map_ != nil {
		b.map_.SetElem(b.key, b.val)
	}
}

func (b *structBuilder) Int64(i int64) {
	if b == nil {
		return
	}
	v := b.val;
	if isfloat(v) {
		setfloat(v, float64(i))
	} else {
		setint(v, i)
	}
}

func (b *structBuilder) Date(d int64) {
	if b == nil {
		return
	}
	v := b.val;
	setint(v, d);
}

func (b *structBuilder) Int32(i int32) {
	if b == nil {
		return
	}
	v := b.val;
	if isfloat(v) {
		setfloat(v, float64(i))
	} else {
		setint(v, int64(i))
	}
}

func (b *structBuilder) Float64(f float64) {
	if b == nil {
		return
	}
	v := b.val;
	if isfloat(v) {
		setfloat(v, f)
	} else {
		setint(v, int64(f))
	}
}

func (b *structBuilder) Null()	{}

func (b *structBuilder) String(s string) {
	if b == nil {
		return
	}
	if v, ok := b.val.(*reflect.StringValue); ok {
		v.Set(s)
	}
}

func (b *structBuilder) Regex(regex, options string) {
	// Ignore options for now...
	if b == nil {
		return
	}
	if v, ok := b.val.(*reflect.StringValue); ok {
		v.Set(regex)
	}
}

func (b *structBuilder) Bool(tf bool) {
	if b == nil {
		return
	}
	if v, ok := b.val.(*reflect.BoolValue); ok {
		v.Set(tf)
	}
}

func (b *structBuilder) OID(oid []byte) {
	if b == nil {
		return
	}
	if v, ok := b.val.(*reflect.SliceValue); ok {
		if v.Cap() < 12 {
			nv := reflect.MakeSlice(v.Type().(*reflect.SliceType), 12, 12);
			v.Set(nv);
		}
		for i := 0; i < 12; i++ {
			v.Elem(i).(*reflect.Uint8Value).Set(oid[i])
		}
	}
}

func (b *structBuilder) Array() {
	if b == nil {
		return
	}
	if v, ok := b.val.(*reflect.SliceValue); ok {
		if v.IsNil() {
			v.Set(reflect.MakeSlice(v.Type().(*reflect.SliceType), 0, 8))
		}
	}
}

func (b *structBuilder) Elem(i int) Builder {
	if b == nil || i < 0 {
		return nobuilder
	}
	switch v := b.val.(type) {
	case *reflect.ArrayValue:
		if i < v.Len() {
			return &structBuilder{val: v.Elem(i)}
		}
	case *reflect.SliceValue:
		if i > v.Cap() {
			n := v.Cap();
			if n < 8 {
				n = 8
			}
			for n <= i {
				n *= 2
			}
			nv := reflect.MakeSlice(v.Type().(*reflect.SliceType), v.Len(), n);
			reflect.ArrayCopy(nv, v);
			v.Set(nv);
		}
		if v.Len() <= i && i < v.Cap() {
			v.SetLen(i + 1)
		}
		if i < v.Len() {
			return &structBuilder{val: v.Elem(i)}
		}
	}
	return nobuilder;
}

func (b *structBuilder) Object() {
	if b == nil {
		return
	}
	if v, ok := b.val.(*reflect.PtrValue); ok && v.IsNil() {
		if v.IsNil() {
			v.PointTo(reflect.MakeZero(v.Type().(*reflect.PtrType).Elem()));
			b.Flush();
		}
		b.map_ = nil;
		b.val = v.Elem();
	}
	if v, ok := b.val.(*reflect.MapValue); ok && v.IsNil() {
		v.Set(reflect.MakeMap(v.Type().(*reflect.MapType)))
	}
}

func (b *structBuilder) Key(k string) Builder {
	if b == nil {
		return nobuilder
	}
	switch v := reflect.Indirect(b.val).(type) {
	case *reflect.StructValue:
		t := v.Type().(*reflect.StructType);
		// Case-insensitive field lookup.
		k = strings.ToLower(k);
		for i := 0; i < t.NumField(); i++ {
			if strings.ToLower(t.Field(i).Name) == k {
				return &structBuilder{val: v.Field(i)}
			}
		}
	case *reflect.MapValue:
		t := v.Type().(*reflect.MapType);
		if t.Key() != reflect.Typeof(k) {
			break
		}
		key := reflect.NewValue(k);
		elem := v.Elem(key);
		if elem == nil {
			v.SetElem(key, reflect.MakeZero(t.Elem()));
			elem = v.Elem(key);
		}
		return &structBuilder{val: elem, map_: v, key: key};
	}
	return nobuilder;
}

func Unmarshal(b []byte, val interface{}) (ok bool) {
	sb := &structBuilder{val: reflect.NewValue(val)};
	ok = Parse(bytes.NewBuffer(b[4:len(b)]), sb);
	if !ok {
		return false
	}
	return true;
}
