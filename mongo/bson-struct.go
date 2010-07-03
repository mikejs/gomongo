// Based on the Go json package.

// Copyright 2009 The Go Authors.  All rights reserved.
// Copyright 2009,2010, The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the 3-clause BSD License
// that can be found in the LICENSE and LICENSE.GO files.

package mongo

import (
	"reflect"
	"strings"
	"fmt"
	"os"
	"bytes"
	"time"
	"container/vector"
)

type structBuilder struct {
	val reflect.Value

	// if map_ != nil, write val to map_[key] on each change
	map_ *reflect.MapValue
	key  reflect.Value
}

var nobuilder *structBuilder

func isfloat(v reflect.Value) bool {
	switch v.(type) {
	case *reflect.FloatValue:
		return true
	}
	return false
}

func setfloat(v reflect.Value, f float64) {
	switch v := v.(type) {
	case *reflect.FloatValue:
		v.Set(f)
	}
}

func setint(v reflect.Value, i int64) {
	switch v := v.(type) {
	case *reflect.IntValue:
		v.Set(i)
	case *reflect.UintValue:
		v.Set(uint64(i))
	}
}

// If updating self.val is not enough to update the original,
// copy a changed self.val out to the original.
func (self *structBuilder) Flush() {
	if self == nil {
		return
	}
	if self.map_ != nil {
		self.map_.SetElem(self.key, self.val)
	}
}

func (self *structBuilder) Int64(i int64) {
	if self == nil {
		return
	}
	v := self.val
	if isfloat(v) {
		setfloat(v, float64(i))
	} else {
		setint(v, i)
	}
}

func (self *structBuilder) Date(t *time.Time) {
	if self == nil {
		return
	}
	if v, ok := self.val.(*reflect.PtrValue); ok {
		v.PointTo(reflect.Indirect(reflect.NewValue(t)))
	}
}

func (self *structBuilder) Int32(i int32) {
	if self == nil {
		return
	}
	v := self.val
	if isfloat(v) {
		setfloat(v, float64(i))
	} else {
		setint(v, int64(i))
	}
}

func (self *structBuilder) Float64(f float64) {
	if self == nil {
		return
	}
	v := self.val
	if isfloat(v) {
		setfloat(v, f)
	} else {
		setint(v, int64(f))
	}
}

func (self *structBuilder) Null() {}

func (self *structBuilder) String(s string) {
	if self == nil {
		return
	}
	if v, ok := self.val.(*reflect.StringValue); ok {
		v.Set(s)
	}
}

func (self *structBuilder) Regex(regex, options string) {
	// Ignore options for now...
	if self == nil {
		return
	}
	if v, ok := self.val.(*reflect.StringValue); ok {
		v.Set(regex)
	}
}

func (self *structBuilder) Bool(tf bool) {
	if self == nil {
		return
	}
	if v, ok := self.val.(*reflect.BoolValue); ok {
		v.Set(tf)
	}
}

func (self *structBuilder) OID(oid []byte) {
	if self == nil {
		return
	}
	if v, ok := self.val.(*reflect.SliceValue); ok {
		if v.Cap() < 12 {
			nv := reflect.MakeSlice(v.Type().(*reflect.SliceType), 12, 12)
			v.Set(nv)
		}
		for i := 0; i < 12; i++ {
			v.Elem(i).(*reflect.UintValue).Set(uint64(oid[i]))
		}
	}
}

func (self *structBuilder) Array() {
	if self == nil {
		return
	}
	if v, ok := self.val.(*reflect.SliceValue); ok {
		if v.IsNil() {
			v.Set(reflect.MakeSlice(v.Type().(*reflect.SliceType), 0, 8))
		}
	}
}

func (self *structBuilder) Elem(i int) Builder {
	if self == nil || i < 0 {
		return nobuilder
	}
	switch v := self.val.(type) {
	case *reflect.ArrayValue:
		if i < v.Len() {
			return &structBuilder{val: v.Elem(i)}
		}
	case *reflect.SliceValue:
		if i > v.Cap() {
			n := v.Cap()
			if n < 8 {
				n = 8
			}
			for n <= i {
				n *= 2
			}
			nv := reflect.MakeSlice(v.Type().(*reflect.SliceType), v.Len(), n)
			reflect.ArrayCopy(nv, v)
			v.Set(nv)
		}
		if v.Len() <= i && i < v.Cap() {
			v.SetLen(i + 1)
		}
		if i < v.Len() {
			return &structBuilder{val: v.Elem(i)}
		}
	}
	return nobuilder
}

func (self *structBuilder) Object() {
	if self == nil {
		return
	}
	if v, ok := self.val.(*reflect.PtrValue); ok && v.IsNil() {
		if v.IsNil() {
			v.PointTo(reflect.MakeZero(v.Type().(*reflect.PtrType).Elem()))
			self.Flush()
		}
		self.map_ = nil
		self.val = v.Elem()
	}
	if v, ok := self.val.(*reflect.MapValue); ok && v.IsNil() {
		v.Set(reflect.MakeMap(v.Type().(*reflect.MapType)))
	}
}

func (self *structBuilder) Key(k string) Builder {
	if self == nil {
		return nobuilder
	}
	switch v := reflect.Indirect(self.val).(type) {
	case *reflect.StructValue:
		t := v.Type().(*reflect.StructType)
		// Case-insensitive field lookup.
		k = strings.ToLower(k)
		for i := 0; i < t.NumField(); i++ {
			if strings.ToLower(t.Field(i).Name) == k {
				return &structBuilder{val: v.Field(i)}
			}
		}
	case *reflect.MapValue:
		t := v.Type().(*reflect.MapType)
		if t.Key() != reflect.Typeof(k) {
			break
		}
		key := reflect.NewValue(k)
		elem := v.Elem(key)
		if elem == nil {
			v.SetElem(key, reflect.MakeZero(t.Elem()))
			elem = v.Elem(key)
		}
		return &structBuilder{val: elem, map_: v, key: key}
	}
	return nobuilder
}

func Unmarshal(b []byte, val interface{}) (err os.Error) {
	sb := &structBuilder{val: reflect.NewValue(val)}
	err = Parse(bytes.NewBuffer(b[4:len(b)]), sb)
	return
}

func Marshal(val interface{}) (BSON, os.Error) {
	if val == nil {
		return Null, nil
	}

	switch v := val.(type) {
	case float64:
		return &_Number{v, _Null{}}, nil
	case string:
		return &_String{v, _Null{}}, nil
	case bool:
		return &_Boolean{v, _Null{}}, nil
	case int32:
		return &_Int{v, _Null{}}, nil
	case int64:
		return &_Long{v, _Null{}}, nil
	case int:
		return &_Long{int64(v), _Null{}}, nil
	case *time.Time:
		return &_Date{v, _Null{}}, nil
	}

	var value reflect.Value
	switch nv := reflect.NewValue(val).(type) {
	case *reflect.PtrValue:
		value = nv.Elem()
	default:
		value = nv
	}

	switch fv := value.(type) {
	case *reflect.StructValue:
		o := &_Object{map[string]BSON{}, _Null{}}
		t := fv.Type().(*reflect.StructType)
		for i := 0; i < t.NumField(); i++ {
			key := strings.ToLower(t.Field(i).Name)
			el, err := Marshal(fv.Field(i).Interface())
			if err != nil {
				return nil, err
			}
			o.value[key] = el
		}
		return o, nil
	case *reflect.MapValue:
		o := &_Object{map[string]BSON{}, _Null{}}
		mt := fv.Type().(*reflect.MapType)
		if mt.Key() != reflect.Typeof("") {
			return nil, os.NewError("can't marshall maps with non-string key types")
		}

		keys := fv.Keys()
		for _, k := range keys {
			sk := k.(*reflect.StringValue).Get()
			el, err := Marshal(fv.Elem(k).Interface())
			if err != nil {
				return nil, err
			}
			o.value[sk] = el
		}
		return o, nil
	case *reflect.SliceValue:
		a := &_Array{new(vector.Vector), _Null{}}
		for i := 0; i < fv.Len(); i++ {
			el, err := Marshal(fv.Elem(i).Interface())
			if err != nil {
				return nil, err
			}
			a.value.Push(el)
		}
		return a, nil
	default:
		return nil, os.NewError(fmt.Sprintf("don't know how to marshal %v\n", value.Type()))
	}

	return nil, nil
}

