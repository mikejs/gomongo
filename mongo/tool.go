// Copyright 2009,2010 The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the New BSD License
// that can be found in the LICENSE file.

package mongo

import (
	"encoding/binary"
)


// Like BSON documents, all data in the mongo wire protocol is little-endian.
var pack = binary.LittleEndian


// *** Bits data
// ***

func setBit32(num *int32, position ...byte) {
	const MASK = 1

	for _, pos := range position {
		*num |= MASK << pos
	}
}

