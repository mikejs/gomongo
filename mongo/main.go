// Copyright 2010, The 'gomongo' Authors.  All rights reserved.
// Use of this source code is governed by the 3-clause BSD License
// that can be found in the LICENSE file.

// Note that Go's cryptography library is copyrighted by an USA company,
// so it's liable to cryptography regulations from USA.
// That viral law affects to any program where it's being used,
// so it can not be used in some third countries (where USA say you).

package mongo

import (
	"encoding/binary"
	"rand"
	//crand "crypto/rand"
	
	crand "freecrypto/rand"
)


// Like BSON documents, all data in the mongo wire protocol is little-endian.
var pack = binary.LittleEndian

// Words size in bytes.
var (
	_WORD32 = 4
	_WORD64 = 8
)

var lastRequestID int32


func init() {
	// Uses the 'urandom' device to get a seed which will be used by 'rand'.
	randombytes := make([]byte, _WORD64)

	//if _, err := crand.Read(randombytes); err != nil {
	if err := crand.ReadUrandom(randombytes); err != nil {
		panic(err)
	}

	random := pack.Uint64(randombytes)
	// If you seed it with something predictable like the time, the risk is obvious.
	// rand.Seed(time.Nanoseconds())
	rand.Seed(int64(random))
}


// === Utility functions
// ===

/* Gets a random request identifier different to the last one.

To check anytime the server is sending a response ('opQuery', 'opGetMore').
*/
func getRequestID() int32 {
	id := rand.Int31()

	if id == lastRequestID {
		return getRequestID()
	}
	lastRequestID = id

	return id
}

// === Bits data

func setBit32(num *int32, position ...byte) {
	const MASK = 1

	for _, pos := range position {
		*num |= MASK << pos
	}
}

