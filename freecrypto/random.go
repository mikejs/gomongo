// To the extent possible under law, Authors have waived all copyright and
// related or neighboring rights to 'freecrypto'.

package rand

import (
	"os"
)

const _URANDOM = "/dev/urandom"


/* Reads the pseudo-random device to fills the array. */
func ReadUrandom(destination []byte) os.Error {
	fd, err := os.Open(_URANDOM, os.O_RDONLY, 0)
	if err != nil {
		return os.NewError("pseudo-random source malfunction: " + _URANDOM)
	}

	fd.Read(destination)
	fd.Close()

	return nil
}

/* Chooses random bytes from the source. */
func Choice(destination, source []byte) os.Error {
	// Fills the array `destination` with random bytes
	if err := ReadUrandom(destination); err != nil {
		return err
	}

	for i, b := range destination {
		tmp := int(b) % len(source)
		destination[i] = source[byte(tmp)]
	}

	return nil
}

