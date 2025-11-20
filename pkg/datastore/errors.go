package datastore

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidEntityType is returned when functions like Get or Next are
	// passed a dst or src argument of invalid type.
	ErrInvalidEntityType = errors.New("datastore: invalid entity type")

	// ErrInvalidKey is returned when an invalid key is presented.
	ErrInvalidKey = errors.New("datastore: invalid key")

	// ErrNoSuchEntity is returned when no entity was found for a given key.
	ErrNoSuchEntity = errors.New("datastore: no such entity")

	// ErrConcurrentTransaction is returned when a transaction is used concurrently.
	ErrConcurrentTransaction = errors.New("datastore: concurrent transaction")

	// Done is returned by Iterator.Next when no more results are available.
	// This matches the official cloud.google.com/go/datastore API.
	//
	//nolint:revive,errname,staticcheck // Name must match official API (iterator.Done)
	Done = errors.New("datastore: no more items in iterator")
)

// MultiError is returned by batch operations when there are errors with
// particular elements. Errors will be in a one-to-one correspondence with
// the input elements; successful elements will have a nil entry.
type MultiError []error

func (m MultiError) Error() string {
	s, n := "", 0
	for _, e := range m {
		if e != nil {
			if n == 0 {
				s = e.Error()
			}
			n++
		}
	}
	switch n {
	case 0:
		return "(0 errors)"
	case 1:
		return s
	case 2:
		return s + " (and 1 other error)"
	}
	return fmt.Sprintf("%s (and %d other errors)", s, n-1)
}
