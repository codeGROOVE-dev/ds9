package datastore

import "errors"

var (
	// ErrNoSuchEntity is returned when an entity is not found.
	ErrNoSuchEntity = errors.New("datastore: no such entity")

	// ErrDone is returned by Iterator.Next when no more results are available.
	ErrDone = errors.New("datastore: no more results")
)
