package datastore

import "errors"

// Cursor represents a query cursor for pagination.
// API compatible with cloud.google.com/go/datastore.
type Cursor string

// String returns the cursor as a string.
func (c Cursor) String() string {
	return string(c)
}

// DecodeCursor decodes a cursor string.
// API compatible with cloud.google.com/go/datastore.
func DecodeCursor(s string) (Cursor, error) {
	if s == "" {
		return "", errors.New("empty cursor string")
	}
	return Cursor(s), nil
}
