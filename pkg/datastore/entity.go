package datastore

import "errors"

// Entity encoding/decoding errors.
var (
	errNotStruct     = errors.New("src must be a struct or pointer to struct")
	errNotStructPtr  = errors.New("dst must be a pointer to struct")
	errInvalidEntity = errors.New("invalid entity format")
)
