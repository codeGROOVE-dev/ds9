package datastore_test

import (
	"time"
)

// testEntity represents a simple test entity used across multiple test files.
type testEntity struct {
	UpdatedAt time.Time `datastore:"updated_at"`
	Name      string    `datastore:"name"`
	Notes     string    `datastore:"notes,noindex"`
	Count     int64     `datastore:"count"`
	Score     float64   `datastore:"score"`
	Active    bool      `datastore:"active"`
}

// arrayEntity is used for testing slice/array fields.
type arrayEntity struct {
	Strings []string  `datastore:"strings,omitempty"`
	Ints    []int64   `datastore:"ints,omitempty"`
	Floats  []float64 `datastore:"floats,omitempty"`
	Bools   []bool    `datastore:"bools,omitempty"`
}
