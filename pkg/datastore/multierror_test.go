package datastore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

// TestMultiErrorGetMulti tests that GetMulti returns MultiError with per-item errors
func TestMultiErrorGetMulti(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create some test entities
	type TestEntity struct {
		Name string
		Age  int
	}

	key1 := datastore.NameKey("TestEntity", "exists1", nil)
	key2 := datastore.NameKey("TestEntity", "notfound", nil)
	key3 := datastore.NameKey("TestEntity", "exists2", nil)

	// Put only key1 and key3
	entity1 := &TestEntity{Name: "Alice", Age: 30}
	entity3 := &TestEntity{Name: "Charlie", Age: 35}

	if _, err := client.Put(ctx, key1, entity1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if _, err := client.Put(ctx, key3, entity3); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// GetMulti with missing key
	keys := []*datastore.Key{key1, key2, key3}
	dst := make([]TestEntity, len(keys))

	err := client.GetMulti(ctx, keys, &dst)
	if err == nil {
		t.Fatal("Expected error for missing entity, got nil")
	}

	// Check that it's a MultiError
	var multiErr datastore.MultiError
	ok := errors.As(err, &multiErr)
	if !ok {
		t.Fatalf("Expected MultiError, got %T: %v", err, err)
	}

	// Verify the MultiError has correct length
	if len(multiErr) != len(keys) {
		t.Errorf("Expected MultiError length %d, got %d", len(keys), len(multiErr))
	}

	// Verify individual errors
	if multiErr[0] != nil {
		t.Errorf("Expected no error for key1, got: %v", multiErr[0])
	}
	if !errors.Is(multiErr[1], datastore.ErrNoSuchEntity) {
		t.Errorf("Expected ErrNoSuchEntity for key2, got: %v", multiErr[1])
	}
	if multiErr[2] != nil {
		t.Errorf("Expected no error for key3, got: %v", multiErr[2])
	}

	// Verify successful entities were decoded
	if dst[0].Name != "Alice" {
		t.Errorf("Expected dst[0].Name = 'Alice', got %q", dst[0].Name)
	}
	if dst[2].Name != "Charlie" {
		t.Errorf("Expected dst[2].Name = 'Charlie', got %q", dst[2].Name)
	}
}

// TestMultiErrorGetMulti_AllMissing tests GetMulti when all keys are missing
func TestMultiErrorGetMulti_AllMissing(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string
	}

	keys := []*datastore.Key{
		datastore.NameKey("TestEntity", "missing1", nil),
		datastore.NameKey("TestEntity", "missing2", nil),
	}
	dst := make([]TestEntity, len(keys))

	err := client.GetMulti(ctx, keys, &dst)
	if err == nil {
		t.Fatal("Expected error for missing entities, got nil")
	}

	var multiErr datastore.MultiError
	ok := errors.As(err, &multiErr)
	if !ok {
		t.Fatalf("Expected MultiError, got %T", err)
	}

	for i, e := range multiErr {
		if !errors.Is(e, datastore.ErrNoSuchEntity) {
			t.Errorf("Expected ErrNoSuchEntity at index %d, got: %v", i, e)
		}
	}
}

// TestMultiErrorGetMulti_NilKeys tests GetMulti with nil keys
func TestMultiErrorGetMulti_NilKeys(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string
	}

	key1 := datastore.NameKey("TestEntity", "valid", nil)
	keys := []*datastore.Key{key1, nil, datastore.NameKey("TestEntity", "valid2", nil)}
	dst := make([]TestEntity, len(keys))

	err := client.GetMulti(ctx, keys, &dst)
	if err == nil {
		t.Fatal("Expected error for nil key, got nil")
	}

	var multiErr datastore.MultiError
	ok := errors.As(err, &multiErr)
	if !ok {
		t.Fatalf("Expected MultiError, got %T", err)
	}

	if multiErr[0] != nil {
		t.Errorf("Expected no error for key[0], got: %v", multiErr[0])
	}
	if multiErr[1] == nil {
		t.Error("Expected error for nil key at index 1")
	} else if !errors.Is(multiErr[1], datastore.ErrInvalidKey) {
		t.Errorf("Expected ErrInvalidKey for nil key, got: %v", multiErr[1])
	}
	if multiErr[2] != nil {
		t.Errorf("Expected no error for key[2], got: %v", multiErr[2])
	}
}

// TestMultiErrorPutMulti tests that PutMulti returns MultiError for encoding errors
func TestMultiErrorPutMulti(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string
	}

	key1 := datastore.NameKey("TestEntity", "valid1", nil)
	key2 := datastore.NameKey("TestEntity", "valid2", nil)

	keys := []*datastore.Key{key1, nil, key2}
	entitiesWithNil := []TestEntity{
		{Name: "Alice"},
		{Name: "Bob"},
		{Name: "Charlie"},
	}

	// Try to put with a nil key
	_, err := client.PutMulti(ctx, keys, entitiesWithNil)
	if err == nil {
		t.Fatal("Expected error for nil key, got nil")
	}

	var multiErr datastore.MultiError
	ok := errors.As(err, &multiErr)
	if !ok {
		t.Fatalf("Expected MultiError, got %T: %v", err, err)
	}

	if len(multiErr) != len(keys) {
		t.Errorf("Expected MultiError length %d, got %d", len(keys), len(multiErr))
	}

	if multiErr[0] != nil {
		t.Errorf("Expected no error for key[0], got: %v", multiErr[0])
	}
	if !errors.Is(multiErr[1], datastore.ErrInvalidKey) {
		t.Errorf("Expected ErrInvalidKey for nil key, got: %v", multiErr[1])
	}
	if multiErr[2] != nil {
		t.Errorf("Expected no error for key[2], got: %v", multiErr[2])
	}
}

// TestMultiErrorDeleteMulti tests that DeleteMulti returns MultiError for invalid keys
func TestMultiErrorDeleteMulti(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	key1 := datastore.NameKey("TestEntity", "valid1", nil)
	key2 := datastore.NameKey("TestEntity", "valid2", nil)

	keys := []*datastore.Key{key1, nil, key2}

	err := client.DeleteMulti(ctx, keys)
	if err == nil {
		t.Fatal("Expected error for nil key, got nil")
	}

	var multiErr datastore.MultiError
	ok := errors.As(err, &multiErr)
	if !ok {
		t.Fatalf("Expected MultiError, got %T", err)
	}

	if len(multiErr) != len(keys) {
		t.Errorf("Expected MultiError length %d, got %d", len(keys), len(multiErr))
	}

	if multiErr[0] != nil {
		t.Errorf("Expected no error for key[0], got: %v", multiErr[0])
	}
	if !errors.Is(multiErr[1], datastore.ErrInvalidKey) {
		t.Errorf("Expected ErrInvalidKey for nil key, got: %v", multiErr[1])
	}
	if multiErr[2] != nil {
		t.Errorf("Expected no error for key[2], got: %v", multiErr[2])
	}
}

// TestMultiErrorFormatting tests the MultiError.Error() method
func TestMultiErrorFormatting(t *testing.T) {
	tests := []struct {
		name     string
		err      datastore.MultiError
		expected string
	}{
		{
			name:     "zero errors",
			err:      datastore.MultiError{nil, nil},
			expected: "(0 errors)",
		},
		{
			name:     "one error",
			err:      datastore.MultiError{errors.New("first error"), nil},
			expected: "first error",
		},
		{
			name:     "two errors",
			err:      datastore.MultiError{errors.New("first error"), errors.New("second error")},
			expected: "first error (and 1 other error)",
		},
		{
			name:     "three errors",
			err:      datastore.MultiError{errors.New("first error"), errors.New("second error"), errors.New("third error")},
			expected: "first error (and 2 other errors)",
		},
		{
			name:     "mixed nil and errors",
			err:      datastore.MultiError{nil, errors.New("second error"), nil, errors.New("fourth error")},
			expected: "second error (and 1 other error)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.err.Error()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// TestMultiErrorGetMulti_Success tests that no error is returned when all gets succeed
func TestMultiErrorGetMulti_Success(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string
	}

	key1 := datastore.NameKey("TestEntity", "exists1", nil)
	key2 := datastore.NameKey("TestEntity", "exists2", nil)

	entity1 := &TestEntity{Name: "Alice"}
	entity2 := &TestEntity{Name: "Bob"}

	if _, err := client.Put(ctx, key1, entity1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if _, err := client.Put(ctx, key2, entity2); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	keys := []*datastore.Key{key1, key2}
	dst := make([]TestEntity, len(keys))

	err := client.GetMulti(ctx, keys, &dst)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	if dst[0].Name != "Alice" {
		t.Errorf("Expected dst[0].Name = 'Alice', got %q", dst[0].Name)
	}
	if dst[1].Name != "Bob" {
		t.Errorf("Expected dst[1].Name = 'Bob', got %q", dst[1].Name)
	}
}
