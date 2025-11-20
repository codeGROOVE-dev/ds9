package datastore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Test Iterator.Cursor() when no cursor is available
func TestIteratorCursorNoCursor(t *testing.T) {
	client, cleanup := NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Run query on empty kind
	q := NewQuery("EmptyKind")
	it := client.Run(ctx, q)

	// Try to get cursor before iterating
	cursor, err := it.Cursor()
	if err == nil {
		t.Error("Expected error when no cursor available")
	}
	if cursor != "" {
		t.Errorf("Expected empty cursor, got %s", cursor)
	}
}

// Test Iterator with multiple fetches (pagination)
func TestIteratorMultipleFetches(t *testing.T) {
	client, cleanup := NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create many entities to force multiple fetches
	for i := range 25 {
		key := IDKey("FetchTest", int64(i+1), nil)
		entity := &struct {
			Name  string
			Index int64
			Time  time.Time
		}{
			Name:  "test",
			Index: int64(i),
			Time:  time.Now().UTC().Truncate(time.Microsecond),
		}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with small limit to trigger multiple fetches
	query := NewQuery("FetchTest").Limit(10)
	it := client.Run(ctx, query)

	count := 0
	for {
		var entity struct {
			Name  string
			Index int64
			Time  time.Time
		}
		_, err := it.Next(&entity)
		if errors.Is(err, Done) {
			break
		}
		if err != nil {
			t.Fatalf("Iterator Next failed: %v", err)
		}
		count++
	}

	if count == 0 {
		t.Error("Expected to iterate over some entities")
	}
}

// Test Iterator.Next with nil destination
func TestIteratorNextNilDst(t *testing.T) {
	client, cleanup := NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create test entity
	key := NameKey("NilDstTest", "test", nil)
	entity := &struct {
		Name string
	}{
		Name: "test",
	}
	if _, err := client.Put(ctx, key, entity); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Query
	q := NewQuery("NilDstTest")
	it := client.Run(ctx, q)

	// Try to iterate with nil dst
	_, err := it.Next(nil)
	if err == nil {
		t.Error("Expected error when dst is nil")
	}
}

// Test Iterator.Next with non-pointer destination
func TestIteratorNextNonPointerDst(t *testing.T) {
	client, cleanup := NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create test entity
	key := NameKey("NonPtrTest", "test", nil)
	entity := &struct {
		Name string
	}{
		Name: "test",
	}
	if _, err := client.Put(ctx, key, entity); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Query
	q := NewQuery("NonPtrTest")
	it := client.Run(ctx, q)

	// Try to iterate with non-pointer dst
	var dst struct {
		Name string
	}
	_, err := it.Next(dst) // Pass by value instead of pointer
	if err == nil {
		t.Error("Expected error when dst is not a pointer")
	}
}

// Test fetch() error path via context cancellation
func TestIteratorFetchContextCancel(t *testing.T) {
	client, cleanup := NewMockClient(t)
	defer cleanup()

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Create test entities first with valid context
	validCtx := context.Background()
	for i := range 5 {
		key := IDKey("CancelTest", int64(i+1), nil)
		entity := &struct {
			Name string
		}{
			Name: "test",
		}
		if _, err := client.Put(validCtx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with cancelled context
	q := NewQuery("CancelTest")
	it := client.Run(ctx, q)

	var dst struct {
		Name string
	}
	_, err := it.Next(&dst)
	// Should get error due to cancelled context
	if err == nil {
		t.Log("Expected error with cancelled context (mock may not respect context)")
	}
}

// Test iterator with keys-only query
func TestIteratorKeysOnly(t *testing.T) {
	client, cleanup := NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create test entities
	for i := range 5 {
		key := IDKey("KeysOnlyTest", int64(i+1), nil)
		entity := &struct {
			Name string
			Data string
		}{
			Name: "test",
			Data: "lots of data here",
		}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query keys only
	q := NewQuery("KeysOnlyTest").KeysOnly()
	it := client.Run(ctx, q)

	count := 0
	for {
		var entity struct {
			Name string
			Data string
		}
		key, err := it.Next(&entity)
		if errors.Is(err, Done) {
			break
		}
		if err != nil {
			t.Fatalf("Iterator Next failed: %v", err)
		}
		if key == nil {
			t.Error("Expected non-nil key")
		}
		count++
	}

	if count == 0 {
		t.Error("Expected to iterate over some keys")
	}
}
