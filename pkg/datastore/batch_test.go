package datastore_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

func TestBatchOperations(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type Item struct {
		ID int
	}

	// Number of items > 1000 to test batching limits
	// Put limit: 500, Get limit: 1000
	const count = 1200
	keys := make([]*datastore.Key, count)
	items := make([]Item, count)

	for i := range count {
		keys[i] = datastore.NameKey("Item", fmt.Sprintf("item-%d", i), nil)
		items[i] = Item{ID: i}
	}

	// Test PutMulti
	if _, err := client.PutMulti(ctx, keys, items); err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	// Test GetMulti
	results := make([]Item, count)
	if err := client.GetMulti(ctx, keys, &results); err != nil {
		t.Fatalf("GetMulti failed: %v", err)
	}

	for i := range count {
		if results[i].ID != i {
			t.Errorf("Item %d mismatch: got %d, want %d", i, results[i].ID, i)
		}
	}

	// Test DeleteMulti
	if err := client.DeleteMulti(ctx, keys); err != nil {
		t.Fatalf("DeleteMulti failed: %v", err)
	}

	// Verify deletion
	err := client.GetMulti(ctx, keys, &results)
	// Should return MultiError with all ErrNoSuchEntity
	if err == nil {
		t.Fatal("Expected error after deletion, got nil")
	}
}

func TestAllocateIDsBatch(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Test AllocateIDs > 500
	const count = 600
	keys := make([]*datastore.Key, count)
	for i := range count {
		keys[i] = datastore.IncompleteKey("Item", nil)
	}

	allocated, err := client.AllocateIDs(ctx, keys)
	if err != nil {
		t.Fatalf("AllocateIDs failed: %v", err)
	}

	if len(allocated) != count {
		t.Fatalf("Expected %d keys, got %d", count, len(allocated))
	}

	seen := make(map[int64]bool)
	for i, k := range allocated {
		if k.Incomplete() {
			t.Errorf("Key %d is incomplete", i)
		}
		if seen[k.ID] {
			t.Errorf("Duplicate ID %d at index %d", k.ID, i)
		}
		seen[k.ID] = true
	}
}
