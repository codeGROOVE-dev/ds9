package datastore_test

import (
	"context"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

// TestFlushPattern verifies the pattern used in sfcache's Flush method.
// It simulates GetAll with a KeysOnly query and a dummy destination.
func TestFlushPattern(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()
	kind := "CacheEntry"

	// 1. Put some entries
	keys := []*datastore.Key{
		datastore.NameKey(kind, "key1", nil),
		datastore.NameKey(kind, "key2", nil),
	}

	type Entry struct {
		Value string
	}

	entries := []Entry{
		{Value: "val1"},
		{Value: "val2"},
	}

	if _, err := client.PutMulti(ctx, keys, entries); err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	// 2. Simulate Flush: Query KeysOnly
	query := datastore.NewQuery(kind).KeysOnly()
	var dst []Entry // Dummy destination

	// 3. GetAll with dst
	gotKeys, err := client.GetAll(ctx, query, &dst)
	if err != nil {
		t.Fatalf("GetAll failed with KeysOnly: %v", err)
	}

	// 4. Verify keys returned
	if len(gotKeys) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(gotKeys))
	}

	// 5. Verify dst was NOT populated (optional, but expected behavior)
	if len(dst) != 0 {
		t.Errorf("Expected dst to remain empty, got %d items", len(dst))
	}

	// 6. DeleteMulti using returned keys
	if err := client.DeleteMulti(ctx, gotKeys); err != nil {
		t.Fatalf("DeleteMulti failed: %v", err)
	}

	// 7. Verify deletion
	qCheck := datastore.NewQuery(kind).KeysOnly()
	remaining, err := client.GetAll(ctx, qCheck, nil)
	if err != nil {
		t.Fatalf("Verification query failed: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("Expected 0 remaining items, got %d", len(remaining))
	}
}
