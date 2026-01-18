package datastore

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestMock_KeyRangeQuery(t *testing.T) {
	client, cleanup := NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Define entity type
	type CacheEntry struct {
		Value string `datastore:"value"`
	}

	// Store some entities
	keys := []string{"user:alice.j", "user:bob.j", "user:charlie.j", "post:1.j", "post:2.j"}
	for _, keyName := range keys {
		key := NameKey("CacheEntry", keyName, nil)
		entity := &CacheEntry{
			Value: keyName,
		}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put(%s): %v", keyName, err)
		}
	}

	// Query with __key__ range filter
	start := NameKey("CacheEntry", "user:.j", nil)
	end := NameKey("CacheEntry", "user:\xff.j", nil)

	q := NewQuery("CacheEntry").
		Filter("__key__ >=", start).
		Filter("__key__ <", end).
		KeysOnly()

	it := client.Run(ctx, q)
	var found []string
	for {
		key, err := it.Next(nil)
		if err != nil {
			if errors.Is(err, Done) {
				break
			}
			t.Fatalf("Next: %v", err)
		}
		found = append(found, key.Name)
	}

	fmt.Printf("Found keys: %v\n", found)

	// Should find 3 user keys
	if len(found) != 3 {
		t.Errorf("Found %d keys; want 3. Keys: %v", len(found), found)
	}

	// Verify they're the right ones
	wantKeys := map[string]bool{
		"user:alice.j":   true,
		"user:bob.j":     true,
		"user:charlie.j": true,
	}
	for _, k := range found {
		if !wantKeys[k] {
			t.Errorf("Unexpected key: %s", k)
		}
	}
}
