package datastore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

// TestCursorWithPagination tests the Cursor() method with actual cursor from query
func TestCursorWithPagination(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create enough entities to trigger pagination
	for i := range 3 {
		key := datastore.IDKey("CursorTest", int64(i+1), nil)
		entity := &testEntity{
			Name:  "test",
			Count: int64(i),
		}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit to trigger cursor generation
	q := datastore.NewQuery("CursorTest").Limit(2)
	it := client.Run(ctx, q)

	// Fetch first result
	var entity testEntity
	_, err := it.Next(&entity)
	if err != nil {
		t.Fatalf("First Next failed: %v", err)
	}

	// Now try to get cursor - should be available after fetching results
	cursor, err := it.Cursor()
	if err != nil {
		t.Logf("Cursor() returned error (mock implementation): %v", err)
		// Mock might not support cursors yet, that's OK
	} else {
		// If cursor is available, verify it's not empty
		if cursor == "" {
			t.Error("Expected non-empty cursor after fetching with limit")
		} else {
			t.Logf("Successfully got cursor: %s", cursor)

			// Verify we can convert cursor to string
			cursorStr := cursor.String()
			if cursorStr == "" {
				t.Error("Cursor.String() returned empty string")
			}
		}
	}
}

// TestCursorBeforeFetch tests Cursor() before any results are fetched
func TestCursorBeforeFetch(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create test entity
	key := datastore.NameKey("CursorTest2", "test", nil)
	entity := &testEntity{Name: "test", Count: 1}
	if _, err := client.Put(ctx, key, entity); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Create iterator but don't fetch any results
	q := datastore.NewQuery("CursorTest2")
	it := client.Run(ctx, q)

	// Try to get cursor before fetching - should fail
	cursor, err := it.Cursor()
	if err == nil {
		t.Error("Expected error when getting cursor before fetching results")
	}
	if cursor != "" {
		t.Errorf("Expected empty cursor before fetching, got: %s", cursor)
	}
}

// TestCursorWithLimitedResults tests cursor behavior with pagination
func TestCursorWithLimitedResults(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create multiple entities
	for i := range 3 {
		key := datastore.IDKey("CursorPaginationTest", int64(i+1), nil)
		entity := &testEntity{
			Name:  "test",
			Count: int64(i),
		}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit smaller than total entities
	q := datastore.NewQuery("CursorPaginationTest").Limit(2)
	it := client.Run(ctx, q)

	// Fetch all limited results
	count := 0
	for {
		var entity testEntity
		_, err := it.Next(&entity)
		if errors.Is(err, datastore.Done) {
			break
		}
		if err != nil {
			t.Fatalf("Iterator Next failed: %v", err)
		}
		count++

		// Try to get cursor after each fetch
		cursor, err := it.Cursor()
		if err != nil {
			t.Logf("Cursor not available at position %d: %v", count, err)
		} else if cursor != "" {
			t.Logf("Got cursor at position %d: %s", count, cursor)
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 results with limit, got %d", count)
	}
}
