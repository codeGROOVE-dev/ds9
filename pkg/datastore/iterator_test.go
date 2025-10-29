package datastore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

func TestIterator(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("IterateAll", func(t *testing.T) {
		// Create test entities
		for i := range 5 {
			key := datastore.IDKey("IterTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  fmt.Sprintf("entity-%d", i),
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		q := datastore.NewQuery("IterTest")
		it := client.Run(ctx, q)

		count := 0
		for {
			var entity testEntity
			key, err := it.Next(&entity)
			if errors.Is(err, datastore.ErrDone) {
				break
			}
			if err != nil {
				t.Fatalf("Iterator.Next failed: %v", err)
			}
			if key == nil {
				t.Errorf("Expected non-nil key")
			}
			count++
		}

		if count != 5 {
			t.Errorf("Expected to iterate over 5 entities, got %d", count)
		}
	})

	t.Run("IteratorCursor", func(t *testing.T) {
		// Create test entities
		for i := range 3 {
			key := datastore.IDKey("CursorTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  fmt.Sprintf("entity-%d", i),
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		q := datastore.NewQuery("CursorTest")
		it := client.Run(ctx, q)

		var entity testEntity
		_, err := it.Next(&entity)
		if err != nil {
			t.Fatalf("Iterator.Next failed: %v", err)
		}

		// Get cursor after first entity
		cursor, err := it.Cursor()
		if err != nil {
			t.Logf("Cursor not available: %v", err)
		} else if cursor == "" {
			t.Logf("Empty cursor returned")
		}
	})

	t.Run("EmptyIterator", func(t *testing.T) {
		q := datastore.NewQuery("NonExistent")
		it := client.Run(ctx, q)

		var entity testEntity
		_, err := it.Next(&entity)
		if !errors.Is(err, datastore.ErrDone) {
			t.Errorf("Expected datastore.ErrDone, got %v", err)
		}
	})
}
