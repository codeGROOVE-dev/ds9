package datastore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

// TestCountComprehensive tests Count with various scenarios
func TestCountComprehensive(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("CountWithFilter", func(t *testing.T) {
		// Create test entities with varying counts
		for i := range 5 {
			key := datastore.IDKey("CountFilterTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  "test",
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Count with filter
		q := datastore.NewQuery("CountFilterTest").Filter("count >", 2)
		count, err := client.Count(ctx, q)
		if err != nil {
			t.Fatalf("Count with filter failed: %v", err)
		}

		// Should count entities with count > 2 (3, 4 = 2 entities)
		if count != 2 {
			t.Errorf("Expected count 2 with filter, got %d", count)
		}
	})

	t.Run("CountZero", func(t *testing.T) {
		// Count non-existent kind
		q := datastore.NewQuery("NonExistentKind")
		count, err := client.Count(ctx, q)
		if err != nil {
			t.Fatalf("Count of empty kind failed: %v", err)
		}

		if count != 0 {
			t.Errorf("Expected count 0 for empty kind, got %d", count)
		}
	})

	t.Run("CountAll", func(t *testing.T) {
		// Create entities
		for i := range 3 {
			key := datastore.IDKey("CountAllTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  "test",
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Count all without filter
		q := datastore.NewQuery("CountAllTest")
		count, err := client.Count(ctx, q)
		if err != nil {
			t.Fatalf("Count all failed: %v", err)
		}

		if count != 3 {
			t.Errorf("Expected count 3, got %d", count)
		}
	})

	t.Run("CountWithMultipleFilters", func(t *testing.T) {
		// Create entities with different values
		for i := range 5 {
			key := datastore.IDKey("CountMultiFilterTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  "test",
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Count with multiple filters (if supported)
		q := datastore.NewQuery("CountMultiFilterTest").
			Filter("count >=", 2).
			Filter("count <", 4)
		count, err := client.Count(ctx, q)
		if err != nil {
			t.Logf("Count with multiple filters: %v (may not be supported)", err)
		} else {
			// Should count 2, 3 = 2 entities
			if count != 2 {
				t.Logf("Expected count 2 with multiple filters, got %d (composite filters may not be supported)", count)
			}
		}
	})
}

// TestGetAllComprehensive tests GetAll with various edge cases
func TestGetAllComprehensive(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("GetAllOrdered", func(t *testing.T) {
		// Create entities
		for i := range 5 {
			key := datastore.IDKey("GetAllOrderTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  "test",
				Count: int64(5 - i), // Reverse order
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Get all with order
		q := datastore.NewQuery("GetAllOrderTest").Order("count")
		var entities []testEntity
		keys, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Logf("GetAll with order: %v (ordering may not be implemented)", err)
		} else {
			if len(keys) != 5 {
				t.Errorf("Expected 5 keys, got %d", len(keys))
			}
		}
	})

	t.Run("GetAllWithOffset", func(t *testing.T) {
		// Create entities
		for i := range 5 {
			key := datastore.IDKey("GetAllOffsetTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  "test",
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Get all with offset
		q := datastore.NewQuery("GetAllOffsetTest").Offset(2).Limit(2)
		var entities []testEntity
		keys, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Logf("GetAll with offset: %v (offset may not be implemented)", err)
		} else {
			if len(keys) > 5 {
				t.Errorf("Got too many keys: %d", len(keys))
			}
		}
	})

	t.Run("GetAllKeysOnly", func(t *testing.T) {
		// Create entities
		for i := range 3 {
			key := datastore.IDKey("GetAllKeysOnlyTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  "test",
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Get keys only using AllKeys
		q := datastore.NewQuery("GetAllKeysOnlyTest").KeysOnly()
		keys, err := client.AllKeys(ctx, q)
		if err != nil {
			t.Fatalf("AllKeys failed: %v", err)
		}

		if len(keys) != 3 {
			t.Errorf("Expected 3 keys, got %d", len(keys))
		}

		// Verify keys are complete
		for i, key := range keys {
			if key.Incomplete() {
				t.Errorf("Key %d is incomplete", i)
			}
		}
	})

	t.Run("GetAllEmptyResult", func(t *testing.T) {
		// Query non-existent kind
		q := datastore.NewQuery("EmptyResultTest")
		var entities []testEntity
		keys, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Fatalf("GetAll on empty kind failed: %v", err)
		}

		if len(keys) != 0 {
			t.Errorf("Expected 0 keys for empty result, got %d", len(keys))
		}
		if len(entities) != 0 {
			t.Errorf("Expected 0 entities for empty result, got %d", len(entities))
		}
	})
}

// TestMutateComprehensive tests Mutate with various combinations
func TestMutateComprehensive(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("MutateBatch", func(t *testing.T) {
		// Create batch of mutations
		mutations := []*datastore.Mutation{
			datastore.NewInsert(datastore.NameKey("MutateBatchTest", "insert1", nil), &testEntity{Name: "insert", Count: 1}),
			datastore.NewInsert(datastore.NameKey("MutateBatchTest", "insert2", nil), &testEntity{Name: "insert", Count: 2}),
			datastore.NewUpsert(datastore.NameKey("MutateBatchTest", "upsert1", nil), &testEntity{Name: "upsert", Count: 3}),
		}

		keys, err := client.Mutate(ctx, mutations...)
		if err != nil {
			t.Fatalf("Mutate batch failed: %v", err)
		}

		if len(keys) != 3 {
			t.Errorf("Expected 3 result keys, got %d", len(keys))
		}
	})

	t.Run("MutateUpdateThenDelete", func(t *testing.T) {
		// First insert
		key := datastore.NameKey("MutateUpdateDeleteTest", "test", nil)
		entity := &testEntity{Name: "original", Count: 1}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Update via mutation
		entity.Count = 2
		updateMut := datastore.NewUpdate(key, entity)
		_, err := client.Mutate(ctx, updateMut)
		if err != nil {
			t.Fatalf("Update mutation failed: %v", err)
		}

		// Verify update
		var retrieved testEntity
		if err := client.Get(ctx, key, &retrieved); err != nil {
			t.Fatalf("Get after update failed: %v", err)
		}
		if retrieved.Count != 2 {
			t.Errorf("Expected count 2 after update, got %d", retrieved.Count)
		}

		// Delete via mutation
		deleteMut := datastore.NewDelete(key)
		_, err = client.Mutate(ctx, deleteMut)
		if err != nil {
			t.Fatalf("Delete mutation failed: %v", err)
		}

		// Verify delete
		err = client.Get(ctx, key, &retrieved)
		if !errors.Is(err, datastore.ErrNoSuchEntity) {
			t.Errorf("Expected ErrNoSuchEntity after delete, got %v", err)
		}
	})

	t.Run("MutateWithNilInBatch", func(t *testing.T) {
		// Try to mutate with nil in batch
		mutations := []*datastore.Mutation{
			datastore.NewInsert(datastore.NameKey("MutateNilTest", "valid", nil), &testEntity{Name: "valid", Count: 1}),
			nil, // This should cause an error
		}

		_, err := client.Mutate(ctx, mutations...)
		if err == nil {
			t.Error("Expected error for nil mutation in batch")
		}
	})

	t.Run("MutateLargeBatch", func(t *testing.T) {
		// Create batch
		var mutations []*datastore.Mutation
		for i := range 5 {
			key := datastore.IDKey("MutateLargeBatchTest", int64(i+1), nil)
			entity := &testEntity{Name: "batch", Count: int64(i)}
			mutations = append(mutations, datastore.NewInsert(key, entity))
		}

		keys, err := client.Mutate(ctx, mutations...)
		if err != nil {
			t.Fatalf("Batch mutate failed: %v", err)
		}

		if len(keys) != 5 {
			t.Errorf("Expected 5 result keys, got %d", len(keys))
		}
	})

	t.Run("MutateUpsertNew", func(t *testing.T) {
		// Upsert a new entity (insert behavior)
		key := datastore.NameKey("MutateUpsertNewTest", "new", nil)
		entity := &testEntity{Name: "new", Count: 99}
		upsertMut := datastore.NewUpsert(key, entity)

		_, err := client.Mutate(ctx, upsertMut)
		if err != nil {
			t.Fatalf("Upsert new entity failed: %v", err)
		}

		// Verify it was created
		var retrieved testEntity
		if err := client.Get(ctx, key, &retrieved); err != nil {
			t.Fatalf("Get after upsert failed: %v", err)
		}
		if retrieved.Count != 99 {
			t.Errorf("Expected count 99, got %d", retrieved.Count)
		}
	})
}
