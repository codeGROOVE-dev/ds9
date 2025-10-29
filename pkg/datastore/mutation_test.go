package datastore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

func TestMutate(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("MutateInsert", func(t *testing.T) {
		key := datastore.NameKey("MutateTest", "insert", nil)
		entity := &testEntity{
			Name:  "inserted",
			Count: 42,
		}

		mut := datastore.NewInsert(key, entity)
		keys, err := client.Mutate(ctx, mut)
		if err != nil {
			t.Fatalf("Mutate insert failed: %v", err)
		}

		if len(keys) != 1 {
			t.Errorf("Expected 1 key, got %d", len(keys))
		}

		// Verify entity was created
		var result testEntity
		if err := client.Get(ctx, key, &result); err != nil {
			t.Fatalf("Get after insert failed: %v", err)
		}
		if result.Name != "inserted" {
			t.Errorf("Expected Name 'inserted', got '%s'", result.Name)
		}
	})

	t.Run("MutateUpdate", func(t *testing.T) {
		key := datastore.NameKey("MutateTest", "update", nil)
		entity := &testEntity{Name: "original", Count: 1}

		// Create entity first
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Update via mutation
		updated := &testEntity{Name: "updated", Count: 2}
		mut := datastore.NewUpdate(key, updated)
		_, err := client.Mutate(ctx, mut)
		if err != nil {
			t.Fatalf("Mutate update failed: %v", err)
		}

		// Verify entity was updated
		var result testEntity
		if err := client.Get(ctx, key, &result); err != nil {
			t.Fatalf("Get after update failed: %v", err)
		}
		if result.Name != "updated" {
			t.Errorf("Expected Name 'updated', got '%s'", result.Name)
		}
	})

	t.Run("MutateUpsert", func(t *testing.T) {
		key := datastore.NameKey("MutateTest", "upsert", nil)
		entity := &testEntity{Name: "upserted", Count: 100}

		mut := datastore.NewUpsert(key, entity)
		keys, err := client.Mutate(ctx, mut)
		if err != nil {
			t.Fatalf("Mutate upsert failed: %v", err)
		}

		if len(keys) != 1 {
			t.Errorf("Expected 1 key, got %d", len(keys))
		}

		// Verify entity exists
		var result testEntity
		if err := client.Get(ctx, key, &result); err != nil {
			t.Fatalf("Get after upsert failed: %v", err)
		}
		if result.Name != "upserted" {
			t.Errorf("Expected Name 'upserted', got '%s'", result.Name)
		}
	})

	t.Run("MutateDelete", func(t *testing.T) {
		key := datastore.NameKey("MutateTest", "delete", nil)
		entity := &testEntity{Name: "to-delete", Count: 1}

		// Create entity first
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Delete via mutation
		mut := datastore.NewDelete(key)
		keys, err := client.Mutate(ctx, mut)
		if err != nil {
			t.Fatalf("Mutate delete failed: %v", err)
		}

		if len(keys) != 1 {
			t.Errorf("Expected 1 key, got %d", len(keys))
		}

		// Verify entity was deleted
		var result testEntity
		err = client.Get(ctx, key, &result)
		if !errors.Is(err, datastore.ErrNoSuchEntity) {
			t.Errorf("Expected datastore.ErrNoSuchEntity after delete, got %v", err)
		}
	})

	t.Run("MutateMultiple", func(t *testing.T) {
		key1 := datastore.NameKey("MutateTest", "multi1", nil)
		key2 := datastore.NameKey("MutateTest", "multi2", nil)
		key3 := datastore.NameKey("MutateTest", "multi3", nil)

		entity1 := &testEntity{Name: "first", Count: 1}
		entity2 := &testEntity{Name: "second", Count: 2}
		entity3 := &testEntity{Name: "third", Count: 3}

		// Pre-create entity3 for update
		if _, err := client.Put(ctx, key3, &testEntity{Name: "old", Count: 0}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Apply multiple mutations
		muts := []*datastore.Mutation{
			datastore.NewInsert(key1, entity1),
			datastore.NewUpsert(key2, entity2),
			datastore.NewUpdate(key3, entity3),
		}

		keys, err := client.Mutate(ctx, muts...)
		if err != nil {
			t.Fatalf("Mutate multiple failed: %v", err)
		}

		if len(keys) != 3 {
			t.Errorf("Expected 3 keys, got %d", len(keys))
		}

		// Verify all mutations applied
		var result1, result2, result3 testEntity
		if err := client.Get(ctx, key1, &result1); err != nil {
			t.Errorf("Get key1 failed: %v", err)
		}
		if err := client.Get(ctx, key2, &result2); err != nil {
			t.Errorf("Get key2 failed: %v", err)
		}
		if err := client.Get(ctx, key3, &result3); err != nil {
			t.Errorf("Get key3 failed: %v", err)
		}

		if result1.Name != "first" || result2.Name != "second" || result3.Name != "third" {
			t.Errorf("Mutation results incorrect")
		}
	})

	t.Run("MutateEmpty", func(t *testing.T) {
		keys, err := client.Mutate(ctx)
		if err != nil {
			t.Fatalf("Mutate with no mutations failed: %v", err)
		}

		if len(keys) != 0 {
			t.Errorf("Expected empty keys, got %d", len(keys))
		}
	})
}
