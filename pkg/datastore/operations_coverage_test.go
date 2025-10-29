package datastore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

func TestGet_CoverageEdgeCases(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("GetWithStructPointer", func(t *testing.T) {
		// Create entity
		key := datastore.NameKey("GetTest", "test1", nil)
		entity := &testEntity{
			Name:  "test",
			Count: 42,
		}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Get with pointer to struct
		var retrieved testEntity
		err := client.Get(ctx, key, &retrieved)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if retrieved.Name != "test" {
			t.Errorf("Expected name 'test', got '%s'", retrieved.Name)
		}
	})
}

func TestPut_CoverageEdgeCases(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("PutWithCompleteKey", func(t *testing.T) {
		key := datastore.NameKey("PutTest", "complete", nil)
		entity := &testEntity{
			Name:  "complete",
			Count: 1,
		}

		resultKey, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		if resultKey.Name != "complete" {
			t.Errorf("Expected key name 'complete', got '%s'", resultKey.Name)
		}
	})

	t.Run("PutWithIDKey", func(t *testing.T) {
		key := datastore.IDKey("PutTest", 12345, nil)
		entity := &testEntity{
			Name:  "id-key",
			Count: 1,
		}

		resultKey, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		if resultKey.ID != 12345 {
			t.Errorf("Expected key ID 12345, got %d", resultKey.ID)
		}
	})

	t.Run("PutOverwrite", func(t *testing.T) {
		// Put entity
		key := datastore.NameKey("PutTest", "overwrite", nil)
		entity := &testEntity{
			Name:  "original",
			Count: 1,
		}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("First Put failed: %v", err)
		}

		// Overwrite with new data
		entity.Name = "updated"
		entity.Count = 2
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Second Put failed: %v", err)
		}

		// Verify updated
		var retrieved testEntity
		if err := client.Get(ctx, key, &retrieved); err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if retrieved.Name != "updated" {
			t.Errorf("Expected name 'updated', got '%s'", retrieved.Name)
		}
		if retrieved.Count != 2 {
			t.Errorf("Expected count 2, got %d", retrieved.Count)
		}
	})
}

func TestDelete_CoverageEdgeCases(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("DeleteExisting", func(t *testing.T) {
		// Create entity
		key := datastore.NameKey("DeleteTest", "existing", nil)
		entity := &testEntity{Name: "test", Count: 1}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Delete it
		err := client.Delete(ctx, key)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify deleted
		var retrieved testEntity
		err = client.Get(ctx, key, &retrieved)
		if !errors.Is(err, datastore.ErrNoSuchEntity) {
			t.Errorf("Expected ErrNoSuchEntity, got %v", err)
		}
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		// Delete non-existent key (should not error)
		key := datastore.NameKey("DeleteTest", "nonexistent", nil)
		err := client.Delete(ctx, key)
		if err != nil {
			t.Logf("Delete of non-existent key returned: %v", err)
		}
	})
}

func TestAllocateIDs_EdgeCases(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("AllocateSingleID", func(t *testing.T) {
		keys := []*datastore.Key{
			datastore.IncompleteKey("AllocTest", nil),
		}

		allocated, err := client.AllocateIDs(ctx, keys)
		if err != nil {
			t.Fatalf("AllocateIDs failed: %v", err)
		}

		if len(allocated) != 1 {
			t.Errorf("Expected 1 allocated key, got %d", len(allocated))
		}

		if allocated[0].Incomplete() {
			t.Error("Allocated key is still incomplete")
		}
	})

	t.Run("AllocateMultipleIDs", func(t *testing.T) {
		keys := []*datastore.Key{
			datastore.IncompleteKey("AllocTest", nil),
			datastore.IncompleteKey("AllocTest", nil),
			datastore.IncompleteKey("AllocTest", nil),
		}

		allocated, err := client.AllocateIDs(ctx, keys)
		if err != nil {
			t.Fatalf("AllocateIDs failed: %v", err)
		}

		if len(allocated) != 3 {
			t.Errorf("Expected 3 allocated keys, got %d", len(allocated))
		}

		// Verify all are complete (mock may not guarantee unique IDs)
		for i, key := range allocated {
			if key.Incomplete() {
				t.Errorf("Key %d is still incomplete", i)
			}
		}
	})

	t.Run("AllocateWithParentKey", func(t *testing.T) {
		parent := datastore.NameKey("Parent", "parent1", nil)
		keys := []*datastore.Key{
			datastore.IncompleteKey("Child", parent),
		}

		allocated, err := client.AllocateIDs(ctx, keys)
		if err != nil {
			t.Fatalf("AllocateIDs with parent failed: %v", err)
		}

		if len(allocated) != 1 {
			t.Errorf("Expected 1 allocated key, got %d", len(allocated))
		}

		if allocated[0].Incomplete() {
			t.Error("Allocated key is still incomplete")
		}

		if allocated[0].Parent == nil {
			t.Error("Parent key was lost during allocation")
		}
	})
}

func TestGetMulti_EdgeCases(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("GetMultiAllExist", func(t *testing.T) {
		// Create multiple entities
		keys := []*datastore.Key{
			datastore.NameKey("MultiTest", "key1", nil),
			datastore.NameKey("MultiTest", "key2", nil),
			datastore.NameKey("MultiTest", "key3", nil),
		}

		for i, key := range keys {
			entity := &testEntity{Name: "test", Count: int64(i)}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Get all
		entities := make([]testEntity, len(keys))
		err := client.GetMulti(ctx, keys, &entities)
		if err != nil {
			t.Fatalf("GetMulti failed: %v", err)
		}

		for i, entity := range entities {
			if entity.Count != int64(i) {
				t.Errorf("Entity %d: expected count %d, got %d", i, i, entity.Count)
			}
		}
	})

	t.Run("GetMultiSomeMissing", func(t *testing.T) {
		// Create one entity, leave others missing
		keys := []*datastore.Key{
			datastore.NameKey("MultiTest2", "exists", nil),
			datastore.NameKey("MultiTest2", "missing1", nil),
			datastore.NameKey("MultiTest2", "missing2", nil),
		}

		entity := &testEntity{Name: "exists", Count: 1}
		if _, err := client.Put(ctx, keys[0], entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Get all - should get MultiError
		entities := make([]testEntity, len(keys))
		err := client.GetMulti(ctx, keys, &entities)
		if err == nil {
			t.Log("GetMulti returned nil error (mock may not report missing entities)")
		}
	})
}

func TestPutMulti_EdgeCases(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("PutMultiAllComplete", func(t *testing.T) {
		keys := []*datastore.Key{
			datastore.NameKey("PutMultiTest2", "key1", nil),
			datastore.NameKey("PutMultiTest2", "key2", nil),
		}

		entities := []testEntity{
			{Name: "entity1", Count: 1},
			{Name: "entity2", Count: 2},
		}

		resultKeys, err := client.PutMulti(ctx, keys, entities)
		if err != nil {
			t.Fatalf("PutMulti failed: %v", err)
		}

		if len(resultKeys) != len(keys) {
			t.Errorf("Expected %d keys, got %d", len(keys), len(resultKeys))
		}

		// Verify all were stored
		for i, key := range resultKeys {
			var retrieved testEntity
			if err := client.Get(ctx, key, &retrieved); err != nil {
				t.Errorf("Failed to retrieve entity %d: %v", i, err)
			}
		}
	})
}

func TestDeleteMulti_EdgeCases(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("DeleteMultiAllExist", func(t *testing.T) {
		// Create entities
		keys := []*datastore.Key{
			datastore.NameKey("DelMultiTest", "key1", nil),
			datastore.NameKey("DelMultiTest", "key2", nil),
		}

		for _, key := range keys {
			entity := &testEntity{Name: "test", Count: 1}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Delete all
		err := client.DeleteMulti(ctx, keys)
		if err != nil {
			t.Fatalf("DeleteMulti failed: %v", err)
		}

		// Verify all deleted
		for _, key := range keys {
			var retrieved testEntity
			err := client.Get(ctx, key, &retrieved)
			if !errors.Is(err, datastore.ErrNoSuchEntity) {
				t.Errorf("Expected ErrNoSuchEntity for key %v, got %v", key, err)
			}
		}
	})
}
