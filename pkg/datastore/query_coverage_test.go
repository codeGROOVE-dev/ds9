package datastore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

func TestCount_Coverage(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("CountEmptyKind", func(t *testing.T) {
		// Test counting entities in an empty kind
		q := datastore.NewQuery("CountTest")
		count, err := client.Count(ctx, q)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected count 0 for empty kind, got %d", count)
		}
	})

	t.Run("CountWithEntities", func(t *testing.T) {
		// Create test entities
		for i := range 5 {
			key := datastore.IDKey("CountTest2", int64(i+1), nil)
			entity := &testEntity{
				Name:  "test",
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Count entities
		q := datastore.NewQuery("CountTest2")
		count, err := client.Count(ctx, q)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 5 {
			t.Errorf("Expected count 5, got %d", count)
		}
	})

	t.Run("CountWithFilter", func(t *testing.T) {
		// Create test entities with different counts
		for i := range 5 {
			key := datastore.IDKey("CountTest3", int64(i+1), nil)
			entity := &testEntity{
				Name:  "test",
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Count entities with filter
		q := datastore.NewQuery("CountTest3").Filter("count >", 2)
		count, err := client.Count(ctx, q)
		if err != nil {
			t.Fatalf("Count with filter failed: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected count 2 for filtered query, got %d", count)
		}
	})
}

func TestGetAll_Coverage(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("GetAllEmpty", func(t *testing.T) {
		// Test GetAll on empty kind
		q := datastore.NewQuery("GetAllTest")
		var entities []testEntity
		keys, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Fatalf("GetAll failed: %v", err)
		}
		if len(keys) != 0 {
			t.Errorf("Expected 0 keys, got %d", len(keys))
		}
		if len(entities) != 0 {
			t.Errorf("Expected 0 entities, got %d", len(entities))
		}
	})

	t.Run("GetAllWithEntities", func(t *testing.T) {
		// Create test entities
		expectedCount := 7
		for i := range expectedCount {
			key := datastore.IDKey("GetAllTest2", int64(i+1), nil)
			entity := &testEntity{
				Name:      "test",
				Count:     int64(i),
				UpdatedAt: time.Now().UTC().Truncate(time.Microsecond),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Get all entities
		q := datastore.NewQuery("GetAllTest2")
		var entities []testEntity
		keys, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Fatalf("GetAll failed: %v", err)
		}
		if len(keys) != expectedCount {
			t.Errorf("Expected %d keys, got %d", expectedCount, len(keys))
		}
		if len(entities) != expectedCount {
			t.Errorf("Expected %d entities, got %d", expectedCount, len(entities))
		}
	})

	t.Run("GetAllWithLimit", func(t *testing.T) {
		// Create test entities
		for i := range 5 {
			key := datastore.IDKey("GetAllTest3", int64(i+1), nil)
			entity := &testEntity{
				Name:  "test",
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Get all entities with limit
		q := datastore.NewQuery("GetAllTest3").Limit(3)
		var entities []testEntity
		keys, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Fatalf("GetAll with limit failed: %v", err)
		}
		if len(keys) != 3 {
			t.Errorf("Expected 3 keys, got %d", len(keys))
		}
		if len(entities) != 3 {
			t.Errorf("Expected 3 entities, got %d", len(entities))
		}
	})

	t.Run("GetAllWithFilter", func(t *testing.T) {
		// Create test entities
		for i := range 5 {
			key := datastore.IDKey("GetAllTest4", int64(i+1), nil)
			entity := &testEntity{
				Name:  "test",
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Get all entities with filter - mock now supports filtering
		q := datastore.NewQuery("GetAllTest4").Filter("count >=", 3)
		var entities []testEntity
		keys, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Fatalf("GetAll with filter failed: %v", err)
		}
		// Should get entities with count >= 3 (3, 4)
		if len(keys) != 2 {
			t.Errorf("Expected 2 keys with filter, got %d", len(keys))
		}
		if len(entities) != 2 {
			t.Errorf("Expected 2 entities with filter, got %d", len(entities))
		}
	})

	t.Run("GetAllErrorInvalidDst", func(t *testing.T) {
		// Test error case: dst is not a pointer to slice
		q := datastore.NewQuery("GetAllTest5")
		var entity testEntity
		_, err := client.GetAll(ctx, q, &entity) // Pass pointer to struct instead of slice
		if err == nil {
			t.Error("Expected error for invalid dst, got nil")
		}
		if !errors.Is(err, errors.New("dst must be a pointer to slice")) && err.Error() != "dst must be a pointer to slice" {
			t.Errorf("Expected 'dst must be a pointer to slice' error, got: %v", err)
		}
	})

	t.Run("GetAllErrorNotPointer", func(t *testing.T) {
		// Test error case: dst is not a pointer
		q := datastore.NewQuery("GetAllTest6")
		var entities []testEntity
		_, err := client.GetAll(ctx, q, entities) // Pass slice instead of pointer to slice
		if err == nil {
			t.Error("Expected error for non-pointer dst, got nil")
		}
	})
}

func TestAllKeys_Coverage(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("AllKeysEmpty", func(t *testing.T) {
		// Test AllKeys on empty kind
		q := datastore.NewQuery("AllKeysTest").KeysOnly()
		keys, err := client.AllKeys(ctx, q)
		if err != nil {
			t.Fatalf("AllKeys failed: %v", err)
		}
		if len(keys) != 0 {
			t.Errorf("Expected 0 keys, got %d", len(keys))
		}
	})

	t.Run("AllKeysWithEntities", func(t *testing.T) {
		// Create test entities
		expectedCount := 6
		for i := range expectedCount {
			key := datastore.IDKey("AllKeysTest2", int64(i+1), nil)
			entity := &testEntity{
				Name:  "test",
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Get all keys
		q := datastore.NewQuery("AllKeysTest2").KeysOnly()
		keys, err := client.AllKeys(ctx, q)
		if err != nil {
			t.Fatalf("AllKeys failed: %v", err)
		}
		if len(keys) != expectedCount {
			t.Errorf("Expected %d keys, got %d", expectedCount, len(keys))
		}

		// Verify keys are valid
		for i, key := range keys {
			if key.Kind != "AllKeysTest2" {
				t.Errorf("Key %d: expected kind 'AllKeysTest2', got '%s'", i, key.Kind)
			}
			if key.Incomplete() {
				t.Errorf("Key %d is incomplete", i)
			}
		}
	})
}

func TestDeleteMulti_Coverage(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("DeleteMultiSingle", func(t *testing.T) {
		// Create a test entity
		key := datastore.NameKey("DeleteMultiTest", "test1", nil)
		entity := &testEntity{Name: "test", Count: 42}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Delete using DeleteMulti
		err := client.DeleteMulti(ctx, []*datastore.Key{key})
		if err != nil {
			t.Fatalf("DeleteMulti failed: %v", err)
		}

		// Verify entity is deleted
		var result testEntity
		err = client.Get(ctx, key, &result)
		if !errors.Is(err, datastore.ErrNoSuchEntity) {
			t.Errorf("Expected ErrNoSuchEntity, got %v", err)
		}
	})

	t.Run("DeleteMultiMultiple", func(t *testing.T) {
		// Create multiple test entities
		keys := []*datastore.Key{
			datastore.NameKey("DeleteMultiTest2", "test1", nil),
			datastore.NameKey("DeleteMultiTest2", "test2", nil),
			datastore.NameKey("DeleteMultiTest2", "test3", nil),
		}
		for _, key := range keys {
			entity := &testEntity{Name: "test", Count: 42}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Delete using DeleteMulti
		err := client.DeleteMulti(ctx, keys)
		if err != nil {
			t.Fatalf("DeleteMulti failed: %v", err)
		}

		// Verify all entities are deleted
		for _, key := range keys {
			var result testEntity
			err = client.Get(ctx, key, &result)
			if !errors.Is(err, datastore.ErrNoSuchEntity) {
				t.Errorf("Expected ErrNoSuchEntity for key %v, got %v", key, err)
			}
		}
	})
}

func TestPutMulti_Coverage(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("PutMultiMultiple", func(t *testing.T) {
		// Test PutMulti with multiple complete keys
		keys := []*datastore.Key{
			datastore.NameKey("PutMultiTest", "test1", nil),
			datastore.NameKey("PutMultiTest", "test2", nil),
		}
		entities := []testEntity{
			{Name: "test1", Count: 1},
			{Name: "test2", Count: 2},
		}

		resultKeys, err := client.PutMulti(ctx, keys, entities)
		if err != nil {
			t.Fatalf("PutMulti failed: %v", err)
		}

		if len(resultKeys) != len(keys) {
			t.Errorf("Expected %d result keys, got %d", len(keys), len(resultKeys))
		}

		// Verify entities were stored
		for i, key := range resultKeys {
			var retrieved testEntity
			if err := client.Get(ctx, key, &retrieved); err != nil {
				t.Errorf("Failed to retrieve entity %d: %v", i, err)
			}
		}
	})
}

func TestGetMulti_Coverage(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("GetMultiMixedResults", func(t *testing.T) {
		// Create one entity, leave another missing
		key1 := datastore.NameKey("GetMultiTest", "exists", nil)
		key2 := datastore.NameKey("GetMultiTest", "missing", nil)

		entity1 := &testEntity{Name: "test1", Count: 42}
		if _, err := client.Put(ctx, key1, entity1); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Get both keys
		entities := []testEntity{{}, {}}
		err := client.GetMulti(ctx, []*datastore.Key{key1, key2}, entities)

		// Should get MultiError with one ErrNoSuchEntity
		if err == nil {
			t.Error("Expected MultiError, got nil")
		}
	})
}
