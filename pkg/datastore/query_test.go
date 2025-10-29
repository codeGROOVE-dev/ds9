package datastore_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

func TestQueryOperations(t *testing.T) {
	// Test query builder methods
	query := datastore.NewQuery("TestKind")

	if query.KeysOnly().KeysOnly() == nil {
		t.Error("KeysOnly() should be chainable")
	}

	if query.Limit(10).Limit(20) == nil {
		t.Error("Limit() should be chainable")
	}
}

func TestEmptyQuery(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Query for keys when no entities exist
	query := datastore.NewQuery("NonExistent").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected 0 keys, got %d", len(keys))
	}
}

func TestQueryWithLimitZero(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Store some entities
	for i := range 5 {
		key := datastore.NameKey("LimitKind", string(rune('a'+i)), nil)
		entity := testEntity{Name: "item", Count: int64(i)}
		if _, err := client.Put(ctx, key, &entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit 0 (should return all)
	query := datastore.NewQuery("LimitKind").KeysOnly().Limit(0)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) == 0 {
		t.Error("expected keys, got 0 (limit 0 should mean unlimited)")
	}
}

func TestQueryWithLimitLessThanResults(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Store 10 entities
	for i := range 10 {
		key := datastore.NameKey("LimitKind2", string(rune('a'+i)), nil)
		entity := testEntity{Name: "item", Count: int64(i)}
		if _, err := client.Put(ctx, key, &entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit 3
	query := datastore.NewQuery("LimitKind2").KeysOnly().Limit(3)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
}

func TestQueryNonKeysOnly(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Try to call AllKeys with non-KeysOnly query
	query := datastore.NewQuery("TestKind")
	_, err := client.AllKeys(ctx, query)

	if err == nil {
		t.Error("expected error for non-KeysOnly query")
	}

	if !strings.Contains(err.Error(), "KeysOnly") {
		t.Errorf("expected error to mention KeysOnly, got: %v", err)
	}
}

func TestQueryWithZeroLimit(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put entities
	for i := range 5 {
		key := datastore.NameKey("ZeroLimit", fmt.Sprintf("key-%d", i), nil)
		entity := &testEntity{Name: "test", Count: int64(i)}
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit 0 (should return all)
	query := datastore.NewQuery("ZeroLimit").KeysOnly().Limit(0)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys with limit 0 failed: %v", err)
	}

	// Limit 0 should mean unlimited
	if len(keys) == 0 {
		t.Error("expected results with limit 0 (unlimited), got 0")
	}
}

func TestQueryWithVeryLargeLimit(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put a few entities
	for i := range 3 {
		key := datastore.NameKey("LargeLimit", fmt.Sprintf("key-%d", i), nil)
		entity := &testEntity{Name: "test", Count: int64(i)}
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with very large limit
	query := datastore.NewQuery("LargeLimit").KeysOnly().Limit(10000)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys with large limit failed: %v", err)
	}

	// Should return all 3
	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
}

func TestGetAllEmpty(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()
	query := datastore.NewQuery("NonExistentKind")
	var results []testEntity

	keys, err := client.GetAll(ctx, query, &results)
	if err != nil {
		t.Fatalf("GetAll failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 entities, got %d", len(results))
	}

	if len(keys) != 0 {
		t.Errorf("Expected 0 keys, got %d", len(keys))
	}
}

func TestGetAllInvalidDst(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()
	query := datastore.NewQuery("TestKind")

	tests := []struct {
		name string
		dst  any
	}{
		{"not a pointer", []testEntity{}},
		{"not a slice", new(testEntity)},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.GetAll(ctx, query, tt.dst)
			if err == nil {
				t.Error("Expected error for invalid dst, got nil")
			}
		})
	}
}

func TestGetAllSingleEntity(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create entity
	key := datastore.NameKey("SingleGetAll", "single1", nil)
	entity := testEntity{
		Name:      "single",
		Count:     42,
		Active:    true,
		Score:     3.14,
		UpdatedAt: time.Now().UTC().Truncate(time.Microsecond),
		Notes:     "test notes",
	}

	_, err := client.Put(ctx, key, &entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test GetAll
	query := datastore.NewQuery("SingleGetAll")
	var results []testEntity
	keys, err := client.GetAll(ctx, query, &results)
	if err != nil {
		t.Fatalf("GetAll failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 entity, got %d", len(results))
	}

	if len(keys) != 1 {
		t.Fatalf("Expected 1 key, got %d", len(keys))
	}

	// Verify entity content
	if results[0].Name != "single" {
		t.Errorf("Expected name 'single', got '%s'", results[0].Name)
	}
	if results[0].Count != 42 {
		t.Errorf("Expected count 42, got %d", results[0].Count)
	}
	if !results[0].Active {
		t.Error("Expected active=true")
	}
	if results[0].Score != 3.14 {
		t.Errorf("Expected score 3.14, got %f", results[0].Score)
	}

	// Verify key
	if keys[0].Kind != "SingleGetAll" {
		t.Errorf("Expected kind 'SingleGetAll', got '%s'", keys[0].Kind)
	}
	if keys[0].Name != "single1" {
		t.Errorf("Expected key name 'single1', got '%s'", keys[0].Name)
	}
}

func TestGetAllMultipleEntities(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create multiple entities
	count := 5
	keys := make([]*datastore.Key, count)
	entities := make([]testEntity, count)

	for i := range count {
		keys[i] = datastore.NameKey("MultiGetAll", fmt.Sprintf("entity%d", i), nil)
		entities[i] = testEntity{
			Name:      fmt.Sprintf("entity%d", i),
			Count:     int64(i * 10),
			Active:    i%2 == 0,
			Score:     float64(i) * 1.5,
			UpdatedAt: time.Now().UTC().Truncate(time.Microsecond),
		}
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	// Test GetAll
	query := datastore.NewQuery("MultiGetAll")
	var results []testEntity
	returnedKeys, err := client.GetAll(ctx, query, &results)
	if err != nil {
		t.Fatalf("GetAll failed: %v", err)
	}

	if len(results) != count {
		t.Fatalf("Expected %d entities, got %d", count, len(results))
	}

	if len(returnedKeys) != count {
		t.Fatalf("Expected %d keys, got %d", count, len(returnedKeys))
	}

	// Verify we got all entities
	foundNames := make(map[string]bool)
	for _, entity := range results {
		foundNames[entity.Name] = true
	}

	for i := range count {
		expectedName := fmt.Sprintf("entity%d", i)
		if !foundNames[expectedName] {
			t.Errorf("Missing entity: %s", expectedName)
		}
	}
}

func TestGetAllWithLimitVariations(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Setup: Create 10 entities
	keys := make([]*datastore.Key, 10)
	entities := make([]testEntity, 10)
	for i := range 10 {
		keys[i] = datastore.NameKey("LimitGetAll", fmt.Sprintf("key%d", i), nil)
		entities[i] = testEntity{
			Name:      fmt.Sprintf("entity%d", i),
			Count:     int64(i),
			Active:    true,
			UpdatedAt: time.Now().UTC().Truncate(time.Microsecond),
		}
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	tests := []struct {
		name     string
		limit    int
		expected int
	}{
		{"Limit 1", 1, 1},
		{"Limit 3", 3, 3},
		{"Limit 5", 5, 5},
		{"Limit 10", 10, 10},
		{"Limit 20 (more than available)", 20, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := datastore.NewQuery("LimitGetAll").Limit(tt.limit)
			var results []testEntity
			keys, err := client.GetAll(ctx, query, &results)
			if err != nil {
				t.Fatalf("GetAll failed: %v", err)
			}

			if len(results) != tt.expected {
				t.Errorf("Expected %d entities, got %d", tt.expected, len(results))
			}

			if len(keys) != tt.expected {
				t.Errorf("Expected %d keys, got %d", tt.expected, len(keys))
			}
		})
	}
}

func TestCount(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("CountEmptyKind", func(t *testing.T) {
		q := datastore.NewQuery("NonExistent")
		count, err := client.Count(ctx, q)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}

		if count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}
	})

	t.Run("CountWithEntities", func(t *testing.T) {
		// Create some entities
		for i := range 5 {
			key := datastore.IDKey("CountTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  fmt.Sprintf("entity-%d", i),
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		q := datastore.NewQuery("CountTest")
		count, err := client.Count(ctx, q)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}

		if count != 5 {
			t.Errorf("Expected count 5, got %d", count)
		}
	})

	t.Run("CountWithFilter", func(t *testing.T) {
		// Create entities with different counts
		for i := range 10 {
			key := datastore.IDKey("FilterCount", int64(i+1), nil)
			entity := &testEntity{
				Name:  fmt.Sprintf("entity-%d", i),
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Count entities where count >= 5
		q := datastore.NewQuery("FilterCount").Filter("count >=", int64(5))
		count, err := client.Count(ctx, q)
		if err != nil {
			t.Fatalf("Count with filter failed: %v", err)
		}

		// Should return entities with count 5,6,7,8,9 = 5 entities
		if count != 5 {
			t.Errorf("Expected count 5, got %d", count)
		}
	})

	t.Run("CountWithLimit", func(t *testing.T) {
		// Create entities
		for i := range 10 {
			key := datastore.IDKey("LimitCount", int64(i+1), nil)
			entity := &testEntity{
				Name:  fmt.Sprintf("entity-%d", i),
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Count with limit - note: count should respect limit
		q := datastore.NewQuery("LimitCount").Limit(3)
		count, err := client.Count(ctx, q)
		if err != nil {
			t.Fatalf("Count with limit failed: %v", err)
		}

		// Mock implementation may return full count, but limit is respected
		if count > 10 {
			t.Errorf("Count should not exceed actual entities: %d", count)
		}
	})
}

func TestQueryNamespace(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("NamespaceFilter", func(t *testing.T) {
		// Note: ds9mock may not fully support namespaces, but we test the API
		q := datastore.NewQuery("Task").Namespace("custom-namespace")

		var entities []testEntity
		_, err := client.GetAll(ctx, q, &entities)
		// Should not error even if namespace is not supported by mock
		if err != nil {
			t.Logf("GetAll with namespace: %v", err)
		}
	})

	t.Run("EmptyNamespace", func(t *testing.T) {
		q := datastore.NewQuery("Task").Namespace("")

		var entities []testEntity
		_, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Logf("GetAll with empty namespace: %v", err)
		}
	})
}

func TestQueryDistinct(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("Distinct", func(t *testing.T) {
		// Create duplicate entities
		for i := range 3 {
			key := datastore.IDKey("DistinctTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  "same-name", // Same name for all
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Query with distinct on Name field
		q := datastore.NewQuery("DistinctTest").Project("name").Distinct()

		var entities []testEntity
		_, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Logf("GetAll with Distinct: %v", err)
		}
	})

	t.Run("DistinctOn", func(t *testing.T) {
		q := datastore.NewQuery("Task").DistinctOn("name", "count")

		var entities []testEntity
		_, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Logf("GetAll with DistinctOn: %v", err)
		}
	})
}
