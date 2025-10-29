package ds9mock

import (
	"context"
	"testing"

	"github.com/codeGROOVE-dev/ds9"
)

func TestNewStore(t *testing.T) {
	store := NewStore()
	if store == nil {
		t.Fatal("expected non-nil store")
	}

	if store.entities == nil {
		t.Error("expected initialized entities map")
	}

	if len(store.entities) != 0 {
		t.Errorf("expected empty store, got %d entities", len(store.entities))
	}
}

func TestNewClient(t *testing.T) {
	client, cleanup := NewClient(t)
	defer cleanup()

	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestMockBasicOperations(t *testing.T) {
	client, cleanup := NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name  string `datastore:"name"`
		Value int64  `datastore:"value"`
	}

	// Test Put
	key := ds9.NameKey("TestKind", "test-key", nil)
	entity := &TestEntity{
		Name:  "test",
		Value: 42,
	}

	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	var retrieved TestEntity
	err = client.Get(ctx, key, &retrieved)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Name != entity.Name {
		t.Errorf("Name: expected %q, got %q", entity.Name, retrieved.Name)
	}
	if retrieved.Value != entity.Value {
		t.Errorf("Value: expected %d, got %d", entity.Value, retrieved.Value)
	}

	// Test Delete
	err = client.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	err = client.Get(ctx, key, &retrieved)
	if err == nil {
		t.Error("expected error after delete, got nil")
	}
}

func TestMockMultiOperations(t *testing.T) {
	client, cleanup := NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name  string `datastore:"name"`
		Count int64  `datastore:"count"`
	}

	// Test PutMulti
	keys := []*ds9.Key{
		ds9.NameKey("Multi", "key1", nil),
		ds9.NameKey("Multi", "key2", nil),
		ds9.NameKey("Multi", "key3", nil),
	}

	entities := []TestEntity{
		{Name: "entity1", Count: 1},
		{Name: "entity2", Count: 2},
		{Name: "entity3", Count: 3},
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	// Test GetMulti
	var retrieved []TestEntity
	err = client.GetMulti(ctx, keys, &retrieved)
	if err != nil {
		t.Fatalf("GetMulti failed: %v", err)
	}

	if len(retrieved) != 3 {
		t.Errorf("expected 3 entities, got %d", len(retrieved))
	}

	for i, entity := range retrieved {
		if entity.Name != entities[i].Name {
			t.Errorf("entity %d: Name mismatch", i)
		}
		if entity.Count != entities[i].Count {
			t.Errorf("entity %d: Count mismatch", i)
		}
	}

	// Test DeleteMulti
	err = client.DeleteMulti(ctx, keys)
	if err != nil {
		t.Fatalf("DeleteMulti failed: %v", err)
	}

	// Verify all deleted
	err = client.GetMulti(ctx, keys, &retrieved)
	if err == nil {
		t.Error("expected error after DeleteMulti, got nil")
	}
}

func TestMockQuery(t *testing.T) {
	client, cleanup := NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	// Put some entities
	for i := range 5 {
		key := ds9.NameKey("QueryKind", string(rune('a'+i)), nil)
		entity := &TestEntity{Name: "test"}
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query for keys
	query := ds9.NewQuery("QueryKind").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 5 {
		t.Errorf("expected 5 keys, got %d", len(keys))
	}
}

func TestMockQueryWithLimit(t *testing.T) {
	client, cleanup := NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	// Put entities
	for i := range 10 {
		key := ds9.NameKey("LimitKind", string(rune('a'+i)), nil)
		entity := &TestEntity{Name: "test"}
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit
	query := ds9.NewQuery("LimitKind").KeysOnly().Limit(3)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys with limit failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys with limit, got %d", len(keys))
	}
}

func TestMockTransaction(t *testing.T) {
	client, cleanup := NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Counter int64 `datastore:"counter"`
	}

	// Put initial entity
	key := ds9.NameKey("TxKind", "counter", nil)
	entity := &TestEntity{Counter: 0}
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Run transaction
	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var current TestEntity
		if err := tx.Get(key, &current); err != nil {
			return err
		}

		current.Counter++
		_, err := tx.Put(key, &current)
		return err
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Verify update
	var updated TestEntity
	err = client.Get(ctx, key, &updated)
	if err != nil {
		t.Fatalf("Get after transaction failed: %v", err)
	}

	if updated.Counter != 1 {
		t.Errorf("expected Counter 1, got %d", updated.Counter)
	}
}

func TestMockHierarchicalKeys(t *testing.T) {
	client, cleanup := NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	// Create parent key
	parentKey := ds9.NameKey("Parent", "p1", nil)
	parentEntity := &TestEntity{Name: "parent"}
	_, err := client.Put(ctx, parentKey, parentEntity)
	if err != nil {
		t.Fatalf("Put parent failed: %v", err)
	}

	// Create child key
	childKey := ds9.NameKey("Child", "c1", parentKey)
	childEntity := &TestEntity{Name: "child"}
	_, err = client.Put(ctx, childKey, childEntity)
	if err != nil {
		t.Fatalf("Put child failed: %v", err)
	}

	// Get child
	var retrieved TestEntity
	err = client.Get(ctx, childKey, &retrieved)
	if err != nil {
		t.Fatalf("Get child failed: %v", err)
	}

	if retrieved.Name != "child" {
		t.Errorf("expected name 'child', got %q", retrieved.Name)
	}
}

func TestMockIDKeys(t *testing.T) {
	client, cleanup := NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Value int64 `datastore:"value"`
	}

	// Use ID key
	key := ds9.IDKey("IDKind", 12345, nil)
	entity := &TestEntity{Value: 99}
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put with ID key failed: %v", err)
	}

	// Get with ID key
	var retrieved TestEntity
	err = client.Get(ctx, key, &retrieved)
	if err != nil {
		t.Fatalf("Get with ID key failed: %v", err)
	}

	if retrieved.Value != 99 {
		t.Errorf("expected Value 99, got %d", retrieved.Value)
	}
}

func TestMockEmptyQuery(t *testing.T) {
	client, cleanup := NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Query non-existent kind
	query := ds9.NewQuery("NonExistent").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys on empty kind failed: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected 0 keys, got %d", len(keys))
	}
}

func TestMockDeleteNonExistent(t *testing.T) {
	client, cleanup := NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Try to delete non-existent entity (should not error)
	key := ds9.NameKey("Test", "nonexistent", nil)
	err := client.Delete(ctx, key)
	if err != nil {
		t.Errorf("Delete of non-existent entity should not error, got: %v", err)
	}
}

func TestMockConcurrentAccess(t *testing.T) {
	client, cleanup := NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Value int64 `datastore:"value"`
	}

	// Run concurrent operations to stress-test locking
	const goroutines = 50
	const operations = 100

	done := make(chan bool)

	for g := range goroutines {
		go func(id int) {
			defer func() { done <- true }()

			for i := range operations {
				key := ds9.NameKey("ConcurrentKind", string(rune('a'+id%10)), nil)
				entity := &TestEntity{Value: int64(i)}

				// Mix of reads and writes
				if i%3 == 0 {
					// Write
					_, err := client.Put(ctx, key, entity)
					if err != nil {
						t.Errorf("goroutine %d: Put failed: %v", id, err)
						return
					}
				} else {
					// Read - may fail if entity doesn't exist, which is expected
					var result TestEntity
					client.Get(ctx, key, &result) //nolint:errcheck // Expected to fail when entity doesn't exist
				}
			}
		}(g)
	}

	// Wait for all goroutines
	for range goroutines {
		<-done
	}
}

func TestMockConcurrentQuery(t *testing.T) {
	client, cleanup := NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	// Populate some data
	for i := range 20 {
		key := ds9.NameKey("QueryConcurrent", string(rune('a'+i)), nil)
		entity := &TestEntity{Name: "test"}
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Run concurrent queries
	const goroutines = 20
	done := make(chan bool)

	for range goroutines {
		go func() {
			defer func() { done <- true }()

			query := ds9.NewQuery("QueryConcurrent").KeysOnly()
			keys, err := client.AllKeys(ctx, query)
			if err != nil {
				t.Errorf("AllKeys failed: %v", err)
				return
			}

			if len(keys) != 20 {
				t.Errorf("expected 20 keys, got %d", len(keys))
			}
		}()
	}

	// Wait for all goroutines
	for range goroutines {
		<-done
	}
}
