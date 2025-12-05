package mock_test

import (
	"context"
	"errors"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
	"github.com/codeGROOVE-dev/ds9/pkg/mock"
)

func TestNewStore(t *testing.T) {
	store := mock.NewStore()
	if store == nil {
		t.Fatal("expected non-nil store")
	}

	// Store entities are not directly accessible from outside the package
	// but we can verify the store is functional through NewMockServers
}

func TestNewClient(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestMockBasicOperations(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name  string `datastore:"name"`
		Value int64  `datastore:"value"`
	}

	// Test Put
	key := datastore.NameKey("TestKind", "test-key", nil)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name  string `datastore:"name"`
		Count int64  `datastore:"count"`
	}

	// Test PutMulti
	keys := []*datastore.Key{
		datastore.NameKey("Multi", "key1", nil),
		datastore.NameKey("Multi", "key2", nil),
		datastore.NameKey("Multi", "key3", nil),
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
	retrieved := make([]TestEntity, len(keys))
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
	retrieved = make([]TestEntity, len(keys))
	err = client.GetMulti(ctx, keys, &retrieved)
	if err == nil {
		t.Error("expected error after DeleteMulti, got nil")
	}
}

func TestMockQuery(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	// Put some entities
	for i := range 5 {
		key := datastore.NameKey("QueryKind", string(rune('a'+i)), nil)
		entity := &TestEntity{Name: "test"}
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query for keys
	query := datastore.NewQuery("QueryKind").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 5 {
		t.Errorf("expected 5 keys, got %d", len(keys))
	}
}

func TestMockQueryWithLimit(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	// Put entities
	for i := range 10 {
		key := datastore.NameKey("LimitKind", string(rune('a'+i)), nil)
		entity := &TestEntity{Name: "test"}
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit
	query := datastore.NewQuery("LimitKind").KeysOnly().Limit(3)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys with limit failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys with limit, got %d", len(keys))
	}
}

func TestMockTransaction(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Counter int64 `datastore:"counter"`
	}

	// Put initial entity
	key := datastore.NameKey("TxKind", "counter", nil)
	entity := &TestEntity{Counter: 0}
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Run transaction
	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	// Create parent key
	parentKey := datastore.NameKey("Parent", "p1", nil)
	parentEntity := &TestEntity{Name: "parent"}
	_, err := client.Put(ctx, parentKey, parentEntity)
	if err != nil {
		t.Fatalf("Put parent failed: %v", err)
	}

	// Create child key
	childKey := datastore.NameKey("Child", "c1", parentKey)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Value int64 `datastore:"value"`
	}

	// Use ID key
	key := datastore.IDKey("IDKind", 12345, nil)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Query non-existent kind
	query := datastore.NewQuery("NonExistent").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys on empty kind failed: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected 0 keys, got %d", len(keys))
	}
}

func TestMockDeleteNonExistent(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Try to delete non-existent entity (should not error)
	key := datastore.NameKey("Test", "nonexistent", nil)
	err := client.Delete(ctx, key)
	if err != nil {
		t.Errorf("Delete of non-existent entity should not error, got: %v", err)
	}
}

func TestMockConcurrentAccess(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
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
				key := datastore.NameKey("ConcurrentKind", string(rune('a'+id%10)), nil)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	// Populate some data
	for i := range 20 {
		key := datastore.NameKey("QueryConcurrent", string(rune('a'+i)), nil)
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

			query := datastore.NewQuery("QueryConcurrent").KeysOnly()
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

func TestMockInsertAlreadyExists(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	key := datastore.NameKey("InsertTest", "test-key", nil)
	entity := &TestEntity{Name: "first"}

	// First insert should succeed
	mut := datastore.NewInsert(key, entity)
	_, err := client.Mutate(ctx, mut)
	if err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	// Second insert with same key should fail with ALREADY_EXISTS
	entity2 := &TestEntity{Name: "second"}
	mut2 := datastore.NewInsert(key, entity2)
	_, err = client.Mutate(ctx, mut2)
	if err == nil {
		t.Error("Expected error for duplicate insert, got nil")
	}

	// Verify original entity is unchanged
	var retrieved TestEntity
	if err := client.Get(ctx, key, &retrieved); err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved.Name != "first" {
		t.Errorf("Expected Name 'first', got %q", retrieved.Name)
	}
}

func TestMockUpdateNotFound(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	// Try to update non-existent entity
	key := datastore.NameKey("UpdateTest", "nonexistent", nil)
	entity := &TestEntity{Name: "updated"}
	mut := datastore.NewUpdate(key, entity)
	_, err := client.Mutate(ctx, mut)
	if err == nil {
		t.Error("Expected error for update on non-existent entity, got nil")
	}

	// Verify entity was not created
	var retrieved TestEntity
	err = client.Get(ctx, key, &retrieved)
	if err == nil {
		t.Error("Expected ErrNoSuchEntity, but entity was found")
	}
}

func TestMockUpdateExisting(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	key := datastore.NameKey("UpdateTest2", "existing", nil)
	entity := &TestEntity{Name: "original"}

	// Create entity first
	if _, err := client.Put(ctx, key, entity); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Update should succeed
	updated := &TestEntity{Name: "updated"}
	mut := datastore.NewUpdate(key, updated)
	_, err := client.Mutate(ctx, mut)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify update
	var retrieved TestEntity
	if err := client.Get(ctx, key, &retrieved); err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved.Name != "updated" {
		t.Errorf("Expected Name 'updated', got %q", retrieved.Name)
	}
}

func TestMockUpsertAlwaysSucceeds(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	key := datastore.NameKey("UpsertTest", "test-key", nil)

	// First upsert (creates)
	entity1 := &TestEntity{Name: "first"}
	mut1 := datastore.NewUpsert(key, entity1)
	_, err := client.Mutate(ctx, mut1)
	if err != nil {
		t.Fatalf("First upsert failed: %v", err)
	}

	// Second upsert (updates)
	entity2 := &TestEntity{Name: "second"}
	mut2 := datastore.NewUpsert(key, entity2)
	_, err = client.Mutate(ctx, mut2)
	if err != nil {
		t.Fatalf("Second upsert failed: %v", err)
	}

	// Verify update
	var retrieved TestEntity
	if err := client.Get(ctx, key, &retrieved); err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved.Name != "second" {
		t.Errorf("Expected Name 'second', got %q", retrieved.Name)
	}
}

func TestGetAllKeysOnlyNilDst(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	// Create some entities
	for i := range 5 {
		key := datastore.NameKey("GetAllNilTest", string(rune('a'+i)), nil)
		entity := &TestEntity{Name: "test"}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// GetAll with KeysOnly and nil dst should work (like real Datastore)
	query := datastore.NewQuery("GetAllNilTest").KeysOnly()
	keys, err := client.GetAll(ctx, query, nil)
	if err != nil {
		t.Fatalf("GetAll with nil dst failed: %v", err)
	}

	if len(keys) != 5 {
		t.Errorf("Expected 5 keys, got %d", len(keys))
	}

	for _, key := range keys {
		if key.Kind != "GetAllNilTest" {
			t.Errorf("Expected kind 'GetAllNilTest', got %q", key.Kind)
		}
	}
}

func TestMockQueryDeterministicOrder(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	// Create entities with non-alphabetical insertion order
	keys := []string{"zebra", "apple", "mango", "banana"}
	for _, name := range keys {
		key := datastore.NameKey("OrderTest", name, nil)
		entity := &TestEntity{Name: name}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query multiple times and verify order is deterministic
	for run := range 3 {
		query := datastore.NewQuery("OrderTest").KeysOnly()
		resultKeys, err := client.AllKeys(ctx, query)
		if err != nil {
			t.Fatalf("AllKeys failed on run %d: %v", run, err)
		}

		if len(resultKeys) != 4 {
			t.Fatalf("Expected 4 keys, got %d", len(resultKeys))
		}

		// Results should be in alphabetical order by key name
		expected := []string{"apple", "banana", "mango", "zebra"}
		for i, key := range resultKeys {
			if key.Name != expected[i] {
				t.Errorf("Run %d: expected key %d to be %q, got %q", run, i, expected[i], key.Name)
			}
		}
	}
}

func TestMockPaginationWithCursor(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Count int64 `datastore:"count"`
	}

	// Create 10 entities
	for i := range 10 {
		key := datastore.NameKey("PaginationTest", string(rune('a'+i)), nil)
		entity := &TestEntity{Count: int64(i)}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit 3 and iterate through all pages
	var allKeys []*datastore.Key
	query := datastore.NewQuery("PaginationTest").Limit(3)
	it := client.Run(ctx, query)

	for {
		var entity TestEntity
		key, err := it.Next(&entity)
		if errors.Is(err, datastore.Done) {
			break
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		allKeys = append(allKeys, key)
	}

	// With limit=3, we should only get 3 results (not paginate automatically)
	if len(allKeys) != 3 {
		t.Errorf("Expected 3 keys with limit, got %d", len(allKeys))
	}

	// Verify cursor is available after iteration
	cursor, err := it.Cursor()
	if err != nil {
		t.Logf("Cursor not available: %v (this is OK if at end of results)", err)
	} else if cursor == "" {
		t.Error("Expected non-empty cursor")
	}
}

func TestMockTransactionValidation(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	// Test that transactions work correctly
	key := datastore.NameKey("TxTest", "test-key", nil)
	entity := &TestEntity{Name: "original"}

	// Put initial entity
	if _, err := client.Put(ctx, key, entity); err != nil {
		t.Fatalf("Initial Put failed: %v", err)
	}

	// Run a transaction that reads and writes
	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var retrieved TestEntity
		if err := tx.Get(key, &retrieved); err != nil {
			return err
		}

		retrieved.Name = "modified"
		_, err := tx.Put(key, &retrieved)
		return err
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Verify the modification persisted
	var result TestEntity
	if err := client.Get(ctx, key, &result); err != nil {
		t.Fatalf("Get after transaction failed: %v", err)
	}
	if result.Name != "modified" {
		t.Errorf("Expected Name 'modified', got %q", result.Name)
	}
}
