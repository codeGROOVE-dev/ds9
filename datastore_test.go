package ds9_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/codeGROOVE-dev/ds9"
	"github.com/codeGROOVE-dev/ds9/ds9mock"
)

// testEntity represents a simple test entity.
type testEntity struct {
	Name      string    `datastore:"name"`
	Count     int64     `datastore:"count"`
	Active    bool      `datastore:"active"`
	Score     float64   `datastore:"score"`
	UpdatedAt time.Time `datastore:"updated_at"`
	Notes     string    `datastore:"notes,noindex"`
}

func TestNewClient(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	// Just verify we got a valid client
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewClientWithDatabase(t *testing.T) {
	// Setup mock servers
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/project/project-id" {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("test-project")); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		if r.URL.Path == "/instance/service-accounts/default/token" {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"access_token": "test-token",
				"expires_in":   3600,
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metadataServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()

	// Test with explicit databaseID
	client, err := ds9.NewClientWithDatabase(ctx, "test-project", "custom-db")
	if err != nil {
		t.Fatalf("NewClientWithDatabase failed: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestPutAndGet(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create test entity
	now := time.Now().UTC().Truncate(time.Second)
	entity := &testEntity{
		Name:      "test-item",
		Count:     42,
		Active:    true,
		Score:     3.14,
		UpdatedAt: now,
		Notes:     "This is a test note",
	}

	// Put entity
	key := ds9.NameKey("TestKind", "test-key", nil)
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get entity
	var retrieved testEntity
	err = client.Get(ctx, key, &retrieved)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Verify fields
	if retrieved.Name != entity.Name {
		t.Errorf("Name: expected %q, got %q", entity.Name, retrieved.Name)
	}
	if retrieved.Count != entity.Count {
		t.Errorf("Count: expected %d, got %d", entity.Count, retrieved.Count)
	}
	if retrieved.Active != entity.Active {
		t.Errorf("Active: expected %v, got %v", entity.Active, retrieved.Active)
	}
	if retrieved.Score != entity.Score {
		t.Errorf("Score: expected %f, got %f", entity.Score, retrieved.Score)
	}
	if !retrieved.UpdatedAt.Equal(entity.UpdatedAt) {
		t.Errorf("UpdatedAt: expected %v, got %v", entity.UpdatedAt, retrieved.UpdatedAt)
	}
	if retrieved.Notes != entity.Notes {
		t.Errorf("Notes: expected %q, got %q", entity.Notes, retrieved.Notes)
	}
}

func TestGetNotFound(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	key := ds9.NameKey("TestKind", "nonexistent", nil)
	var entity testEntity
	err := client.Get(ctx, key, &entity)

	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("expected ErrNoSuchEntity, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put entity
	entity := &testEntity{
		Name:   "test-item",
		Count:  42,
		Active: true,
	}

	key := ds9.NameKey("TestKind", "test-key", nil)
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Delete entity
	err = client.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	var retrieved testEntity
	err = client.Get(ctx, key, &retrieved)
	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("expected ErrNoSuchEntity after delete, got %v", err)
	}
}

func TestAllKeys(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put multiple entities
	for i := range 5 {
		entity := &testEntity{
			Name:  "test-item",
			Count: int64(i),
		}
		key := ds9.NameKey("TestKind", string(rune('a'+i)), nil)
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query for all keys
	query := ds9.NewQuery("TestKind").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 5 {
		t.Errorf("expected 5 keys, got %d", len(keys))
	}
}

func TestAllKeysWithLimit(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put multiple entities
	for i := range 10 {
		entity := &testEntity{
			Name:  "test-item",
			Count: int64(i),
		}
		key := ds9.NameKey("TestKind", string(rune('a'+i)), nil)
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit
	query := ds9.NewQuery("TestKind").KeysOnly().Limit(3)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
}

func TestRunInTransaction(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put initial entity
	entity := &testEntity{
		Name:  "counter",
		Count: 0,
	}

	key := ds9.NameKey("TestKind", "counter", nil)
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Run transaction to read and update
	err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var current testEntity
		if err := tx.Get(key, &current); err != nil {
			return err
		}

		current.Count++
		_, err := tx.Put(key, &current)
		return err
	})
	if err != nil {
		t.Fatalf("RunInTransaction failed: %v", err)
	}

	// Verify the update
	var updated testEntity
	err = client.Get(ctx, key, &updated)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if updated.Count != 1 {
		t.Errorf("expected Count to be 1, got %d", updated.Count)
	}
}

func TestTransactionNotFound(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	key := ds9.NameKey("TestKind", "nonexistent", nil)

	err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var entity testEntity
		return tx.Get(key, &entity)
	})

	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("expected ErrNoSuchEntity, got %v", err)
	}
}

func TestIDKey(t *testing.T) {
	key := ds9.IDKey("TestKind", 12345, nil)

	if key.Kind != "TestKind" {
		t.Errorf("expected Kind %q, got %q", "TestKind", key.Kind)
	}

	if key.ID != 12345 {
		t.Errorf("expected ID %d, got %d", 12345, key.ID)
	}

	if key.Name != "" {
		t.Errorf("expected empty Name, got %q", key.Name)
	}
}

func TestNameKey(t *testing.T) {
	key := ds9.NameKey("TestKind", "test-name", nil)

	if key.Kind != "TestKind" {
		t.Errorf("expected Kind %q, got %q", "TestKind", key.Kind)
	}

	if key.Name != "test-name" {
		t.Errorf("expected Name %q, got %q", "test-name", key.Name)
	}

	if key.ID != 0 {
		t.Errorf("expected ID 0, got %d", key.ID)
	}
}

func TestMultiPutAndMultiGet(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create test entities
	now := time.Now().UTC().Truncate(time.Second)
	entities := []testEntity{
		{
			Name:      "item-1",
			Count:     1,
			Active:    true,
			Score:     1.1,
			UpdatedAt: now,
		},
		{
			Name:      "item-2",
			Count:     2,
			Active:    false,
			Score:     2.2,
			UpdatedAt: now,
		},
		{
			Name:      "item-3",
			Count:     3,
			Active:    true,
			Score:     3.3,
			UpdatedAt: now,
		},
	}

	keys := []*ds9.Key{
		ds9.NameKey("TestKind", "key-1", nil),
		ds9.NameKey("TestKind", "key-2", nil),
		ds9.NameKey("TestKind", "key-3", nil),
	}

	// MultiPut
	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("MultiPut failed: %v", err)
	}

	// MultiGet
	var retrieved []testEntity
	err = client.GetMulti(ctx, keys, &retrieved)
	if err != nil {
		t.Fatalf("MultiGet failed: %v", err)
	}

	if len(retrieved) != 3 {
		t.Fatalf("expected 3 entities, got %d", len(retrieved))
	}

	// Verify entities
	for i, entity := range retrieved {
		if entity.Name != entities[i].Name {
			t.Errorf("entity %d: Name mismatch: expected %q, got %q", i, entities[i].Name, entity.Name)
		}
		if entity.Count != entities[i].Count {
			t.Errorf("entity %d: Count mismatch: expected %d, got %d", i, entities[i].Count, entity.Count)
		}
		if entity.Active != entities[i].Active {
			t.Errorf("entity %d: Active mismatch: expected %v, got %v", i, entities[i].Active, entity.Active)
		}
		if entity.Score != entities[i].Score {
			t.Errorf("entity %d: Score mismatch: expected %f, got %f", i, entities[i].Score, entity.Score)
		}
	}
}

func TestMultiGetNotFound(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put only one entity
	entity := &testEntity{Name: "exists", Count: 1}
	key1 := ds9.NameKey("TestKind", "exists", nil)
	_, err := client.Put(ctx, key1, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Try to get multiple, one missing
	keys := []*ds9.Key{
		key1,
		ds9.NameKey("TestKind", "missing", nil),
	}

	var retrieved []testEntity
	err = client.GetMulti(ctx, keys, &retrieved)

	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("expected ErrNoSuchEntity when some keys missing, got %v", err)
	}
}

func TestMultiDelete(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put multiple entities
	entities := []testEntity{
		{Name: "item-1", Count: 1},
		{Name: "item-2", Count: 2},
		{Name: "item-3", Count: 3},
	}

	keys := []*ds9.Key{
		ds9.NameKey("TestKind", "key-1", nil),
		ds9.NameKey("TestKind", "key-2", nil),
		ds9.NameKey("TestKind", "key-3", nil),
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("MultiPut failed: %v", err)
	}

	// MultiDelete
	err = client.DeleteMulti(ctx, keys)
	if err != nil {
		t.Fatalf("MultiDelete failed: %v", err)
	}

	// Verify they're gone by trying to get them
	var retrieved []testEntity
	err = client.GetMulti(ctx, keys, &retrieved)
	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("expected ErrNoSuchEntity after delete, got %v", err)
	}
}

func TestMultiPutEmptyKeys(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	var entities []testEntity
	var keys []*ds9.Key

	_, err := client.PutMulti(ctx, keys, entities)
	if err == nil {
		t.Error("expected error for empty keys, got nil")
	}
}

func TestMultiGetEmptyKeys(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	var keys []*ds9.Key
	var retrieved []testEntity

	err := client.GetMulti(ctx, keys, &retrieved)
	if err == nil {
		t.Error("expected error for empty keys, got nil")
	}
}

func TestMultiDeleteEmptyKeys(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	var keys []*ds9.Key

	err := client.DeleteMulti(ctx, keys)
	if err == nil {
		t.Error("expected error for empty keys, got nil")
	}
}

func TestIDKeyOperations(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Test with ID key
	entity := &testEntity{
		Name:  "id-test",
		Count: 123,
	}

	key := ds9.IDKey("TestKind", 999, nil)
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put with ID key failed: %v", err)
	}

	// Get with ID key
	var retrieved testEntity
	err = client.Get(ctx, key, &retrieved)
	if err != nil {
		t.Fatalf("Get with ID key failed: %v", err)
	}

	if retrieved.Name != "id-test" {
		t.Errorf("expected Name 'id-test', got %q", retrieved.Name)
	}
}

func TestPutWithNilKey(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	entity := &testEntity{Name: "test"}
	_, err := client.Put(ctx, nil, entity)
	if err == nil {
		t.Error("expected error for nil key, got nil")
	}
}

func TestGetWithNilKey(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	var entity testEntity
	err := client.Get(ctx, nil, &entity)
	if err == nil {
		t.Error("expected error for nil key, got nil")
	}
}

func TestDeleteWithNilKey(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	err := client.Delete(ctx, nil)
	if err == nil {
		t.Error("expected error for nil key, got nil")
	}
}

func TestMultiGetWithNilKey(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*ds9.Key{
		ds9.NameKey("TestKind", "key-1", nil),
		nil,
		ds9.NameKey("TestKind", "key-2", nil),
	}

	var entities []testEntity
	err := client.GetMulti(ctx, keys, &entities)
	if err == nil {
		t.Error("expected error for nil key in slice, got nil")
	}
}

func TestMultiPutWithNilKey(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	entities := []testEntity{
		{Name: "item-1"},
		{Name: "item-2"},
	}

	keys := []*ds9.Key{
		ds9.NameKey("TestKind", "key-1", nil),
		nil,
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err == nil {
		t.Error("expected error for nil key in slice, got nil")
	}
}

func TestMultiDeleteWithNilKey(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*ds9.Key{
		ds9.NameKey("TestKind", "key-1", nil),
		nil,
	}

	err := client.DeleteMulti(ctx, keys)
	if err == nil {
		t.Error("expected error for nil key in slice, got nil")
	}
}

func TestMultiPutMismatchedSlices(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	entities := []testEntity{
		{Name: "item-1"},
		{Name: "item-2"},
	}

	keys := []*ds9.Key{
		ds9.NameKey("TestKind", "key-1", nil),
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err == nil {
		t.Error("expected error for mismatched slices, got nil")
	}
}

func TestAllKeysNonKeysOnlyQuery(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create a query without KeysOnly
	query := ds9.NewQuery("TestKind")
	_, err := client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error for non-KeysOnly query, got nil")
	}
}

func TestQueryOperations(t *testing.T) {
	// Test query builder methods
	query := ds9.NewQuery("TestKind")

	if query.KeysOnly().KeysOnly() == nil {
		t.Error("KeysOnly() should be chainable")
	}

	if query.Limit(10).Limit(20) == nil {
		t.Error("Limit() should be chainable")
	}
}

func TestEntityWithAllTypes(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type AllTypes struct {
		StringVal  string    `datastore:"str"`
		Int64Val   int64     `datastore:"i64"`
		Int32Val   int32     `datastore:"i32"`
		IntVal     int       `datastore:"i"`
		BoolVal    bool      `datastore:"b"`
		Float64Val float64   `datastore:"f64"`
		TimeVal    time.Time `datastore:"t"`
		NoIndex    string    `datastore:"noindex,noindex"`
		Skip       string    `datastore:"-"`
	}

	now := time.Now().UTC().Truncate(time.Second)
	entity := &AllTypes{
		StringVal:  "test",
		Int64Val:   int64(123),
		Int32Val:   int32(456),
		IntVal:     789,
		BoolVal:    true,
		Float64Val: 3.14,
		TimeVal:    now,
		NoIndex:    "not indexed",
		Skip:       "should not be stored",
	}

	key := ds9.NameKey("AllTypes", "test", nil)
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	var retrieved AllTypes
	err = client.Get(ctx, key, &retrieved)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.StringVal != entity.StringVal {
		t.Errorf("StringVal: expected %v, got %v", entity.StringVal, retrieved.StringVal)
	}
	if retrieved.Int64Val != entity.Int64Val {
		t.Errorf("Int64Val: expected %v, got %v", entity.Int64Val, retrieved.Int64Val)
	}
	if retrieved.Int32Val != entity.Int32Val {
		t.Errorf("Int32Val: expected %v, got %v", entity.Int32Val, retrieved.Int32Val)
	}
	if retrieved.IntVal != entity.IntVal {
		t.Errorf("IntVal: expected %v, got %v", entity.IntVal, retrieved.IntVal)
	}
	if retrieved.BoolVal != entity.BoolVal {
		t.Errorf("BoolVal: expected %v, got %v", entity.BoolVal, retrieved.BoolVal)
	}
	if retrieved.Float64Val != entity.Float64Val {
		t.Errorf("Float64Val: expected %v, got %v", entity.Float64Val, retrieved.Float64Val)
	}
	if !retrieved.TimeVal.Equal(entity.TimeVal) {
		t.Errorf("TimeVal: expected %v, got %v", entity.TimeVal, retrieved.TimeVal)
	}
	if retrieved.NoIndex != entity.NoIndex {
		t.Errorf("NoIndex: expected %v, got %v", entity.NoIndex, retrieved.NoIndex)
	}
	if retrieved.Skip != "" {
		t.Errorf("Skip field should be empty, got %q", retrieved.Skip)
	}
}

func TestSetTestURLs(t *testing.T) {
	// Save original values
	restore := ds9.SetTestURLs("http://test1", "http://test2")

	// Restore should work
	restore()

	// Should be chainable
	restore2 := ds9.SetTestURLs("http://test3", "http://test4")
	restore2()
}

func TestTransactionMultipleOperations(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put initial entities
	for i := range 3 {
		entity := &testEntity{
			Name:  "item",
			Count: int64(i),
		}
		key := ds9.NameKey("TestKind", string(rune('a'+i)), nil)
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Run transaction that reads and updates multiple entities
	err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		for i := range 3 {
			key := ds9.NameKey("TestKind", string(rune('a'+i)), nil)
			var current testEntity
			if err := tx.Get(key, &current); err != nil {
				return err
			}

			current.Count += 10
			_, err := tx.Put(key, &current)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunInTransaction failed: %v", err)
	}

	// Verify updates
	for i := range 3 {
		key := ds9.NameKey("TestKind", string(rune('a'+i)), nil)
		var retrieved testEntity
		err = client.Get(ctx, key, &retrieved)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		expectedCount := int64(i + 10)
		if retrieved.Count != expectedCount {
			t.Errorf("entity %d: expected Count %d, got %d", i, expectedCount, retrieved.Count)
		}
	}
}

func TestMultiGetPartialResults(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put some entities
	entities := []testEntity{
		{Name: "item-1", Count: 1},
		{Name: "item-3", Count: 3},
	}
	keys := []*ds9.Key{
		ds9.NameKey("TestKind", "key-1", nil),
		ds9.NameKey("TestKind", "key-3", nil),
	}
	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("MultiPut failed: %v", err)
	}

	// Try to get more keys than exist
	getAllKeys := []*ds9.Key{
		ds9.NameKey("TestKind", "key-1", nil),
		ds9.NameKey("TestKind", "key-2", nil), // doesn't exist
		ds9.NameKey("TestKind", "key-3", nil),
	}

	var retrieved []testEntity
	err = client.GetMulti(ctx, getAllKeys, &retrieved)
	if err == nil {
		t.Error("expected error when some keys don't exist")
	}
}

func TestEmptyQuery(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Query for keys when no entities exist
	query := ds9.NewQuery("NonExistent").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected 0 keys, got %d", len(keys))
	}
}

func TestKeyComparison(t *testing.T) {
	nameKey1 := ds9.NameKey("Kind", "name", nil)
	nameKey2 := ds9.NameKey("Kind", "name", nil)

	if nameKey1.Kind != nameKey2.Kind || nameKey1.Name != nameKey2.Name {
		t.Error("identical name keys should have same values")
	}

	idKey1 := ds9.IDKey("Kind", 123, nil)
	idKey2 := ds9.IDKey("Kind", 123, nil)

	if idKey1.Kind != idKey2.Kind || idKey1.ID != idKey2.ID {
		t.Error("identical ID keys should have same values")
	}
}

func TestLargeEntityBatch(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create a larger batch
	const batchSize = 50
	entities := make([]testEntity, batchSize)
	keys := make([]*ds9.Key, batchSize)

	for i := range batchSize {
		entities[i] = testEntity{
			Name:  "batch-item",
			Count: int64(i),
		}
		keys[i] = ds9.NameKey("BatchKind", string(rune('0'+i/10))+string(rune('0'+i%10)), nil)
	}

	// MultiPut
	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("MultiPut failed: %v", err)
	}

	// MultiGet
	var retrieved []testEntity
	err = client.GetMulti(ctx, keys, &retrieved)
	if err != nil {
		t.Fatalf("MultiGet failed: %v", err)
	}

	if len(retrieved) != batchSize {
		t.Errorf("expected %d entities, got %d", batchSize, len(retrieved))
	}

	// MultiDelete
	err = client.DeleteMulti(ctx, keys)
	if err != nil {
		t.Fatalf("MultiDelete failed: %v", err)
	}

	// Verify deletion
	var retrieved2 []testEntity
	err = client.GetMulti(ctx, keys, &retrieved2)
	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("expected ErrNoSuchEntity after batch delete, got %v", err)
	}
}

func TestUnsupportedEncodeType(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Entity with unsupported type (slice)
	type BadEntity struct {
		Name  string
		Items []string // slices not supported
	}

	key := ds9.NameKey("TestKind", "bad", nil)
	entity := BadEntity{
		Name:  "test",
		Items: []string{"a", "b"},
	}

	_, err := client.Put(ctx, key, &entity)
	if err == nil {
		t.Error("expected error for unsupported type, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported type") {
		t.Errorf("expected 'unsupported type' error, got: %v", err)
	}
}

func TestDecodeNonPointer(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Store entity
	key := ds9.NameKey("TestKind", "test", nil)
	entity := testEntity{Name: "test", Count: 42}
	_, err := client.Put(ctx, key, &entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Try to decode into non-pointer
	var notPtr testEntity
	err = client.Get(ctx, key, notPtr) // Should be &notPtr
	if err == nil {
		t.Error("expected error for non-pointer dst, got nil")
	}
	if !strings.Contains(err.Error(), "pointer to struct") {
		t.Errorf("expected 'pointer to struct' error, got: %v", err)
	}
}

func TestDecodePointerToNonStruct(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Store entity
	key := ds9.NameKey("TestKind", "test", nil)
	entity := testEntity{Name: "test", Count: 42}
	_, err := client.Put(ctx, key, &entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Try to decode into pointer to string
	var str string
	err = client.Get(ctx, key, &str)
	if err == nil {
		t.Error("expected error for pointer to non-struct, got nil")
	}
	if !strings.Contains(err.Error(), "pointer to struct") {
		t.Errorf("expected 'pointer to struct' error, got: %v", err)
	}
}

func TestEntityWithSkippedFields(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type EntityWithSkip struct {
		Name    string `datastore:"name"`
		Count   int64  `datastore:"count"`
		Skipped string `datastore:"-"` // Should not be stored
		private string // Should not be stored (unexported)
	}

	key := ds9.NameKey("TestKind", "skip", nil)
	entity := EntityWithSkip{
		Name:    "test",
		Count:   42,
		Skipped: "should not store",
		private: "also not stored",
	}

	_, err := client.Put(ctx, key, &entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	var retrieved EntityWithSkip
	err = client.Get(ctx, key, &retrieved)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Name != entity.Name || retrieved.Count != entity.Count {
		t.Errorf("wrong values: got %+v", retrieved)
	}

	// Skipped field should be zero value
	if retrieved.Skipped != "" {
		t.Errorf("Skipped field should be empty, got %q", retrieved.Skipped)
	}
}

func TestZeroValueEntity(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type ZeroEntity struct {
		Name   string
		Count  int64
		Active bool
		Score  float64
	}

	key := ds9.NameKey("TestKind", "zero", nil)
	entity := ZeroEntity{} // All zero values

	_, err := client.Put(ctx, key, &entity)
	if err != nil {
		t.Fatalf("Put with zero values failed: %v", err)
	}

	var retrieved ZeroEntity
	err = client.Get(ctx, key, &retrieved)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Name != "" || retrieved.Count != 0 || retrieved.Active != false || retrieved.Score != 0.0 {
		t.Errorf("expected zero values, got %+v", retrieved)
	}
}

func TestQueryWithLimitZero(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Store some entities
	for i := range 5 {
		key := ds9.NameKey("LimitKind", string(rune('a'+i)), nil)
		entity := testEntity{Name: "item", Count: int64(i)}
		if _, err := client.Put(ctx, key, &entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit 0 (should return all)
	query := ds9.NewQuery("LimitKind").KeysOnly().Limit(0)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) == 0 {
		t.Error("expected keys, got 0 (limit 0 should mean unlimited)")
	}
}

func TestQueryWithLimitLessThanResults(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Store 10 entities
	for i := range 10 {
		key := ds9.NameKey("LimitKind2", string(rune('a'+i)), nil)
		entity := testEntity{Name: "item", Count: int64(i)}
		if _, err := client.Put(ctx, key, &entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit 3
	query := ds9.NewQuery("LimitKind2").KeysOnly().Limit(3)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
}

func TestMultiGetEmptySlices(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Call MultiGet with empty slices - should return error
	var entities []testEntity
	err := client.GetMulti(ctx, []*ds9.Key{}, &entities)
	if err == nil {
		t.Error("expected error for MultiGet with empty keys, got nil")
	}
}

func TestMultiPutEmptySlices(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Call MultiPut with empty slices - should return error
	_, err := client.PutMulti(ctx, []*ds9.Key{}, []testEntity{})
	if err == nil {
		t.Error("expected error for MultiPut with empty keys, got nil")
	}
}

func TestMultiDeleteEmptySlice(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Call MultiDelete with empty slice - should return error
	err := client.DeleteMulti(ctx, []*ds9.Key{})
	if err == nil {
		t.Error("expected error for MultiDelete with empty keys, got nil")
	}
}

func TestNewClientWithDatabaseEmptyProjectID(t *testing.T) {
	// Setup mock servers
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/project/project-id" {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("auto-detected-project")); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		if r.URL.Path == "/instance/service-accounts/default/token" {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"access_token": "test-token",
				"expires_in":   3600,
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metadataServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()

	// Test with empty projectID - should fetch from metadata
	client, err := ds9.NewClientWithDatabase(ctx, "", "my-db")
	if err != nil {
		t.Fatalf("NewClientWithDatabase with empty projectID failed: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewClientWithDatabaseProjectIDFetchFailure(t *testing.T) {
	// Setup mock servers that fail to provide projectID
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/project/project-id" {
			// Return error instead of project ID
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/instance/service-accounts/default/token" {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"access_token": "test-token",
				"expires_in":   3600,
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metadataServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()

	// Test with empty projectID and failing metadata server
	client, err := ds9.NewClientWithDatabase(ctx, "", "my-db")
	if err == nil {
		t.Fatal("expected error when projectID fetch fails, got nil")
	}
	if client != nil {
		t.Errorf("expected nil client on error, got %v", client)
	}
	if !strings.Contains(err.Error(), "project ID required") {
		t.Errorf("expected 'project ID required' error, got: %v", err)
	}
}

func TestTransactionWithError(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Store initial entity
	key := ds9.NameKey("TestKind", "tx-err", nil)
	entity := testEntity{Name: "initial", Count: 1}
	_, err := client.Put(ctx, key, &entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Run transaction that errors
	err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var current testEntity
		if err := tx.Get(key, &current); err != nil {
			return err
		}

		current.Count = 999

		if _, err := tx.Put(key, &current); err != nil {
			return err
		}

		// Return error to trigger rollback
		return errors.New("intentional error")
	})

	if err == nil {
		t.Fatal("expected transaction to fail, got nil error")
	}
	if !strings.Contains(err.Error(), "intentional error") {
		t.Errorf("expected 'intentional error', got: %v", err)
	}

	// Verify entity was not modified (transaction rolled back)
	// Note: In a real implementation this would check rollback, but our mock doesn't support it
	// This test at least exercises the error path
}

func TestTransactionWithDatabaseID(t *testing.T) {
	// Setup mock servers
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/project/project-id" {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("test-project")); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		if r.URL.Path == "/instance/service-accounts/default/token" {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"access_token": "test-token",
				"expires_in":   3600,
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metadataServer.Close()

	txID := "test-tx-123"
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		var reqBody map[string]any
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Check for databaseId in request
		if dbID, ok := reqBody["databaseId"].(string); ok && dbID != "tx-db" {
			t.Errorf("expected databaseId 'tx-db', got %v", dbID)
		}

		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path == "/projects/test-project:beginTransaction" {
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": txID,
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if r.URL.Path == "/projects/test-project:commit" {
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if r.URL.Path == "/projects/test-project:lookup" {
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClientWithDatabase(ctx, "test-project", "tx-db")
	if err != nil {
		t.Fatalf("NewClientWithDatabase failed: %v", err)
	}

	// Run transaction with databaseID
	err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		key := ds9.NameKey("TestKind", "tx-test", nil)
		entity := testEntity{Name: "in-tx", Count: 42}
		_, err := tx.Put(key, &entity)
		return err
	})
	if err != nil {
		t.Fatalf("Transaction with databaseID failed: %v", err)
	}
}

func TestDeleteWithDatabaseID(t *testing.T) {
	// Setup with databaseID
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/project/project-id" {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("test-project")); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		if r.URL.Path == "/instance/service-accounts/default/token" {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"access_token": "test-token",
				"expires_in":   3600,
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metadataServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"mutationResults": []any{},
		}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClientWithDatabase(ctx, "test-project", "del-db")
	if err != nil {
		t.Fatalf("NewClientWithDatabase failed: %v", err)
	}

	// Delete with databaseID
	key := ds9.NameKey("TestKind", "to-delete", nil)
	err = client.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete with databaseID failed: %v", err)
	}
}

func TestAllKeysWithDatabaseID(t *testing.T) {
	// Setup with databaseID
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/project/project-id" {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("test-project")); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		if r.URL.Path == "/instance/service-accounts/default/token" {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"access_token": "test-token",
				"expires_in":   3600,
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metadataServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"batch": map[string]any{
				"entityResults": []any{},
			},
		}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClientWithDatabase(ctx, "test-project", "query-db")
	if err != nil {
		t.Fatalf("NewClientWithDatabase failed: %v", err)
	}

	// Query with databaseID
	query := ds9.NewQuery("TestKind").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys with databaseID failed: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected 0 keys, got %d", len(keys))
	}
}

func TestMultiGetWithDatabaseID(t *testing.T) {
	// Setup with databaseID
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/project/project-id" {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("test-project")); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		if r.URL.Path == "/instance/service-accounts/default/token" {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"access_token": "test-token",
				"expires_in":   3600,
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metadataServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Return missing entities to trigger ErrNoSuchEntity
		if err := json.NewEncoder(w).Encode(map[string]any{
			"found": []any{},
			"missing": []any{
				map[string]any{"entity": map[string]any{"key": map[string]any{}}},
			},
		}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClientWithDatabase(ctx, "test-project", "multiget-db")
	if err != nil {
		t.Fatalf("NewClientWithDatabase failed: %v", err)
	}

	// MultiGet with databaseID
	keys := []*ds9.Key{
		ds9.NameKey("TestKind", "key1", nil),
		ds9.NameKey("TestKind", "key2", nil),
	}
	var entities []testEntity
	err = client.GetMulti(ctx, keys, &entities)
	// Expect error since entities don't exist
	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("expected ErrNoSuchEntity, got: %v", err)
	}
}

func TestMultiDeleteWithDatabaseID(t *testing.T) {
	// Setup with databaseID
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/project/project-id" {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("test-project")); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		if r.URL.Path == "/instance/service-accounts/default/token" {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"access_token": "test-token",
				"expires_in":   3600,
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metadataServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"mutationResults": []any{},
		}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClientWithDatabase(ctx, "test-project", "multidel-db")
	if err != nil {
		t.Fatalf("NewClientWithDatabase failed: %v", err)
	}

	// MultiDelete with databaseID
	keys := []*ds9.Key{
		ds9.NameKey("TestKind", "key1", nil),
		ds9.NameKey("TestKind", "key2", nil),
	}
	err = client.DeleteMulti(ctx, keys)
	if err != nil {
		t.Fatalf("MultiDelete with databaseID failed: %v", err)
	}
}
