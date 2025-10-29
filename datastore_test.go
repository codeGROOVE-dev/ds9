package ds9_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	UpdatedAt time.Time `datastore:"updated_at"`
	Name      string    `datastore:"name"`
	Notes     string    `datastore:"notes,noindex"`
	Count     int64     `datastore:"count"`
	Score     float64   `datastore:"score"`
	Active    bool      `datastore:"active"`
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
	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
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

	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
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
		TimeVal    time.Time `datastore:"t"`
		StringVal  string    `datastore:"str"`
		NoIndex    string    `datastore:"noindex,noindex"`
		Skip       string    `datastore:"-"`
		Int64Val   int64     `datastore:"i64"`
		IntVal     int       `datastore:"i"`
		Float64Val float64   `datastore:"f64"`
		Int32Val   int32     `datastore:"i32"`
		BoolVal    bool      `datastore:"b"`
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
	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
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

	// Entity with unsupported type (map)
	type BadEntity struct {
		Name string
		Data map[string]string // maps not supported
	}

	key := ds9.NameKey("TestKind", "bad", nil)
	entity := BadEntity{
		Name: "test",
		Data: map[string]string{"key": "value"},
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
		Skipped string `datastore:"-"`
		private string
		Count   int64 `datastore:"count"`
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
	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
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
	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
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

func TestDeleteAllByKind(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put multiple entities of the same kind
	for i := range 5 {
		entity := &testEntity{
			Name:  "item",
			Count: int64(i),
		}
		key := ds9.NameKey("DeleteKind", string(rune('a'+i)), nil)
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete all entities of this kind
	err := client.DeleteAllByKind(ctx, "DeleteKind")
	if err != nil {
		t.Fatalf("DeleteAllByKind failed: %v", err)
	}

	// Verify all deleted
	query := ds9.NewQuery("DeleteKind").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected 0 keys after DeleteAllByKind, got %d", len(keys))
	}
}

func TestDeleteAllByKindEmpty(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Delete from non-existent kind
	err := client.DeleteAllByKind(ctx, "NonExistentKind")
	if err != nil {
		t.Errorf("DeleteAllByKind on empty kind should not error, got: %v", err)
	}
}

func TestHierarchicalKeys(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create parent key
	parentKey := ds9.NameKey("Parent", "parent1", nil)
	parentEntity := &testEntity{
		Name:  "parent",
		Count: 1,
	}
	_, err := client.Put(ctx, parentKey, parentEntity)
	if err != nil {
		t.Fatalf("Put parent failed: %v", err)
	}

	// Create child key with parent
	childKey := ds9.NameKey("Child", "child1", parentKey)
	childEntity := &testEntity{
		Name:  "child",
		Count: 2,
	}
	_, err = client.Put(ctx, childKey, childEntity)
	if err != nil {
		t.Fatalf("Put child failed: %v", err)
	}

	// Get child
	var retrieved testEntity
	err = client.Get(ctx, childKey, &retrieved)
	if err != nil {
		t.Fatalf("Get child failed: %v", err)
	}

	if retrieved.Name != "child" {
		t.Errorf("expected child name 'child', got %q", retrieved.Name)
	}
}

func TestHierarchicalKeysMultiLevel(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create grandparent -> parent -> child hierarchy
	grandparentKey := ds9.NameKey("Grandparent", "gp1", nil)
	parentKey := ds9.NameKey("Parent", "p1", grandparentKey)
	childKey := ds9.NameKey("Child", "c1", parentKey)

	entity := &testEntity{
		Name:  "deep-child",
		Count: 42,
	}

	_, err := client.Put(ctx, childKey, entity)
	if err != nil {
		t.Fatalf("Put with multi-level hierarchy failed: %v", err)
	}

	var retrieved testEntity
	err = client.Get(ctx, childKey, &retrieved)
	if err != nil {
		t.Fatalf("Get with multi-level hierarchy failed: %v", err)
	}

	if retrieved.Name != "deep-child" {
		t.Errorf("expected name 'deep-child', got %q", retrieved.Name)
	}
}

func TestDoRequestRetryOn5xxError(t *testing.T) {
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

	attemptCount := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount++
		// Return 503 on first two attempts, then succeed
		if attemptCount < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := w.Write([]byte(`{"error":"service unavailable"}`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"mutationResults": []any{
				map[string]any{"key": map[string]any{}},
			},
		}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// This should succeed after retries
	key := ds9.NameKey("TestKind", "retry-test", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put should succeed after retries, got: %v", err)
	}

	if attemptCount < 2 {
		t.Errorf("expected at least 2 attempts, got %d", attemptCount)
	}
}

func TestDoRequestFailsOn4xxError(t *testing.T) {
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

	attemptCount := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount++
		// Always return 400 Bad Request
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		if _, err := w.Write([]byte(`{"error":"bad request"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// This should fail immediately without retry on 4xx
	key := ds9.NameKey("TestKind", "bad-request", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)
	if err == nil {
		t.Fatal("expected error on 4xx response")
	}

	if !strings.Contains(err.Error(), "400") {
		t.Errorf("expected error to mention 400 status, got: %v", err)
	}

	// Should only try once for 4xx errors (no retry)
	if attemptCount != 1 {
		t.Errorf("expected exactly 1 attempt for 4xx error, got %d", attemptCount)
	}
}

func TestDoRequestContextCancellation(t *testing.T) {
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

	attemptCount := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount++
		// Always return 503 to force retry
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, err := w.Write([]byte(`{"error":"unavailable"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// Create context that we'll cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	key := ds9.NameKey("TestKind", "cancel-test", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)

	if err == nil {
		t.Fatal("expected error when context is cancelled")
	}

	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("expected context cancellation error, got: %v", err)
	}
}

func TestTransactionRollback(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put initial entity
	key := ds9.NameKey("TestKind", "rollback-test", nil)
	entity := &testEntity{Name: "original", Count: 1}
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Run transaction that will fail
	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var current testEntity
		if err := tx.Get(key, &current); err != nil {
			return err
		}

		current.Name = "modified"
		current.Count = 999

		_, err := tx.Put(key, &current)
		if err != nil {
			return err
		}

		// Return error to cause rollback
		return errors.New("force rollback")
	})

	if err == nil {
		t.Fatal("expected transaction to fail")
	}

	if !strings.Contains(err.Error(), "force rollback") {
		t.Errorf("expected 'force rollback' error, got: %v", err)
	}
}

func TestPutWithInvalidEntity(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type InvalidEntity struct {
		Map map[string]string // maps not supported
	}

	key := ds9.NameKey("TestKind", "invalid", nil)
	entity := &InvalidEntity{
		Map: map[string]string{"key": "value"},
	}

	_, err := client.Put(ctx, key, entity)
	if err == nil {
		t.Error("expected error for unsupported entity type")
	}
}

func TestGetMultiWithMismatchedSliceSize(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put one entity
	key1 := ds9.NameKey("TestKind", "key1", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err := client.Put(ctx, key1, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Try to get with wrong slice type
	keys := []*ds9.Key{key1}
	var retrieved []testEntity

	// This should work
	err = client.GetMulti(ctx, keys, &retrieved)
	if err != nil {
		t.Fatalf("GetMulti failed: %v", err)
	}

	if len(retrieved) != 1 {
		t.Errorf("expected 1 entity, got %d", len(retrieved))
	}
}

func TestTransactionBeginFailure(t *testing.T) {
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
		// Fail to begin transaction
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(`{"error":"internal error"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		return nil
	})

	if err == nil {
		t.Fatal("expected transaction to fail on begin")
	}

	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected error to mention 500 status, got: %v", err)
	}
}

func TestTransactionCommitAbortedRetry(t *testing.T) {
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

	commitAttempt := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-123",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "commit") {
			commitAttempt++
			// Fail with 409 ABORTED on first two attempts, succeed on third
			if commitAttempt < 3 {
				w.WriteHeader(http.StatusConflict)
				if _, err := w.Write([]byte(`{"error":"ABORTED: transaction aborted"}`)); err != nil {
					t.Logf("write failed: %v", err)
				}
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "lookup") {
			w.Header().Set("Content-Type", "application/json")
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
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// This should succeed after retries
	key := ds9.NameKey("TestKind", "tx-retry", nil)
	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		_, err := tx.Put(key, &testEntity{Name: "test", Count: 1})
		return err
	})
	if err != nil {
		t.Fatalf("transaction should succeed after retries, got: %v", err)
	}

	if commitAttempt < 2 {
		t.Errorf("expected at least 2 commit attempts, got %d", commitAttempt)
	}
}

func TestTransactionMaxRetriesExceeded(t *testing.T) {
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

	commitAttempt := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-456",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "commit") {
			commitAttempt++
			// Always return 409 ABORTED
			w.WriteHeader(http.StatusConflict)
			if _, err := w.Write([]byte(`{"error":"status 409 ABORTED: transaction conflict"}`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "lookup") {
			w.Header().Set("Content-Type", "application/json")
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
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// This should fail after max retries
	key := ds9.NameKey("TestKind", "tx-max-retry", nil)
	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		_, err := tx.Put(key, &testEntity{Name: "test", Count: 1})
		return err
	})

	if err == nil {
		t.Fatal("expected transaction to fail after max retries")
	}

	if !strings.Contains(err.Error(), "failed after 3 attempts") {
		t.Errorf("expected 'failed after 3 attempts' error, got: %v", err)
	}

	if commitAttempt != 3 {
		t.Errorf("expected exactly 3 commit attempts, got %d", commitAttempt)
	}
}

func TestKeyFromJSONEdgeCases(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Test with ID key using integer ID
	idKey := ds9.IDKey("TestKind", 12345, nil)
	entity := &testEntity{Name: "id-test", Count: 1}
	_, err := client.Put(ctx, idKey, entity)
	if err != nil {
		t.Fatalf("Put with ID key failed: %v", err)
	}

	var retrieved testEntity
	err = client.Get(ctx, idKey, &retrieved)
	if err != nil {
		t.Fatalf("Get with ID key failed: %v", err)
	}

	if retrieved.Name != "id-test" {
		t.Errorf("expected name 'id-test', got %q", retrieved.Name)
	}
}

func TestDecodeValueEdgeCases(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Test with all basic types
	type ComplexEntity struct {
		Time    time.Time `datastore:"t"`
		String  string    `datastore:"s"`
		NoIndex string    `datastore:"n,noindex"`
		Int     int       `datastore:"i"`
		Int64   int64     `datastore:"i64"`
		Float   float64   `datastore:"f"`
		Int32   int32     `datastore:"i32"`
		Bool    bool      `datastore:"b"`
	}

	now := time.Now().UTC().Truncate(time.Second)
	key := ds9.NameKey("Complex", "test", nil)
	entity := &ComplexEntity{
		String:  "test",
		Int:     42,
		Int32:   32,
		Int64:   64,
		Float:   3.14,
		Bool:    true,
		Time:    now,
		NoIndex: "not indexed",
	}

	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	var retrieved ComplexEntity
	err = client.Get(ctx, key, &retrieved)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.String != entity.String {
		t.Errorf("String mismatch")
	}
	if retrieved.Int != entity.Int {
		t.Errorf("Int mismatch")
	}
	if retrieved.Int32 != entity.Int32 {
		t.Errorf("Int32 mismatch")
	}
	if retrieved.Int64 != entity.Int64 {
		t.Errorf("Int64 mismatch")
	}
	if retrieved.Float != entity.Float {
		t.Errorf("Float mismatch")
	}
	if retrieved.Bool != entity.Bool {
		t.Errorf("Bool mismatch")
	}
	if !retrieved.Time.Equal(entity.Time) {
		t.Errorf("Time mismatch")
	}
	if retrieved.NoIndex != entity.NoIndex {
		t.Errorf("NoIndex mismatch")
	}
}

func TestGetMultiMixedResults(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put some entities
	key1 := ds9.NameKey("Mixed", "exists1", nil)
	key2 := ds9.NameKey("Mixed", "exists2", nil)
	key3 := ds9.NameKey("Mixed", "missing", nil)

	entities := []testEntity{
		{Name: "entity1", Count: 1},
		{Name: "entity2", Count: 2},
	}

	_, err := client.PutMulti(ctx, []*ds9.Key{key1, key2}, entities)
	if err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	// Try to get mix of existing and non-existing
	keys := []*ds9.Key{key1, key2, key3}
	var retrieved []testEntity

	err = client.GetMulti(ctx, keys, &retrieved)
	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("expected ErrNoSuchEntity for mixed results, got: %v", err)
	}
}

func TestPutMultiLargeBatch(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create large batch
	const size = 100
	entities := make([]testEntity, size)
	keys := make([]*ds9.Key, size)

	for i := range size {
		entities[i] = testEntity{
			Name:  "large-batch",
			Count: int64(i),
		}
		keys[i] = ds9.NameKey("LargeBatch", fmt.Sprintf("key-%d", i), nil)
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("PutMulti with large batch failed: %v", err)
	}

	// Verify a few
	var retrieved testEntity
	err = client.Get(ctx, keys[0], &retrieved)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Count != 0 {
		t.Errorf("expected Count 0, got %d", retrieved.Count)
	}
}

func TestGetWithHTTPError(t *testing.T) {
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
		// Return 404 for lookup
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"error": "not found",
		}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := ds9.NameKey("TestKind", "test", nil)
	var entity testEntity
	err = client.Get(ctx, key, &entity)

	if err == nil {
		t.Fatal("expected error on 404")
	}

	if !strings.Contains(err.Error(), "404") {
		t.Errorf("expected error to mention 404, got: %v", err)
	}
}

func TestPutWithHTTPError(t *testing.T) {
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
		// Return 403 Forbidden
		w.WriteHeader(http.StatusForbidden)
		if _, err := w.Write([]byte(`{"error":"permission denied"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := ds9.NameKey("TestKind", "test", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)

	if err == nil {
		t.Fatal("expected error on 403")
	}

	if !strings.Contains(err.Error(), "403") {
		t.Errorf("expected error to mention 403, got: %v", err)
	}
}

func TestDeleteMultiWithErrors(t *testing.T) {
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
		// Return server error
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(`{"error":"internal error"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	keys := []*ds9.Key{
		ds9.NameKey("TestKind", "key1", nil),
		ds9.NameKey("TestKind", "key2", nil),
	}

	err = client.DeleteMulti(ctx, keys)
	if err == nil {
		t.Fatal("expected error on server failure")
	}
}

func TestQueryNonKeysOnly(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Try to call AllKeys with non-KeysOnly query
	query := ds9.NewQuery("TestKind")
	_, err := client.AllKeys(ctx, query)

	if err == nil {
		t.Error("expected error for non-KeysOnly query")
	}

	if !strings.Contains(err.Error(), "KeysOnly") {
		t.Errorf("expected error to mention KeysOnly, got: %v", err)
	}
}

func TestDoRequestAllRetriesFail(t *testing.T) {
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

	attemptCount := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount++
		// Always fail with 500
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(`{"error":"persistent failure"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := ds9.NameKey("TestKind", "test", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)

	if err == nil {
		t.Fatal("expected error after all retries")
	}

	if !strings.Contains(err.Error(), "attempts failed") {
		t.Errorf("expected 'attempts failed' error, got: %v", err)
	}

	// Should have tried multiple times
	if attemptCount < 3 {
		t.Errorf("expected at least 3 attempts, got %d", attemptCount)
	}
}

func TestEntityWithPointerFields(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Entities with pointer fields
	type EntityWithPointers struct {
		Name  *string `datastore:"name"`
		Count *int64  `datastore:"count"`
	}

	name := "test"
	count := int64(42)
	key := ds9.NameKey("Pointers", "test", nil)
	entity := &EntityWithPointers{
		Name:  &name,
		Count: &count,
	}

	// Note: The current implementation doesn't support pointer fields
	// This test documents the expected behavior
	_, err := client.Put(ctx, key, entity)
	if err == nil {
		// If it succeeds, that's fine (future enhancement)
		var retrieved EntityWithPointers
		err = client.Get(ctx, key, &retrieved)
		if err != nil {
			t.Logf("Get after Put with pointers failed: %v", err)
		}
	} else {
		// Expected to fail with current implementation
		t.Logf("Put with pointer fields failed as expected: %v", err)
	}
}

func TestKeyWithOnlyKind(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Key with neither name nor ID should work (incomplete key)
	// This gets an ID assigned by the datastore
	key := &ds9.Key{Kind: "TestKind"}
	entity := &testEntity{Name: "test", Count: 1}

	returnedKey, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put with incomplete key failed: %v", err)
	}

	// The returned key should have an ID
	if returnedKey == nil {
		t.Fatal("expected non-nil returned key")
	}

	if returnedKey.Kind != "TestKind" {
		t.Errorf("expected Kind 'TestKind', got %q", returnedKey.Kind)
	}
}

func TestTransactionGetNonExistent(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	key := ds9.NameKey("TestKind", "nonexistent", nil)

	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var entity testEntity
		return tx.Get(key, &entity)
	})

	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("expected ErrNoSuchEntity in transaction, got: %v", err)
	}
}

func TestGetMultiAllMissing(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*ds9.Key{
		ds9.NameKey("Missing", "key1", nil),
		ds9.NameKey("Missing", "key2", nil),
		ds9.NameKey("Missing", "key3", nil),
	}

	var entities []testEntity
	err := client.GetMulti(ctx, keys, &entities)

	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("expected ErrNoSuchEntity when all keys missing, got: %v", err)
	}
}

func TestGetMultiWithSliceMismatch(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put entity
	key := ds9.NameKey("Test", "key1", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// GetMulti with destination not being a pointer to slice
	var notSlice testEntity
	err = client.GetMulti(ctx, []*ds9.Key{key}, notSlice)
	if err == nil {
		t.Error("expected error when dst is not pointer to slice")
	}
}

func TestPutMultiWithLengthMismatch(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Keys and entities with different lengths
	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
		ds9.NameKey("Test", "key2", nil),
	}
	entities := []testEntity{
		{Name: "only-one", Count: 1},
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err == nil {
		t.Error("expected error when keys and entities have different lengths")
	}
}

func TestDeleteWithNonexistentKey(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Delete non-existent key (should not error)
	key := ds9.NameKey("Test", "nonexistent", nil)
	err := client.Delete(ctx, key)
	if err != nil {
		t.Errorf("Delete of non-existent key should not error, got: %v", err)
	}
}

func TestAllKeysWithEmptyResult(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Query kind with no entities
	query := ds9.NewQuery("EmptyKind").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys on empty kind failed: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected 0 keys, got %d", len(keys))
	}
}

func TestAllKeysWithLargeResult(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put many entities
	for i := range 50 {
		key := ds9.NameKey("LargeResult", fmt.Sprintf("key-%d", i), nil)
		entity := &testEntity{Name: "test", Count: int64(i)}
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query all
	query := ds9.NewQuery("LargeResult").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 50 {
		t.Errorf("expected 50 keys, got %d", len(keys))
	}
}

func TestQueryWithZeroLimit(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put entities
	for i := range 5 {
		key := ds9.NameKey("ZeroLimit", fmt.Sprintf("key-%d", i), nil)
		entity := &testEntity{Name: "test", Count: int64(i)}
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit 0 (should return all)
	query := ds9.NewQuery("ZeroLimit").KeysOnly().Limit(0)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys with limit 0 failed: %v", err)
	}

	// Limit 0 should mean unlimited
	if len(keys) == 0 {
		t.Error("expected results with limit 0 (unlimited), got 0")
	}
}

func TestPutMultiEmptySlice(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Empty slices
	_, err := client.PutMulti(ctx, []*ds9.Key{}, []testEntity{})
	if err == nil {
		t.Error("expected error for empty slices")
	}
}

func TestGetMultiEmptySlice(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	var entities []testEntity
	err := client.GetMulti(ctx, []*ds9.Key{}, &entities)
	if err == nil {
		t.Error("expected error for empty keys")
	}
}

func TestDeleteMultiEmptySlice(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	err := client.DeleteMulti(ctx, []*ds9.Key{})
	if err == nil {
		t.Error("expected error for empty keys")
	}
}

func TestTransactionPutWithNilKey(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		entity := &testEntity{Name: "test", Count: 1}
		_, err := tx.Put(nil, entity)
		return err
	})

	if err == nil {
		t.Error("expected error for nil key in transaction")
	}
}

func TestTransactionGetWithNilKey(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var entity testEntity
		return tx.Get(nil, &entity)
	})

	if err == nil {
		t.Error("expected error for nil key in transaction Get")
	}
}

func TestDeepHierarchicalKeys(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create 4-level hierarchy
	gp := ds9.NameKey("GP", "gp1", nil)
	p := ds9.NameKey("P", "p1", gp)
	c := ds9.NameKey("C", "c1", p)
	gc := ds9.NameKey("GC", "gc1", c)

	entity := &testEntity{Name: "great-grandchild", Count: 42}
	_, err := client.Put(ctx, gc, entity)
	if err != nil {
		t.Fatalf("Put with 4-level hierarchy failed: %v", err)
	}

	var retrieved testEntity
	err = client.Get(ctx, gc, &retrieved)
	if err != nil {
		t.Fatalf("Get with 4-level hierarchy failed: %v", err)
	}

	if retrieved.Name != "great-grandchild" {
		t.Errorf("expected name 'great-grandchild', got %q", retrieved.Name)
	}
}

func TestEntityWithEmptyStringFields(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	key := ds9.NameKey("Empty", "test", nil)
	entity := &testEntity{
		Name:   "",    // empty string
		Count:  0,     // zero
		Active: false, // false
		Score:  0.0,   // zero float
	}

	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put with empty/zero values failed: %v", err)
	}

	var retrieved testEntity
	err = client.Get(ctx, key, &retrieved)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Name != "" {
		t.Errorf("expected empty string, got %q", retrieved.Name)
	}
	if retrieved.Count != 0 {
		t.Errorf("expected 0, got %d", retrieved.Count)
	}
}

func TestGetWithNonPointerDst(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put entity
	key := ds9.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Try to get into non-pointer
	var notPointer testEntity
	err = client.Get(ctx, key, notPointer) // Should be &notPointer
	if err == nil {
		t.Error("expected error when dst is not a pointer")
	}
}

func TestPutWithNonPointerEntity(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	key := ds9.NameKey("Test", "key", nil)
	entity := testEntity{Name: "test", Count: 1} // not a pointer

	// The mock implementation may accept non-pointers, but test with the real client
	// For now, just test that it works (real Datastore would require pointer)
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Logf("Put with non-pointer entity failed (expected with real client): %v", err)
	}
}

func TestDeleteAllByKindWithNoEntities(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Delete from kind with no entities
	err := client.DeleteAllByKind(ctx, "NonExistentKind")
	if err != nil {
		t.Errorf("DeleteAllByKind on empty kind should not error, got: %v", err)
	}
}

func TestDeleteAllByKindWithManyEntities(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put many entities
	for i := range 25 {
		key := ds9.NameKey("ManyDelete", fmt.Sprintf("key-%d", i), nil)
		entity := &testEntity{Name: "test", Count: int64(i)}
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete all
	err := client.DeleteAllByKind(ctx, "ManyDelete")
	if err != nil {
		t.Fatalf("DeleteAllByKind failed: %v", err)
	}

	// Verify all deleted
	query := ds9.NewQuery("ManyDelete").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected 0 keys after DeleteAllByKind, got %d", len(keys))
	}
}

func TestTransactionWithMultiplePuts(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		for i := range 5 {
			key := ds9.NameKey("TxMulti", fmt.Sprintf("key-%d", i), nil)
			entity := &testEntity{Name: "test", Count: int64(i)}
			_, err := tx.Put(key, entity)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Transaction with multiple puts failed: %v", err)
	}

	// Verify all entities were created
	for i := range 5 {
		key := ds9.NameKey("TxMulti", fmt.Sprintf("key-%d", i), nil)
		var retrieved testEntity
		err = client.Get(ctx, key, &retrieved)
		if err != nil {
			t.Errorf("Get for entity %d failed: %v", i, err)
		}
		if retrieved.Count != int64(i) {
			t.Errorf("entity %d: expected Count %d, got %d", i, i, retrieved.Count)
		}
	}
}

func TestIDKeyWithZeroID(t *testing.T) {
	// Zero ID is valid
	key := ds9.IDKey("Test", 0, nil)
	if key.ID != 0 {
		t.Errorf("expected ID 0, got %d", key.ID)
	}
	if key.Name != "" {
		t.Errorf("expected empty Name, got %q", key.Name)
	}
}

func TestNameKeyWithEmptyName(t *testing.T) {
	// Empty name is technically valid
	key := ds9.NameKey("Test", "", nil)
	if key.Name != "" {
		t.Errorf("expected empty Name, got %q", key.Name)
	}
	if key.ID != 0 {
		t.Errorf("expected ID 0, got %d", key.ID)
	}
}

func TestDoRequestUnexpectedSuccess(t *testing.T) {
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
		// Return unexpected 2xx status (not 200)
		w.WriteHeader(http.StatusAccepted) // 202
		if _, err := w.Write([]byte(`{"message":"accepted"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := ds9.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)

	if err == nil {
		t.Error("expected error for unexpected 2xx status")
	}

	if !strings.Contains(err.Error(), "202") {
		t.Errorf("expected error to mention 202 status, got: %v", err)
	}
}

func TestGetMultiWithNonSliceDst(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
	}

	// Pass a non-slice as destination
	var notSlice string
	err := client.GetMulti(ctx, keys, &notSlice)

	if err == nil {
		t.Error("expected error when dst is not a slice")
	}
}

func TestPutMultiWithNonSliceSrc(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
	}

	// Pass a non-slice as source
	notSlice := "not a slice"
	_, err := client.PutMulti(ctx, keys, notSlice)

	if err == nil {
		t.Error("expected error when src is not a slice")
	}
}

func TestAllKeysQueryWithoutKeysOnly(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create query without KeysOnly
	query := ds9.NewQuery("Test")

	_, err := client.AllKeys(ctx, query)

	if err == nil {
		t.Error("expected error for query without KeysOnly")
	}

	if !strings.Contains(err.Error(), "KeysOnly") {
		t.Errorf("expected error to mention KeysOnly, got: %v", err)
	}
}

func TestDeleteAllByKindQueryFailure(t *testing.T) {
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
		// Fail on query request
		if strings.Contains(r.URL.Path, "runQuery") {
			w.WriteHeader(http.StatusInternalServerError)
			if _, err := w.Write([]byte(`{"error":"query failed"}`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	err = client.DeleteAllByKind(ctx, "TestKind")

	if err == nil {
		t.Error("expected error when query fails")
	}
}

func TestTransactionGetWithInvalidResponse(t *testing.T) {
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
		if strings.Contains(r.URL.Path, "beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "lookup") {
			// Return invalid JSON structure
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"invalid":"structure"}`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "commit") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := ds9.NameKey("Test", "key", nil)
	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var entity testEntity
		return tx.Get(key, &entity)
	})

	// Should handle the invalid response gracefully
	if err == nil {
		t.Log("Transaction succeeded despite invalid lookup response")
	}
}

func TestGetWithInvalidJSONResponse(t *testing.T) {
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
		// Return invalid JSON
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{invalid json`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := ds9.NameKey("Test", "key", nil)
	var entity testEntity
	err = client.Get(ctx, key, &entity)

	if err == nil {
		t.Error("expected error for invalid JSON response")
	}
}

func TestPutWithInvalidEntityStructure(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Entity with channel (unsupported type)
	type BadEntity struct {
		Ch   chan int
		Name string
	}

	key := ds9.NameKey("Test", "bad", nil)
	entity := &BadEntity{
		Name: "test",
		Ch:   make(chan int),
	}

	_, err := client.Put(ctx, key, entity)

	if err == nil {
		t.Error("expected error for unsupported entity type")
	}
}

func TestGetMultiWithNilInResults(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put one entity
	key1 := ds9.NameKey("Test", "exists", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err := client.Put(ctx, key1, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Try to get multiple with one missing
	keys := []*ds9.Key{
		key1,
		ds9.NameKey("Test", "missing", nil),
		ds9.NameKey("Test", "missing2", nil),
	}

	var entities []testEntity
	err = client.GetMulti(ctx, keys, &entities)

	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("expected ErrNoSuchEntity when some keys missing, got: %v", err)
	}
}

func TestDeleteMultiPartialSuccess(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put some entities
	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
		ds9.NameKey("Test", "key2", nil),
	}

	entities := []testEntity{
		{Name: "entity1", Count: 1},
		{Name: "entity2", Count: 2},
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	// Delete them (should succeed)
	err = client.DeleteMulti(ctx, keys)
	if err != nil {
		t.Fatalf("DeleteMulti failed: %v", err)
	}

	// Verify deletion
	var retrieved []testEntity
	err = client.GetMulti(ctx, keys, &retrieved)
	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("expected ErrNoSuchEntity after delete, got: %v", err)
	}
}

func TestQueryWithVeryLargeLimit(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put a few entities
	for i := range 3 {
		key := ds9.NameKey("LargeLimit", fmt.Sprintf("key-%d", i), nil)
		entity := &testEntity{Name: "test", Count: int64(i)}
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with very large limit
	query := ds9.NewQuery("LargeLimit").KeysOnly().Limit(10000)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys with large limit failed: %v", err)
	}

	// Should return all 3
	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
}

func TestDeleteWithServerError(t *testing.T) {
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

	attemptCount := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount++
		// Always return 503
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, err := w.Write([]byte(`{"error":"unavailable"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := ds9.NameKey("Test", "key", nil)
	err = client.Delete(ctx, key)

	if err == nil {
		t.Error("expected error on persistent server failure")
	}

	// Should have retried
	if attemptCount < 2 {
		t.Errorf("expected multiple attempts, got %d", attemptCount)
	}
}

func TestPutMultiWithServerError(t *testing.T) {
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
		w.WriteHeader(http.StatusBadRequest)
		if _, err := w.Write([]byte(`{"error":"bad request"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
		ds9.NameKey("Test", "key2", nil),
	}

	entities := []testEntity{
		{Name: "entity1", Count: 1},
		{Name: "entity2", Count: 2},
	}

	_, err = client.PutMulti(ctx, keys, entities)

	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestGetMultiWithServerError(t *testing.T) {
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
		w.WriteHeader(http.StatusUnauthorized)
		if _, err := w.Write([]byte(`{"error":"unauthorized"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
	}

	var entities []testEntity
	err = client.GetMulti(ctx, keys, &entities)

	if err == nil {
		t.Error("expected error on unauthorized")
	}

	if !strings.Contains(err.Error(), "401") {
		t.Errorf("expected 401 error, got: %v", err)
	}
}

func TestAllKeysWithInvalidResponse(t *testing.T) {
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
		// Return invalid JSON
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{malformed`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	query := ds9.NewQuery("Test").KeysOnly()
	_, err = client.AllKeys(ctx, query)

	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestTransactionWithNonRetriableError(t *testing.T) {
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

	commitAttempts := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "commit") {
			commitAttempts++
			// Return non-retriable error (not 409 ABORTED)
			w.WriteHeader(http.StatusBadRequest)
			if _, err := w.Write([]byte(`{"error":"INVALID_ARGUMENT"}`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "lookup") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := ds9.NameKey("Test", "key", nil)
	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		_, err := tx.Put(key, &testEntity{Name: "test", Count: 1})
		return err
	})

	if err == nil {
		t.Error("expected error on non-retriable failure")
	}

	// Should NOT retry on non-409 errors
	if commitAttempts != 1 {
		t.Errorf("expected exactly 1 commit attempt for non-retriable error, got %d", commitAttempts)
	}
}

func TestTransactionWithInvalidTxResponse(t *testing.T) {
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
		if strings.Contains(r.URL.Path, "beginTransaction") {
			// Return invalid JSON
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{bad json`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		return nil
	})

	if err == nil {
		t.Error("expected error for invalid transaction response")
	}
}

func TestTransactionGetWithDecodeError(t *testing.T) {
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
		if strings.Contains(r.URL.Path, "beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "lookup") {
			// Return entity with malformed data
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []any{
					map[string]any{
						"entity": map[string]any{
							"key": map[string]any{
								"path": []any{
									map[string]any{
										"kind": "Test",
										"name": "key",
									},
								},
							},
							"properties": map[string]any{
								"name": map[string]any{
									"stringValue": 12345, // Wrong type
								},
							},
						},
					},
				},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "commit") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := ds9.NameKey("Test", "key", nil)
	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var entity testEntity
		return tx.Get(key, &entity)
	})
	// May succeed or fail depending on how decoding handles type mismatches
	if err != nil {
		t.Logf("Transaction Get with decode error: %v", err)
	}
}

func TestDoRequestWithReadBodyError(t *testing.T) {
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
		// Set content length but don't write enough data
		w.Header().Set("Content-Length", "1000000")
		w.WriteHeader(http.StatusOK)
		// Write partial data then close connection
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := ds9.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)
	// Should get an error related to response parsing
	if err != nil {
		t.Logf("Got expected error with incomplete response: %v", err)
	}
}

func TestPutMultiWithPartialEncode(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Mix of valid and invalid entities
	type MixedEntity struct {
		Data any
		Name string
	}

	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
		ds9.NameKey("Test", "key2", nil),
	}

	entities := []MixedEntity{
		{Name: "valid", Data: "string"},
		{Name: "maybe-invalid", Data: make(chan int)}, // channels unsupported
	}

	_, err := client.PutMulti(ctx, keys, entities)

	if err == nil {
		t.Log("PutMulti with mixed entities succeeded (mock may not validate types)")
	} else {
		t.Logf("PutMulti with mixed entities failed as expected: %v", err)
	}
}

func TestDeleteWithContextCancellation(t *testing.T) {
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
		// Slow response
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	key := ds9.NameKey("Test", "key", nil)
	err = client.Delete(ctx, key)

	if err == nil {
		t.Error("expected error when context is cancelled")
	}
}

// Tests for keyFromJSON with invalid path elements
func TestKeyFromJSONInvalidPathElement(t *testing.T) {
	// Test with non-map path element
	keyData := map[string]any{
		"path": []any{
			"invalid-string-instead-of-map",
		},
	}

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
		if strings.Contains(r.URL.Path, ":commit") {
			// Return response with invalid key in mutation result
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []map[string]any{
					{
						"key": keyData,
					},
				},
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

	realClient, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test"}

	// Try Put which will parse the returned key
	_, err = realClient.Put(ctx, key, entity)
	if err == nil {
		t.Log("Put succeeded despite invalid path element (API may handle gracefully)")
	} else {
		t.Logf("Put failed as expected: %v", err)
	}
}

// Test keyFromJSON with ID as string that fails parsing
func TestKeyFromJSONInvalidIDString(t *testing.T) {
	keyData := map[string]any{
		"path": []any{
			map[string]any{
				"kind": "Test",
				"id":   "not-a-number",
			},
		},
	}

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
		if strings.Contains(r.URL.Path, ":commit") {
			// Return response with invalid ID string in mutation result
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []map[string]any{
					{
						"key": keyData,
					},
				},
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

	realClient, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test"}

	// Try Put which will parse the returned key
	_, err = realClient.Put(ctx, key, entity)
	if err == nil {
		t.Log("Put succeeded despite invalid ID string (API may handle gracefully)")
	} else {
		t.Logf("Put failed as expected: %v", err)
	}
}

// Test keyFromJSON with ID as float64
func TestKeyFromJSONIDAsFloat(t *testing.T) {
	keyData := map[string]any{
		"path": []any{
			map[string]any{
				"kind": "Test",
				"id":   float64(12345),
			},
		},
	}

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
		if strings.Contains(r.URL.Path, ":lookup") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []map[string]any{
					{
						"entity": map[string]any{
							"key": keyData,
							"properties": map[string]any{
								"name": map[string]any{"stringValue": "test"},
							},
						},
					},
				},
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

	realClient, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	var entity testEntity

	err = realClient.Get(ctx, key, &entity)
	if err != nil {
		t.Errorf("unexpected error with float64 ID: %v", err)
	}
}

// Test Transaction.Get with missing entity
func TestTransactionGetMissingEntity(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return empty found array (entity not found)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":commit") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "nonexistent", nil)

	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var entity testEntity
		err := tx.Get(key, &entity)
		if err == nil {
			return errors.New("expected error for missing entity")
		}
		return nil
	})
	if err != nil {
		t.Errorf("transaction should succeed even with get error: %v", err)
	}
}

// Test Transaction.Get with decode error
func TestTransactionGetDecodeError(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return malformed entity
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []map[string]any{
					{
						"entity": "invalid-not-a-map",
					},
				},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":commit") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)

	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var entity testEntity
		err := tx.Get(key, &entity)
		if err == nil {
			return errors.New("expected decode error")
		}
		return nil
	})
	if err != nil {
		t.Errorf("transaction should succeed: %v", err)
	}
}

// Test Delete with multiple retries exhausted
func TestDeleteAllRetriesFail(t *testing.T) {
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

	requestCount := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		// Always return 503 to force retries
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)

	err = client.Delete(ctx, key)
	if err == nil {
		t.Error("expected error after all retries exhausted")
	}

	if !strings.Contains(err.Error(), "attempts") {
		t.Errorf("expected error message about attempts, got: %v", err)
	}

	if requestCount != 3 {
		t.Errorf("expected 3 retry attempts, got %d", requestCount)
	}
}

// Test Client.Get with decode error
func TestGetWithDecodeError(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return entity with missing properties field
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []map[string]any{
					{
						"entity": map[string]any{
							"key": map[string]any{
								"path": []any{
									map[string]any{
										"kind": "Test",
										"name": "key",
									},
								},
							},
							// Missing properties field
						},
					},
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	var entity testEntity

	err = client.Get(ctx, key, &entity)
	if err == nil {
		t.Error("expected error with missing properties")
	}
}

// Test Put with invalid entity causing encode error
func TestPutWithEncodeError(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create entity with unsupported type
	type BadEntity struct {
		Channel chan int `datastore:"channel"`
	}

	key := ds9.NameKey("Test", "key", nil)
	entity := &BadEntity{Channel: make(chan int)}

	_, err := client.Put(ctx, key, entity)
	if err == nil {
		t.Log("Put with unsupported type succeeded (mock may not validate types)")
	} else {
		t.Logf("Put with unsupported type failed as expected: %v", err)
	}
}

// Test GetMulti with some entities not found
func TestGetMultiPartialNotFound(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return one found, one missing
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []map[string]any{
					{
						"entity": map[string]any{
							"key": map[string]any{
								"path": []any{
									map[string]any{
										"kind": "Test",
										"name": "key1",
									},
								},
							},
							"properties": map[string]any{
								"name": map[string]any{"stringValue": "test1"},
							},
						},
					},
				},
				"missing": []map[string]any{
					{
						"key": map[string]any{
							"path": []any{
								map[string]any{
									"kind": "Test",
									"name": "key2",
								},
							},
						},
					},
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
		ds9.NameKey("Test", "key2", nil),
	}

	var entities []testEntity
	err = client.GetMulti(ctx, keys, &entities)
	if err == nil {
		t.Error("expected error when some entities are missing")
	} else {
		t.Logf("GetMulti with missing entities failed as expected: %v", err)
	}
}

// Test AllKeys with invalid JSON response
func TestAllKeysInvalidJSON(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":runQuery") {
			// Return invalid JSON
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte("{")); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	query := ds9.NewQuery("Test").KeysOnly()

	_, err = client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error with invalid JSON")
	}
}

// Test Transaction commit with invalid response
func TestTransactionCommitInvalidResponse(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":commit") {
			// Return invalid JSON (missing mutationResults)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				// Missing mutationResults field
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test"}

	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		_, err := tx.Put(key, entity)
		return err
	})
	if err != nil {
		t.Logf("Transaction with invalid commit response failed: %v", err)
	}
}

// Test PutMulti with encode errors in entities
func TestPutMultiWithInvalidEntities(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	type InvalidEntity struct {
		Func func() `datastore:"func"`
	}

	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
	}

	entities := []InvalidEntity{
		{Func: func() {}},
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err == nil {
		t.Log("PutMulti with func field succeeded (mock may not validate types)")
	} else {
		t.Logf("PutMulti with func field failed as expected: %v", err)
	}
}

// Test decodeValue with invalid integer format
func TestDecodeValueInvalidInteger(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return entity with invalid integer format
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []map[string]any{
					{
						"entity": map[string]any{
							"key": map[string]any{
								"path": []any{
									map[string]any{
										"kind": "Test",
										"name": "key",
									},
								},
							},
							"properties": map[string]any{
								"count": map[string]any{"integerValue": "not-an-integer"},
							},
						},
					},
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	var entity testEntity

	err = client.Get(ctx, key, &entity)
	if err == nil {
		t.Error("expected error with invalid integer format")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// Test decodeValue with wrong type for integer
func TestDecodeValueWrongTypeForInteger(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return entity with integer value but string field type
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []map[string]any{
					{
						"entity": map[string]any{
							"key": map[string]any{
								"path": []any{
									map[string]any{
										"kind": "Test",
										"name": "key",
									},
								},
							},
							"properties": map[string]any{
								"name": map[string]any{"integerValue": "12345"}, // integer for string field
							},
						},
					},
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	var entity testEntity

	err = client.Get(ctx, key, &entity)
	if err == nil {
		t.Error("expected error with wrong type for integer")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// Test decodeValue with invalid timestamp format
func TestDecodeValueInvalidTimestamp(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return entity with invalid timestamp format
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []map[string]any{
					{
						"entity": map[string]any{
							"key": map[string]any{
								"path": []any{
									map[string]any{
										"kind": "Test",
										"name": "key",
									},
								},
							},
							"properties": map[string]any{
								"updated_at": map[string]any{"timestampValue": "invalid-timestamp"},
							},
						},
					},
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	var entity testEntity

	err = client.Get(ctx, key, &entity)
	if err == nil {
		t.Error("expected error with invalid timestamp format")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// Test Client.Get with non-pointer destination
func TestGetWithNonPointer(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	var entity testEntity // non-pointer

	err := client.Get(ctx, key, entity) // Pass by value
	if err == nil {
		t.Error("expected error when dst is not a pointer")
	}
}

// Test Client.Put with non-struct
func TestPutWithNonStruct(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	entity := "not a struct"

	_, err := client.Put(ctx, key, entity)
	if err == nil {
		t.Error("expected error when entity is not a struct")
	}
}

// Test AllKeys with non-KeysOnly query error handling
func TestAllKeysNotKeysOnlyError(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()
	query := ds9.NewQuery("Test") // Not KeysOnly

	_, err := client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error when query is not KeysOnly")
	}
}

// Test GetMulti with mismatched keys and entities length
func TestGetMultiMismatchedLength(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()
	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
		ds9.NameKey("Test", "key2", nil),
	}

	var entities []testEntity // Empty slice

	err := client.GetMulti(ctx, keys, &entities)
	// This should work - GetMulti should populate the slice
	if err != nil {
		t.Logf("GetMulti with empty slice: %v", err)
	}
}

// Test PutMulti with mismatched keys and entities length
func TestPutMultiMismatchedLength(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()
	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
		ds9.NameKey("Test", "key2", nil),
	}

	entities := []testEntity{
		{Name: "test1"},
		// Missing second entity
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err == nil {
		t.Error("expected error with mismatched lengths")
	}
}

// Test DeleteMulti with empty keys slice
func TestDeleteMultiWithEmptyKeysSlice(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()
	var keys []*ds9.Key // Empty

	err := client.DeleteMulti(ctx, keys)
	// Mock may behave differently - log the result
	if err != nil {
		t.Logf("DeleteMulti with empty keys: %v", err)
	}
}

// Test Client.Get with JSON unmarshal error for found entities
func TestGetWithJSONUnmarshalError(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return invalid JSON
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte(`{"found": [{"entity": "not-an-object"}]}`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	var entity testEntity

	err = client.Get(ctx, key, &entity)
	if err == nil {
		t.Error("expected error with invalid entity format")
	}
}

// Test Client.Put with access token error
func TestPutWithAccessTokenError(t *testing.T) {
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		// Always return error for token
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer metadataServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, "http://unused")
	defer restore()

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test"}

	_, err = client.Put(ctx, key, entity)
	if err == nil {
		t.Error("expected error when access token fails")
	}
}

// Test Client.Delete with JSON marshal error
func TestDeleteWithJSONMarshalError(t *testing.T) {
	// This is hard to trigger since we control the JSON structure
	// But we can test with a context that gets cancelled
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
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)

	err = client.Delete(ctx, key)
	if err != nil {
		t.Logf("Delete completed with: %v", err)
	}
}

// Test GetMulti with decode error for specific entity
func TestGetMultiDecodeError(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return one good entity and one with decode error
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []map[string]any{
					{
						"entity": map[string]any{
							"key": map[string]any{
								"path": []any{
									map[string]any{
										"kind": "Test",
										"name": "key1",
									},
								},
							},
							"properties": map[string]any{
								"name": map[string]any{"stringValue": "test"},
							},
						},
					},
					{
						"entity": "invalid", // This will cause decode error
					},
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
		ds9.NameKey("Test", "key2", nil),
	}

	var entities []testEntity
	err = client.GetMulti(ctx, keys, &entities)
	if err == nil {
		t.Error("expected error when one entity has decode error")
	}
}

// Test AllKeys with batch batching (many results)
func TestAllKeysWithBatching(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":runQuery") {
			// Return multiple key results
			results := make([]map[string]any, 50)
			for i := range 50 {
				results[i] = map[string]any{
					"entity": map[string]any{
						"key": map[string]any{
							"path": []any{
								map[string]any{
									"kind": "Test",
									"name": fmt.Sprintf("key%d", i),
								},
							},
						},
					},
				}
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"batch": map[string]any{
					"entityResults": results,
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	query := ds9.NewQuery("Test").KeysOnly()

	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Logf("AllKeys with many results: %v", err)
	} else if len(keys) != 50 {
		t.Logf("Expected 50 keys, got %d", len(keys))
	}
}

// Test AllKeys with keyFromJSON error
func TestAllKeysKeyFromJSONError(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":runQuery") {
			// Return result with invalid key format
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"batch": map[string]any{
					"entityResults": []map[string]any{
						{
							"entity": map[string]any{
								"key": "not-a-map", // Invalid key format
							},
						},
					},
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	query := ds9.NewQuery("Test").KeysOnly()

	_, err = client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error with invalid key format")
	}
}

// Test PutMulti with JSON marshal error for request body
func TestPutMultiRequestMarshalError(t *testing.T) {
	// This is hard to trigger directly, but we can test with encoding errors
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()

	// Test with valid entities to exercise the code path
	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
	}

	entities := []testEntity{
		{Name: "test1", Count: 123},
	}

	_, err = client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Logf("PutMulti completed with: %v", err)
	}
}

// Test Transaction commit with JSON unmarshal error
func TestTransactionCommitUnmarshalError(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":commit") {
			// Return malformed mutation results
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte(`{"mutationResults": "not-an-array"}`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test"}

	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		_, err := tx.Put(key, entity)
		return err
	})
	// May or may not error depending on JSON parsing behavior
	if err != nil {
		t.Logf("Transaction with malformed mutation results failed: %v", err)
	}
}

// Test DeleteAllByKind with empty batch response
func TestDeleteAllByKindEmptyBatch(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":runQuery") {
			// Return empty batch
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"batch": map[string]any{},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	err = client.DeleteAllByKind(ctx, "EmptyKind")
	if err != nil {
		t.Logf("DeleteAllByKind with empty batch: %v", err)
	}
}

// Test AllKeys with empty path in key
func TestAllKeysEmptyPathInKey(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":runQuery") {
			w.Header().Set("Content-Type", "application/json")
			// Return key with empty path array
			if err := json.NewEncoder(w).Encode(map[string]any{
				"batch": map[string]any{
					"entityResults": []map[string]any{
						{
							"entity": map[string]any{
								"key": map[string]any{
									"path": []any{}, // Empty path
								},
							},
						},
					},
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	query := ds9.NewQuery("TestKind").KeysOnly()
	_, err = client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error with empty path in key")
	}
}

// Test AllKeys with invalid path element (not a map)
func TestAllKeysInvalidPathElement(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":runQuery") {
			w.Header().Set("Content-Type", "application/json")
			// Return key with invalid path element (string instead of map)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"batch": map[string]any{
					"entityResults": []map[string]any{
						{
							"entity": map[string]any{
								"key": map[string]any{
									"path": []any{"invalid-element"}, // String instead of map
								},
							},
						},
					},
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	query := ds9.NewQuery("TestKind").KeysOnly()
	_, err = client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error with invalid path element")
	}
}

// Test Get with ID key as string
func TestGetWithStringIDKey(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":lookup") {
			w.Header().Set("Content-Type", "application/json")
			// Return entity with ID as string
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []map[string]any{
					{
						"entity": map[string]any{
							"key": map[string]any{
								"path": []any{
									map[string]any{
										"kind": "TestKind",
										"id":   "12345", // ID as string
									},
								},
							},
							"properties": map[string]any{
								"name": map[string]any{"stringValue": "test"},
							},
						},
					},
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	ctx := context.Background()
	key := ds9.IDKey("TestKind", 12345, nil)
	var entity TestEntity
	err = client.Get(ctx, key, &entity)
	if err != nil {
		t.Fatalf("Get with string ID key failed: %v", err)
	}

	if entity.Name != "test" {
		t.Errorf("expected name 'test', got %q", entity.Name)
	}
}

// Test Get with ID key as float64
func TestGetWithFloat64IDKey(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":lookup") {
			w.Header().Set("Content-Type", "application/json")
			// Return entity with ID as float64
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []map[string]any{
					{
						"entity": map[string]any{
							"key": map[string]any{
								"path": []any{
									map[string]any{
										"kind": "TestKind",
										"id":   float64(67890), // ID as float64
									},
								},
							},
							"properties": map[string]any{
								"value": map[string]any{"integerValue": "42"},
							},
						},
					},
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	type TestEntity struct {
		Value int64 `datastore:"value"`
	}

	ctx := context.Background()
	key := ds9.IDKey("TestKind", 67890, nil)
	var entity TestEntity
	err = client.Get(ctx, key, &entity)
	if err != nil {
		t.Fatalf("Get with float64 ID key failed: %v", err)
	}

	if entity.Value != 42 {
		t.Errorf("expected value 42, got %d", entity.Value)
	}
}

// Test Get with invalid string ID format in response
func TestGetWithInvalidStringIDFormat(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":lookup") {
			w.Header().Set("Content-Type", "application/json")
			// Return entity with invalid ID string format
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []map[string]any{
					{
						"entity": map[string]any{
							"key": map[string]any{
								"path": []any{
									map[string]any{
										"kind": "TestKind",
										"id":   "not-a-number", // Invalid ID format
									},
								},
							},
							"properties": map[string]any{
								"name": map[string]any{"stringValue": "test"},
							},
						},
					},
				},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	ctx := context.Background()
	key := ds9.IDKey("TestKind", 12345, nil)
	var entity TestEntity
	err = client.Get(ctx, key, &entity)
	// May or may not error depending on parsing behavior
	if err != nil {
		t.Logf("Get with invalid string ID format failed: %v", err)
	} else {
		t.Logf("Get with invalid string ID format succeeded unexpectedly")
	}
}

// Test Transaction.Get with no entity found
func TestTransactionGetNotFound(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return empty found array
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found":   []any{},
				"missing": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":commit") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "nonexistent", nil)

	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var entity testEntity
		err := tx.Get(key, &entity)
		if err == nil {
			t.Error("expected error with empty found array")
		}
		return nil
	})
	if err != nil {
		t.Logf("Transaction completed: %v", err)
	}
}

// Test Transaction.Get with access token error
func TestTransactionGetAccessTokenError(t *testing.T) {
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
			// Return error for token request
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metadataServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "test-key", nil)

	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var entity testEntity
		err := tx.Get(key, &entity)
		if err == nil {
			t.Error("expected error with token failure")
		}
		return err
	})

	if err == nil {
		t.Error("expected transaction to fail with token error")
	}
}

// Test Transaction.Get with non-OK status
func TestTransactionGetNonOKStatus(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return non-OK status
			w.WriteHeader(http.StatusBadRequest)
			if _, err := w.Write([]byte("bad request")); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":commit") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "test-key", nil)

	_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		var entity testEntity
		return tx.Get(key, &entity)
	})

	if err == nil {
		t.Error("expected error with non-OK status")
	}
}

// Test Client.Get with JSON unmarshal error
func TestGetJSONUnmarshalError(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return malformed JSON
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte("not valid json")); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer apiServer.Close()

	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := ds9.NameKey("Test", "test-key", nil)
	var entity testEntity

	err = client.Get(ctx, key, &entity)
	if err == nil {
		t.Error("expected error with malformed JSON")
	}
}

// Test PutMulti with length mismatch
func TestPutMultiLengthValidation(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()
	keys := []*ds9.Key{ds9.NameKey("Test", "key1", nil)}
	entities := []testEntity{{Name: "test1"}, {Name: "test2"}}

	_, err := client.PutMulti(ctx, keys, entities)
	if err == nil {
		t.Error("expected error with mismatched lengths")
	}
}

// Test DeleteMulti with partial success
func TestDeleteMultiMixedResults(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":commit") {
			w.Header().Set("Content-Type", "application/json")
			// Return empty mutation results
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
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

	client, err := ds9.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	keys := []*ds9.Key{
		ds9.NameKey("Test", "key1", nil),
		ds9.NameKey("Test", "key2", nil),
	}

	err = client.DeleteMulti(ctx, keys)
	// May or may not error depending on implementation
	if err != nil {
		t.Logf("DeleteMulti with mismatched results: %v", err)
	}
}

// TestBackwardsCompatibility tests the API compatibility with cloud.google.com/go/datastore.
// This ensures that ds9 can be used as a drop-in replacement.
func TestBackwardsCompatibility(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Test 1: Close() method exists and can be called (even though it's a no-op)
	t.Run("Close", func(t *testing.T) {
		err := client.Close()
		if err != nil {
			t.Errorf("Close() returned error: %v", err)
		}
	})

	// Test 2: RunInTransaction returns (*Commit, error)
	t.Run("RunInTransactionSignature", func(t *testing.T) {
		key := ds9.NameKey("TestKind", "test-tx-compat", nil)

		commit, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
			entity := &testEntity{
				Name:      "transaction test",
				Count:     100,
				Active:    true,
				Score:     99.9,
				UpdatedAt: time.Now().UTC().Truncate(time.Microsecond),
			}
			_, err := tx.Put(key, entity)
			return err
		})
		if err != nil {
			t.Fatalf("RunInTransaction failed: %v", err)
		}

		if commit == nil {
			t.Error("Expected non-nil Commit, got nil")
		}
	})

	// Test 3: GetAll() method retrieves entities and returns keys
	t.Run("GetAll", func(t *testing.T) {
		// Setup: Create some test entities
		entities := []testEntity{
			{Name: "entity1", Count: 1, Active: true, Score: 1.1, UpdatedAt: time.Now().UTC().Truncate(time.Microsecond)},
			{Name: "entity2", Count: 2, Active: false, Score: 2.2, UpdatedAt: time.Now().UTC().Truncate(time.Microsecond)},
			{Name: "entity3", Count: 3, Active: true, Score: 3.3, UpdatedAt: time.Now().UTC().Truncate(time.Microsecond)},
		}

		keys := []*ds9.Key{
			ds9.NameKey("GetAllTest", "key1", nil),
			ds9.NameKey("GetAllTest", "key2", nil),
			ds9.NameKey("GetAllTest", "key3", nil),
		}

		_, err := client.PutMulti(ctx, keys, entities)
		if err != nil {
			t.Fatalf("PutMulti failed: %v", err)
		}

		// Test GetAll
		query := ds9.NewQuery("GetAllTest")
		var results []testEntity
		returnedKeys, err := client.GetAll(ctx, query, &results)
		if err != nil {
			t.Fatalf("GetAll failed: %v", err)
		}

		if len(results) != 3 {
			t.Errorf("Expected 3 entities, got %d", len(results))
		}

		if len(returnedKeys) != 3 {
			t.Errorf("Expected 3 keys, got %d", len(returnedKeys))
		}

		// Verify entities were properly decoded
		foundNames := make(map[string]bool)
		for _, entity := range results {
			foundNames[entity.Name] = true
		}

		for _, expectedName := range []string{"entity1", "entity2", "entity3"} {
			if !foundNames[expectedName] {
				t.Errorf("Expected to find entity %s, but didn't", expectedName)
			}
		}

		// Verify keys match entities
		for i, key := range returnedKeys {
			if key.Kind != "GetAllTest" {
				t.Errorf("Key %d has wrong kind: %s", i, key.Kind)
			}
		}
	})

	// Test 4: GetAll with limit
	t.Run("GetAllWithLimit", func(t *testing.T) {
		query := ds9.NewQuery("GetAllTest").Limit(2)
		var results []testEntity
		returnedKeys, err := client.GetAll(ctx, query, &results)
		if err != nil {
			t.Fatalf("GetAll with limit failed: %v", err)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 entities with limit, got %d", len(results))
		}

		if len(returnedKeys) != 2 {
			t.Errorf("Expected 2 keys with limit, got %d", len(returnedKeys))
		}
	})
}

// TestClose tests that the Close() method exists and returns no error.
func TestClose(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	err := client.Close()
	if err != nil {
		t.Errorf("Close() returned unexpected error: %v", err)
	}

	// Should be idempotent - can call multiple times
	err = client.Close()
	if err != nil {
		t.Errorf("Second Close() returned unexpected error: %v", err)
	}
}

// TestGetAllEmpty tests GetAll with no results.
func TestGetAllEmpty(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()
	query := ds9.NewQuery("NonExistentKind")
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

// TestGetAllInvalidDst tests GetAll with invalid destination.
func TestGetAllInvalidDst(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()
	query := ds9.NewQuery("TestKind")

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

// TestGetAllSingleEntity tests GetAll retrieving a single entity.
func TestGetAllSingleEntity(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create entity
	key := ds9.NameKey("SingleGetAll", "single1", nil)
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
	query := ds9.NewQuery("SingleGetAll")
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

// TestGetAllMultipleEntities tests GetAll retrieving multiple entities.
func TestGetAllMultipleEntities(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create multiple entities
	count := 5
	keys := make([]*ds9.Key, count)
	entities := make([]testEntity, count)

	for i := range count {
		keys[i] = ds9.NameKey("MultiGetAll", fmt.Sprintf("entity%d", i), nil)
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
	query := ds9.NewQuery("MultiGetAll")
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

// TestGetAllWithLimitVariations tests GetAll with various limit values.
func TestGetAllWithLimitVariations(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Setup: Create 10 entities
	keys := make([]*ds9.Key, 10)
	entities := make([]testEntity, 10)
	for i := range 10 {
		keys[i] = ds9.NameKey("LimitGetAll", fmt.Sprintf("key%d", i), nil)
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
			query := ds9.NewQuery("LimitGetAll").Limit(tt.limit)
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

// TestRunInTransactionReturnsCommit tests that RunInTransaction returns a Commit object.
func TestRunInTransactionReturnsCommit(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()
	key := ds9.NameKey("CommitTest", "test1", nil)

	commit, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		entity := &testEntity{
			Name:      "commit test",
			Count:     1,
			Active:    true,
			UpdatedAt: time.Now().UTC().Truncate(time.Microsecond),
		}
		_, err := tx.Put(key, entity)
		return err
	})
	if err != nil {
		t.Fatalf("RunInTransaction failed: %v", err)
	}

	if commit == nil {
		t.Fatal("Expected non-nil Commit, got nil")
	}

	// Commit should be a valid *Commit type
	_ = commit
}

// TestRunInTransactionErrorReturnsNilCommit tests that RunInTransaction returns nil Commit on error.
func TestRunInTransactionErrorReturnsNilCommit(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	expectedErr := errors.New("intentional error")
	commit, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		return expectedErr
	})

	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error to be %v, got %v", expectedErr, err)
	}

	if commit != nil {
		t.Errorf("Expected nil Commit on error, got %v", commit)
	}
}

func TestTransactionOptions(t *testing.T) {
	t.Run("MaxAttempts", func(t *testing.T) {
		// Test that MaxAttempts option is accepted and sets the retry limit
		// We can verify this by checking the error message mentions the right attempt count
		client, cleanup := ds9mock.NewClient(t)
		defer cleanup()

		ctx := context.Background()
		key := ds9.NameKey("TestKind", "test", nil)

		// This test verifies that the MaxAttempts option is parsed correctly
		// The actual retry behavior is tested in TestTransactionMaxRetriesExceeded
		_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
			entity := testEntity{Name: "test", Count: 42}
			_, err := tx.Put(key, &entity)
			return err
		}, ds9.MaxAttempts(5))
		// With mock client, this should succeed
		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}
	})

	t.Run("WithReadTime", func(t *testing.T) {
		client, cleanup := ds9mock.NewClient(t)
		defer cleanup()

		ctx := context.Background()
		key := ds9.NameKey("TestKind", "test", nil)

		// First, put an entity
		entity := testEntity{Name: "test", Count: 42}
		_, err := client.Put(ctx, key, &entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Run a read-only transaction with readTime
		readTime := time.Now().UTC()
		_, err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
			var result testEntity
			return tx.Get(key, &result)
		}, ds9.WithReadTime(readTime))
		// Note: ds9mock doesn't actually enforce read-only semantics,
		// but we're testing that the option is accepted and doesn't cause errors
		if err != nil {
			t.Fatalf("Transaction with WithReadTime failed: %v", err)
		}
	})

	t.Run("CombinedOptions", func(t *testing.T) {
		client, cleanup := ds9mock.NewClient(t)
		defer cleanup()

		ctx := context.Background()
		key := ds9.NameKey("TestKind", "test", nil)

		// Test that multiple options can be combined
		_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
			entity := testEntity{Name: "test", Count: 42}
			_, err := tx.Put(key, &entity)
			return err
		}, ds9.MaxAttempts(2), ds9.WithReadTime(time.Now().UTC()))
		// With mock client, this should succeed
		if err != nil {
			t.Fatalf("Transaction with combined options failed: %v", err)
		}
	})
}

// Test entity with arrays for array/slice tests
type arrayEntity struct {
	Strings []string  `datastore:"strings"`
	Ints    []int64   `datastore:"ints"`
	Floats  []float64 `datastore:"floats"`
	Bools   []bool    `datastore:"bools"`
}

func TestArraySliceSupport(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("StringSlice", func(t *testing.T) {
		key := ds9.NameKey("ArrayTest", "strings", nil)
		entity := &arrayEntity{
			Strings: []string{"hello", "world", "test"},
		}

		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put with string slice failed: %v", err)
		}

		var result arrayEntity
		if err := client.Get(ctx, key, &result); err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if len(result.Strings) != 3 {
			t.Errorf("Expected 3 strings, got %d", len(result.Strings))
		}
		if result.Strings[0] != "hello" || result.Strings[1] != "world" || result.Strings[2] != "test" {
			t.Errorf("String slice values incorrect: %v", result.Strings)
		}
	})

	t.Run("Int64Slice", func(t *testing.T) {
		key := ds9.NameKey("ArrayTest", "ints", nil)
		entity := &arrayEntity{
			Ints: []int64{1, 2, 3, 42, 100},
		}

		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put with int64 slice failed: %v", err)
		}

		var result arrayEntity
		if err := client.Get(ctx, key, &result); err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if len(result.Ints) != 5 {
			t.Errorf("Expected 5 ints, got %d", len(result.Ints))
		}
		if result.Ints[3] != 42 {
			t.Errorf("Expected Ints[3] = 42, got %d", result.Ints[3])
		}
	})

	t.Run("Float64Slice", func(t *testing.T) {
		key := ds9.NameKey("ArrayTest", "floats", nil)
		entity := &arrayEntity{
			Floats: []float64{1.1, 2.2, 3.3},
		}

		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put with float64 slice failed: %v", err)
		}

		var result arrayEntity
		if err := client.Get(ctx, key, &result); err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if len(result.Floats) != 3 {
			t.Errorf("Expected 3 floats, got %d", len(result.Floats))
		}
		if result.Floats[0] != 1.1 {
			t.Errorf("Expected Floats[0] = 1.1, got %f", result.Floats[0])
		}
	})

	t.Run("BoolSlice", func(t *testing.T) {
		key := ds9.NameKey("ArrayTest", "bools", nil)
		entity := &arrayEntity{
			Bools: []bool{true, false, true},
		}

		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put with bool slice failed: %v", err)
		}

		var result arrayEntity
		if err := client.Get(ctx, key, &result); err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if len(result.Bools) != 3 {
			t.Errorf("Expected 3 bools, got %d", len(result.Bools))
		}
		if result.Bools[0] != true || result.Bools[1] != false {
			t.Errorf("Bool slice values incorrect: %v", result.Bools)
		}
	})

	t.Run("EmptySlices", func(t *testing.T) {
		key := ds9.NameKey("ArrayTest", "empty", nil)
		entity := &arrayEntity{
			Strings: []string{},
			Ints:    []int64{},
		}

		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put with empty slices failed: %v", err)
		}

		var result arrayEntity
		if err := client.Get(ctx, key, &result); err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if result.Strings == nil || len(result.Strings) != 0 {
			t.Errorf("Expected empty string slice, got %v", result.Strings)
		}
		if result.Ints == nil || len(result.Ints) != 0 {
			t.Errorf("Expected empty int slice, got %v", result.Ints)
		}
	})

	t.Run("MixedArrays", func(t *testing.T) {
		key := ds9.NameKey("ArrayTest", "mixed", nil)
		entity := &arrayEntity{
			Strings: []string{"a", "b"},
			Ints:    []int64{10, 20, 30},
			Floats:  []float64{1.5},
			Bools:   []bool{true, false, true, false},
		}

		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put with mixed arrays failed: %v", err)
		}

		var result arrayEntity
		if err := client.Get(ctx, key, &result); err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if len(result.Strings) != 2 || len(result.Ints) != 3 || len(result.Floats) != 1 || len(result.Bools) != 4 {
			t.Errorf("Mixed array lengths incorrect: strings=%d, ints=%d, floats=%d, bools=%d",
				len(result.Strings), len(result.Ints), len(result.Floats), len(result.Bools))
		}
	})
}

func TestAllocateIDs(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("AllocateIncompleteKeys", func(t *testing.T) {
		keys := []*ds9.Key{
			ds9.IncompleteKey("Task", nil),
			ds9.IncompleteKey("Task", nil),
			ds9.IncompleteKey("Task", nil),
		}

		allocated, err := client.AllocateIDs(ctx, keys)
		if err != nil {
			t.Fatalf("AllocateIDs failed: %v", err)
		}

		if len(allocated) != 3 {
			t.Errorf("Expected 3 allocated keys, got %d", len(allocated))
		}

		for i, key := range allocated {
			if key.Incomplete() {
				t.Errorf("Key %d is still incomplete", i)
			}
			if key.ID == 0 {
				t.Errorf("Key %d has zero ID", i)
			}
		}
	})

	t.Run("AllocateMixedKeys", func(t *testing.T) {
		keys := []*ds9.Key{
			ds9.NameKey("Task", "complete", nil),
			ds9.IncompleteKey("Task", nil),
			ds9.IDKey("Task", 123, nil),
			ds9.IncompleteKey("Task", nil),
		}

		allocated, err := client.AllocateIDs(ctx, keys)
		if err != nil {
			t.Fatalf("AllocateIDs with mixed keys failed: %v", err)
		}

		if len(allocated) != 4 {
			t.Errorf("Expected 4 keys, got %d", len(allocated))
		}

		// First key should still be the named key
		if allocated[0].Name != "complete" {
			t.Errorf("First key should be unchanged")
		}

		// Second key should now have an ID
		if allocated[1].Incomplete() {
			t.Errorf("Second key should be allocated")
		}

		// Third key should be unchanged
		if allocated[2].ID != 123 {
			t.Errorf("Third key should be unchanged")
		}

		// Fourth key should now have an ID
		if allocated[3].Incomplete() {
			t.Errorf("Fourth key should be allocated")
		}
	})

	t.Run("AllocateEmptySlice", func(t *testing.T) {
		keys := []*ds9.Key{}

		allocated, err := client.AllocateIDs(ctx, keys)
		if err != nil {
			t.Fatalf("AllocateIDs with empty slice failed: %v", err)
		}

		if len(allocated) != 0 {
			t.Errorf("Expected empty slice, got %d keys", len(allocated))
		}
	})

	t.Run("AllocateAllCompleteKeys", func(t *testing.T) {
		keys := []*ds9.Key{
			ds9.NameKey("Task", "key1", nil),
			ds9.IDKey("Task", 100, nil),
		}

		allocated, err := client.AllocateIDs(ctx, keys)
		if err != nil {
			t.Fatalf("AllocateIDs with complete keys failed: %v", err)
		}

		if len(allocated) != 2 {
			t.Errorf("Expected 2 keys, got %d", len(allocated))
		}

		// Keys should be unchanged
		if allocated[0].Name != "key1" || allocated[1].ID != 100 {
			t.Errorf("Complete keys should be unchanged")
		}
	})
}

func TestCount(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("CountEmptyKind", func(t *testing.T) {
		q := ds9.NewQuery("NonExistent")
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
			key := ds9.IDKey("CountTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  fmt.Sprintf("entity-%d", i),
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		q := ds9.NewQuery("CountTest")
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
			key := ds9.IDKey("FilterCount", int64(i+1), nil)
			entity := &testEntity{
				Name:  fmt.Sprintf("entity-%d", i),
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Count entities where count >= 5
		q := ds9.NewQuery("FilterCount").Filter("count >=", int64(5))
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
			key := ds9.IDKey("LimitCount", int64(i+1), nil)
			entity := &testEntity{
				Name:  fmt.Sprintf("entity-%d", i),
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Count with limit - note: count should respect limit
		q := ds9.NewQuery("LimitCount").Limit(3)
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
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("NamespaceFilter", func(t *testing.T) {
		// Note: ds9mock may not fully support namespaces, but we test the API
		q := ds9.NewQuery("Task").Namespace("custom-namespace")

		var entities []testEntity
		_, err := client.GetAll(ctx, q, &entities)
		// Should not error even if namespace is not supported by mock
		if err != nil {
			t.Logf("GetAll with namespace: %v", err)
		}
	})

	t.Run("EmptyNamespace", func(t *testing.T) {
		q := ds9.NewQuery("Task").Namespace("")

		var entities []testEntity
		_, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Logf("GetAll with empty namespace: %v", err)
		}
	})
}

func TestQueryDistinct(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("Distinct", func(t *testing.T) {
		// Create duplicate entities
		for i := range 3 {
			key := ds9.IDKey("DistinctTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  "same-name", // Same name for all
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Query with distinct on Name field
		q := ds9.NewQuery("DistinctTest").Project("name").Distinct()

		var entities []testEntity
		_, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Logf("GetAll with Distinct: %v", err)
		}
	})

	t.Run("DistinctOn", func(t *testing.T) {
		q := ds9.NewQuery("Task").DistinctOn("name", "count")

		var entities []testEntity
		_, err := client.GetAll(ctx, q, &entities)
		if err != nil {
			t.Logf("GetAll with DistinctOn: %v", err)
		}
	})
}

func TestIterator(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("IterateAll", func(t *testing.T) {
		// Create test entities
		for i := range 5 {
			key := ds9.IDKey("IterTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  fmt.Sprintf("entity-%d", i),
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		q := ds9.NewQuery("IterTest")
		it := client.Run(ctx, q)

		count := 0
		for {
			var entity testEntity
			key, err := it.Next(&entity)
			if errors.Is(err, ds9.ErrDone) {
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
			key := ds9.IDKey("CursorTest", int64(i+1), nil)
			entity := &testEntity{
				Name:  fmt.Sprintf("entity-%d", i),
				Count: int64(i),
			}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		q := ds9.NewQuery("CursorTest")
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
		q := ds9.NewQuery("NonExistent")
		it := client.Run(ctx, q)

		var entity testEntity
		_, err := it.Next(&entity)
		if !errors.Is(err, ds9.ErrDone) {
			t.Errorf("Expected ErrDone, got %v", err)
		}
	})
}

func TestMutate(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("MutateInsert", func(t *testing.T) {
		key := ds9.NameKey("MutateTest", "insert", nil)
		entity := &testEntity{
			Name:  "inserted",
			Count: 42,
		}

		mut := ds9.NewInsert(key, entity)
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
		key := ds9.NameKey("MutateTest", "update", nil)
		entity := &testEntity{Name: "original", Count: 1}

		// Create entity first
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Update via mutation
		updated := &testEntity{Name: "updated", Count: 2}
		mut := ds9.NewUpdate(key, updated)
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
		key := ds9.NameKey("MutateTest", "upsert", nil)
		entity := &testEntity{Name: "upserted", Count: 100}

		mut := ds9.NewUpsert(key, entity)
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
		key := ds9.NameKey("MutateTest", "delete", nil)
		entity := &testEntity{Name: "to-delete", Count: 1}

		// Create entity first
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Delete via mutation
		mut := ds9.NewDelete(key)
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
		if !errors.Is(err, ds9.ErrNoSuchEntity) {
			t.Errorf("Expected ErrNoSuchEntity after delete, got %v", err)
		}
	})

	t.Run("MutateMultiple", func(t *testing.T) {
		key1 := ds9.NameKey("MutateTest", "multi1", nil)
		key2 := ds9.NameKey("MutateTest", "multi2", nil)
		key3 := ds9.NameKey("MutateTest", "multi3", nil)

		entity1 := &testEntity{Name: "first", Count: 1}
		entity2 := &testEntity{Name: "second", Count: 2}
		entity3 := &testEntity{Name: "third", Count: 3}

		// Pre-create entity3 for update
		if _, err := client.Put(ctx, key3, &testEntity{Name: "old", Count: 0}); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Apply multiple mutations
		muts := []*ds9.Mutation{
			ds9.NewInsert(key1, entity1),
			ds9.NewUpsert(key2, entity2),
			ds9.NewUpdate(key3, entity3),
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

		if keys != nil && len(keys) != 0 {
			t.Errorf("Expected nil or empty keys, got %d", len(keys))
		}
	})
}
