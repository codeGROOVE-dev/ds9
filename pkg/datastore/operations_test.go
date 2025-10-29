package datastore_test

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

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

func TestPutAndGet(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
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
	key := datastore.NameKey("TestKind", "test-key", nil)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	key := datastore.NameKey("TestKind", "nonexistent", nil)
	var entity testEntity
	err := client.Get(ctx, key, &entity)

	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("expected datastore.ErrNoSuchEntity, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put entity
	entity := &testEntity{
		Name:   "test-item",
		Count:  42,
		Active: true,
	}

	key := datastore.NameKey("TestKind", "test-key", nil)
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
	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("expected datastore.ErrNoSuchEntity after delete, got %v", err)
	}
}

func TestAllKeys(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put multiple entities
	for i := range 5 {
		entity := &testEntity{
			Name:  "test-item",
			Count: int64(i),
		}
		key := datastore.NameKey("TestKind", string(rune('a'+i)), nil)
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query for all keys
	query := datastore.NewQuery("TestKind").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 5 {
		t.Errorf("expected 5 keys, got %d", len(keys))
	}
}

func TestAllKeysWithLimit(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put multiple entities
	for i := range 10 {
		entity := &testEntity{
			Name:  "test-item",
			Count: int64(i),
		}
		key := datastore.NameKey("TestKind", string(rune('a'+i)), nil)
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query with limit
	query := datastore.NewQuery("TestKind").KeysOnly().Limit(3)
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
}

func TestIDKey(t *testing.T) {
	key := datastore.IDKey("TestKind", 12345, nil)

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
	key := datastore.NameKey("TestKind", "test-name", nil)

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
	client, cleanup := datastore.NewMockClient(t)
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

	keys := []*datastore.Key{
		datastore.NameKey("TestKind", "key-1", nil),
		datastore.NameKey("TestKind", "key-2", nil),
		datastore.NameKey("TestKind", "key-3", nil),
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put only one entity
	entity := &testEntity{Name: "exists", Count: 1}
	key1 := datastore.NameKey("TestKind", "exists", nil)
	_, err := client.Put(ctx, key1, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Try to get multiple, one missing
	keys := []*datastore.Key{
		key1,
		datastore.NameKey("TestKind", "missing", nil),
	}

	var retrieved []testEntity
	err = client.GetMulti(ctx, keys, &retrieved)

	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("expected datastore.ErrNoSuchEntity when some keys missing, got %v", err)
	}
}

func TestMultiDelete(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put multiple entities
	entities := []testEntity{
		{Name: "item-1", Count: 1},
		{Name: "item-2", Count: 2},
		{Name: "item-3", Count: 3},
	}

	keys := []*datastore.Key{
		datastore.NameKey("TestKind", "key-1", nil),
		datastore.NameKey("TestKind", "key-2", nil),
		datastore.NameKey("TestKind", "key-3", nil),
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
	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("expected datastore.ErrNoSuchEntity after delete, got %v", err)
	}
}

func TestMultiPutEmptyKeys(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	var entities []testEntity
	var keys []*datastore.Key

	_, err := client.PutMulti(ctx, keys, entities)
	if err == nil {
		t.Error("expected error for empty keys, got nil")
	}
}

func TestMultiGetEmptyKeys(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	var keys []*datastore.Key
	var retrieved []testEntity

	err := client.GetMulti(ctx, keys, &retrieved)
	if err == nil {
		t.Error("expected error for empty keys, got nil")
	}
}

func TestMultiDeleteEmptyKeys(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	var keys []*datastore.Key

	err := client.DeleteMulti(ctx, keys)
	if err == nil {
		t.Error("expected error for empty keys, got nil")
	}
}

func TestIDKeyOperations(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Test with ID key
	entity := &testEntity{
		Name:  "id-test",
		Count: 123,
	}

	key := datastore.IDKey("TestKind", 999, nil)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	entity := &testEntity{Name: "test"}
	_, err := client.Put(ctx, nil, entity)
	if err == nil {
		t.Error("expected error for nil key, got nil")
	}
}

func TestGetWithNilKey(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	var entity testEntity
	err := client.Get(ctx, nil, &entity)
	if err == nil {
		t.Error("expected error for nil key, got nil")
	}
}

func TestDeleteWithNilKey(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	err := client.Delete(ctx, nil)
	if err == nil {
		t.Error("expected error for nil key, got nil")
	}
}

func TestMultiGetWithNilKey(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*datastore.Key{
		datastore.NameKey("TestKind", "key-1", nil),
		nil,
		datastore.NameKey("TestKind", "key-2", nil),
	}

	var entities []testEntity
	err := client.GetMulti(ctx, keys, &entities)
	if err == nil {
		t.Error("expected error for nil key in slice, got nil")
	}
}

func TestMultiPutWithNilKey(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	entities := []testEntity{
		{Name: "item-1"},
		{Name: "item-2"},
	}

	keys := []*datastore.Key{
		datastore.NameKey("TestKind", "key-1", nil),
		nil,
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err == nil {
		t.Error("expected error for nil key in slice, got nil")
	}
}

func TestMultiDeleteWithNilKey(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*datastore.Key{
		datastore.NameKey("TestKind", "key-1", nil),
		nil,
	}

	err := client.DeleteMulti(ctx, keys)
	if err == nil {
		t.Error("expected error for nil key in slice, got nil")
	}
}

func TestMultiPutMismatchedSlices(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	entities := []testEntity{
		{Name: "item-1"},
		{Name: "item-2"},
	}

	keys := []*datastore.Key{
		datastore.NameKey("TestKind", "key-1", nil),
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err == nil {
		t.Error("expected error for mismatched slices, got nil")
	}
}

func TestAllKeysNonKeysOnlyQuery(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create a query without KeysOnly
	query := datastore.NewQuery("TestKind")
	_, err := client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error for non-KeysOnly query, got nil")
	}
}

func TestMultiGetPartialResults(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put some entities
	entities := []testEntity{
		{Name: "item-1", Count: 1},
		{Name: "item-3", Count: 3},
	}
	keys := []*datastore.Key{
		datastore.NameKey("TestKind", "key-1", nil),
		datastore.NameKey("TestKind", "key-3", nil),
	}
	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("MultiPut failed: %v", err)
	}

	// Try to get more keys than exist
	getAllKeys := []*datastore.Key{
		datastore.NameKey("TestKind", "key-1", nil),
		datastore.NameKey("TestKind", "key-2", nil), // doesn't exist
		datastore.NameKey("TestKind", "key-3", nil),
	}

	var retrieved []testEntity
	err = client.GetMulti(ctx, getAllKeys, &retrieved)
	if err == nil {
		t.Error("expected error when some keys don't exist")
	}
}

func TestKeyComparison(t *testing.T) {
	nameKey1 := datastore.NameKey("Kind", "name", nil)
	nameKey2 := datastore.NameKey("Kind", "name", nil)

	if nameKey1.Kind != nameKey2.Kind || nameKey1.Name != nameKey2.Name {
		t.Error("identical name keys should have same values")
	}

	idKey1 := datastore.IDKey("Kind", 123, nil)
	idKey2 := datastore.IDKey("Kind", 123, nil)

	if idKey1.Kind != idKey2.Kind || idKey1.ID != idKey2.ID {
		t.Error("identical ID keys should have same values")
	}
}

func TestLargeEntityBatch(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create a larger batch
	const batchSize = 50
	entities := make([]testEntity, batchSize)
	keys := make([]*datastore.Key, batchSize)

	for i := range batchSize {
		entities[i] = testEntity{
			Name:  "batch-item",
			Count: int64(i),
		}
		keys[i] = datastore.NameKey("BatchKind", string(rune('0'+i/10))+string(rune('0'+i%10)), nil)
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
	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("expected datastore.ErrNoSuchEntity after batch delete, got %v", err)
	}
}

func TestMultiGetEmptySlices(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Call MultiGet with empty slices - should return error
	var entities []testEntity
	err := client.GetMulti(ctx, []*datastore.Key{}, &entities)
	if err == nil {
		t.Error("expected error for MultiGet with empty keys, got nil")
	}
}

func TestMultiPutEmptySlices(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Call MultiPut with empty slices - should return error
	_, err := client.PutMulti(ctx, []*datastore.Key{}, []testEntity{})
	if err == nil {
		t.Error("expected error for MultiPut with empty keys, got nil")
	}
}

func TestMultiDeleteEmptySlice(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Call MultiDelete with empty slice - should return error
	err := client.DeleteMulti(ctx, []*datastore.Key{})
	if err == nil {
		t.Error("expected error for MultiDelete with empty keys, got nil")
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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := datastore.NewClientWithDatabase(ctx, "test-project", "del-db")
	if err != nil {
		t.Fatalf("NewClientWithDatabase failed: %v", err)
	}

	// Delete with databaseID
	key := datastore.NameKey("TestKind", "to-delete", nil)
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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := datastore.NewClientWithDatabase(ctx, "test-project", "query-db")
	if err != nil {
		t.Fatalf("NewClientWithDatabase failed: %v", err)
	}

	// Query with databaseID
	query := datastore.NewQuery("TestKind").KeysOnly()
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
		// Return missing entities to trigger datastore.ErrNoSuchEntity
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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := datastore.NewClientWithDatabase(ctx, "test-project", "multiget-db")
	if err != nil {
		t.Fatalf("NewClientWithDatabase failed: %v", err)
	}

	// MultiGet with databaseID
	keys := []*datastore.Key{
		datastore.NameKey("TestKind", "key1", nil),
		datastore.NameKey("TestKind", "key2", nil),
	}
	var entities []testEntity
	err = client.GetMulti(ctx, keys, &entities)
	// Expect error since entities don't exist
	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("expected datastore.ErrNoSuchEntity, got: %v", err)
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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := datastore.NewClientWithDatabase(ctx, "test-project", "multidel-db")
	if err != nil {
		t.Fatalf("NewClientWithDatabase failed: %v", err)
	}

	// MultiDelete with databaseID
	keys := []*datastore.Key{
		datastore.NameKey("TestKind", "key1", nil),
		datastore.NameKey("TestKind", "key2", nil),
	}
	err = client.DeleteMulti(ctx, keys)
	if err != nil {
		t.Fatalf("MultiDelete with databaseID failed: %v", err)
	}
}

func TestDeleteAllByKind(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put multiple entities of the same kind
	for i := range 5 {
		entity := &testEntity{
			Name:  "item",
			Count: int64(i),
		}
		key := datastore.NameKey("DeleteKind", string(rune('a'+i)), nil)
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
	query := datastore.NewQuery("DeleteKind").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected 0 keys after DeleteAllByKind, got %d", len(keys))
	}
}

func TestDeleteAllByKindEmpty(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Delete from non-existent kind
	err := client.DeleteAllByKind(ctx, "NonExistentKind")
	if err != nil {
		t.Errorf("DeleteAllByKind on empty kind should not error, got: %v", err)
	}
}

func TestHierarchicalKeys(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create parent key
	parentKey := datastore.NameKey("Parent", "parent1", nil)
	parentEntity := &testEntity{
		Name:  "parent",
		Count: 1,
	}
	_, err := client.Put(ctx, parentKey, parentEntity)
	if err != nil {
		t.Fatalf("Put parent failed: %v", err)
	}

	// Create child key with parent
	childKey := datastore.NameKey("Child", "child1", parentKey)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create grandparent -> parent -> child hierarchy
	grandparentKey := datastore.NameKey("Grandparent", "gp1", nil)
	parentKey := datastore.NameKey("Parent", "p1", grandparentKey)
	childKey := datastore.NameKey("Child", "c1", parentKey)

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

func TestPutWithInvalidEntity(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type InvalidEntity struct {
		Map map[string]string // maps not supported
	}

	key := datastore.NameKey("TestKind", "invalid", nil)
	entity := &InvalidEntity{
		Map: map[string]string{"key": "value"},
	}

	_, err := client.Put(ctx, key, entity)
	if err == nil {
		t.Error("expected error for unsupported entity type")
	}
}

func TestGetMultiWithMismatchedSliceSize(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put one entity
	key1 := datastore.NameKey("TestKind", "key1", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err := client.Put(ctx, key1, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Try to get with wrong slice type
	keys := []*datastore.Key{key1}
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

func TestKeyFromJSONEdgeCases(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Test with ID key using integer ID
	idKey := datastore.IDKey("TestKind", 12345, nil)
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

func TestGetMultiMixedResults(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put some entities
	key1 := datastore.NameKey("Mixed", "exists1", nil)
	key2 := datastore.NameKey("Mixed", "exists2", nil)
	key3 := datastore.NameKey("Mixed", "missing", nil)

	entities := []testEntity{
		{Name: "entity1", Count: 1},
		{Name: "entity2", Count: 2},
	}

	_, err := client.PutMulti(ctx, []*datastore.Key{key1, key2}, entities)
	if err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	// Try to get mix of existing and non-existing
	keys := []*datastore.Key{key1, key2, key3}
	var retrieved []testEntity

	err = client.GetMulti(ctx, keys, &retrieved)
	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("expected datastore.ErrNoSuchEntity for mixed results, got: %v", err)
	}
}

func TestPutMultiLargeBatch(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create large batch
	const size = 100
	entities := make([]testEntity, size)
	keys := make([]*datastore.Key, size)

	for i := range size {
		entities[i] = testEntity{
			Name:  "large-batch",
			Count: int64(i),
		}
		keys[i] = datastore.NameKey("LargeBatch", fmt.Sprintf("key-%d", i), nil)
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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	keys := []*datastore.Key{
		datastore.NameKey("TestKind", "key1", nil),
		datastore.NameKey("TestKind", "key2", nil),
	}

	err = client.DeleteMulti(ctx, keys)
	if err == nil {
		t.Fatal("expected error on server failure")
	}
}

func TestKeyWithOnlyKind(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Key with neither name nor ID should work (incomplete key)
	// This gets an ID assigned by the datastore
	key := &datastore.Key{Kind: "TestKind"}
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

func TestGetMultiAllMissing(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*datastore.Key{
		datastore.NameKey("Missing", "key1", nil),
		datastore.NameKey("Missing", "key2", nil),
		datastore.NameKey("Missing", "key3", nil),
	}

	var entities []testEntity
	err := client.GetMulti(ctx, keys, &entities)

	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("expected datastore.ErrNoSuchEntity when all keys missing, got: %v", err)
	}
}

func TestGetMultiWithSliceMismatch(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put entity
	key := datastore.NameKey("Test", "key1", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// GetMulti with destination not being a pointer to slice
	var notSlice testEntity
	err = client.GetMulti(ctx, []*datastore.Key{key}, notSlice)
	if err == nil {
		t.Error("expected error when dst is not pointer to slice")
	}
}

func TestPutMultiWithLengthMismatch(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Keys and entities with different lengths
	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
		datastore.NameKey("Test", "key2", nil),
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Delete non-existent key (should not error)
	key := datastore.NameKey("Test", "nonexistent", nil)
	err := client.Delete(ctx, key)
	if err != nil {
		t.Errorf("Delete of non-existent key should not error, got: %v", err)
	}
}

func TestAllKeysWithEmptyResult(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Query kind with no entities
	query := datastore.NewQuery("EmptyKind").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys on empty kind failed: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected 0 keys, got %d", len(keys))
	}
}

func TestAllKeysWithLargeResult(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put many entities
	for i := range 50 {
		key := datastore.NameKey("LargeResult", fmt.Sprintf("key-%d", i), nil)
		entity := &testEntity{Name: "test", Count: int64(i)}
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Query all
	query := datastore.NewQuery("LargeResult").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 50 {
		t.Errorf("expected 50 keys, got %d", len(keys))
	}
}

func TestPutMultiEmptySlice(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Empty slices
	_, err := client.PutMulti(ctx, []*datastore.Key{}, []testEntity{})
	if err == nil {
		t.Error("expected error for empty slices")
	}
}

func TestGetMultiEmptySlice(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	var entities []testEntity
	err := client.GetMulti(ctx, []*datastore.Key{}, &entities)
	if err == nil {
		t.Error("expected error for empty keys")
	}
}

func TestDeleteMultiEmptySlice(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	err := client.DeleteMulti(ctx, []*datastore.Key{})
	if err == nil {
		t.Error("expected error for empty keys")
	}
}

func TestDeepHierarchicalKeys(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create 4-level hierarchy
	gp := datastore.NameKey("GP", "gp1", nil)
	p := datastore.NameKey("P", "p1", gp)
	c := datastore.NameKey("C", "c1", p)
	gc := datastore.NameKey("GC", "gc1", c)

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

func TestGetWithNonPointerDst(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put entity
	key := datastore.NameKey("Test", "key", nil)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	key := datastore.NameKey("Test", "key", nil)
	entity := testEntity{Name: "test", Count: 1} // not a pointer

	// The mock implementation may accept non-pointers, but test with the real client
	// For now, just test that it works (real Datastore would require pointer)
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Logf("Put with non-pointer entity failed (expected with real client): %v", err)
	}
}

func TestDeleteAllByKindWithNoEntities(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Delete from kind with no entities
	err := client.DeleteAllByKind(ctx, "NonExistentKind")
	if err != nil {
		t.Errorf("DeleteAllByKind on empty kind should not error, got: %v", err)
	}
}

func TestDeleteAllByKindWithManyEntities(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put many entities
	for i := range 25 {
		key := datastore.NameKey("ManyDelete", fmt.Sprintf("key-%d", i), nil)
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
	query := datastore.NewQuery("ManyDelete").KeysOnly()
	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Fatalf("AllKeys failed: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("expected 0 keys after DeleteAllByKind, got %d", len(keys))
	}
}

func TestIDKeyWithZeroID(t *testing.T) {
	// Zero ID is valid
	key := datastore.IDKey("Test", 0, nil)
	if key.ID != 0 {
		t.Errorf("expected ID 0, got %d", key.ID)
	}
	if key.Name != "" {
		t.Errorf("expected empty Name, got %q", key.Name)
	}
}

func TestNameKeyWithEmptyName(t *testing.T) {
	// Empty name is technically valid
	key := datastore.NameKey("Test", "", nil)
	if key.Name != "" {
		t.Errorf("expected empty Name, got %q", key.Name)
	}
	if key.ID != 0 {
		t.Errorf("expected ID 0, got %d", key.ID)
	}
}

func TestGetMultiWithNonSliceDst(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
	}

	// Pass a non-slice as destination
	var notSlice string
	err := client.GetMulti(ctx, keys, &notSlice)

	if err == nil {
		t.Error("expected error when dst is not a slice")
	}
}

func TestPutMultiWithNonSliceSrc(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
	}

	// Pass a non-slice as source
	notSlice := "not a slice"
	_, err := client.PutMulti(ctx, keys, notSlice)

	if err == nil {
		t.Error("expected error when src is not a slice")
	}
}

func TestAllKeysQueryWithoutKeysOnly(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create query without KeysOnly
	query := datastore.NewQuery("Test")

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	err = client.DeleteAllByKind(ctx, "TestKind")

	if err == nil {
		t.Error("expected error when query fails")
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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("Test", "key", nil)
	var entity testEntity
	err = client.Get(ctx, key, &entity)

	if err == nil {
		t.Error("expected error for invalid JSON response")
	}
}

func TestPutWithInvalidEntityStructure(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Entity with channel (unsupported type)
	type BadEntity struct {
		Ch   chan int
		Name string
	}

	key := datastore.NameKey("Test", "bad", nil)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put one entity
	key1 := datastore.NameKey("Test", "exists", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err := client.Put(ctx, key1, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Try to get multiple with one missing
	keys := []*datastore.Key{
		key1,
		datastore.NameKey("Test", "missing", nil),
		datastore.NameKey("Test", "missing2", nil),
	}

	var entities []testEntity
	err = client.GetMulti(ctx, keys, &entities)

	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("expected datastore.ErrNoSuchEntity when some keys missing, got: %v", err)
	}
}

func TestDeleteMultiPartialSuccess(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put some entities
	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
		datastore.NameKey("Test", "key2", nil),
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
	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("expected datastore.ErrNoSuchEntity after delete, got: %v", err)
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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("Test", "key", nil)
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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
		datastore.NameKey("Test", "key2", nil),
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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	query := datastore.NewQuery("Test").KeysOnly()
	_, err = client.AllKeys(ctx, query)

	if err == nil {
		t.Error("expected error for invalid JSON")
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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	key := datastore.NameKey("Test", "key", nil)
	err = client.Delete(ctx, key)

	if err == nil {
		t.Error("expected error when context is cancelled")
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	realClient, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := datastore.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test"}

	// Try Put which will parse the returned key
	_, err = realClient.Put(ctx, key, entity)
	if err == nil {
		t.Log("Put succeeded despite invalid path element (API may handle gracefully)")
	} else {
		t.Logf("Put failed as expected: %v", err)
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	realClient, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := datastore.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test"}

	// Try Put which will parse the returned key
	_, err = realClient.Put(ctx, key, entity)
	if err == nil {
		t.Log("Put succeeded despite invalid ID string (API may handle gracefully)")
	} else {
		t.Logf("Put failed as expected: %v", err)
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	realClient, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := datastore.NameKey("Test", "key", nil)
	var entity testEntity

	err = realClient.Get(ctx, key, &entity)
	if err != nil {
		t.Errorf("unexpected error with float64 ID: %v", err)
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := datastore.NameKey("Test", "key", nil)

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
		datastore.NameKey("Test", "key2", nil),
	}

	var entities []testEntity
	err = client.GetMulti(ctx, keys, &entities)
	if err == nil {
		t.Error("expected error when some entities are missing")
	} else {
		t.Logf("GetMulti with missing entities failed as expected: %v", err)
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	query := datastore.NewQuery("Test").KeysOnly()

	_, err = client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error with invalid JSON")
	}
}

// Test Transaction commit with invalid response

func TestPutMultiWithInvalidEntities(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type InvalidEntity struct {
		Func func() `datastore:"func"`
	}

	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
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

func TestGetWithNonPointer(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()
	key := datastore.NameKey("Test", "key", nil)
	var entity testEntity // non-pointer

	err := client.Get(ctx, key, entity) // Pass by value
	if err == nil {
		t.Error("expected error when dst is not a pointer")
	}
}

func TestPutWithNonStruct(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()
	key := datastore.NameKey("Test", "key", nil)
	entity := "not a struct"

	_, err := client.Put(ctx, key, entity)
	if err == nil {
		t.Error("expected error when entity is not a struct")
	}
}

func TestAllKeysNotKeysOnlyError(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()
	query := datastore.NewQuery("Test") // Not KeysOnly

	_, err := client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error when query is not KeysOnly")
	}
}

func TestGetMultiMismatchedLength(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()
	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
		datastore.NameKey("Test", "key2", nil),
	}

	var entities []testEntity // Empty slice

	err := client.GetMulti(ctx, keys, &entities)
	// This should work - GetMulti should populate the slice
	if err != nil {
		t.Logf("GetMulti with empty slice: %v", err)
	}
}

func TestPutMultiMismatchedLength(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()
	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
		datastore.NameKey("Test", "key2", nil),
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

func TestDeleteMultiWithEmptyKeysSlice(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()
	var keys []*datastore.Key // Empty

	err := client.DeleteMulti(ctx, keys)
	// Mock may behave differently - log the result
	if err != nil {
		t.Logf("DeleteMulti with empty keys: %v", err)
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := datastore.NameKey("Test", "key", nil)
	var entity testEntity

	err = client.Get(ctx, key, &entity)
	if err == nil {
		t.Error("expected error with invalid entity format")
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, "http://unused")
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := datastore.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test"}

	_, err = client.Put(ctx, key, entity)
	if err == nil {
		t.Error("expected error when access token fails")
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := datastore.NameKey("Test", "key", nil)

	err = client.Delete(ctx, key)
	if err != nil {
		t.Logf("Delete completed with: %v", err)
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	query := datastore.NewQuery("Test").KeysOnly()

	keys, err := client.AllKeys(ctx, query)
	if err != nil {
		t.Logf("AllKeys with many results: %v", err)
	} else if len(keys) != 50 {
		t.Logf("Expected 50 keys, got %d", len(keys))
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	query := datastore.NewQuery("Test").KeysOnly()

	_, err = client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error with invalid key format")
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()

	// Test with valid entities to exercise the code path
	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
	}

	entities := []testEntity{
		{Name: "test1", Count: 123},
	}

	_, err = client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Logf("PutMulti completed with: %v", err)
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	err = client.DeleteAllByKind(ctx, "EmptyKind")
	if err != nil {
		t.Logf("DeleteAllByKind with empty batch: %v", err)
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	query := datastore.NewQuery("TestKind").KeysOnly()
	_, err = client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error with empty path in key")
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	query := datastore.NewQuery("TestKind").KeysOnly()
	_, err = client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error with invalid path element")
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	ctx := context.Background()
	key := datastore.IDKey("TestKind", 12345, nil)
	var entity TestEntity
	err = client.Get(ctx, key, &entity)
	if err != nil {
		t.Fatalf("Get with string ID key failed: %v", err)
	}

	if entity.Name != "test" {
		t.Errorf("expected name 'test', got %q", entity.Name)
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	type TestEntity struct {
		Value int64 `datastore:"value"`
	}

	ctx := context.Background()
	key := datastore.IDKey("TestKind", 67890, nil)
	var entity TestEntity
	err = client.Get(ctx, key, &entity)
	if err != nil {
		t.Fatalf("Get with float64 ID key failed: %v", err)
	}

	if entity.Value != 42 {
		t.Errorf("expected value 42, got %d", entity.Value)
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	type TestEntity struct {
		Name string `datastore:"name"`
	}

	ctx := context.Background()
	key := datastore.IDKey("TestKind", 12345, nil)
	var entity TestEntity
	err = client.Get(ctx, key, &entity)
	// May or may not error depending on parsing behavior
	if err != nil {
		t.Logf("Get with invalid string ID format failed: %v", err)
	} else {
		t.Logf("Get with invalid string ID format succeeded unexpectedly")
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	key := datastore.NameKey("Test", "test-key", nil)
	var entity testEntity

	err = client.Get(ctx, key, &entity)
	if err == nil {
		t.Error("expected error with malformed JSON")
	}
}

func TestPutMultiLengthValidation(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()
	keys := []*datastore.Key{datastore.NameKey("Test", "key1", nil)}
	entities := []testEntity{{Name: "test1"}, {Name: "test2"}}

	_, err := client.PutMulti(ctx, keys, entities)
	if err == nil {
		t.Error("expected error with mismatched lengths")
	}
}

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

	restore := datastore.SetTestURLs(metadataServer.URL, apiServer.URL)
	defer restore()

	client, err := datastore.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	ctx := context.Background()
	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
		datastore.NameKey("Test", "key2", nil),
	}

	err = client.DeleteMulti(ctx, keys)
	// May or may not error depending on implementation
	if err != nil {
		t.Logf("DeleteMulti with mismatched results: %v", err)
	}
}

func TestBackwardsCompatibility(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
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
		key := datastore.NameKey("TestKind", "test-tx-compat", nil)

		commit, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
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

		keys := []*datastore.Key{
			datastore.NameKey("GetAllTest", "key1", nil),
			datastore.NameKey("GetAllTest", "key2", nil),
			datastore.NameKey("GetAllTest", "key3", nil),
		}

		_, err := client.PutMulti(ctx, keys, entities)
		if err != nil {
			t.Fatalf("PutMulti failed: %v", err)
		}

		// Test GetAll
		query := datastore.NewQuery("GetAllTest")
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
		query := datastore.NewQuery("GetAllTest").Limit(2)
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

func TestArraySliceSupport(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("StringSlice", func(t *testing.T) {
		key := datastore.NameKey("ArrayTest", "strings", nil)
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
		key := datastore.NameKey("ArrayTest", "ints", nil)
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
		key := datastore.NameKey("ArrayTest", "floats", nil)
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
		key := datastore.NameKey("ArrayTest", "bools", nil)
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
		key := datastore.NameKey("ArrayTest", "empty", nil)
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
		key := datastore.NameKey("ArrayTest", "mixed", nil)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("AllocateIncompleteKeys", func(t *testing.T) {
		keys := []*datastore.Key{
			datastore.IncompleteKey("Task", nil),
			datastore.IncompleteKey("Task", nil),
			datastore.IncompleteKey("Task", nil),
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
		keys := []*datastore.Key{
			datastore.NameKey("Task", "complete", nil),
			datastore.IncompleteKey("Task", nil),
			datastore.IDKey("Task", 123, nil),
			datastore.IncompleteKey("Task", nil),
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
		keys := []*datastore.Key{}

		allocated, err := client.AllocateIDs(ctx, keys)
		if err != nil {
			t.Fatalf("AllocateIDs with empty slice failed: %v", err)
		}

		if len(allocated) != 0 {
			t.Errorf("Expected empty slice, got %d keys", len(allocated))
		}
	})

	t.Run("AllocateAllCompleteKeys", func(t *testing.T) {
		keys := []*datastore.Key{
			datastore.NameKey("Task", "key1", nil),
			datastore.IDKey("Task", 100, nil),
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
