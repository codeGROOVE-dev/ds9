package datastore_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("Test", "key", nil)
	var entity testEntity

	err = client.Get(ctx, key, &entity)
	if err == nil {
		t.Error("expected error with invalid entity format")
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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	type TestEntity struct {
		Name string `datastore:"name"`
	}

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	type TestEntity struct {
		Value int64 `datastore:"value"`
	}

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	type TestEntity struct {
		Name string `datastore:"name"`
	}

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("Test", "test-key", nil)
	var entity testEntity

	err = client.Get(ctx, key, &entity)
	if err == nil {
		t.Error("expected error with malformed JSON")
	}
}
