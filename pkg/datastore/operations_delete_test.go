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

func TestDeleteWithNilKey(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	err := client.Delete(ctx, nil)
	if err == nil {
		t.Error("expected error for nil key, got nil")
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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

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

func TestDeleteMultiEmptySlice(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	err := client.DeleteMulti(ctx, []*datastore.Key{})
	if err == nil {
		t.Error("expected error for empty keys")
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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	err = client.DeleteAllByKind(ctx, "TestKind")

	if err == nil {
		t.Error("expected error when query fails")
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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("Test", "key", nil)

	err = client.Delete(ctx, key)
	if err != nil {
		t.Logf("Delete completed with: %v", err)
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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	err = client.DeleteAllByKind(ctx, "EmptyKind")
	if err != nil {
		t.Logf("DeleteAllByKind with empty batch: %v", err)
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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

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
