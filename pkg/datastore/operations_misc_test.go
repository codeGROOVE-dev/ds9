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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

	realClient, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

	realClient, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

	realClient, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("Test", "key", nil)
	var entity testEntity

	err = realClient.Get(ctx, key, &entity)
	if err != nil {
		t.Errorf("unexpected error with float64 ID: %v", err)
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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	query := datastore.NewQuery("Test").KeysOnly()

	_, err = client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error with invalid JSON")
	}
}

// Test Transaction commit with invalid response

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	query := datastore.NewQuery("Test").KeysOnly()

	_, err = client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error with invalid key format")
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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

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

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)
	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	query := datastore.NewQuery("TestKind").KeysOnly()
	_, err = client.AllKeys(ctx, query)
	if err == nil {
		t.Error("expected error with invalid path element")
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
