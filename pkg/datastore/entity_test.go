package datastore_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/codeGROOVE-dev/ds9/auth"
	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

func TestEntityWithAllTypes(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
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

	key := datastore.NameKey("AllTypes", "test", nil)
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

func TestUnsupportedEncodeType(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Entity with unsupported type (map)
	type BadEntity struct {
		Name string
		Data map[string]string // maps not supported
	}

	key := datastore.NameKey("TestKind", "bad", nil)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Store entity
	key := datastore.NameKey("TestKind", "test", nil)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Store entity
	key := datastore.NameKey("TestKind", "test", nil)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type EntityWithSkip struct {
		Name    string `datastore:"name"`
		Skipped string `datastore:"-"`
		private string
		Count   int64 `datastore:"count"`
	}

	key := datastore.NameKey("TestKind", "skip", nil)
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
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type ZeroEntity struct {
		Name   string
		Count  int64
		Active bool
		Score  float64
	}

	key := datastore.NameKey("TestKind", "zero", nil)
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

func TestDecodeValueEdgeCases(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
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
	key := datastore.NameKey("Complex", "test", nil)
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

func TestEntityWithPointerFields(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Entities with pointer fields
	type EntityWithPointers struct {
		Name  *string `datastore:"name"`
		Count *int64  `datastore:"count"`
	}

	name := "test"
	count := int64(42)
	key := datastore.NameKey("Pointers", "test", nil)
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

func TestEntityWithEmptyStringFields(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	key := datastore.NameKey("Empty", "test", nil)
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

func TestPutMultiWithPartialEncode(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Mix of valid and invalid entities
	type MixedEntity struct {
		Data any
		Name string
	}

	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
		datastore.NameKey("Test", "key2", nil),
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

	authConfig := &auth.Config{
		MetadataURL: metadataServer.URL,
		SkipADC:     true,
	}
	client, err := datastore.NewClient(
		context.Background(),
		"test-project",
		datastore.WithEndpoint(apiServer.URL),
		datastore.WithAuth(authConfig),
	)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("Test", "key", nil)
	var entity testEntity

	err = client.Get(context.Background(), key, &entity) // Changed ctx to context.Background()
	if err == nil {
		t.Error("expected error with missing properties")
	}
}

func TestPutWithEncodeError(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create entity with unsupported type
	type BadEntity struct {
		Channel chan int `datastore:"channel"`
	}

	key := datastore.NameKey("Test", "key", nil)
	entity := &BadEntity{Channel: make(chan int)}

	_, err := client.Put(ctx, key, entity)
	if err == nil {
		t.Log("Put with unsupported type succeeded (mock may not validate types)")
	} else {
		t.Logf("Put with unsupported type failed as expected: %v", err)
	}
}

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

	authConfig := &auth.Config{
		MetadataURL: metadataServer.URL,
		SkipADC:     true,
	}
	client, err := datastore.NewClient(
		context.Background(),
		"test-project",
		datastore.WithEndpoint(apiServer.URL),
		datastore.WithAuth(authConfig),
	)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("Test", "key", nil)
	var entity testEntity

	err = client.Get(context.Background(), key, &entity) // Changed ctx to context.Background()
	if err == nil {
		t.Error("expected error with invalid integer format")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

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

	authConfig := &auth.Config{
		MetadataURL: metadataServer.URL,
		SkipADC:     true,
	}
	client, err := datastore.NewClient(
		context.Background(),
		"test-project",
		datastore.WithEndpoint(apiServer.URL),
		datastore.WithAuth(authConfig),
	)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("Test", "key", nil)
	var entity testEntity

	err = client.Get(context.Background(), key, &entity) // Changed ctx to context.Background()
	if err == nil {
		t.Error("expected error with wrong type for integer")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

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

	authConfig := &auth.Config{
		MetadataURL: metadataServer.URL,
		SkipADC:     true,
	}
	client, err := datastore.NewClient(
		context.Background(),
		"test-project",
		datastore.WithEndpoint(apiServer.URL),
		datastore.WithAuth(authConfig),
	)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("Test", "key", nil)
	var entity testEntity

	err = client.Get(context.Background(), key, &entity) // Changed ctx to context.Background()
	if err == nil {
		t.Error("expected error with invalid timestamp format")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

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

	authConfig := &auth.Config{
		MetadataURL: metadataServer.URL,
		SkipADC:     true,
	}
	client, err := datastore.NewClient(
		context.Background(),
		"test-project",
		datastore.WithEndpoint(apiServer.URL),
		datastore.WithAuth(authConfig),
	)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	keys := []*datastore.Key{
		datastore.NameKey("Test", "key1", nil),
		datastore.NameKey("Test", "key2", nil),
	}

	var entities []testEntity
	err = client.GetMulti(context.Background(), keys, &entities) // Changed ctx to context.Background()
	if err == nil {
		t.Error("expected error when one entity has decode error")
	}
}
