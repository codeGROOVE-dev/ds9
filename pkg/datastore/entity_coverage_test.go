package datastore

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Additional tests to improve coverage for entity encoding/decoding

func TestEncodeValue_AllTypes(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"nil", nil},
		{"string", "test"},
		{"int", int(42)},
		{"int64", int64(42)},
		{"int32", int32(42)},
		{"bool", true},
		{"float64", float64(3.14)},
		{"time", time.Now().UTC()},
		{"string slice", []string{"a", "b"}},
		{"int64 slice", []int64{1, 2}},
		{"int slice", []int{1, 2}},
		{"float64 slice", []float64{1.1, 2.2}},
		{"bool slice", []bool{true, false}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := encodeAny(tt.value)
			if err != nil {
				t.Errorf("encodeValue failed: %v", err)
			}
		})
	}
}

func TestEncodeValue_UnsupportedType(t *testing.T) {
	_, err := encodeAny(map[string]string{"key": "value"})
	if err == nil {
		t.Error("Expected error for unsupported type, got nil")
	}
}

func TestMutate_Coverage(t *testing.T) {
	client, cleanup := NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	key := NameKey("MutateTest", "test1", nil)
	entity := &struct {
		Name  string
		Count int64
		Time  time.Time
	}{
		Name:  "test",
		Count: 42,
		Time:  time.Now().UTC().Truncate(time.Microsecond),
	}

	t.Run("InsertMutation", func(t *testing.T) {
		insertMut := NewInsert(key, entity)
		_, err := client.Mutate(ctx, insertMut)
		if err != nil {
			t.Fatalf("Insert mutation failed: %v", err)
		}
	})

	t.Run("UpdateMutation", func(t *testing.T) {
		entity.Count = 100
		updateMut := NewUpdate(key, entity)
		_, err := client.Mutate(ctx, updateMut)
		if err != nil {
			t.Fatalf("Update mutation failed: %v", err)
		}
	})

	t.Run("UpsertMutation", func(t *testing.T) {
		entity.Count = 200
		upsertMut := NewUpsert(key, entity)
		_, err := client.Mutate(ctx, upsertMut)
		if err != nil {
			t.Fatalf("Upsert mutation failed: %v", err)
		}
	})

	t.Run("DeleteMutation", func(t *testing.T) {
		deleteMut := NewDelete(key)
		_, err := client.Mutate(ctx, deleteMut)
		if err != nil {
			t.Fatalf("Delete mutation failed: %v", err)
		}
	})

	t.Run("EmptyMutations", func(t *testing.T) {
		// Test with no mutations
		keys, err := client.Mutate(ctx)
		if err != nil {
			t.Errorf("Empty Mutate should not error: %v", err)
		}
		if keys != nil {
			t.Errorf("Expected nil keys for empty mutate, got %v", keys)
		}
	})

	t.Run("NilMutation", func(t *testing.T) {
		// Test with nil mutation
		_, err := client.Mutate(ctx, nil)
		if err == nil {
			t.Error("Expected error for nil mutation")
		}
	})

	t.Run("NilKey", func(t *testing.T) {
		// Test mutation with nil key
		mut := &Mutation{
			key:    nil,
			op:     MutationInsert,
			entity: entity,
		}
		_, err := client.Mutate(ctx, mut)
		if err == nil {
			t.Error("Expected error for mutation with nil key")
		}
	})

	t.Run("NilEntityInsert", func(t *testing.T) {
		// Test insert with nil entity
		mut := NewInsert(key, nil)
		_, err := client.Mutate(ctx, mut)
		if err == nil {
			t.Error("Expected error for insert with nil entity")
		}
	})

	t.Run("NilEntityUpdate", func(t *testing.T) {
		// Test update with nil entity
		mut := NewUpdate(key, nil)
		_, err := client.Mutate(ctx, mut)
		if err == nil {
			t.Error("Expected error for update with nil entity")
		}
	})

	t.Run("NilEntityUpsert", func(t *testing.T) {
		// Test upsert with nil entity
		mut := NewUpsert(key, nil)
		_, err := client.Mutate(ctx, mut)
		if err == nil {
			t.Error("Expected error for upsert with nil entity")
		}
	})

	t.Run("MultipleMutations", func(t *testing.T) {
		// Test multiple mutations at once
		key1 := NameKey("MutateTest", "batch1", nil)
		key2 := NameKey("MutateTest", "batch2", nil)
		key3 := NameKey("MutateTest", "batch3", nil)

		muts := []*Mutation{
			NewInsert(key1, entity),
			NewUpsert(key2, entity),
			NewDelete(key3),
		}

		_, err := client.Mutate(ctx, muts...)
		if err != nil {
			t.Fatalf("Multiple mutations failed: %v", err)
		}
	})
}

func TestIterator_Coverage(t *testing.T) {
	client, cleanup := NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create test entities
	for i := range 5 {
		key := IDKey("IteratorTest", int64(i+1), nil)
		entity := &struct {
			Name  string
			Index int64
			Time  time.Time
		}{
			Name:  "test",
			Index: int64(i),
			Time:  time.Now().UTC().Truncate(time.Microsecond),
		}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Test iterator with cursor
	query := NewQuery("IteratorTest").Limit(2)
	it := client.Run(ctx, query)

	count := 0
	for {
		var entity struct {
			Name  string
			Index int64
			Time  time.Time
		}
		_, err := it.Next(&entity)
		if errors.Is(err, Done) {
			break
		}
		if err != nil {
			t.Fatalf("Iterator Next failed: %v", err)
		}
		count++

		// Test Cursor() method
		cursor, err := it.Cursor()
		if err != nil {
			t.Logf("Cursor() error (expected for some backends): %v", err)
		} else if cursor.String() != "" {
			t.Logf("Got cursor: %s", cursor.String())
		}
	}

	if count == 0 {
		t.Error("Expected at least some entities from iterator")
	}
}

func TestAllocateIDs_Coverage(t *testing.T) {
	client, cleanup := NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create incomplete keys
	keys := []*Key{
		IncompleteKey("AllocateTest", nil),
		IncompleteKey("AllocateTest", nil),
		IncompleteKey("AllocateTest", nil),
	}

	// Allocate IDs
	allocatedKeys, err := client.AllocateIDs(ctx, keys)
	if err != nil {
		t.Fatalf("AllocateIDs failed: %v", err)
	}

	if len(allocatedKeys) != len(keys) {
		t.Errorf("Expected %d allocated keys, got %d", len(keys), len(allocatedKeys))
	}

	// Verify keys have IDs
	for i, key := range allocatedKeys {
		if key.Incomplete() {
			t.Errorf("Key %d is still incomplete after allocation", i)
		}
	}
}

func TestGet_ErrorCases(t *testing.T) {
	client, cleanup := NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("NonExistent", func(t *testing.T) {
		key := NameKey("NonExistent", "test", nil)
		var entity struct {
			Name string
		}
		err := client.Get(ctx, key, &entity)
		if !errors.Is(err, ErrNoSuchEntity) {
			t.Errorf("Expected ErrNoSuchEntity, got %v", err)
		}
	})

	t.Run("NilKey", func(t *testing.T) {
		var entity struct {
			Name string
		}
		err := client.Get(ctx, nil, &entity)
		if err == nil {
			t.Error("Expected error for nil key, got nil")
		}
	})
}

func TestNewClientWithDatabase_Coverage(t *testing.T) {
	// Use NewMockClient instead of hardcoded URLs to avoid port conflicts
	client, cleanup := NewMockClient(t)
	defer cleanup()

	if client == nil {
		t.Fatal("expected non-nil client")
	}

	// Test with database ID using mock servers
	ctx := context.Background()
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metadataServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer apiServer.Close()

	opts := TestConfig(ctx, metadataServer.URL, apiServer.URL)

	// Test with empty project (error case)
	_, err := NewClientWithDatabase(ctx, "", "test-db", opts...)
	if err == nil {
		t.Error("Expected error for empty project ID, got nil")
	}
}
