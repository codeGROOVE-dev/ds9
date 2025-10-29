package ds9_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

const (
	defaultTestProject = "integration-testing-476513"
	testDatabaseID     = "ds9-test"
	testKind           = "DS9IntegrationTest"
)

func testProject() string {
	if project := os.Getenv("DS9_TEST_PROJECT"); project != "" {
		return project
	}
	return defaultTestProject
}

// integrationClient returns either a real GCP client or a mock client
// based on whether DS9_TEST_PROJECT is set.
func integrationClient(t *testing.T) (client *datastore.Client, cleanup func()) {
	t.Helper()

	if os.Getenv("DS9_TEST_PROJECT") != "" {
		// Real GCP integration test
		ctx := context.Background()
		client, err := datastore.NewClientWithDatabase(ctx, testProject(), testDatabaseID)
		if err != nil {
			t.Fatalf("Failed to create GCP client: %v", err)
		}
		return client, func() {} // No cleanup needed
	}

	// Mock client for unit testing
	return datastore.NewMockClient(t)
}

func TestIntegrationBasicOperations(t *testing.T) {
	client, cleanup := integrationClient(t)
	defer cleanup()

	ctx := context.Background()

	// Generate unique key for this test run
	testID := t.Name() + "-" + time.Now().Format("20060102-150405.000000")
	key := datastore.NameKey(testKind, testID, nil)

	// Cleanup at the end
	defer func() {
		if err := client.Delete(ctx, key); err != nil {
			t.Logf("Warning: failed to cleanup test entity: %v", err)
		}
	}()

	t.Run("Put", func(t *testing.T) {
		entity := &integrationEntity{
			Name:      "integration-test",
			Count:     42,
			Timestamp: time.Now().UTC().Truncate(time.Microsecond),
		}

		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	})

	t.Run("Get", func(t *testing.T) {
		var retrieved integrationEntity
		err := client.Get(ctx, key, &retrieved)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if retrieved.Name != "integration-test" {
			t.Errorf("expected Name 'integration-test', got %q", retrieved.Name)
		}
		if retrieved.Count != 42 {
			t.Errorf("expected Count 42, got %d", retrieved.Count)
		}
	})

	t.Run("Update", func(t *testing.T) {
		var entity integrationEntity
		err := client.Get(ctx, key, &entity)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		entity.Count = 100
		_, err = client.Put(ctx, key, &entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		var updated integrationEntity
		err = client.Get(ctx, key, &updated)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if updated.Count != 100 {
			t.Errorf("expected Count 100, got %d", updated.Count)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		err := client.Delete(ctx, key)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		var entity integrationEntity
		err = client.Get(ctx, key, &entity)
		if !errors.Is(err, datastore.ErrNoSuchEntity) {
			t.Errorf("expected datastore.ErrNoSuchEntity after delete, got %v", err)
		}
	})
}

func TestIntegrationBatchOperations(t *testing.T) {
	client, cleanup := integrationClient(t)
	defer cleanup()

	ctx := context.Background()

	// Generate unique keys for this test run
	testID := t.Name() + "-" + time.Now().Format("20060102-150405.000000")
	keys := []*datastore.Key{
		datastore.NameKey(testKind, testID+"-1", nil),
		datastore.NameKey(testKind, testID+"-2", nil),
		datastore.NameKey(testKind, testID+"-3", nil),
	}

	// Cleanup at the end
	defer func() {
		if err := client.DeleteMulti(ctx, keys); err != nil {
			t.Logf("Warning: failed to cleanup test entities: %v", err)
		}
	}()

	t.Run("PutMulti", func(t *testing.T) {
		entities := []integrationEntity{
			{Name: "batch-1", Count: 1, Timestamp: time.Now().UTC().Truncate(time.Microsecond)},
			{Name: "batch-2", Count: 2, Timestamp: time.Now().UTC().Truncate(time.Microsecond)},
			{Name: "batch-3", Count: 3, Timestamp: time.Now().UTC().Truncate(time.Microsecond)},
		}

		_, err := client.PutMulti(ctx, keys, entities)
		if err != nil {
			t.Fatalf("PutMulti failed: %v", err)
		}
	})

	t.Run("GetMulti", func(t *testing.T) {
		var retrieved []integrationEntity
		err := client.GetMulti(ctx, keys, &retrieved)
		if err != nil {
			t.Fatalf("GetMulti failed: %v", err)
		}

		if len(retrieved) != 3 {
			t.Fatalf("expected 3 entities, got %d", len(retrieved))
		}

		for i, entity := range retrieved {
			expectedCount := int64(i + 1)
			if entity.Count != expectedCount {
				t.Errorf("entity %d: expected Count %d, got %d", i, expectedCount, entity.Count)
			}
		}
	})

	t.Run("DeleteMulti", func(t *testing.T) {
		err := client.DeleteMulti(ctx, keys)
		if err != nil {
			t.Fatalf("DeleteMulti failed: %v", err)
		}

		var retrieved []integrationEntity
		err = client.GetMulti(ctx, keys, &retrieved)
		if !errors.Is(err, datastore.ErrNoSuchEntity) {
			t.Errorf("expected datastore.ErrNoSuchEntity after DeleteMulti, got %v", err)
		}
	})
}

func TestIntegrationTransaction(t *testing.T) {
	client, cleanup := integrationClient(t)
	defer cleanup()

	ctx := context.Background()

	testID := t.Name() + "-" + time.Now().Format("20060102-150405.000000")
	key := datastore.NameKey(testKind, testID, nil)

	defer func() {
		if err := client.Delete(ctx, key); err != nil {
			t.Logf("Warning: failed to cleanup test entity: %v", err)
		}
	}()

	t.Run("Transaction", func(t *testing.T) {
		// Create entity inside transaction to avoid contention with non-transactional operations
		_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			// Create new entity inside transaction
			initial := &integrationEntity{
				Name:      "counter",
				Count:     0,
				Timestamp: time.Now().UTC().Truncate(time.Microsecond),
			}
			_, err := tx.Put(key, initial)
			return err
		})
		if err != nil {
			t.Fatalf("Initial transaction failed: %v", err)
		}

		// Now run another transaction to update it
		_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			var entity integrationEntity
			if err := tx.Get(key, &entity); err != nil {
				return err
			}

			entity.Count += 10
			_, err := tx.Put(key, &entity)
			return err
		})
		if err != nil {
			t.Fatalf("Update transaction failed: %v", err)
		}

		// Verify the update
		var final integrationEntity
		err = client.Get(ctx, key, &final)
		if err != nil {
			t.Fatalf("Get after transaction failed: %v", err)
		}

		if final.Count != 10 {
			t.Errorf("expected Count 10, got %d", final.Count)
		}
	})
}

func TestIntegrationQuery(t *testing.T) {
	client, cleanup := integrationClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create test entities
	testID := t.Name() + "-" + time.Now().Format("20060102-150405.000000")
	keys := make([]*datastore.Key, 5)
	entities := make([]integrationEntity, 5)
	for i := range 5 {
		keys[i] = datastore.NameKey(testKind, testID+"-"+string(rune('a'+i)), nil)
		entities[i] = integrationEntity{
			Name:      "query-test",
			Count:     int64(i),
			Timestamp: time.Now().UTC().Truncate(time.Microsecond),
		}
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	defer func() {
		if err := client.DeleteMulti(ctx, keys); err != nil {
			t.Logf("Warning: failed to cleanup test entities: %v", err)
		}
	}()

	t.Run("AllKeys", func(t *testing.T) {
		query := datastore.NewQuery(testKind).KeysOnly().Limit(10)
		resultKeys, err := client.AllKeys(ctx, query)
		if err != nil {
			t.Fatalf("datastore.AllKeys failed: %v", err)
		}

		// We expect at least our 5 keys (there might be more from other tests)
		if len(resultKeys) < 5 {
			t.Errorf("expected at least 5 keys, got %d", len(resultKeys))
		}

		// Verify our keys are in the results
		found := 0
		for _, key := range resultKeys {
			for _, testKey := range keys {
				if key.Kind == testKey.Kind && key.Name == testKey.Name {
					found++
					break
				}
			}
		}

		if found < 5 {
			t.Errorf("expected to find all 5 test keys, found %d", found)
		}
	})
}

func TestIntegrationCleanup(t *testing.T) {
	client, cleanup := integrationClient(t)
	defer cleanup()

	ctx := context.Background()

	// First create some test entities
	testID := t.Name() + "-" + time.Now().Format("20060102-150405.000000")
	keys := []*datastore.Key{
		datastore.NameKey(testKind, testID+"-1", nil),
		datastore.NameKey(testKind, testID+"-2", nil),
	}
	entities := []integrationEntity{
		{Name: "cleanup-1", Count: 1, Timestamp: time.Now().UTC().Truncate(time.Microsecond)},
		{Name: "cleanup-2", Count: 2, Timestamp: time.Now().UTC().Truncate(time.Microsecond)},
	}

	_, err := client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	t.Run("CleanupTestEntities", func(t *testing.T) {
		// Delete all test entities
		err := client.DeleteAllByKind(ctx, testKind)
		if err != nil {
			t.Fatalf("Failed to delete test entities: %v", err)
		}

		// Verify all entities are deleted
		q := datastore.NewQuery(testKind).KeysOnly()
		keys, err := client.AllKeys(ctx, q)
		if err != nil {
			t.Fatalf("Failed to query after cleanup: %v", err)
		}

		if len(keys) != 0 {
			t.Errorf("expected 0 keys after cleanup, found %d", len(keys))
		}
	})
}

// integrationEntity for integration tests
type integrationEntity struct {
	Timestamp time.Time `datastore:"timestamp"`
	Name      string    `datastore:"name"`
	Count     int64     `datastore:"count"`
}

// TestIntegrationGetAll tests the GetAll method with real GCP or mock.
func TestIntegrationGetAll(t *testing.T) {
	client, cleanup := integrationClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("GetAllWithMultipleEntities", func(t *testing.T) {
		// Setup: Create test entities
		kind := "DS9GetAllTest"
		count := 5
		keys := make([]*datastore.Key, count)
		entities := make([]integrationEntity, count)

		for i := range count {
			keys[i] = datastore.IDKey(kind, int64(i+1000), nil) // Use IDs to avoid conflicts
			entities[i] = integrationEntity{
				Name:      "getall-entity-" + string(rune('A'+i)),
				Count:     int64(i * 100),
				Timestamp: time.Now().UTC().Truncate(time.Microsecond),
			}
		}

		// Put entities
		_, err := client.PutMulti(ctx, keys, entities)
		if err != nil {
			t.Fatalf("PutMulti failed: %v", err)
		}

		// Test GetAll
		query := datastore.NewQuery(kind)
		var results []integrationEntity
		returnedKeys, err := client.GetAll(ctx, query, &results)
		if err != nil {
			t.Fatalf("datastore.GetAll failed: %v", err)
		}

		if len(results) < count {
			t.Errorf("Expected at least %d entities, got %d", count, len(results))
		}

		if len(returnedKeys) < count {
			t.Errorf("Expected at least %d keys, got %d", count, len(returnedKeys))
		}

		// Verify we got the entities we created
		foundCount := 0
		for _, entity := range results {
			if entity.Name >= "getall-entity-A" && entity.Name <= "getall-entity-E" {
				foundCount++
			}
		}

		if foundCount < count {
			t.Errorf("Expected to find at least %d of our entities, found %d", count, foundCount)
		}

		// Cleanup
		err = client.DeleteMulti(ctx, keys)
		if err != nil {
			t.Logf("Warning: cleanup failed: %v", err)
		}
	})

	t.Run("GetAllWithLimit", func(t *testing.T) {
		kind := "DS9GetAllLimitTest"
		// Create 10 entities
		keys := make([]*datastore.Key, 10)
		entities := make([]integrationEntity, 10)

		for i := range 10 {
			keys[i] = datastore.IDKey(kind, int64(i+2000), nil)
			entities[i] = integrationEntity{
				Name:      "limit-test-" + string(rune('0'+i)),
				Count:     int64(i),
				Timestamp: time.Now().UTC().Truncate(time.Microsecond),
			}
		}

		_, err := client.PutMulti(ctx, keys, entities)
		if err != nil {
			t.Fatalf("PutMulti failed: %v", err)
		}

		// Test GetAll with limit
		query := datastore.NewQuery(kind).Limit(3)
		var results []integrationEntity
		returnedKeys, err := client.GetAll(ctx, query, &results)
		if err != nil {
			t.Fatalf("datastore.GetAll with limit failed: %v", err)
		}

		// Should get at most 3 results
		if len(results) > 3 {
			t.Errorf("Expected at most 3 entities with limit, got %d", len(results))
		}

		if len(returnedKeys) > 3 {
			t.Errorf("Expected at most 3 keys with limit, got %d", len(returnedKeys))
		}

		// Cleanup
		err = client.DeleteMulti(ctx, keys)
		if err != nil {
			t.Logf("Warning: cleanup failed: %v", err)
		}
	})

	t.Run("GetAllEmpty", func(t *testing.T) {
		kind := "DS9NonExistentKind"
		query := datastore.NewQuery(kind)
		var results []integrationEntity

		keys, err := client.GetAll(ctx, query, &results)
		if err != nil {
			t.Fatalf("datastore.GetAll on empty kind failed: %v", err)
		}

		if len(results) != 0 {
			t.Errorf("Expected 0 entities, got %d", len(results))
		}

		if len(keys) != 0 {
			t.Errorf("Expected 0 keys, got %d", len(keys))
		}
	})
}

// TestIntegrationClose tests the Close method.
func TestIntegrationClose(t *testing.T) {
	client, cleanup := integrationClient(t)
	defer cleanup()

	// Close should not error
	err := client.Close()
	if err != nil {
		t.Errorf("Close() returned unexpected error: %v", err)
	}

	// Should be idempotent
	err = client.Close()
	if err != nil {
		t.Errorf("Second Close() returned unexpected error: %v", err)
	}
}

// TestIntegrationCommitReturn tests that datastore.RunInTransaction returns a datastore.Commit.
func TestIntegrationCommitReturn(t *testing.T) {
	client, cleanup := integrationClient(t)
	defer cleanup()

	ctx := context.Background()
	key := datastore.IDKey("DS9CommitTest", 9999, nil)

	commit, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		entity := &integrationEntity{
			Name:      "commit-test",
			Count:     42,
			Timestamp: time.Now().UTC().Truncate(time.Microsecond),
		}
		_, err := tx.Put(key, entity)
		return err
	})
	if err != nil {
		t.Fatalf("datastore.RunInTransaction failed: %v", err)
	}

	if commit == nil {
		t.Fatal("Expected non-nil datastore.Commit, got nil")
	}

	// Verify entity was created
	var retrieved integrationEntity
	err = client.Get(ctx, key, &retrieved)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Name != "commit-test" {
		t.Errorf("Expected name 'commit-test', got '%s'", retrieved.Name)
	}

	// Cleanup
	err = client.Delete(ctx, key)
	if err != nil {
		t.Logf("Warning: cleanup failed: %v", err)
	}
}
