//go:build integration
// +build integration

package ds9_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/codeGROOVE-dev/ds9"
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

// testEntity for integration tests
type integrationEntity struct {
	Name      string    `datastore:"name"`
	Count     int64     `datastore:"count"`
	Timestamp time.Time `datastore:"timestamp"`
}

func TestIntegrationBasicOperations(t *testing.T) {
	if os.Getenv("DS9_TEST_PROJECT") == "" {
		t.Skip("Skipping integration test. Set DS9_TEST_PROJECT=your-project to run.")
	}

	ctx := context.Background()
	client, err := ds9.NewClientWithDatabase(ctx, testProject(), testDatabaseID)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Generate unique key for this test run
	testID := t.Name() + "-" + time.Now().Format("20060102-150405.000000")
	key := ds9.NameKey(testKind, testID, nil)

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
		if !errors.Is(err, ds9.ErrNoSuchEntity) {
			t.Errorf("expected ErrNoSuchEntity after delete, got %v", err)
		}
	})
}

func TestIntegrationBatchOperations(t *testing.T) {
	if os.Getenv("DS9_TEST_PROJECT") == "" {
		t.Skip("Skipping integration test. Set DS9_TEST_PROJECT=your-project to run.")
	}

	ctx := context.Background()
	client, err := ds9.NewClientWithDatabase(ctx, testProject(), testDatabaseID)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Generate unique keys for this test run
	testID := t.Name() + "-" + time.Now().Format("20060102-150405.000000")
	keys := []*ds9.Key{
		ds9.NameKey(testKind, testID+"-1", nil),
		ds9.NameKey(testKind, testID+"-2", nil),
		ds9.NameKey(testKind, testID+"-3", nil),
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
		if !errors.Is(err, ds9.ErrNoSuchEntity) {
			t.Errorf("expected ErrNoSuchEntity after DeleteMulti, got %v", err)
		}
	})
}

func TestIntegrationTransaction(t *testing.T) {
	if os.Getenv("DS9_TEST_PROJECT") == "" {
		t.Skip("Skipping integration test. Set DS9_TEST_PROJECT=your-project to run.")
	}

	ctx := context.Background()
	client, err := ds9.NewClientWithDatabase(ctx, testProject(), testDatabaseID)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	testID := t.Name() + "-" + time.Now().Format("20060102-150405.000000")
	key := ds9.NameKey(testKind, testID, nil)

	defer func() {
		if err := client.Delete(ctx, key); err != nil {
			t.Logf("Warning: failed to cleanup test entity: %v", err)
		}
	}()

	t.Run("Transaction", func(t *testing.T) {
		// Create entity inside transaction to avoid contention with non-transactional operations
		err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
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
		err = client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
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
	if os.Getenv("DS9_TEST_PROJECT") == "" {
		t.Skip("Skipping integration test. Set DS9_TEST_PROJECT=your-project to run.")
	}

	ctx := context.Background()
	client, err := ds9.NewClientWithDatabase(ctx, testProject(), testDatabaseID)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Create test entities
	testID := t.Name() + "-" + time.Now().Format("20060102-150405.000000")
	keys := make([]*ds9.Key, 5)
	entities := make([]integrationEntity, 5)
	for i := range 5 {
		keys[i] = ds9.NameKey(testKind, testID+"-"+string(rune('a'+i)), nil)
		entities[i] = integrationEntity{
			Name:      "query-test",
			Count:     int64(i),
			Timestamp: time.Now().UTC().Truncate(time.Microsecond),
		}
	}

	_, err = client.PutMulti(ctx, keys, entities)
	if err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	defer func() {
		if err := client.DeleteMulti(ctx, keys); err != nil {
			t.Logf("Warning: failed to cleanup test entities: %v", err)
		}
	}()

	t.Run("AllKeys", func(t *testing.T) {
		query := ds9.NewQuery(testKind).KeysOnly().Limit(10)
		resultKeys, err := client.AllKeys(ctx, query)
		if err != nil {
			t.Fatalf("AllKeys failed: %v", err)
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
