package datastore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

// Test manual transaction API (NewTransaction, Get, Put, Delete, Commit, Rollback)
func TestManualTransaction(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("NewTransactionAndCommit", func(t *testing.T) {
		// Begin transaction
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction failed: %v", err)
		}

		// Perform operations
		key := datastore.NameKey("TxTest", "manual1", nil)
		entity := &testEntity{Name: "manual", Count: 1}

		// Put in transaction
		if _, err := tx.Put(key, entity); err != nil {
			t.Fatalf("Transaction Put failed: %v", err)
		}

		// Commit transaction
		if _, err := tx.Commit(); err != nil {
			t.Fatalf("Transaction Commit failed: %v", err)
		}

		// Verify entity was saved
		var retrieved testEntity
		if err := client.Get(ctx, key, &retrieved); err != nil {
			t.Fatalf("Get after commit failed: %v", err)
		}
		if retrieved.Name != "manual" {
			t.Errorf("Expected name 'manual', got '%s'", retrieved.Name)
		}
	})

	t.Run("NewTransactionAndRollback", func(t *testing.T) {
		// Begin transaction
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction failed: %v", err)
		}

		// Perform operations
		key := datastore.NameKey("TxTest", "rollback1", nil)
		entity := &testEntity{Name: "rollback", Count: 1}

		// Put in transaction
		if _, err := tx.Put(key, entity); err != nil {
			t.Fatalf("Transaction Put failed: %v", err)
		}

		// Rollback transaction
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Transaction Rollback failed: %v", err)
		}

		// Verify entity was NOT saved
		var retrieved testEntity
		err = client.Get(ctx, key, &retrieved)
		if !errors.Is(err, datastore.ErrNoSuchEntity) {
			t.Errorf("Expected ErrNoSuchEntity after rollback, got %v", err)
		}
	})

	t.Run("TransactionGet", func(t *testing.T) {
		// Create entity first
		key := datastore.NameKey("TxTest", "get1", nil)
		entity := &testEntity{Name: "existing", Count: 42}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Begin transaction and read
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction failed: %v", err)
		}

		var retrieved testEntity
		if err := tx.Get(key, &retrieved); err != nil {
			t.Fatalf("Transaction Get failed: %v", err)
		}

		if retrieved.Count != 42 {
			t.Errorf("Expected count 42, got %d", retrieved.Count)
		}

		// Rollback since we're just reading
		if err := tx.Rollback(); err != nil {
			t.Logf("Rollback returned: %v", err)
		}
	})

	t.Run("TransactionDelete", func(t *testing.T) {
		// Create entity first
		key := datastore.NameKey("TxTest", "delete1", nil)
		entity := &testEntity{Name: "to-delete", Count: 1}
		if _, err := client.Put(ctx, key, entity); err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Begin transaction and delete
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction failed: %v", err)
		}

		if err := tx.Delete(key); err != nil {
			t.Fatalf("Transaction Delete failed: %v", err)
		}

		// Commit
		if _, err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify entity was deleted
		var retrieved testEntity
		err = client.Get(ctx, key, &retrieved)
		if !errors.Is(err, datastore.ErrNoSuchEntity) {
			t.Errorf("Expected ErrNoSuchEntity after delete, got %v", err)
		}
	})

	t.Run("TransactionPutMulti", func(t *testing.T) {
		// Begin transaction
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction failed: %v", err)
		}

		keys := []*datastore.Key{
			datastore.NameKey("TxTest", "multi1", nil),
			datastore.NameKey("TxTest", "multi2", nil),
		}

		entities := []testEntity{
			{Name: "multi1", Count: 1},
			{Name: "multi2", Count: 2},
		}

		if _, err := tx.PutMulti(keys, entities); err != nil {
			t.Fatalf("Transaction PutMulti failed: %v", err)
		}

		// Commit
		if _, err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify entities were saved
		for i, key := range keys {
			var retrieved testEntity
			if err := client.Get(ctx, key, &retrieved); err != nil {
				t.Errorf("Get for key %d failed: %v", i, err)
			}
		}
	})

	t.Run("TransactionGetMulti", func(t *testing.T) {
		// Create entities first
		keys := []*datastore.Key{
			datastore.NameKey("TxTest", "getmulti1", nil),
			datastore.NameKey("TxTest", "getmulti2", nil),
		}

		entities := []testEntity{
			{Name: "getmulti1", Count: 1},
			{Name: "getmulti2", Count: 2},
		}

		for i, key := range keys {
			if _, err := client.Put(ctx, key, &entities[i]); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Begin transaction and read multiple
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction failed: %v", err)
		}

		retrieved := make([]testEntity, len(keys))
		if err := tx.GetMulti(keys, &retrieved); err != nil {
			t.Fatalf("Transaction GetMulti failed: %v", err)
		}

		for i, entity := range retrieved {
			if entity.Count != int64(i+1) {
				t.Errorf("Entity %d: expected count %d, got %d", i, i+1, entity.Count)
			}
		}

		// Rollback since we're just reading
		if err := tx.Rollback(); err != nil {
			t.Logf("Rollback returned: %v", err)
		}
	})

	t.Run("TransactionDeleteMulti", func(t *testing.T) {
		// Create entities first
		keys := []*datastore.Key{
			datastore.NameKey("TxTest", "delmulti1", nil),
			datastore.NameKey("TxTest", "delmulti2", nil),
		}

		for _, key := range keys {
			entity := &testEntity{Name: "to-delete", Count: 1}
			if _, err := client.Put(ctx, key, entity); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		// Begin transaction and delete multiple
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction failed: %v", err)
		}

		if err := tx.DeleteMulti(keys); err != nil {
			t.Fatalf("Transaction DeleteMulti failed: %v", err)
		}

		// Commit
		if _, err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify entities were deleted
		for _, key := range keys {
			var retrieved testEntity
			err := client.Get(ctx, key, &retrieved)
			if !errors.Is(err, datastore.ErrNoSuchEntity) {
				t.Errorf("Expected ErrNoSuchEntity for key %v, got %v", key, err)
			}
		}
	})

	t.Run("TransactionMutate", func(t *testing.T) {
		// Begin transaction
		tx, err := client.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("NewTransaction failed: %v", err)
		}

		key := datastore.NameKey("TxTest", "mutate1", nil)
		entity := &testEntity{Name: "mutate", Count: 1}

		// Create mutation
		mut := datastore.NewInsert(key, entity)

		if _, err := tx.Mutate(mut); err != nil {
			t.Fatalf("Transaction Mutate failed: %v", err)
		}

		// Commit
		if _, err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify entity was saved
		var retrieved testEntity
		if err := client.Get(ctx, key, &retrieved); err != nil {
			t.Fatalf("Get after mutate failed: %v", err)
		}
		if retrieved.Name != "mutate" {
			t.Errorf("Expected name 'mutate', got '%s'", retrieved.Name)
		}
	})
}
