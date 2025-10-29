package ds9_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/codeGROOVE-dev/ds9"
	"github.com/codeGROOVE-dev/ds9/ds9mock"
)

type txTestEntity struct {
	Time  time.Time
	Name  string
	Count int64
}

func TestTransactionDelete(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()
	key := ds9.NameKey("TxDeleteTest", "test", nil)

	// Create an entity
	entity := &txTestEntity{
		Name:  "test",
		Count: 42,
		Time:  time.Now().UTC().Truncate(time.Microsecond),
	}
	if _, err := client.Put(ctx, key, entity); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Delete in transaction
	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		return tx.Delete(key)
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Verify deletion
	var result txTestEntity
	err = client.Get(ctx, key, &result)
	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("Expected ds9.ErrNoSuchEntity, got %v", err)
	}
}

func TestTransactionDeleteMulti(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create multiple entities
	keys := []*ds9.Key{
		ds9.NameKey("TxDeleteMultiTest", "test1", nil),
		ds9.NameKey("TxDeleteMultiTest", "test2", nil),
		ds9.NameKey("TxDeleteMultiTest", "test3", nil),
	}

	entities := []txTestEntity{
		{Name: "test1", Count: 1, Time: time.Now().UTC().Truncate(time.Microsecond)},
		{Name: "test2", Count: 2, Time: time.Now().UTC().Truncate(time.Microsecond)},
		{Name: "test3", Count: 3, Time: time.Now().UTC().Truncate(time.Microsecond)},
	}

	if _, err := client.PutMulti(ctx, keys, entities); err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	// Delete in transaction
	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		return tx.DeleteMulti(keys)
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}
}

func TestTransactionGetMulti(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Create multiple entities
	keys := []*ds9.Key{
		ds9.NameKey("TxGetMultiTest", "test1", nil),
		ds9.NameKey("TxGetMultiTest", "test2", nil),
	}

	entities := []txTestEntity{
		{Name: "test1", Count: 1, Time: time.Now().UTC().Truncate(time.Microsecond)},
		{Name: "test2", Count: 2, Time: time.Now().UTC().Truncate(time.Microsecond)},
	}

	if _, err := client.PutMulti(ctx, keys, entities); err != nil {
		t.Fatalf("PutMulti failed: %v", err)
	}

	// Get in transaction
	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		results := make([]txTestEntity, 2)
		if err := tx.GetMulti(keys, &results); err != nil {
			return err
		}

		if results[0].Name != "test1" {
			t.Errorf("Expected Name 'test1', got '%s'", results[0].Name)
		}
		if results[1].Name != "test2" {
			t.Errorf("Expected Name 'test2', got '%s'", results[1].Name)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}
}

func TestTransactionPutMulti(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*ds9.Key{
		ds9.NameKey("TxPutMultiTest", "test1", nil),
		ds9.NameKey("TxPutMultiTest", "test2", nil),
	}

	entities := []txTestEntity{
		{Name: "test1", Count: 1, Time: time.Now().UTC().Truncate(time.Microsecond)},
		{Name: "test2", Count: 2, Time: time.Now().UTC().Truncate(time.Microsecond)},
	}

	// Put in transaction
	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		_, err := tx.PutMulti(keys, entities)
		return err
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Verify entities were created
	results := make([]txTestEntity, 2)
	if err := client.GetMulti(ctx, keys, &results); err != nil {
		t.Fatalf("GetMulti failed: %v", err)
	}

	if results[0].Name != "test1" {
		t.Errorf("Expected Name 'test1', got '%s'", results[0].Name)
	}
	if results[1].Name != "test2" {
		t.Errorf("Expected Name 'test2', got '%s'", results[1].Name)
	}
}

func TestTransactionMixedOperations(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	key1 := ds9.NameKey("TxMixedTest", "read", nil)
	key2 := ds9.NameKey("TxMixedTest", "write", nil)
	key3 := ds9.NameKey("TxMixedTest", "delete", nil)

	// Create initial entities
	if _, err := client.Put(ctx, key1, &txTestEntity{Name: "read", Count: 1, Time: time.Now().UTC().Truncate(time.Microsecond)}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if _, err := client.Put(ctx, key3, &txTestEntity{Name: "delete", Count: 3, Time: time.Now().UTC().Truncate(time.Microsecond)}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Transaction with mixed operations
	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		// Read
		var entity txTestEntity
		if err := tx.Get(key1, &entity); err != nil {
			return err
		}

		// Write
		if _, err := tx.Put(key2, &txTestEntity{Name: "write", Count: 2, Time: time.Now().UTC().Truncate(time.Microsecond)}); err != nil {
			return err
		}

		// Delete
		if err := tx.Delete(key3); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Verify write succeeded
	var entity txTestEntity
	if err := client.Get(ctx, key2, &entity); err != nil {
		t.Fatalf("Get after transaction failed: %v", err)
	}
	if entity.Name != "write" {
		t.Errorf("Expected Name 'write', got '%s'", entity.Name)
	}

	// Verify delete succeeded
	err = client.Get(ctx, key3, &entity)
	if !errors.Is(err, ds9.ErrNoSuchEntity) {
		t.Errorf("Expected ds9.ErrNoSuchEntity for deleted entity, got %v", err)
	}
}

func TestNewTransactionCommit(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction failed: %v", err)
	}

	key := ds9.NameKey("TxCommitTest", "test", nil)
	entity := &txTestEntity{
		Name:  "test",
		Count: 42,
		Time:  time.Now().UTC().Truncate(time.Microsecond),
	}

	if _, err := tx.Put(key, entity); err != nil {
		t.Fatalf("tx.Put failed: %v", err)
	}

	commit, err := tx.Commit()
	if err != nil {
		t.Fatalf("tx.Commit failed: %v", err)
	}

	if commit == nil {
		t.Error("Expected non-nil Commit")
	}

	// Verify entity was created
	var result txTestEntity
	if err := client.Get(ctx, key, &result); err != nil {
		t.Fatalf("Get after commit failed: %v", err)
	}

	if result.Name != "test" {
		t.Errorf("Expected Name 'test', got '%s'", result.Name)
	}
}

func TestNewTransactionRollback(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction failed: %v", err)
	}

	key := ds9.NameKey("TxRollbackTest", "test", nil)
	entity := &txTestEntity{
		Name:  "test",
		Count: 42,
		Time:  time.Now().UTC().Truncate(time.Microsecond),
	}

	if _, err := tx.Put(key, entity); err != nil {
		t.Fatalf("tx.Put failed: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("tx.Rollback failed: %v", err)
	}

	// After rollback, transaction should not commit (but we can't verify internal state)
}

func TestTransactionDeleteNilKey(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		return tx.Delete(nil)
	})

	if err == nil {
		t.Error("Expected error for nil key")
	}
}

func TestTransactionGetMultiLengthMismatch(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*ds9.Key{
		ds9.NameKey("Test", "test1", nil),
		ds9.NameKey("Test", "test2", nil),
	}

	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		results := make([]txTestEntity, 1) // Wrong length
		return tx.GetMulti(keys, &results)
	})

	if err == nil {
		t.Error("Expected error for length mismatch")
	}
}

func TestTransactionPutMultiLengthMismatch(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	keys := []*ds9.Key{
		ds9.NameKey("Test", "test1", nil),
	}

	entities := []txTestEntity{
		{Name: "test1", Count: 1, Time: time.Now().UTC()},
		{Name: "test2", Count: 2, Time: time.Now().UTC()},
	}

	_, err := client.RunInTransaction(ctx, func(tx *ds9.Transaction) error {
		_, err := tx.PutMulti(keys, entities)
		return err
	})

	if err == nil {
		t.Error("Expected error for length mismatch")
	}
}

func TestTransactionWithOptions(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	// Test that transaction options are accepted
	_, err := client.NewTransaction(ctx, ds9.MaxAttempts(5))
	if err != nil {
		t.Fatalf("NewTransaction with options failed: %v", err)
	}

	// Test with read time option
	_, err = client.NewTransaction(ctx, ds9.WithReadTime(time.Now().UTC()))
	if err != nil {
		t.Fatalf("NewTransaction with ds9.WithReadTime failed: %v", err)
	}
}

func TestNewTransactionMultipleOperations(t *testing.T) {
	client, cleanup := ds9mock.NewClient(t)
	defer cleanup()

	ctx := context.Background()

	tx, err := client.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("NewTransaction failed: %v", err)
	}

	// Multiple puts
	for i := range 3 {
		key := ds9.IDKey("TxMultiOp", int64(i+1), nil)
		entity := &txTestEntity{
			Name:  "test",
			Count: int64(i),
			Time:  time.Now().UTC().Truncate(time.Microsecond),
		}
		if _, err := tx.Put(key, entity); err != nil {
			t.Fatalf("tx.Put failed: %v", err)
		}
	}

	// Commit the transaction
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit failed: %v", err)
	}
}
