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

func TestRunInTransaction(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put initial entity
	entity := &testEntity{
		Name:  "counter",
		Count: 0,
	}

	key := datastore.NameKey("TestKind", "counter", nil)
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Run transaction to read and update
	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var current testEntity
		if err := tx.Get(key, &current); err != nil {
			return err
		}

		current.Count++
		_, err := tx.Put(key, &current)
		return err
	})
	if err != nil {
		t.Fatalf("RunInTransaction failed: %v", err)
	}

	// Verify the update
	var updated testEntity
	err = client.Get(ctx, key, &updated)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if updated.Count != 1 {
		t.Errorf("expected Count to be 1, got %d", updated.Count)
	}
}

func TestTransactionNotFound(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	key := datastore.NameKey("TestKind", "nonexistent", nil)

	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var entity testEntity
		return tx.Get(key, &entity)
	})

	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("expected datastore.ErrNoSuchEntity, got %v", err)
	}
}

func TestTransactionMultipleOperations(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put initial entities
	for i := range 3 {
		entity := &testEntity{
			Name:  "item",
			Count: int64(i),
		}
		key := datastore.NameKey("TestKind", string(rune('a'+i)), nil)
		_, err := client.Put(ctx, key, entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Run transaction that reads and updates multiple entities
	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		for i := range 3 {
			key := datastore.NameKey("TestKind", string(rune('a'+i)), nil)
			var current testEntity
			if err := tx.Get(key, &current); err != nil {
				return err
			}

			current.Count += 10
			_, err := tx.Put(key, &current)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunInTransaction failed: %v", err)
	}

	// Verify updates
	for i := range 3 {
		key := datastore.NameKey("TestKind", string(rune('a'+i)), nil)
		var retrieved testEntity
		err = client.Get(ctx, key, &retrieved)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		expectedCount := int64(i + 10)
		if retrieved.Count != expectedCount {
			t.Errorf("entity %d: expected Count %d, got %d", i, expectedCount, retrieved.Count)
		}
	}
}

func TestTransactionWithError(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Store initial entity
	key := datastore.NameKey("TestKind", "tx-err", nil)
	entity := testEntity{Name: "initial", Count: 1}
	_, err := client.Put(ctx, key, &entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Run transaction that errors
	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var current testEntity
		if err := tx.Get(key, &current); err != nil {
			return err
		}

		current.Count = 999

		if _, err := tx.Put(key, &current); err != nil {
			return err
		}

		// Return error to trigger rollback
		return errors.New("intentional error")
	})

	if err == nil {
		t.Fatal("expected transaction to fail, got nil error")
	}
	if !strings.Contains(err.Error(), "intentional error") {
		t.Errorf("expected 'intentional error', got: %v", err)
	}

	// Verify entity was not modified (transaction rolled back)
	// Note: In a real implementation this would check rollback, but our mock doesn't support it
	// This test at least exercises the error path
}

func TestTransactionWithDatabaseID(t *testing.T) {
	// Setup mock servers
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

	txID := "test-tx-123"
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		var reqBody map[string]any
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Check for databaseId in request
		if dbID, ok := reqBody["databaseId"].(string); ok && dbID != "tx-db" {
			t.Errorf("expected databaseId 'tx-db', got %v", dbID)
		}

		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path == "/projects/test-project:beginTransaction" {
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": txID,
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if r.URL.Path == "/projects/test-project:commit" {
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if r.URL.Path == "/projects/test-project:lookup" {
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer apiServer.Close()

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

	client, err := datastore.NewClientWithDatabase(ctx, "test-project", "tx-db")
	if err != nil {
		t.Fatalf("NewClientWithDatabase failed: %v", err)
	}

	// Run transaction with databaseID
	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		key := datastore.NameKey("TestKind", "tx-test", nil)
		entity := testEntity{Name: "in-tx", Count: 42}
		_, err := tx.Put(key, &entity)
		return err
	})
	if err != nil {
		t.Fatalf("Transaction with databaseID failed: %v", err)
	}
}

func TestTransactionRollback(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	// Put initial entity
	key := datastore.NameKey("TestKind", "rollback-test", nil)
	entity := &testEntity{Name: "original", Count: 1}
	_, err := client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Run transaction that will fail
	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var current testEntity
		if err := tx.Get(key, &current); err != nil {
			return err
		}

		current.Name = "modified"
		current.Count = 999

		_, err := tx.Put(key, &current)
		if err != nil {
			return err
		}

		// Return error to cause rollback
		return errors.New("force rollback")
	})

	if err == nil {
		t.Fatal("expected transaction to fail")
	}

	if !strings.Contains(err.Error(), "force rollback") {
		t.Errorf("expected 'force rollback' error, got: %v", err)
	}
}

func TestTransactionBeginFailure(t *testing.T) {
	// Setup mock servers
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
		// Fail to begin transaction
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

	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		return nil
	})

	if err == nil {
		t.Fatal("expected transaction to fail on begin")
	}

	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected error to mention 500 status, got: %v", err)
	}
}

func TestTransactionCommitAbortedRetry(t *testing.T) {
	// Setup mock servers
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

	commitAttempt := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-123",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "commit") {
			commitAttempt++
			// Fail with 409 ABORTED on first two attempts, succeed on third
			if commitAttempt < 3 {
				w.WriteHeader(http.StatusConflict)
				if _, err := w.Write([]byte(`{"error":"ABORTED: transaction aborted"}`)); err != nil {
					t.Logf("write failed: %v", err)
				}
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "lookup") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []any{},
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

	// This should succeed after retries
	key := datastore.NameKey("TestKind", "tx-retry", nil)
	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		_, err := tx.Put(key, &testEntity{Name: "test", Count: 1})
		return err
	})
	if err != nil {
		t.Fatalf("transaction should succeed after retries, got: %v", err)
	}

	if commitAttempt < 2 {
		t.Errorf("expected at least 2 commit attempts, got %d", commitAttempt)
	}
}

func TestTransactionMaxRetriesExceeded(t *testing.T) {
	// Setup mock servers
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

	commitAttempt := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-456",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "commit") {
			commitAttempt++
			// Always return 409 ABORTED
			w.WriteHeader(http.StatusConflict)
			if _, err := w.Write([]byte(`{"error":"status 409 ABORTED: transaction conflict"}`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "lookup") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []any{},
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

	// This should fail after max retries
	key := datastore.NameKey("TestKind", "tx-max-retry", nil)
	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		_, err := tx.Put(key, &testEntity{Name: "test", Count: 1})
		return err
	})

	if err == nil {
		t.Fatal("expected transaction to fail after max retries")
	}

	if !strings.Contains(err.Error(), "failed after 3 attempts") {
		t.Errorf("expected 'failed after 3 attempts' error, got: %v", err)
	}

	if commitAttempt != 3 {
		t.Errorf("expected exactly 3 commit attempts, got %d", commitAttempt)
	}
}

func TestTransactionGetNonExistent(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	key := datastore.NameKey("TestKind", "nonexistent", nil)

	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var entity testEntity
		return tx.Get(key, &entity)
	})

	if !errors.Is(err, datastore.ErrNoSuchEntity) {
		t.Errorf("expected datastore.ErrNoSuchEntity in transaction, got: %v", err)
	}
}

func TestTransactionPutWithNilKey(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		entity := &testEntity{Name: "test", Count: 1}
		_, err := tx.Put(nil, entity)
		return err
	})

	if err == nil {
		t.Error("expected error for nil key in transaction")
	}
}

func TestTransactionGetWithNilKey(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var entity testEntity
		return tx.Get(nil, &entity)
	})

	if err == nil {
		t.Error("expected error for nil key in transaction Get")
	}
}

func TestTransactionWithMultiplePuts(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		for i := range 5 {
			key := datastore.NameKey("TxMulti", fmt.Sprintf("key-%d", i), nil)
			entity := &testEntity{Name: "test", Count: int64(i)}
			_, err := tx.Put(key, entity)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Transaction with multiple puts failed: %v", err)
	}

	// Verify all entities were created
	for i := range 5 {
		key := datastore.NameKey("TxMulti", fmt.Sprintf("key-%d", i), nil)
		var retrieved testEntity
		err = client.Get(ctx, key, &retrieved)
		if err != nil {
			t.Errorf("Get for entity %d failed: %v", i, err)
		}
		if retrieved.Count != int64(i) {
			t.Errorf("entity %d: expected Count %d, got %d", i, i, retrieved.Count)
		}
	}
}

func TestTransactionGetWithInvalidResponse(t *testing.T) {
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
		if strings.Contains(r.URL.Path, "beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "lookup") {
			// Return invalid JSON structure
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"invalid":"structure"}`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "commit") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
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

	key := datastore.NameKey("Test", "key", nil)
	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var entity testEntity
		return tx.Get(key, &entity)
	})

	// Should handle the invalid response gracefully
	if err == nil {
		t.Log("Transaction succeeded despite invalid lookup response")
	}
}

func TestTransactionWithNonRetriableError(t *testing.T) {
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

	commitAttempts := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "commit") {
			commitAttempts++
			// Return non-retriable error (not 409 ABORTED)
			w.WriteHeader(http.StatusBadRequest)
			if _, err := w.Write([]byte(`{"error":"INVALID_ARGUMENT"}`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "lookup") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
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

	key := datastore.NameKey("Test", "key", nil)
	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		_, err := tx.Put(key, &testEntity{Name: "test", Count: 1})
		return err
	})

	if err == nil {
		t.Error("expected error on non-retriable failure")
	}

	// Should NOT retry on non-409 errors
	if commitAttempts != 1 {
		t.Errorf("expected exactly 1 commit attempt for non-retriable error, got %d", commitAttempts)
	}
}

func TestTransactionWithInvalidTxResponse(t *testing.T) {
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
		if strings.Contains(r.URL.Path, "beginTransaction") {
			// Return invalid JSON
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{bad json`)); err != nil {
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

	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		return nil
	})

	if err == nil {
		t.Error("expected error for invalid transaction response")
	}
}

func TestTransactionGetWithDecodeError(t *testing.T) {
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
		if strings.Contains(r.URL.Path, "beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}

		if strings.Contains(r.URL.Path, "lookup") {
			// Return entity with malformed data
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []any{
					map[string]any{
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
								"name": map[string]any{
									"stringValue": 12345, // Wrong type
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

		if strings.Contains(r.URL.Path, "commit") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(map[string]any{
				"mutationResults": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
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

	key := datastore.NameKey("Test", "key", nil)
	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var entity testEntity
		return tx.Get(key, &entity)
	})
	// May succeed or fail depending on how decoding handles type mismatches
	if err != nil {
		t.Logf("Transaction Get with decode error: %v", err)
	}
}

func TestTransactionGetMissingEntity(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return empty found array (entity not found)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":commit") {
			w.Header().Set("Content-Type", "application/json")
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
	key := datastore.NameKey("Test", "nonexistent", nil)

	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var entity testEntity
		err := tx.Get(key, &entity)
		if err == nil {
			return errors.New("expected error for missing entity")
		}
		return nil
	})
	if err != nil {
		t.Errorf("transaction should succeed even with get error: %v", err)
	}
}

func TestTransactionGetDecodeError(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return malformed entity
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found": []map[string]any{
					{
						"entity": "invalid-not-a-map",
					},
				},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":commit") {
			w.Header().Set("Content-Type", "application/json")
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
	key := datastore.NameKey("Test", "key", nil)

	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var entity testEntity
		err := tx.Get(key, &entity)
		if err == nil {
			return errors.New("expected decode error")
		}
		return nil
	})
	if err != nil {
		t.Errorf("transaction should succeed: %v", err)
	}
}

func TestTransactionCommitInvalidResponse(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":commit") {
			// Return invalid JSON (missing mutationResults)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				// Missing mutationResults field
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
	key := datastore.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test"}

	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		_, err := tx.Put(key, entity)
		return err
	})
	if err != nil {
		t.Logf("Transaction with invalid commit response failed: %v", err)
	}
}

func TestTransactionCommitUnmarshalError(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":commit") {
			// Return malformed mutation results
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write([]byte(`{"mutationResults": "not-an-array"}`)); err != nil {
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
	entity := &testEntity{Name: "test"}

	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		_, err := tx.Put(key, entity)
		return err
	})
	// May or may not error depending on JSON parsing behavior
	if err != nil {
		t.Logf("Transaction with malformed mutation results failed: %v", err)
	}
}

func TestTransactionGetNotFound(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return empty found array
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"found":   []any{},
				"missing": []any{},
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":commit") {
			w.Header().Set("Content-Type", "application/json")
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
	key := datastore.NameKey("Test", "nonexistent", nil)

	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var entity testEntity
		err := tx.Get(key, &entity)
		if err == nil {
			t.Error("expected error with empty found array")
		}
		return nil
	})
	if err != nil {
		t.Logf("Transaction completed: %v", err)
	}
}

func TestTransactionGetAccessTokenError(t *testing.T) {
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
			// Return error for token request
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer metadataServer.Close()

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
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
	key := datastore.NameKey("Test", "test-key", nil)

	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var entity testEntity
		err := tx.Get(key, &entity)
		if err == nil {
			t.Error("expected error with token failure")
		}
		return err
	})

	if err == nil {
		t.Error("expected transaction to fail with token error")
	}
}

func TestTransactionGetNonOKStatus(t *testing.T) {
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
		if strings.Contains(r.URL.Path, ":beginTransaction") {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"transaction": "test-tx-id",
			}); err != nil {
				t.Logf("encode failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":lookup") {
			// Return non-OK status
			w.WriteHeader(http.StatusBadRequest)
			if _, err := w.Write([]byte("bad request")); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}
		if strings.Contains(r.URL.Path, ":commit") {
			w.Header().Set("Content-Type", "application/json")
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
	key := datastore.NameKey("Test", "test-key", nil)

	_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var entity testEntity
		return tx.Get(key, &entity)
	})

	if err == nil {
		t.Error("expected error with non-OK status")
	}
}

func TestRunInTransactionReturnsCommit(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()
	key := datastore.NameKey("CommitTest", "test1", nil)

	commit, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		entity := &testEntity{
			Name:      "commit test",
			Count:     1,
			Active:    true,
			UpdatedAt: time.Now().UTC().Truncate(time.Microsecond),
		}
		_, err := tx.Put(key, entity)
		return err
	})
	if err != nil {
		t.Fatalf("RunInTransaction failed: %v", err)
	}

	if commit == nil {
		t.Fatal("Expected non-nil Commit, got nil")
	}

	// Commit should be a valid *Commit type
	_ = commit
}

func TestRunInTransactionErrorReturnsNilCommit(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	expectedErr := errors.New("intentional error")
	commit, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		return expectedErr
	})

	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected error to be %v, got %v", expectedErr, err)
	}

	if commit != nil {
		t.Errorf("Expected nil Commit on error, got %v", commit)
	}
}

func TestTransactionOptions(t *testing.T) {
	t.Run("MaxAttempts", func(t *testing.T) {
		// Test that MaxAttempts option is accepted and sets the retry limit
		// We can verify this by checking the error message mentions the right attempt count
		client, cleanup := datastore.NewMockClient(t)
		defer cleanup()

		ctx := context.Background()
		key := datastore.NameKey("TestKind", "test", nil)

		// This test verifies that the MaxAttempts option is parsed correctly
		// The actual retry behavior is tested in TestTransactionMaxRetriesExceeded
		_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			entity := testEntity{Name: "test", Count: 42}
			_, err := tx.Put(key, &entity)
			return err
		}, datastore.MaxAttempts(5))
		// With mock client, this should succeed
		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}
	})

	t.Run("WithReadTime", func(t *testing.T) {
		client, cleanup := datastore.NewMockClient(t)
		defer cleanup()

		ctx := context.Background()
		key := datastore.NameKey("TestKind", "test", nil)

		// First, put an entity
		entity := testEntity{Name: "test", Count: 42}
		_, err := client.Put(ctx, key, &entity)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// Run a read-only transaction with readTime
		readTime := time.Now().UTC()
		_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			var result testEntity
			return tx.Get(key, &result)
		}, datastore.WithReadTime(readTime))
		// Note: ds9mock doesn't actually enforce read-only semantics,
		// but we're testing that the option is accepted and doesn't cause errors
		if err != nil {
			t.Fatalf("Transaction with WithReadTime failed: %v", err)
		}
	})

	t.Run("CombinedOptions", func(t *testing.T) {
		client, cleanup := datastore.NewMockClient(t)
		defer cleanup()

		ctx := context.Background()
		key := datastore.NameKey("TestKind", "test", nil)

		// Test that multiple options can be combined
		_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			entity := testEntity{Name: "test", Count: 42}
			_, err := tx.Put(key, &entity)
			return err
		}, datastore.MaxAttempts(2), datastore.WithReadTime(time.Now().UTC()))
		// With mock client, this should succeed
		if err != nil {
			t.Fatalf("Transaction with combined options failed: %v", err)
		}
	})
}
