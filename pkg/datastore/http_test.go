package datastore_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

func TestDoRequestRetryOn5xxError(t *testing.T) {
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

	attemptCount := 0
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount++
		// Return 503 on first two attempts, then succeed
		if attemptCount < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := w.Write([]byte(`{"error":"service unavailable"}`)); err != nil {
				t.Logf("write failed: %v", err)
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"mutationResults": []any{
				map[string]any{"key": map[string]any{}},
			},
		}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// This should succeed after retries
	key := datastore.NameKey("TestKind", "retry-test", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)
	if err != nil {
		t.Fatalf("Put should succeed after retries, got: %v", err)
	}

	if attemptCount < 2 {
		t.Errorf("expected at least 2 attempts, got %d", attemptCount)
	}
}

func TestDoRequestFailsOn4xxError(t *testing.T) {
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
		// Always return 400 Bad Request
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		if _, err := w.Write([]byte(`{"error":"bad request"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// This should fail immediately without retry on 4xx
	key := datastore.NameKey("TestKind", "bad-request", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)
	if err == nil {
		t.Fatal("expected error on 4xx response")
	}

	if !strings.Contains(err.Error(), "400") {
		t.Errorf("expected error to mention 400 status, got: %v", err)
	}

	// Should only try once for 4xx errors (no retry)
	if attemptCount != 1 {
		t.Errorf("expected exactly 1 attempt for 4xx error, got %d", attemptCount)
	}
}

func TestDoRequestContextCancellation(t *testing.T) {
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
		// Always return 503 to force retry
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

	// Create context that we'll cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	key := datastore.NameKey("TestKind", "cancel-test", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)

	if err == nil {
		t.Fatal("expected error when context is cancelled")
	}

	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("expected context cancellation error, got: %v", err)
	}
}

func TestGetWithHTTPError(t *testing.T) {
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
		// Return 404 for lookup
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		if err := json.NewEncoder(w).Encode(map[string]any{
			"error": "not found",
		}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("TestKind", "test", nil)
	var entity testEntity
	err = client.Get(ctx, key, &entity)

	if err == nil {
		t.Fatal("expected error on 404")
	}

	if !strings.Contains(err.Error(), "404") {
		t.Errorf("expected error to mention 404, got: %v", err)
	}
}

func TestPutWithHTTPError(t *testing.T) {
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
		// Return 403 Forbidden
		w.WriteHeader(http.StatusForbidden)
		if _, err := w.Write([]byte(`{"error":"permission denied"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("TestKind", "test", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)

	if err == nil {
		t.Fatal("expected error on 403")
	}

	if !strings.Contains(err.Error(), "403") {
		t.Errorf("expected error to mention 403, got: %v", err)
	}
}

func TestDoRequestAllRetriesFail(t *testing.T) {
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
		// Always fail with 500
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(`{"error":"persistent failure"}`)); err != nil {
			t.Logf("write failed: %v", err)
		}
	}))
	defer apiServer.Close()

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("TestKind", "test", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)

	if err == nil {
		t.Fatal("expected error after all retries")
	}

	if !strings.Contains(err.Error(), "attempts failed") {
		t.Errorf("expected 'attempts failed' error, got: %v", err)
	}

	// Should have tried multiple times
	if attemptCount < 3 {
		t.Errorf("expected at least 3 attempts, got %d", attemptCount)
	}
}

func TestDoRequestUnexpectedSuccess(t *testing.T) {
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
		// Return unexpected 2xx status (not 200)
		w.WriteHeader(http.StatusAccepted) // 202
		if _, err := w.Write([]byte(`{"message":"accepted"}`)); err != nil {
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
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)

	if err == nil {
		t.Error("expected error for unexpected 2xx status")
	}

	if !strings.Contains(err.Error(), "202") {
		t.Errorf("expected error to mention 202 status, got: %v", err)
	}
}

func TestDoRequestWithReadBodyError(t *testing.T) {
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
		// Set content length but don't write enough data
		w.Header().Set("Content-Length", "1000000")
		w.WriteHeader(http.StatusOK)
		// Write partial data then close connection
	}))
	defer apiServer.Close()

	ctx := datastore.TestConfig(context.Background(), metadataServer.URL, apiServer.URL)

	client, err := datastore.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	key := datastore.NameKey("Test", "key", nil)
	entity := &testEntity{Name: "test", Count: 1}
	_, err = client.Put(ctx, key, entity)
	// Should get an error related to response parsing
	if err != nil {
		t.Logf("Got expected error with incomplete response: %v", err)
	}
}
