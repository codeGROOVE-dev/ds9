package datastore_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/codeGROOVE-dev/ds9/auth"
	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

func TestNewClient(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	// Just verify we got a valid client
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewClientWithDatabase(t *testing.T) {
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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	authConfig := &auth.Config{
		MetadataURL: metadataServer.URL,
		SkipADC:     true,
	}
	// Test with explicit databaseID
	client, err := datastore.NewClientWithDatabase(
		context.Background(),
		"test-project",
		"custom-db",
		datastore.WithEndpoint(apiServer.URL),
		datastore.WithAuth(authConfig),
	)
	if err != nil {
		t.Fatalf("NewClientWithDatabase failed: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewClientWithDatabaseEmptyProjectID(t *testing.T) {
	// Setup mock servers
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/project/project-id" {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("auto-detected-project")); err != nil {
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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	authConfig := &auth.Config{
		MetadataURL: metadataServer.URL,
		SkipADC:     true,
	}
	// Test with empty projectID - should fetch from metadata
	client, err := datastore.NewClientWithDatabase(
		context.Background(),
		"",
		"my-db",
		datastore.WithEndpoint(apiServer.URL),
		datastore.WithAuth(authConfig),
	)
	if err != nil {
		t.Fatalf("NewClientWithDatabase with empty projectID failed: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewClientWithDatabaseProjectIDFetchFailure(t *testing.T) {
	// Setup mock servers that fail to provide projectID
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/project/project-id" {
			// Return error instead of project ID
			w.WriteHeader(http.StatusInternalServerError)
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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]any{}); err != nil {
			t.Logf("encode failed: %v", err)
		}
	}))
	defer apiServer.Close()

	authConfig := &auth.Config{
		MetadataURL: metadataServer.URL,
		SkipADC:     true,
	}
	// Test with empty projectID and failing metadata server
	client, err := datastore.NewClientWithDatabase(
		context.Background(),
		"",
		"my-db",
		datastore.WithEndpoint(apiServer.URL),
		datastore.WithAuth(authConfig),
	)
	if err == nil {
		t.Fatal("expected error when projectID fetch fails, got nil")
	}
	if client != nil {
		t.Errorf("expected nil client on error, got %v", client)
	}
	if !strings.Contains(err.Error(), "project ID required") {
		t.Errorf("expected 'project ID required' error, got: %v", err)
	}
}

func TestClose(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	err := client.Close()
	if err != nil {
		t.Errorf("Close() returned unexpected error: %v", err)
	}

	// Should be idempotent - can call multiple times
	err = client.Close()
	if err != nil {
		t.Errorf("Second Close() returned unexpected error: %v", err)
	}
}
