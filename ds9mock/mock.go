// Package ds9mock provides an in-memory mock Datastore server for testing.
//
// This package can be used by both ds9 internal tests and by end-users who want
// to test their code that depends on ds9 without hitting real Datastore APIs.
//
// Example usage:
//
//	func TestMyCode(t *testing.T) {
//	    client, cleanup := ds9mock.NewClient(t)
//	    defer cleanup()
//
//	    // Use client in your tests
//	    key := ds9.NameKey("Task", "task-1", nil)
//	    _, err := client.Put(ctx, key, &myTask)
//	}
package ds9mock

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/codeGROOVE-dev/ds9"
)

const metadataFlavor = "Google"

// Store holds the in-memory entity storage.
type Store struct {
	entities map[string]map[string]any
}

// NewStore creates a new in-memory store.
func NewStore() *Store {
	return &Store{
		entities: make(map[string]map[string]any),
	}
}

// NewClient creates a ds9 client connected to mock servers with in-memory storage.
// Returns the client and a cleanup function that should be deferred.
func NewClient(t *testing.T) (client *ds9.Client, cleanup func()) {
	t.Helper()

	store := NewStore()

	// Mock metadata server
	metadataServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != metadataFlavor {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		if r.URL.Path == "/project/project-id" {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("test-project")); err != nil {
				log.Printf("failed to write response: %v", err)
			}
			return
		}

		if r.URL.Path == "/instance/service-accounts/default/token" {
			w.WriteHeader(http.StatusOK)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"access_token": "test-token",
				"expires_in":   3600,
			}); err != nil {
				log.Printf("failed to encode token response: %v", err)
			}
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))

	// Mock API server
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify authorization header
		if r.Header.Get("Authorization") != "Bearer test-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Route based on path
		if r.URL.Path == "/projects/test-project:lookup" {
			store.handleLookup(w, r)
			return
		}

		if r.URL.Path == "/projects/test-project:commit" {
			store.handleCommit(w, r)
			return
		}

		if r.URL.Path == "/projects/test-project:runQuery" {
			store.handleRunQuery(w, r)
			return
		}

		if r.URL.Path == "/projects/test-project:beginTransaction" {
			handleBeginTransaction(w, r)
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))

	// Set test URLs in ds9
	restore := ds9.SetTestURLs(metadataServer.URL, apiServer.URL)

	// Create client
	ctx := context.Background()
	var err error
	client, err = ds9.NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("failed to create mock client: %v", err)
	}

	cleanup = func() {
		restore()
		metadataServer.Close()
		apiServer.Close()
	}

	return client, cleanup
}

// handleLookup handles lookup (get) requests.
func (s *Store) handleLookup(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DatabaseID string           `json:"databaseId"`
		Keys       []map[string]any `json:"keys"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Validate routing header for named databases
	if req.DatabaseID != "" {
		routingHeader := r.Header.Get("X-Goog-Request-Params")
		if routingHeader == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"code":    400,
					"message": "Missing routing header for named database",
					"status":  "INVALID_ARGUMENT",
				},
			}); err != nil {
				log.Printf("failed to encode error response: %v", err)
			}
			return
		}
	}

	// Process all keys
	var found []map[string]any
	var missing []map[string]any

	for _, keyData := range req.Keys {
		path, ok := keyData["path"].([]any)
		if !ok {
			continue
		}
		if len(path) == 0 {
			continue
		}
		pathElem, ok := path[0].(map[string]any)
		if !ok {
			continue
		}
		kind, ok := pathElem["kind"].(string)
		if !ok {
			continue
		}

		// Handle both name and ID keys
		var keyStr string
		if name, ok := pathElem["name"].(string); ok {
			keyStr = kind + "/" + name
		} else if id, ok := pathElem["id"].(string); ok {
			keyStr = kind + "/" + id
		} else {
			continue
		}

		if entity, exists := s.entities[keyStr]; exists {
			found = append(found, map[string]any{
				"entity": entity,
			})
		} else {
			missing = append(missing, map[string]any{
				"entity": keyData,
			})
		}
	}

	// Return results
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"found":   found,
		"missing": missing,
	}); err != nil {
		log.Printf("failed to encode lookup response: %v", err)
	}
}

// handleCommit handles commit (put/delete) requests.
func (s *Store) handleCommit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mode       string           `json:"mode"`
		DatabaseID string           `json:"databaseId"`
		Mutations  []map[string]any `json:"mutations"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Validate routing header for named databases
	if req.DatabaseID != "" {
		routingHeader := r.Header.Get("X-Goog-Request-Params")
		if routingHeader == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"code":    400,
					"message": "Missing routing header for named database",
					"status":  "INVALID_ARGUMENT",
				},
			}); err != nil {
				log.Printf("failed to encode error response: %v", err)
			}
			return
		}
	}

	for _, mutation := range req.Mutations {
		// Handle upsert
		if upsert, ok := mutation["upsert"].(map[string]any); ok {
			keyData, ok := upsert["key"].(map[string]any)
			if !ok {
				continue
			}
			path, ok := keyData["path"].([]any)
			if !ok || len(path) == 0 {
				continue
			}
			pathElem, ok := path[0].(map[string]any)
			if !ok {
				continue
			}
			kind, ok := pathElem["kind"].(string)
			if !ok {
				continue
			}

			// Handle both name and ID keys
			var keyStr string
			if name, ok := pathElem["name"].(string); ok {
				keyStr = kind + "/" + name
			} else if id, ok := pathElem["id"].(string); ok {
				keyStr = kind + "/" + id
			} else {
				continue
			}

			s.entities[keyStr] = upsert
		}

		// Handle delete
		if deleteKey, ok := mutation["delete"].(map[string]any); ok {
			path, ok := deleteKey["path"].([]any)
			if !ok || len(path) == 0 {
				continue
			}
			pathElem, ok := path[0].(map[string]any)
			if !ok {
				continue
			}
			kind, ok := pathElem["kind"].(string)
			if !ok {
				continue
			}

			// Handle both name and ID keys
			var keyStr string
			if name, ok := pathElem["name"].(string); ok {
				keyStr = kind + "/" + name
			} else if id, ok := pathElem["id"].(string); ok {
				keyStr = kind + "/" + id
			} else {
				continue
			}

			delete(s.entities, keyStr)
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"mutationResults": []any{},
	}); err != nil {
		log.Printf("failed to encode commit response: %v", err)
	}
}

// handleRunQuery handles query requests.
func (s *Store) handleRunQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query      map[string]any `json:"query"`
		DatabaseID string         `json:"databaseId"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Validate routing header for named databases
	if req.DatabaseID != "" {
		routingHeader := r.Header.Get("X-Goog-Request-Params")
		if routingHeader == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"code":    400,
					"message": "Missing routing header for named database",
					"status":  "INVALID_ARGUMENT",
				},
			}); err != nil {
				log.Printf("failed to encode error response: %v", err)
			}
			return
		}
	}

	query := req.Query
	kinds, ok := query["kind"].([]any)
	if !ok || len(kinds) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	kindMap, ok := kinds[0].(map[string]any)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	kind, ok := kindMap["name"].(string)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var limit int
	if l, ok := query["limit"].(float64); ok {
		limit = int(l)
	}

	// Find all entities of this kind
	var results []any
	for _, entity := range s.entities {
		keyData, ok := entity["key"].(map[string]any)
		if !ok {
			continue
		}
		path, ok := keyData["path"].([]any)
		if !ok || len(path) == 0 {
			continue
		}
		pathElem, ok := path[0].(map[string]any)
		if !ok {
			continue
		}
		entityKind, ok := pathElem["kind"].(string)
		if !ok {
			continue
		}

		if entityKind == kind {
			results = append(results, map[string]any{
				"entity": entity,
			})

			if limit > 0 && len(results) >= limit {
				break
			}
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"batch": map[string]any{
			"entityResults": results,
		},
	}); err != nil {
		log.Printf("failed to encode query response: %v", err)
	}
}

// handleBeginTransaction handles transaction begin requests.
func handleBeginTransaction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DatabaseID string `json:"databaseId"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Validate routing header for named databases
	if req.DatabaseID != "" {
		routingHeader := r.Header.Get("X-Goog-Request-Params")
		if routingHeader == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"code":    400,
					"message": "Missing routing header for named database",
					"status":  "INVALID_ARGUMENT",
				},
			}); err != nil {
				log.Printf("failed to encode error response: %v", err)
			}
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"transaction": "test-transaction-id",
	}); err != nil {
		log.Printf("failed to encode transaction response: %v", err)
	}
}
