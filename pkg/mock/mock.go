// Package mock provides an in-memory mock Datastore server for testing.
//
// This package can be used by both ds9 internal tests and by end-users who want
// to test their code that depends on ds9 without hitting real Datastore APIs.
//
// Example usage:
//
//	import "github.com/codeGROOVE-dev/ds9/pkg/datastore"
//
//	func TestMyCode(t *testing.T) {
//	    client, cleanup := datastore.NewMockClient(t)
//	    defer cleanup()
//
//	    // Use client in your tests
//	    key := datastore.NameKey("Task", "task-1", nil)
//	    _, err := client.Put(ctx, key, &myTask)
//	}
package mock

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
)

const metadataFlavor = "Google"

// Store holds the in-memory entity storage.
//
//nolint:govet // Field alignment not optimized to maintain readability
type Store struct {
	mu       sync.RWMutex
	entities map[string]map[string]any
	nextID   int64 // Counter for allocating unique IDs
}

// NewStore creates a new in-memory store.
func NewStore() *Store {
	return &Store{
		entities: make(map[string]map[string]any),
		nextID:   1000, // Start IDs at 1000
	}
}

// NewMockServers creates mock metadata and API servers for testing.
// Returns the metadata URL, API URL, and a cleanup function.
// This function doesn't import datastore to avoid import cycles.
func NewMockServers(t *testing.T) (metadataURL, apiURL string, cleanup func()) {
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

		if r.URL.Path == "/projects/test-project:allocateIds" {
			store.handleAllocateIDs(w, r)
			return
		}

		if r.URL.Path == "/projects/test-project:runAggregationQuery" {
			store.handleRunAggregationQuery(w, r)
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))

	cleanup = func() {
		metadataServer.Close()
		apiServer.Close()
	}

	return metadataServer.URL, apiServer.URL, cleanup
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

	s.mu.RLock()
	defer s.mu.RUnlock()

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
//
//nolint:gocognit,maintidx // Complex logic required for handling multiple mutation types
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

	s.mu.Lock()
	defer s.mu.Unlock()

	var mutationResults []map[string]any

	for _, mutation := range req.Mutations {
		var resultKey map[string]any

		// Handle insert
		if insert, ok := mutation["insert"].(map[string]any); ok {
			keyData, ok := insert["key"].(map[string]any)
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

			s.entities[keyStr] = insert
			resultKey = keyData
		}

		// Handle update
		if update, ok := mutation["update"].(map[string]any); ok {
			keyData, ok := update["key"].(map[string]any)
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

			s.entities[keyStr] = update
			resultKey = keyData
		}

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
			resultKey = keyData
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
			resultKey = deleteKey
		}

		// Add mutation result
		if resultKey != nil {
			mutationResults = append(mutationResults, map[string]any{
				"key": resultKey,
			})
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"mutationResults": mutationResults,
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

	// Check for startCursor - if present, we've already returned results
	// For simplicity in the mock, return empty results when cursor is used
	var startCursor string
	if sc, ok := query["startCursor"].(string); ok {
		startCursor = sc
	}

	// Find all entities of this kind
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []any

	// If there's a start cursor, we simulate pagination by returning no more results
	// This is a simplified mock behavior - a real implementation would track position
	if startCursor != "" {
		// Return empty results to indicate end of pagination
		response := map[string]any{
			"batch": map[string]any{
				"entityResults": []any{},
				"moreResults":   "NO_MORE_RESULTS",
			},
		}
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Printf("failed to encode query response: %v", err)
		}
		return
	}

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
			// Apply filters if present
			if filterMap, hasFilter := query["filter"].(map[string]any); hasFilter {
				if !matchesFilter(entity, filterMap) {
					continue
				}
			}

			results = append(results, map[string]any{
				"entity": entity,
			})

			if limit > 0 && len(results) >= limit {
				break
			}
		}
	}

	// Add cursor if there are more results (for pagination testing)
	var endCursor string
	if limit > 0 && len(results) == limit {
		// Generate a simple cursor to indicate more results might exist
		endCursor = fmt.Sprintf("cursor-after-%d", limit)
	}

	// Build response
	batch := map[string]any{
		"entityResults": results,
	}

	// Add cursor if available
	if endCursor != "" {
		batch["endCursor"] = endCursor
		batch["moreResults"] = "MORE_RESULTS_AFTER_LIMIT"
	} else {
		batch["moreResults"] = "NO_MORE_RESULTS"
	}

	response := map[string]any{
		"batch": batch,
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
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

// handleAllocateIDs handles :allocateIds requests.
func (s *Store) handleAllocateIDs(w http.ResponseWriter, r *http.Request) {
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

	// Allocate unique IDs for incomplete keys
	s.mu.Lock()
	defer s.mu.Unlock()

	allocatedKeys := make([]map[string]any, 0, len(req.Keys))
	for _, keyData := range req.Keys {
		// Parse path to check if incomplete
		path, ok := keyData["path"].([]any)
		if !ok || len(path) == 0 {
			continue
		}

		// Get last element
		lastElem, ok := path[len(path)-1].(map[string]any)
		if !ok {
			continue
		}

		// If it has no name or id, allocate a unique ID
		_, hasName := lastElem["name"]
		_, hasID := lastElem["id"]
		if !hasName && !hasID {
			// Allocate a unique sequential ID
			s.nextID++
			lastElem["id"] = strconv.FormatInt(s.nextID, 10)
		}

		allocatedKeys = append(allocatedKeys, keyData)
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"keys": allocatedKeys,
	}); err != nil {
		log.Printf("failed to encode allocateIds response: %v", err)
	}
}

// matchesFilter checks if an entity matches a filter.
//
//nolint:gocognit,nestif // Complex logic required for proper filter evaluation with multiple types and operators
func matchesFilter(entity map[string]any, filterMap map[string]any) bool {
	// Handle propertyFilter
	if propFilter, ok := filterMap["propertyFilter"].(map[string]any); ok {
		property, ok := propFilter["property"].(map[string]any)
		if !ok {
			return true // Invalid filter, allow all
		}
		propertyName, ok := property["name"].(string)
		if !ok {
			return true
		}
		operator, ok := propFilter["op"].(string)
		if !ok {
			return true
		}
		filterValue := propFilter["value"]

		// Get entity properties
		properties, ok := entity["properties"].(map[string]any)
		if !ok {
			return false
		}
		entityProp, ok := properties[propertyName].(map[string]any)
		if !ok {
			return false // Property doesn't exist
		}

		// Extract entity value based on type
		var entityValue any
		if intVal, ok := entityProp["integerValue"].(string); ok {
			var i int64
			if _, err := fmt.Sscanf(intVal, "%d", &i); err == nil {
				entityValue = i
			}
		} else if strVal, ok := entityProp["stringValue"].(string); ok {
			entityValue = strVal
		} else if boolVal, ok := entityProp["booleanValue"].(bool); ok {
			entityValue = boolVal
		} else if floatVal, ok := entityProp["doubleValue"].(float64); ok {
			entityValue = floatVal
		}

		// Extract filter value
		var filterVal any
		if fv, ok := filterValue.(map[string]any); ok {
			if intVal, ok := fv["integerValue"].(string); ok {
				var i int64
				if _, err := fmt.Sscanf(intVal, "%d", &i); err == nil {
					filterVal = i
				}
			} else if strVal, ok := fv["stringValue"].(string); ok {
				filterVal = strVal
			}
		}

		// Compare based on operator
		switch operator {
		case "EQUAL":
			return entityValue == filterVal
		case "GREATER_THAN":
			if ev, ok := entityValue.(int64); ok {
				if fv, ok := filterVal.(int64); ok {
					return ev > fv
				}
			}
		case "GREATER_THAN_OR_EQUAL":
			if ev, ok := entityValue.(int64); ok {
				if fv, ok := filterVal.(int64); ok {
					return ev >= fv
				}
			}
		case "LESS_THAN":
			if ev, ok := entityValue.(int64); ok {
				if fv, ok := filterVal.(int64); ok {
					return ev < fv
				}
			}
		case "LESS_THAN_OR_EQUAL":
			if ev, ok := entityValue.(int64); ok {
				if fv, ok := filterVal.(int64); ok {
					return ev <= fv
				}
			}
		default:
			return false
		}
	}

	// Handle compositeFilter (AND/OR)
	if compFilter, ok := filterMap["compositeFilter"].(map[string]any); ok {
		op, ok := compFilter["op"].(string)
		if !ok {
			return true
		}
		filters, ok := compFilter["filters"].([]any)
		if !ok {
			return true
		}

		switch op {
		case "AND":
			for _, f := range filters {
				if fm, ok := f.(map[string]any); ok {
					if !matchesFilter(entity, fm) {
						return false
					}
				}
			}
			return true
		case "OR":
			for _, f := range filters {
				if fm, ok := f.(map[string]any); ok {
					if matchesFilter(entity, fm) {
						return true
					}
				}
			}
			return false
		default:
			return true
		}
	}

	return true // No filter or unrecognized filter, allow all
}

// handleRunAggregationQuery handles :runAggregationQuery requests.
func (s *Store) handleRunAggregationQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DatabaseID       string         `json:"databaseId"`
		AggregationQuery map[string]any `json:"aggregationQuery"`
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

	// Extract query from aggregationQuery
	nestedQuery, ok := req.AggregationQuery["nestedQuery"].(map[string]any)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Extract kind from query
	kindArray, ok := nestedQuery["kind"].([]any)
	if !ok || len(kindArray) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	kindMap, ok := kindArray[0].(map[string]any)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	kind, ok := kindMap["name"].(string)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Count entities of this kind in store
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	// entities map is keyed by "kind/keyname", so we need to iterate
	for keyStr, entity := range s.entities {
		// Extract kind from entity's key
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
		if !ok || entityKind != kind {
			continue
		}

		// Apply filters if present
		if filterMap, hasFilter := nestedQuery["filter"].(map[string]any); hasFilter {
			if !matchesFilter(entity, filterMap) {
				continue
			}
		}

		_ = keyStr
		count++
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"batch": map[string]any{
			"aggregationResults": []map[string]any{
				{
					"aggregateProperties": map[string]any{
						"total": map[string]any{
							"integerValue": strconv.Itoa(count),
						},
					},
				},
			},
		},
	}); err != nil {
		log.Printf("failed to encode aggregation response: %v", err)
	}
}
