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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

const metadataFlavor = "Google"

// Datastore limits (matching real Google Cloud Datastore).
const (
	maxMutationsPerCommit = 500
	maxEntitySizeBytes    = 1048572 // 1 MiB - 4 bytes
	maxKeySizeBytes       = 6144    // 6 KiB
	transactionTimeout    = 270     // seconds (4.5 minutes, real is ~5 minutes)
)

// Store holds the in-memory entity storage.
//
//nolint:govet // Field alignment not optimized to maintain readability
type Store struct {
	mu           sync.RWMutex
	entities     map[string]map[string]any
	transactions map[string]*transactionState
	nextID       int64 // Counter for allocating unique IDs
	nextTxID     int64 // Counter for transaction IDs
}

// transactionState tracks the state of an active transaction.
//
//nolint:govet // Field order prioritizes logical grouping over memory optimization
type transactionState struct {
	id        string
	createdAt time.Time
	readKeys  map[string]bool // Keys read during this transaction
}

// NewStore creates a new in-memory store.
func NewStore() *Store {
	return &Store{
		entities:     make(map[string]map[string]any),
		transactions: make(map[string]*transactionState),
		nextID:       1000, // Start IDs at 1000
		nextTxID:     1,
	}
}

// NewMockServers creates mock metadata and API servers for testing.
// Returns the metadata URL, API URL, and a cleanup function.
// This function doesn't import datastore to avoid import cycles.
//
// For convenience, use datastore.NewMockClient() instead which handles all setup.
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
			store.handleBeginTransaction(w, r)
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
		keyStr, ok := s.extractKeyString(keyData)
		if !ok {
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
//nolint:gocognit // Complex validation logic required to match real Datastore behavior
func (s *Store) handleCommit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mode        string           `json:"mode"`
		DatabaseID  string           `json:"databaseId"`
		Transaction string           `json:"transaction"`
		Mutations   []map[string]any `json:"mutations"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Validate routing header for named databases
	if req.DatabaseID != "" {
		routingHeader := r.Header.Get("X-Goog-Request-Params")
		if routingHeader == "" {
			s.writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Missing routing header for named database")
			return
		}
	}

	// Validate mutation count limit
	if len(req.Mutations) > maxMutationsPerCommit {
		s.writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			fmt.Sprintf("Too many mutations: %d exceeds limit of %d", len(req.Mutations), maxMutationsPerCommit))
		return
	}

	// Validate transaction mode
	if req.Mode == "TRANSACTIONAL" {
		if req.Transaction == "" {
			s.writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Transaction ID required for TRANSACTIONAL mode")
			return
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate transaction if provided
	if req.Transaction != "" {
		txState, exists := s.transactions[req.Transaction]
		if !exists {
			s.writeErrorLocked(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Invalid or expired transaction")
			return
		}

		// Check transaction timeout
		if time.Since(txState.createdAt) > transactionTimeout*time.Second {
			delete(s.transactions, req.Transaction)
			s.writeErrorLocked(w, http.StatusBadRequest, "ABORTED", "Transaction has expired")
			return
		}

		// Remove transaction after commit (whether successful or not)
		defer delete(s.transactions, req.Transaction)
	}

	var mutationResults []map[string]any

	for _, mutation := range req.Mutations {
		var resultKey map[string]any

		// Handle insert - fails if entity already exists (like real Datastore)
		if insert, ok := mutation["insert"].(map[string]any); ok {
			keyData, ok := insert["key"].(map[string]any)
			if !ok {
				continue
			}

			// Validate entity size
			if err := s.validateEntitySize(insert); err != nil {
				s.writeErrorLocked(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
				return
			}

			// Validate key size
			if err := s.validateKeySize(keyData); err != nil {
				s.writeErrorLocked(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
				return
			}

			// Handle incomplete keys - allocate ID
			keyStr, keyData, ok := s.resolveKey(keyData)
			if !ok {
				continue
			}

			// Check if entity already exists - insert should fail
			if _, exists := s.entities[keyStr]; exists {
				s.writeErrorLocked(w, http.StatusConflict, "ALREADY_EXISTS", "Entity already exists")
				return
			}

			// Update the entity's key with potentially allocated ID
			insert["key"] = keyData
			s.entities[keyStr] = insert
			resultKey = keyData
		}

		// Handle update - fails if entity doesn't exist (like real Datastore)
		if update, ok := mutation["update"].(map[string]any); ok {
			keyData, ok := update["key"].(map[string]any)
			if !ok {
				continue
			}

			// Validate entity size
			if err := s.validateEntitySize(update); err != nil {
				s.writeErrorLocked(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
				return
			}

			// Validate key size
			if err := s.validateKeySize(keyData); err != nil {
				s.writeErrorLocked(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
				return
			}

			keyStr, ok := s.extractKeyString(keyData)
			if !ok {
				continue
			}

			// Check if entity exists - update should fail if not
			if _, exists := s.entities[keyStr]; !exists {
				s.writeErrorLocked(w, http.StatusNotFound, "NOT_FOUND", "No entity to update")
				return
			}

			s.entities[keyStr] = update
			resultKey = keyData
		}

		// Handle upsert - creates or updates (always succeeds)
		if upsert, ok := mutation["upsert"].(map[string]any); ok {
			keyData, ok := upsert["key"].(map[string]any)
			if !ok {
				continue
			}

			// Validate entity size
			if err := s.validateEntitySize(upsert); err != nil {
				s.writeErrorLocked(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
				return
			}

			// Validate key size
			if err := s.validateKeySize(keyData); err != nil {
				s.writeErrorLocked(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
				return
			}

			// Handle incomplete keys - allocate ID
			keyStr, keyData, ok := s.resolveKey(keyData)
			if !ok {
				continue
			}

			// Update the entity's key with potentially allocated ID
			upsert["key"] = keyData
			s.entities[keyStr] = upsert
			resultKey = keyData
		}

		// Handle delete
		if deleteKey, ok := mutation["delete"].(map[string]any); ok {
			keyStr, ok := s.extractKeyString(deleteKey)
			if !ok {
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

// writeError writes an error response (must NOT hold lock).
//
//nolint:unparam // code parameter kept for consistency with writeErrorLocked
func (*Store) writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
			"status":  status,
		},
	}); err != nil {
		log.Printf("failed to encode error response: %v", err)
	}
}

// writeErrorLocked writes an error response (caller holds lock, but we don't release it).
func (*Store) writeErrorLocked(w http.ResponseWriter, statusCode int, status, message string) {
	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    statusCode,
			"message": message,
			"status":  status,
		},
	}); err != nil {
		log.Printf("failed to encode error response: %v", err)
	}
}

// validateEntitySize checks if an entity exceeds the size limit.
func (*Store) validateEntitySize(entity map[string]any) error {
	data, err := json.Marshal(entity)
	if err != nil {
		return fmt.Errorf("failed to measure entity size: %w", err)
	}
	if len(data) > maxEntitySizeBytes {
		return fmt.Errorf("entity size %d exceeds limit of %d bytes", len(data), maxEntitySizeBytes)
	}
	return nil
}

// validateKeySize checks if a key exceeds the size limit.
func (*Store) validateKeySize(keyData map[string]any) error {
	data, err := json.Marshal(keyData)
	if err != nil {
		return fmt.Errorf("failed to measure key size: %w", err)
	}
	if len(data) > maxKeySizeBytes {
		return fmt.Errorf("key size %d exceeds limit of %d bytes", len(data), maxKeySizeBytes)
	}
	return nil
}

// resolveKey handles incomplete keys by allocating an ID if needed.
// Returns the key string, updated key data, and success flag.
func (s *Store) resolveKey(keyData map[string]any) (keyStr string, updatedKey map[string]any, ok bool) {
	path, ok := keyData["path"].([]any)
	if !ok || len(path) == 0 {
		return "", nil, false
	}
	pathElem, ok := path[0].(map[string]any)
	if !ok {
		return "", nil, false
	}
	kind, ok := pathElem["kind"].(string)
	if !ok {
		return "", nil, false
	}

	// Extract namespace
	namespace := ""
	if pid, ok := keyData["partitionId"].(map[string]any); ok {
		if ns, ok := pid["namespaceId"].(string); ok {
			namespace = ns
		}
	}

	// Handle both name and ID keys
	if name, ok := pathElem["name"].(string); ok {
		return namespace + "!" + kind + "/" + name, keyData, true
	}
	if id, ok := pathElem["id"].(string); ok {
		return namespace + "!" + kind + "/" + id, keyData, true
	}

	// Incomplete key - allocate an ID
	s.nextID++
	allocatedID := strconv.FormatInt(s.nextID, 10)
	pathElem["id"] = allocatedID

	return namespace + "!" + kind + "/" + allocatedID, keyData, true
}

// extractKeyString extracts the key string from key data.
func (*Store) extractKeyString(keyData map[string]any) (string, bool) {
	path, ok := keyData["path"].([]any)
	if !ok || len(path) == 0 {
		return "", false
	}
	pathElem, ok := path[0].(map[string]any)
	if !ok {
		return "", false
	}
	kind, ok := pathElem["kind"].(string)
	if !ok {
		return "", false
	}

	// Extract namespace
	namespace := ""
	if pid, ok := keyData["partitionId"].(map[string]any); ok {
		if ns, ok := pid["namespaceId"].(string); ok {
			namespace = ns
		}
	}

	// Handle both name and ID keys
	if name, ok := pathElem["name"].(string); ok {
		return namespace + "!" + kind + "/" + name, true
	}
	if id, ok := pathElem["id"].(string); ok {
		return namespace + "!" + kind + "/" + id, true
	}
	return "", false
}

// queryResult holds an entity with its key for sorting.
//
//nolint:govet // Field order prioritizes logical grouping over memory optimization
type queryResult struct {
	keyStr string
	entity map[string]any
}

// isKeysOnlyQuery checks if the query has a projection for only __key__.
func isKeysOnlyQuery(query map[string]any) bool {
	projection, ok := query["projection"].([]any)
	if !ok || len(projection) != 1 {
		return false
	}
	proj, ok := projection[0].(map[string]any)
	if !ok {
		return false
	}
	prop, ok := proj["property"].(map[string]any)
	if !ok {
		return false
	}
	name, ok := prop["name"].(string)
	return ok && name == "__key__"
}

// handleRunQuery handles query requests.
func (s *Store) handleRunQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query       map[string]any `json:"query"`
		PartitionID map[string]any `json:"partitionId"`
		DatabaseID  string         `json:"databaseId"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Validate routing header for named databases
	if req.DatabaseID != "" {
		routingHeader := r.Header.Get("X-Goog-Request-Params")
		if routingHeader == "" {
			s.writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Missing routing header for named database")
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

	// Filter by namespace
	namespace := ""
	if req.PartitionID != nil {
		if ns, ok := req.PartitionID["namespaceId"].(string); ok {
			namespace = ns
		}
	}

	var limit int
	if l, ok := query["limit"].(float64); ok {
		limit = int(l)
	}

	var offset int
	if o, ok := query["offset"].(float64); ok {
		offset = int(o)
	}

	// Parse cursor to get starting position
	var startIdx int
	if sc, ok := query["startCursor"].(string); ok && sc != "" {
		startIdx = s.decodeCursor(sc)
	}

	// Find all entities of this kind
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect all matching entities
	var matches []queryResult
	for keyStr, entity := range s.entities {
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

		// Check kind and namespace
		entityNamespace := ""
		if pid, ok := keyData["partitionId"].(map[string]any); ok {
			if ns, ok := pid["namespaceId"].(string); ok {
				entityNamespace = ns
			}
		}

		if entityKind == kind && entityNamespace == namespace {
			// Apply filters if present
			if filterMap, hasFilter := query["filter"].(map[string]any); hasFilter {
				if !matchesFilter(entity, filterMap) {
					continue
				}
			}

			matches = append(matches, queryResult{keyStr: keyStr, entity: entity})
		}
	}

	// Sort results deterministically by key string for consistent ordering
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].keyStr < matches[j].keyStr
	})

	// Apply ordering from query if specified
	if orders, ok := query["order"].([]any); ok && len(orders) > 0 {
		s.applyOrdering(matches, orders)
	}

	// Apply offset and cursor
	skipCount := offset + startIdx
	if skipCount > len(matches) {
		skipCount = len(matches)
	}
	matches = matches[skipCount:]

	// Apply limit
	totalMatches := len(matches)
	if limit > 0 && len(matches) > limit {
		matches = matches[:limit]
	}

	// Check if this is a keys-only query (projection contains only __key__)
	keysOnly := isKeysOnlyQuery(query)

	// Build results
	results := make([]any, 0, len(matches))
	for _, m := range matches {
		if keysOnly {
			// For keys-only queries, return entity with only the key (no properties)
			results = append(results, map[string]any{
				"entity": map[string]any{
					"key": m.entity["key"],
				},
			})
		} else {
			results = append(results, map[string]any{
				"entity": m.entity,
			})
		}
	}

	// Generate cursor for pagination
	var endCursor string
	moreResults := "NO_MORE_RESULTS"
	if limit > 0 && totalMatches > limit {
		// Encode cursor as position in sorted results
		endCursor = s.encodeCursor(skipCount + limit)
		moreResults = "MORE_RESULTS_AFTER_LIMIT"
	}

	// Build response
	batch := map[string]any{
		"entityResults": results,
		"moreResults":   moreResults,
	}

	if endCursor != "" {
		batch["endCursor"] = endCursor
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

// encodeCursor creates a base64-encoded cursor from a position.
func (*Store) encodeCursor(pos int) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("pos:%d", pos)))
}

// decodeCursor extracts the position from a cursor string.
func (*Store) decodeCursor(cursor string) int {
	data, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return 0
	}
	var pos int
	if _, err := fmt.Sscanf(string(data), "pos:%d", &pos); err != nil {
		return 0
	}
	return pos
}

// applyOrdering sorts query results based on order specifications.
func (*Store) applyOrdering(matches []queryResult, orders []any) {
	sort.SliceStable(matches, func(i, j int) bool {
		for _, orderAny := range orders {
			order, ok := orderAny.(map[string]any)
			if !ok {
				continue
			}
			propMap, ok := order["property"].(map[string]any)
			if !ok {
				continue
			}
			propName, ok := propMap["name"].(string)
			if !ok {
				continue
			}
			direction, ok := order["direction"].(string)
			descending := ok && direction == "DESCENDING"

			// Get property values from both entities
			propsI, okI := matches[i].entity["properties"].(map[string]any)
			propsJ, okJ := matches[j].entity["properties"].(map[string]any)
			if !okI || !okJ {
				continue
			}

			valI := getPropertyValue(propsI, propName)
			valJ := getPropertyValue(propsJ, propName)

			cmp := compareValues(valI, valJ)
			if cmp != 0 {
				if descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}

// getPropertyValue extracts a comparable value from entity properties.
func getPropertyValue(props map[string]any, name string) any {
	if props == nil {
		return nil
	}
	prop, ok := props[name].(map[string]any)
	if !ok {
		return nil
	}
	if v, ok := prop["integerValue"].(string); ok {
		var i int64
		if _, err := fmt.Sscanf(v, "%d", &i); err == nil {
			return i
		}
	}
	if v, ok := prop["stringValue"].(string); ok {
		return v
	}
	if v, ok := prop["doubleValue"].(float64); ok {
		return v
	}
	if v, ok := prop["booleanValue"].(bool); ok {
		return v
	}
	return nil
}

// compareValues compares two property values.
func compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch va := a.(type) {
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			}
			if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			}
			if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			}
			if va > vb {
				return 1
			}
			return 0
		}
	case bool:
		if vb, ok := b.(bool); ok {
			if !va && vb {
				return -1
			}
			if va && !vb {
				return 1
			}
			return 0
		}
	}
	return 0
}

// handleBeginTransaction handles transaction begin requests.
func (s *Store) handleBeginTransaction(w http.ResponseWriter, r *http.Request) {
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
			s.writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Missing routing header for named database")
			return
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate unique transaction ID
	s.nextTxID++
	txID := fmt.Sprintf("tx-%d-%d", time.Now().UnixNano(), s.nextTxID)

	// Store transaction state
	s.transactions[txID] = &transactionState{
		id:        txID,
		createdAt: time.Now(),
		readKeys:  make(map[string]bool),
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"transaction": txID,
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

		// Handle __key__ filters (special property)
		if propertyName == "__key__" {
			entityKeyData, ok := entity["key"].(map[string]any)
			if !ok {
				return false
			}

			// Extract the filter key - it might be wrapped in keyValue
			var filterKeyData map[string]any
			if fkd, ok := filterValue.(map[string]any); ok {
				// Check if it's wrapped in keyValue
				if kv, hasKeyValue := fkd["keyValue"].(map[string]any); hasKeyValue {
					filterKeyData = kv
				} else {
					// It's a direct key
					filterKeyData = fkd
				}
			} else {
				return false
			}

			// Compare keys based on operator
			cmpResult := compareKeys(entityKeyData, filterKeyData)
			switch operator {
			case "EQUAL":
				return cmpResult == 0
			case "GREATER_THAN":
				return cmpResult > 0
			case "GREATER_THAN_OR_EQUAL":
				return cmpResult >= 0
			case "LESS_THAN":
				return cmpResult < 0
			case "LESS_THAN_OR_EQUAL":
				return cmpResult <= 0
			default:
				return false
			}
		}

		// Handle HAS_ANCESTOR
		if operator == "HAS_ANCESTOR" {
			ancestorKeyData, ok := filterValue.(map[string]any)
			if !ok {
				// Try keyValue if wrapped
				kv, ok := filterValue.(map[string]any)
				if !ok {
					return false
				}
				ak, ok := kv["keyValue"].(map[string]any)
				if !ok {
					return false
				}
				ancestorKeyData = ak
			}
			// Check if entity key has prefix of ancestor key path
			entityKeyData, ok := entity["key"].(map[string]any)
			if !ok {
				return false
			}
			return isAncestor(ancestorKeyData, entityKeyData)
		}

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

// compareKeys compares two Datastore keys and returns:
// -1 if keyA < keyB
//
//	0 if keyA == keyB
//	1 if keyA > keyB
func compareKeys(keyA, keyB map[string]any) int {
	// Extract key strings for comparison
	strA := keyToSortString(keyA)
	strB := keyToSortString(keyB)

	if strA < strB {
		return -1
	}
	if strA > strB {
		return 1
	}
	return 0
}

// keyToSortString converts a key to a string for sorting/comparison.
// Format: "namespace!kind/name_or_id"
// Supports both standard key format (with "path") and entityValue format (with "properties").
func keyToSortString(keyData map[string]any) string {
	// Try entityValue format first (used by encoded filter values)
	if entityValue, ok := keyData["entityValue"].(map[string]any); ok {
		if props, ok := entityValue["properties"].(map[string]any); ok {
			// Extract kind
			kindProp, ok := props["Kind"].(map[string]any)
			if !ok {
				return ""
			}
			kind, ok := kindProp["stringValue"].(string)
			if !ok {
				return ""
			}

			// Extract namespace
			namespace := ""
			if nsProp, ok := props["Namespace"].(map[string]any); ok {
				if ns, ok := nsProp["stringValue"].(string); ok && ns != "" {
					namespace = ns
				}
			}

			// Extract name or ID
			if nameProp, ok := props["Name"].(map[string]any); ok {
				if name, ok := nameProp["stringValue"].(string); ok && name != "" {
					return namespace + "!" + kind + "/" + name
				}
			}
			if idProp, ok := props["ID"].(map[string]any); ok {
				if idInt, ok := idProp["integerValue"].(float64); ok && idInt != 0 {
					return namespace + "!" + kind + "/" + fmt.Sprintf("%.0f", idInt)
				}
				if idStr, ok := idProp["stringValue"].(string); ok && idStr != "" {
					return namespace + "!" + kind + "/" + idStr
				}
			}
			return namespace + "!" + kind + "/"
		}
	}

	// Try standard path format (used by stored entities)
	path, ok := keyData["path"].([]any)
	if !ok || len(path) == 0 {
		return ""
	}
	pathElem, ok := path[0].(map[string]any)
	if !ok {
		return ""
	}
	kind, ok := pathElem["kind"].(string)
	if !ok {
		return ""
	}

	// Extract namespace
	namespace := ""
	if pid, ok := keyData["partitionId"].(map[string]any); ok {
		if ns, ok := pid["namespaceId"].(string); ok {
			namespace = ns
		}
	}

	// Handle both name and ID keys
	if name, ok := pathElem["name"].(string); ok {
		return namespace + "!" + kind + "/" + name
	}
	if id, ok := pathElem["id"].(string); ok {
		return namespace + "!" + kind + "/" + id
	}
	return namespace + "!" + kind + "/"
}

// isAncestor checks if ancestorKey is a prefix of entityKey.
func isAncestor(ancestorKey, entityKey map[string]any) bool {
	ancPath, ok1 := ancestorKey["path"].([]any)
	entPath, ok2 := entityKey["path"].([]any)

	if !ok1 || !ok2 {
		return false
	}

	if len(ancPath) > len(entPath) {
		return false
	}

	// Check equality of path elements
	for i := range ancPath {
		ap, ok1 := ancPath[i].(map[string]any)
		ep, ok2 := entPath[i].(map[string]any)

		if !ok1 || !ok2 {
			return false
		}

		if ap["kind"] != ep["kind"] {
			return false
		}
		if ap["name"] != ep["name"] {
			return false
		}
		if ap["id"] != ep["id"] {
			return false
		}
	}

	return true
}

// handleRunAggregationQuery handles :runAggregationQuery requests.
func (s *Store) handleRunAggregationQuery(w http.ResponseWriter, r *http.Request) {
	var req struct { //nolint:govet // Local anonymous struct for JSON unmarshaling
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
