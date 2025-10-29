// Package ds9 provides a zero-dependency Google Cloud Datastore client.
//
// It uses only the Go standard library and makes direct REST API calls
// to the Datastore API. Authentication is handled via the GCP metadata
// server when running on GCP, or via Application Default Credentials.
//
//nolint:revive // Public structs required for API compatibility with cloud.google.com/go/datastore
package ds9

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	neturl "net/url"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codeGROOVE-dev/ds9/auth"
)

const (
	maxRetries     = 3
	maxBodySize    = 10 * 1024 * 1024 // 10MB
	defaultTimeout = 30 * time.Second
	baseBackoffMS  = 100  // Start with 100ms
	maxBackoffMS   = 2000 // Cap at 2 seconds
	jitterFraction = 0.25 // 25% jitter
)

var (
	// ErrNoSuchEntity is returned when an entity is not found.
	ErrNoSuchEntity = errors.New("datastore: no such entity")

	// atomicAPIURL stores the API URL for thread-safe access.
	// Use getAPIURL() to read and setAPIURL() to write.
	atomicAPIURL atomic.Pointer[string]

	httpClient = &http.Client{
		Timeout: defaultTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			IdleConnTimeout:     30 * time.Second,
			MaxIdleConnsPerHost: 2,
		},
	}

	// operatorMap converts shorthand operators to Datastore API operators.
	operatorMap = map[string]string{
		"=":  "EQUAL",
		"<":  "LESS_THAN",
		"<=": "LESS_THAN_OR_EQUAL",
		">":  "GREATER_THAN",
		">=": "GREATER_THAN_OR_EQUAL",
	}
)

//nolint:gochecknoinits // Required for thread-safe initialization of atomic pointer
func init() {
	defaultURL := "https://datastore.googleapis.com/v1"
	atomicAPIURL.Store(&defaultURL)
}

// getAPIURL returns the current API URL in a thread-safe manner.
func getAPIURL() string {
	return *atomicAPIURL.Load()
}

// setAPIURL sets the API URL in a thread-safe manner.
func setAPIURL(url string) {
	atomicAPIURL.Store(&url)
}

// SetTestURLs configures custom metadata and API URLs for testing.
// This is intended for use by testing packages like ds9mock.
// Returns a function that restores the original URLs.
// WARNING: This function should only be called in test code.
// Set DS9_ALLOW_TEST_OVERRIDES=true to enable in non-test environments.
//
// Example:
//
//	restore := ds9.SetTestURLs("http://localhost:8080", "http://localhost:9090")
//	defer restore()
func SetTestURLs(metadata, api string) (restore func()) {
	// Auth package will log warning if called outside test environment
	oldAPI := getAPIURL()
	setAPIURL(api)
	restoreAuth := auth.SetMetadataURL(metadata)
	return func() {
		setAPIURL(oldAPI)
		restoreAuth()
	}
}

// Client is a Google Cloud Datastore client.
type Client struct {
	logger     *slog.Logger
	projectID  string
	databaseID string
	baseURL    string // API base URL, defaults to production but can be overridden for testing
}

// NewClient creates a new Datastore client.
// If projectID is empty, it will be fetched from the GCP metadata server.
func NewClient(ctx context.Context, projectID string) (*Client, error) {
	return NewClientWithDatabase(ctx, projectID, "")
}

// NewClientWithDatabase creates a new Datastore client with a specific database.
func NewClientWithDatabase(ctx context.Context, projID, dbID string) (*Client, error) {
	logger := slog.Default()

	if projID == "" {
		if !testing.Testing() {
			logger.InfoContext(ctx, "project ID not provided, fetching from metadata server")
		}
		pid, err := auth.ProjectID(ctx)
		if err != nil {
			logger.ErrorContext(ctx, "failed to get project ID from metadata server", "error", err)
			return nil, fmt.Errorf("project ID required: %w", err)
		}
		projID = pid
		if !testing.Testing() {
			logger.InfoContext(ctx, "fetched project ID from metadata server", "project_id", projID)
		}
	}

	if !testing.Testing() {
		logger.InfoContext(ctx, "creating datastore client", "project_id", projID, "database_id", dbID)
	}

	return &Client{
		projectID:  projID,
		databaseID: dbID,
		baseURL:    getAPIURL(),
		logger:     logger,
	}, nil
}

// Close closes the client connection.
// This is a no-op for ds9 since it uses a shared HTTP client with connection pooling,
// but is provided for API compatibility with cloud.google.com/go/datastore.
func (*Client) Close() error {
	return nil
}

// Key represents a Datastore key.
type Key struct {
	Parent *Key // Parent key for hierarchical keys
	Kind   string
	Name   string // For string keys
	ID     int64  // For numeric keys
}

// NameKey creates a new key with a string name.
// The parent parameter can be nil for top-level keys.
// This matches the API of cloud.google.com/go/datastore.
func NameKey(kind, name string, parent *Key) *Key {
	return &Key{
		Kind:   kind,
		Name:   name,
		Parent: parent,
	}
}

// IDKey creates a new key with a numeric ID.
// The parent parameter can be nil for top-level keys.
// This matches the API of cloud.google.com/go/datastore.
func IDKey(kind string, id int64, parent *Key) *Key {
	return &Key{
		Kind:   kind,
		ID:     id,
		Parent: parent,
	}
}

// IncompleteKey creates a new incomplete key.
// The key will be completed (assigned an ID) when the entity is saved.
// API compatible with cloud.google.com/go/datastore.
func IncompleteKey(kind string, parent *Key) *Key {
	return &Key{
		Kind:   kind,
		Parent: parent,
	}
}

// Incomplete returns true if the key does not have an ID or Name.
// API compatible with cloud.google.com/go/datastore.
func (k *Key) Incomplete() bool {
	return k.ID == 0 && k.Name == ""
}

// Equal returns true if this key is equal to the other key.
// API compatible with cloud.google.com/go/datastore.
func (k *Key) Equal(other *Key) bool {
	if k == nil && other == nil {
		return true
	}
	if k == nil || other == nil {
		return false
	}
	if k.Kind != other.Kind || k.Name != other.Name || k.ID != other.ID {
		return false
	}
	// Recursively check parent keys
	return k.Parent.Equal(other.Parent)
}

// String returns a human-readable string representation of the key.
// API compatible with cloud.google.com/go/datastore.
func (k *Key) String() string {
	if k == nil {
		return ""
	}

	var parts []string
	for curr := k; curr != nil; curr = curr.Parent {
		var part string
		switch {
		case curr.Name != "":
			part = fmt.Sprintf("%s,%q", curr.Kind, curr.Name)
		case curr.ID != 0:
			part = fmt.Sprintf("%s,%d", curr.Kind, curr.ID)
		default:
			part = fmt.Sprintf("%s,incomplete", curr.Kind)
		}
		// Prepend to maintain correct order (root to leaf)
		parts = append([]string{part}, parts...)
	}

	return "/" + strings.Join(parts, "/")
}

// Encode returns an opaque representation of the key.
// API compatible with cloud.google.com/go/datastore.
func (k *Key) Encode() string {
	if k == nil {
		return ""
	}

	// Convert key to JSON representation
	keyJSON := keyToJSON(k)

	// Marshal to JSON bytes
	jsonBytes, err := json.Marshal(keyJSON)
	if err != nil {
		return ""
	}

	// Base64 encode
	return base64.URLEncoding.EncodeToString(jsonBytes)
}

// DecodeKey decodes a key from its opaque representation.
// API compatible with cloud.google.com/go/datastore.
func DecodeKey(encoded string) (*Key, error) {
	if encoded == "" {
		return nil, errors.New("empty encoded key")
	}

	// Base64 decode
	jsonBytes, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	// Unmarshal JSON
	var keyData any
	if err := json.Unmarshal(jsonBytes, &keyData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	// Convert from JSON representation
	key, err := keyFromJSON(keyData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key: %w", err)
	}

	return key, nil
}

// Cursor represents a query cursor for pagination.
// API compatible with cloud.google.com/go/datastore.
type Cursor string

// String returns the cursor as a string.
func (c Cursor) String() string {
	return string(c)
}

// DecodeCursor decodes a cursor string.
// API compatible with cloud.google.com/go/datastore.
func DecodeCursor(s string) (Cursor, error) {
	if s == "" {
		return "", errors.New("empty cursor string")
	}
	return Cursor(s), nil
}

// Iterator is an iterator for query results.
// API compatible with cloud.google.com/go/datastore.
//
//nolint:govet // Field alignment optimized for API compatibility over memory layout
type Iterator struct {
	ctx       context.Context //nolint:containedctx // Required for API compatibility with cloud.google.com/go/datastore
	client    *Client
	query     *Query
	results   []iteratorResult
	index     int
	err       error
	cursor    Cursor
	fetchNext bool
}

type iteratorResult struct {
	key    *Key
	entity map[string]any
	cursor Cursor
}

// Next advances the iterator and returns the next key and destination.
// It returns Done when no more results are available.
// API compatible with cloud.google.com/go/datastore.
func (it *Iterator) Next(dst any) (*Key, error) {
	// Check if we need to fetch more results
	if it.index >= len(it.results) {
		if it.err != nil {
			return nil, it.err
		}
		if !it.fetchNext {
			return nil, ErrDone
		}

		// Fetch next batch
		if err := it.fetch(); err != nil {
			it.err = err
			return nil, err
		}

		if len(it.results) == 0 {
			return nil, ErrDone
		}
	}

	result := it.results[it.index]
	it.index++
	it.cursor = result.cursor

	// Decode entity into dst
	if err := decodeEntity(result.entity, dst); err != nil {
		return nil, err
	}

	return result.key, nil
}

// Cursor returns the cursor for the iterator's current position.
// API compatible with cloud.google.com/go/datastore.
func (it *Iterator) Cursor() (Cursor, error) {
	if it.cursor == "" {
		return "", errors.New("no cursor available")
	}
	return it.cursor, nil
}

// fetch retrieves the next batch of results.
func (it *Iterator) fetch() error {
	token, err := auth.AccessToken(it.ctx)
	if err != nil {
		return fmt.Errorf("failed to get access token: %w", err)
	}

	// Build query with current cursor as start
	q := *it.query
	if it.cursor != "" {
		q.startCursor = it.cursor
	}

	queryObj := buildQueryMap(&q)
	reqBody := map[string]any{"query": queryObj}
	if it.client.databaseID != "" {
		reqBody["databaseId"] = it.client.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:runQuery", it.client.baseURL, neturl.PathEscape(it.client.projectID))
	body, err := doRequest(it.ctx, it.client.logger, reqURL, jsonData, token, it.client.projectID, it.client.databaseID)
	if err != nil {
		return err
	}

	var result struct {
		Batch struct {
			EntityResults []struct {
				Entity map[string]any `json:"entity"`
				Cursor string         `json:"cursor"`
			} `json:"entityResults"`
			MoreResults    string `json:"moreResults"`
			EndCursor      string `json:"endCursor"`
			SkippedResults int    `json:"skippedResults"`
		} `json:"batch"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	// Convert results to iterator format
	it.results = make([]iteratorResult, 0, len(result.Batch.EntityResults))
	for _, er := range result.Batch.EntityResults {
		key, err := keyFromJSON(er.Entity["key"])
		if err != nil {
			return err
		}

		it.results = append(it.results, iteratorResult{
			key:    key,
			entity: er.Entity,
			cursor: Cursor(er.Cursor),
		})
	}

	it.index = 0

	// Check if there are more results
	moreResults := result.Batch.MoreResults
	it.fetchNext = moreResults == "NOT_FINISHED" || moreResults == "MORE_RESULTS_AFTER_LIMIT" || moreResults == "MORE_RESULTS_AFTER_CURSOR"

	if result.Batch.EndCursor != "" {
		it.cursor = Cursor(result.Batch.EndCursor)
	}

	return nil
}

// ErrDone is returned by Iterator.Next when no more results are available.
var ErrDone = errors.New("datastore: no more results")

// doRequest performs an HTTP request with exponential backoff retries.
// Returns an error if the status code is not 200 OK.
func doRequest(ctx context.Context, logger *slog.Logger, url string, jsonData []byte, token, projectID, databaseID string) ([]byte, error) {
	var lastErr error

	for attempt := range maxRetries {
		if attempt > 0 {
			// Exponential backoff: 100ms, 200ms, 400ms... capped at maxBackoffMS
			backoffMS := math.Min(float64(baseBackoffMS)*math.Pow(2, float64(attempt-1)), float64(maxBackoffMS))
			// Add jitter: ±25% randomness
			jitter := backoffMS * jitterFraction * (2*rand.Float64() - 1) //nolint:gosec // Weak random is acceptable for jitter
			sleepMS := backoffMS + jitter
			sleepDuration := time.Duration(sleepMS) * time.Millisecond

			logger.DebugContext(ctx, "retrying request",
				"attempt", attempt+1,
				"max_attempts", maxRetries,
				"backoff_ms", int(sleepMS),
				"last_error", lastErr)

			select {
			case <-time.After(sleepDuration):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonData))
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")

		// Add routing header for named databases
		if databaseID != "" {
			// URL-encode values to prevent header injection attacks
			routingHeader := fmt.Sprintf("project_id=%s&database_id=%s", neturl.QueryEscape(projectID), neturl.QueryEscape(databaseID))
			req.Header.Set("X-Goog-Request-Params", routingHeader)
		}

		logger.DebugContext(ctx, "sending request", "url", url, "attempt", attempt+1)

		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
			logger.WarnContext(ctx, "request failed", "error", err, "attempt", attempt+1)
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("request failed after %d attempts: %w", maxRetries, err)
			}
			continue
		}

		// Always close response body
		defer func() { //nolint:revive,gocritic // Defer in loop is intentional - loop exits after successful response
			if closeErr := resp.Body.Close(); closeErr != nil {
				logger.WarnContext(ctx, "failed to close response body", "error", closeErr)
			}
		}()

		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
		if err != nil {
			lastErr = err
			logger.WarnContext(ctx, "failed to read response body", "error", err, "attempt", attempt+1)
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("failed to read response after %d attempts: %w", maxRetries, err)
			}
			continue
		}

		logger.DebugContext(ctx, "received response",
			"status_code", resp.StatusCode,
			"body_size", len(body),
			"attempt", attempt+1)

		// Success
		if resp.StatusCode == http.StatusOK {
			return body, nil
		}

		// Don't retry on 4xx errors (client errors)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			if resp.StatusCode == http.StatusNotFound {
				logger.DebugContext(ctx, "entity not found", "status_code", resp.StatusCode)
			} else {
				logger.WarnContext(ctx, "client error", "status_code", resp.StatusCode, "body", string(body))
			}
			return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
		}

		// Unexpected 2xx/3xx status codes
		if resp.StatusCode < 400 {
			logger.WarnContext(ctx, "unexpected non-200 success status", "status_code", resp.StatusCode)
			return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
		}

		// 5xx errors - retry
		lastErr = fmt.Errorf("server error: status %d", resp.StatusCode)
		logger.WarnContext(ctx, "server error, will retry",
			"status_code", resp.StatusCode,
			"attempt", attempt+1,
			"body", string(body))
	}

	return nil, fmt.Errorf("all %d attempts failed: %w", maxRetries, lastErr)
}

// Get retrieves an entity by key and stores it in dst.
// dst must be a pointer to a struct.
// Returns ErrNoSuchEntity if the key is not found.
func (c *Client) Get(ctx context.Context, key *Key, dst any) error {
	if key == nil {
		c.logger.WarnContext(ctx, "Get called with nil key")
		return errors.New("key cannot be nil")
	}

	c.logger.DebugContext(ctx, "getting entity", "kind", key.Kind, "name", key.Name, "id", key.ID)

	token, err := auth.AccessToken(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to get access token", "error", err)
		return fmt.Errorf("failed to get access token: %w", err)
	}

	reqBody := map[string]any{
		"keys": []map[string]any{keyToJSON(key)},
	}
	if c.databaseID != "" {
		reqBody["databaseId"] = c.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to marshal request", "error", err)
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:lookup", c.baseURL, neturl.PathEscape(c.projectID))
	body, err := doRequest(ctx, c.logger, reqURL, jsonData, token, c.projectID, c.databaseID)
	if err != nil {
		c.logger.ErrorContext(ctx, "lookup request failed", "error", err, "kind", key.Kind)
		return err
	}

	var result struct {
		Found []struct {
			Entity map[string]any `json:"entity"`
		} `json:"found"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		c.logger.ErrorContext(ctx, "failed to parse response", "error", err)
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if len(result.Found) == 0 {
		c.logger.DebugContext(ctx, "entity not found", "kind", key.Kind, "name", key.Name, "id", key.ID)
		return ErrNoSuchEntity
	}

	c.logger.DebugContext(ctx, "entity retrieved successfully", "kind", key.Kind)
	return decodeEntity(result.Found[0].Entity, dst)
}

// Put stores an entity with the given key.
// src must be a struct or pointer to struct.
// Returns the key (useful for auto-generated IDs in the future).
func (c *Client) Put(ctx context.Context, key *Key, src any) (*Key, error) {
	if key == nil {
		c.logger.WarnContext(ctx, "Put called with nil key")
		return nil, errors.New("key cannot be nil")
	}

	c.logger.DebugContext(ctx, "putting entity", "kind", key.Kind, "name", key.Name, "id", key.ID)

	token, err := auth.AccessToken(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to get access token", "error", err)
		return nil, fmt.Errorf("failed to get access token: %w", err)
	}

	entity, err := encodeEntity(key, src)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to encode entity", "error", err, "kind", key.Kind)
		return nil, err
	}

	reqBody := map[string]any{
		"mode":      "NON_TRANSACTIONAL",
		"mutations": []map[string]any{{"upsert": entity}},
	}
	if c.databaseID != "" {
		reqBody["databaseId"] = c.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to marshal request", "error", err)
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:commit", c.baseURL, neturl.PathEscape(c.projectID))
	if _, err := doRequest(ctx, c.logger, reqURL, jsonData, token, c.projectID, c.databaseID); err != nil {
		c.logger.ErrorContext(ctx, "commit request failed", "error", err, "kind", key.Kind)
		return nil, err
	}

	c.logger.DebugContext(ctx, "entity stored successfully", "kind", key.Kind)
	return key, nil
}

// Delete deletes the entity with the given key.
func (c *Client) Delete(ctx context.Context, key *Key) error {
	if key == nil {
		c.logger.WarnContext(ctx, "Delete called with nil key")
		return errors.New("key cannot be nil")
	}

	c.logger.DebugContext(ctx, "deleting entity", "kind", key.Kind, "name", key.Name, "id", key.ID)

	token, err := auth.AccessToken(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to get access token", "error", err)
		return fmt.Errorf("failed to get access token: %w", err)
	}

	reqBody := map[string]any{
		"mode":      "NON_TRANSACTIONAL",
		"mutations": []map[string]any{{"delete": keyToJSON(key)}},
	}
	if c.databaseID != "" {
		reqBody["databaseId"] = c.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to marshal request", "error", err)
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:commit", c.baseURL, neturl.PathEscape(c.projectID))
	if _, err := doRequest(ctx, c.logger, reqURL, jsonData, token, c.projectID, c.databaseID); err != nil {
		c.logger.ErrorContext(ctx, "delete request failed", "error", err, "kind", key.Kind)
		return err
	}

	c.logger.DebugContext(ctx, "entity deleted successfully", "kind", key.Kind)
	return nil
}

// GetMulti retrieves multiple entities by their keys.
// dst must be a pointer to a slice of structs.
// Returns ErrNoSuchEntity if any key is not found.
// This matches the API of cloud.google.com/go/datastore.
func (c *Client) GetMulti(ctx context.Context, keys []*Key, dst any) error {
	if len(keys) == 0 {
		c.logger.WarnContext(ctx, "GetMulti called with no keys")
		return errors.New("keys cannot be empty")
	}

	c.logger.DebugContext(ctx, "getting multiple entities", "count", len(keys))

	token, err := auth.AccessToken(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to get access token", "error", err)
		return fmt.Errorf("failed to get access token: %w", err)
	}

	// Build keys array
	jsonKeys := make([]map[string]any, len(keys))
	for i, key := range keys {
		if key == nil {
			c.logger.WarnContext(ctx, "GetMulti called with nil key", "index", i)
			return fmt.Errorf("key at index %d cannot be nil", i)
		}
		jsonKeys[i] = keyToJSON(key)
	}

	reqBody := map[string]any{
		"keys": jsonKeys,
	}
	if c.databaseID != "" {
		reqBody["databaseId"] = c.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to marshal request", "error", err)
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:lookup", c.baseURL, neturl.PathEscape(c.projectID))
	body, err := doRequest(ctx, c.logger, reqURL, jsonData, token, c.projectID, c.databaseID)
	if err != nil {
		c.logger.ErrorContext(ctx, "lookup request failed", "error", err)
		return err
	}

	var result struct {
		Found []struct {
			Entity map[string]any `json:"entity"`
		} `json:"found"`
		Missing []struct {
			Entity map[string]any `json:"entity"`
		} `json:"missing"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		c.logger.ErrorContext(ctx, "failed to parse response", "error", err)
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if len(result.Missing) > 0 {
		c.logger.DebugContext(ctx, "some entities not found", "missing_count", len(result.Missing))
		return ErrNoSuchEntity
	}

	// Decode into slice
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Slice {
		return errors.New("dst must be a pointer to slice")
	}

	sliceType := v.Elem().Type()
	elemType := sliceType.Elem()

	// Create new slice of correct size
	slice := reflect.MakeSlice(sliceType, 0, len(result.Found))

	for _, found := range result.Found {
		elem := reflect.New(elemType).Elem()
		if err := decodeEntity(found.Entity, elem.Addr().Interface()); err != nil {
			c.logger.ErrorContext(ctx, "failed to decode entity", "error", err)
			return err
		}
		slice = reflect.Append(slice, elem)
	}

	v.Elem().Set(slice)
	c.logger.DebugContext(ctx, "entities retrieved successfully", "count", len(result.Found))
	return nil
}

// PutMulti stores multiple entities with their keys.
// keys and src must have the same length.
// Returns the keys (same as input) and any error.
// This matches the API of cloud.google.com/go/datastore.
func (c *Client) PutMulti(ctx context.Context, keys []*Key, src any) ([]*Key, error) {
	if len(keys) == 0 {
		c.logger.WarnContext(ctx, "PutMulti called with no keys")
		return nil, errors.New("keys cannot be empty")
	}

	c.logger.DebugContext(ctx, "putting multiple entities", "count", len(keys))

	// Verify src is a slice
	v := reflect.ValueOf(src)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Slice {
		return nil, errors.New("src must be a slice")
	}

	if v.Len() != len(keys) {
		return nil, fmt.Errorf("keys and src length mismatch: %d != %d", len(keys), v.Len())
	}

	token, err := auth.AccessToken(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to get access token", "error", err)
		return nil, fmt.Errorf("failed to get access token: %w", err)
	}

	// Build mutations
	mutations := make([]map[string]any, len(keys))
	for i, key := range keys {
		if key == nil {
			c.logger.WarnContext(ctx, "PutMulti called with nil key", "index", i)
			return nil, fmt.Errorf("key at index %d cannot be nil", i)
		}

		entity, err := encodeEntity(key, v.Index(i).Interface())
		if err != nil {
			c.logger.ErrorContext(ctx, "failed to encode entity", "error", err, "index", i)
			return nil, fmt.Errorf("failed to encode entity at index %d: %w", i, err)
		}

		mutations[i] = map[string]any{
			"upsert": entity,
		}
	}

	reqBody := map[string]any{
		"mode":      "NON_TRANSACTIONAL",
		"mutations": mutations,
	}
	if c.databaseID != "" {
		reqBody["databaseId"] = c.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to marshal request", "error", err)
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:commit", c.baseURL, neturl.PathEscape(c.projectID))
	if _, err := doRequest(ctx, c.logger, reqURL, jsonData, token, c.projectID, c.databaseID); err != nil {
		c.logger.ErrorContext(ctx, "commit request failed", "error", err)
		return nil, err
	}

	c.logger.DebugContext(ctx, "entities stored successfully", "count", len(keys))
	return keys, nil
}

// DeleteMulti deletes multiple entities with their keys.
// This matches the API of cloud.google.com/go/datastore.
func (c *Client) DeleteMulti(ctx context.Context, keys []*Key) error {
	if len(keys) == 0 {
		c.logger.WarnContext(ctx, "DeleteMulti called with no keys")
		return errors.New("keys cannot be empty")
	}

	c.logger.DebugContext(ctx, "deleting multiple entities", "count", len(keys))

	token, err := auth.AccessToken(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to get access token", "error", err)
		return fmt.Errorf("failed to get access token: %w", err)
	}

	// Build mutations
	mutations := make([]map[string]any, len(keys))
	for i, key := range keys {
		if key == nil {
			c.logger.WarnContext(ctx, "DeleteMulti called with nil key", "index", i)
			return fmt.Errorf("key at index %d cannot be nil", i)
		}

		mutations[i] = map[string]any{
			"delete": keyToJSON(key),
		}
	}

	reqBody := map[string]any{
		"mode":      "NON_TRANSACTIONAL",
		"mutations": mutations,
	}
	if c.databaseID != "" {
		reqBody["databaseId"] = c.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to marshal request", "error", err)
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:commit", c.baseURL, neturl.PathEscape(c.projectID))
	if _, err := doRequest(ctx, c.logger, reqURL, jsonData, token, c.projectID, c.databaseID); err != nil {
		c.logger.ErrorContext(ctx, "delete request failed", "error", err)
		return err
	}

	c.logger.DebugContext(ctx, "entities deleted successfully", "count", len(keys))
	return nil
}

// DeleteAllByKind deletes all entities of a given kind.
// This method queries for all keys and then deletes them in batches.
func (c *Client) DeleteAllByKind(ctx context.Context, kind string) error {
	c.logger.InfoContext(ctx, "deleting all entities by kind", "kind", kind)

	// Query for all keys of this kind
	q := NewQuery(kind).KeysOnly()
	keys, err := c.AllKeys(ctx, q)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to query keys", "kind", kind, "error", err)
		return fmt.Errorf("failed to query keys: %w", err)
	}

	if len(keys) == 0 {
		c.logger.InfoContext(ctx, "no entities found to delete", "kind", kind)
		return nil
	}

	// Delete all keys
	if err := c.DeleteMulti(ctx, keys); err != nil {
		c.logger.ErrorContext(ctx, "failed to delete entities", "kind", kind, "count", len(keys), "error", err)
		return fmt.Errorf("failed to delete entities: %w", err)
	}

	c.logger.InfoContext(ctx, "deleted all entities", "kind", kind, "count", len(keys))
	return nil
}

// AllocateIDs allocates IDs for incomplete keys.
// Returns keys with IDs filled in. Complete keys are returned unchanged.
// API compatible with cloud.google.com/go/datastore.
func (c *Client) AllocateIDs(ctx context.Context, keys []*Key) ([]*Key, error) {
	if len(keys) == 0 {
		return keys, nil
	}

	c.logger.DebugContext(ctx, "allocating IDs", "count", len(keys))

	// Separate incomplete and complete keys
	var incompleteKeys []*Key
	var incompleteIndices []int
	for i, key := range keys {
		if key != nil && key.Incomplete() {
			incompleteKeys = append(incompleteKeys, key)
			incompleteIndices = append(incompleteIndices, i)
		}
	}

	// If no incomplete keys, return original slice
	if len(incompleteKeys) == 0 {
		c.logger.DebugContext(ctx, "no incomplete keys to allocate")
		return keys, nil
	}

	token, err := auth.AccessToken(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to get access token", "error", err)
		return nil, fmt.Errorf("failed to get access token: %w", err)
	}

	// Build request with incomplete keys
	reqKeys := make([]map[string]any, len(incompleteKeys))
	for i, key := range incompleteKeys {
		reqKeys[i] = keyToJSON(key)
	}

	reqBody := map[string]any{
		"keys": reqKeys,
	}
	if c.databaseID != "" {
		reqBody["databaseId"] = c.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to marshal request", "error", err)
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:allocateIds", c.baseURL, neturl.PathEscape(c.projectID))
	body, err := doRequest(ctx, c.logger, reqURL, jsonData, token, c.projectID, c.databaseID)
	if err != nil {
		c.logger.ErrorContext(ctx, "allocateIds request failed", "error", err)
		return nil, err
	}

	var resp struct {
		Keys []map[string]any `json:"keys"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		c.logger.ErrorContext(ctx, "failed to parse response", "error", err)
		return nil, fmt.Errorf("failed to parse allocateIds response: %w", err)
	}

	// Parse allocated keys
	allocatedKeys := make([]*Key, len(resp.Keys))
	for i, keyData := range resp.Keys {
		key, err := keyFromJSON(keyData)
		if err != nil {
			c.logger.ErrorContext(ctx, "failed to parse allocated key", "index", i, "error", err)
			return nil, fmt.Errorf("failed to parse allocated key at index %d: %w", i, err)
		}
		allocatedKeys[i] = key
	}

	// Create result slice with allocated keys in correct positions
	result := make([]*Key, len(keys))
	copy(result, keys)
	for i, idx := range incompleteIndices {
		result[idx] = allocatedKeys[i]
	}

	c.logger.DebugContext(ctx, "IDs allocated successfully", "count", len(allocatedKeys))
	return result, nil
}

// keyToJSON converts a Key to its JSON representation.
// Supports hierarchical keys with parent relationships.
func keyToJSON(key *Key) map[string]any {
	// Build path from root to leaf (parent -> child)
	var path []map[string]any

	// Collect all keys from root to leaf
	keys := make([]*Key, 0)
	for k := key; k != nil; k = k.Parent {
		keys = append(keys, k)
	}

	// Reverse to go from root to leaf
	for i := len(keys) - 1; i >= 0; i-- {
		k := keys[i]
		elem := map[string]any{
			"kind": k.Kind,
		}

		if k.Name != "" {
			elem["name"] = k.Name
		} else if k.ID != 0 {
			elem["id"] = strconv.FormatInt(k.ID, 10)
		}

		path = append(path, elem)
	}

	return map[string]any{
		"path": path,
	}
}

// encodeEntity converts a Go struct to a Datastore entity.
func encodeEntity(key *Key, src any) (map[string]any, error) {
	v := reflect.ValueOf(src)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, errors.New("src must be a struct or pointer to struct")
	}

	t := v.Type()
	properties := make(map[string]any)

	for i := range v.NumField() {
		field := t.Field(i)
		value := v.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get field name from datastore tag or use field name
		name := field.Name
		noIndex := false

		if tag := field.Tag.Get("datastore"); tag != "" {
			parts := strings.Split(tag, ",")
			if parts[0] != "" && parts[0] != "-" {
				name = parts[0]
			}
			if len(parts) > 1 && parts[1] == "noindex" {
				noIndex = true
			}
			if parts[0] == "-" {
				continue
			}
		}

		prop, err := encodeValue(value.Interface())
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field.Name, err)
		}

		if noIndex {
			if m, ok := prop.(map[string]any); ok {
				m["excludeFromIndexes"] = true
			}
		}

		properties[name] = prop
	}

	return map[string]any{
		"key":        keyToJSON(key),
		"properties": properties,
	}, nil
}

// encodeValue converts a Go value to a Datastore property value.
func encodeValue(v any) (any, error) {
	if v == nil {
		return map[string]any{"nullValue": nil}, nil
	}

	switch val := v.(type) {
	case string:
		return map[string]any{"stringValue": val}, nil
	case int:
		return map[string]any{"integerValue": strconv.Itoa(val)}, nil
	case int64:
		return map[string]any{"integerValue": strconv.FormatInt(val, 10)}, nil
	case int32:
		return map[string]any{"integerValue": strconv.Itoa(int(val))}, nil
	case bool:
		return map[string]any{"booleanValue": val}, nil
	case float64:
		return map[string]any{"doubleValue": val}, nil
	case time.Time:
		return map[string]any{"timestampValue": val.Format(time.RFC3339Nano)}, nil
	case []string:
		values := make([]map[string]any, len(val))
		for i, s := range val {
			values[i] = map[string]any{"stringValue": s}
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
	case []int64:
		values := make([]map[string]any, len(val))
		for i, n := range val {
			values[i] = map[string]any{"integerValue": strconv.FormatInt(n, 10)}
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
	case []int:
		values := make([]map[string]any, len(val))
		for i, n := range val {
			values[i] = map[string]any{"integerValue": strconv.Itoa(n)}
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
	case []float64:
		values := make([]map[string]any, len(val))
		for i, f := range val {
			values[i] = map[string]any{"doubleValue": f}
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
	case []bool:
		values := make([]map[string]any, len(val))
		for i, b := range val {
			values[i] = map[string]any{"booleanValue": b}
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
	default:
		// Try to handle slices/arrays via reflection
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			length := rv.Len()
			values := make([]map[string]any, length)
			for i := range length {
				elem := rv.Index(i).Interface()
				encodedElem, err := encodeValue(elem)
				if err != nil {
					return nil, fmt.Errorf("failed to encode array element %d: %w", i, err)
				}
				// encodedElem is already a map[string]any with the type wrapper
				m, ok := encodedElem.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("unexpected encoded value type for element %d", i)
				}
				values[i] = m
			}
			return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
		}
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
}

// decodeEntity converts a Datastore entity to a Go struct.
func decodeEntity(entity map[string]any, dst any) error {
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return errors.New("dst must be a pointer to struct")
	}

	v = v.Elem()
	t := v.Type()

	properties, ok := entity["properties"].(map[string]any)
	if !ok {
		return errors.New("invalid entity format")
	}

	for i := range v.NumField() {
		field := t.Field(i)
		value := v.Field(i)

		if !field.IsExported() {
			continue
		}

		// Get field name from datastore tag
		name := field.Name
		if tag := field.Tag.Get("datastore"); tag != "" {
			parts := strings.Split(tag, ",")
			if parts[0] != "" && parts[0] != "-" {
				name = parts[0]
			}
			if parts[0] == "-" {
				continue
			}
		}

		prop, ok := properties[name]
		if !ok {
			continue // Field not in entity
		}

		propMap, ok := prop.(map[string]any)
		if !ok {
			continue
		}

		if err := decodeValue(propMap, value); err != nil {
			return fmt.Errorf("field %s: %w", field.Name, err)
		}
	}

	return nil
}

// decodeValue decodes a Datastore property value into a Go reflect.Value.
func decodeValue(prop map[string]any, dst reflect.Value) error {
	// Handle each type
	if val, ok := prop["stringValue"]; ok {
		if dst.Kind() == reflect.String {
			if s, ok := val.(string); ok {
				dst.SetString(s)
				return nil
			}
		}
	}

	if val, ok := prop["integerValue"]; ok {
		var intVal int64
		switch v := val.(type) {
		case string:
			if _, err := fmt.Sscanf(v, "%d", &intVal); err != nil {
				return fmt.Errorf("invalid integer format: %w", err)
			}
		case float64:
			intVal = int64(v)
		}

		switch dst.Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32:
			dst.SetInt(intVal)
			return nil
		default:
			return fmt.Errorf("unsupported integer type: %v", dst.Kind())
		}
	}

	if val, ok := prop["booleanValue"]; ok {
		if dst.Kind() == reflect.Bool {
			if b, ok := val.(bool); ok {
				dst.SetBool(b)
				return nil
			}
		}
	}

	if val, ok := prop["doubleValue"]; ok {
		if dst.Kind() == reflect.Float64 {
			if f, ok := val.(float64); ok {
				dst.SetFloat(f)
				return nil
			}
		}
	}

	if val, ok := prop["timestampValue"]; ok {
		if dst.Type() == reflect.TypeOf(time.Time{}) {
			if s, ok := val.(string); ok {
				t, err := time.Parse(time.RFC3339Nano, s)
				if err != nil {
					return err
				}
				dst.Set(reflect.ValueOf(t))
				return nil
			}
		}
	}

	if val, ok := prop["arrayValue"]; ok {
		if dst.Kind() != reflect.Slice {
			return fmt.Errorf("cannot decode array into non-slice type: %s", dst.Type())
		}

		arrayMap, ok := val.(map[string]any)
		if !ok {
			return errors.New("invalid arrayValue format")
		}

		valuesAny, ok := arrayMap["values"]
		if !ok {
			// Empty array
			dst.Set(reflect.MakeSlice(dst.Type(), 0, 0))
			return nil
		}

		values, ok := valuesAny.([]any)
		if !ok {
			return errors.New("invalid arrayValue.values format")
		}

		// Create slice with appropriate capacity
		slice := reflect.MakeSlice(dst.Type(), len(values), len(values))

		// Decode each element
		for i, elemAny := range values {
			elemMap, ok := elemAny.(map[string]any)
			if !ok {
				return fmt.Errorf("invalid array element %d format", i)
			}

			elemValue := slice.Index(i)
			if err := decodeValue(elemMap, elemValue); err != nil {
				return fmt.Errorf("failed to decode array element %d: %w", i, err)
			}
		}

		dst.Set(slice)
		return nil
	}

	if _, ok := prop["nullValue"]; ok {
		// Set to zero value
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}

	return fmt.Errorf("unsupported property type for %s", dst.Type())
}

// Query represents a Datastore query.
type Query struct {
	ancestor    *Key
	kind        string
	filters     []queryFilter
	orders      []queryOrder
	projection  []string
	distinctOn  []string
	namespace   string
	startCursor Cursor
	endCursor   Cursor
	limit       int
	offset      int
	keysOnly    bool
}

type queryFilter struct {
	value    any
	property string
	operator string
}

type queryOrder struct {
	property  string
	direction string // "ASCENDING" or "DESCENDING"
}

// NewQuery creates a new query for the given kind.
func NewQuery(kind string) *Query {
	return &Query{
		kind: kind,
	}
}

// KeysOnly configures the query to return only keys, not full entities.
func (q *Query) KeysOnly() *Query {
	q.keysOnly = true
	return q
}

// Limit sets the maximum number of results to return.
func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	return q
}

// Offset sets the number of results to skip before returning.
// API compatible with cloud.google.com/go/datastore.
func (q *Query) Offset(offset int) *Query {
	q.offset = offset
	return q
}

// Filter adds a property filter to the query.
// The filterStr should be in the format "Property Operator" (e.g., "Count >", "Name =").
// Deprecated: Use FilterField instead. API compatible with cloud.google.com/go/datastore.
func (q *Query) Filter(filterStr string, value any) *Query {
	// Parse the filter string to extract property and operator
	parts := strings.Fields(filterStr)
	if len(parts) != 2 {
		// Invalid filter format, but we'll be lenient
		return q
	}

	property := parts[0]
	op := parts[1]

	operator, ok := operatorMap[op]
	if !ok {
		operator = "EQUAL"
	}

	q.filters = append(q.filters, queryFilter{
		property: property,
		operator: operator,
		value:    value,
	})

	return q
}

// FilterField adds a property filter to the query with explicit operator.
// API compatible with cloud.google.com/go/datastore.
func (q *Query) FilterField(fieldName, operator string, value any) *Query {
	dsOperator, ok := operatorMap[operator]
	if !ok {
		dsOperator = operator // Use as-is if not in map (might already be EQUAL, etc.)
	}

	q.filters = append(q.filters, queryFilter{
		property: fieldName,
		operator: dsOperator,
		value:    value,
	})

	return q
}

// Order sets the order in which results are returned.
// Prefix the property name with "-" for descending order (e.g., "-Created").
// API compatible with cloud.google.com/go/datastore.
func (q *Query) Order(fieldName string) *Query {
	direction := "ASCENDING"
	property := fieldName

	if strings.HasPrefix(fieldName, "-") {
		direction = "DESCENDING"
		property = fieldName[1:]
	}

	q.orders = append(q.orders, queryOrder{
		property:  property,
		direction: direction,
	})

	return q
}

// Ancestor sets an ancestor filter for the query.
// Only entities with the given ancestor will be returned.
// API compatible with cloud.google.com/go/datastore.
func (q *Query) Ancestor(ancestor *Key) *Query {
	q.ancestor = ancestor
	return q
}

// Project sets the fields to be projected (returned) in the query results.
// API compatible with cloud.google.com/go/datastore.
func (q *Query) Project(fieldNames ...string) *Query {
	q.projection = fieldNames
	return q
}

// Distinct marks the query to return only distinct results.
// This is equivalent to DistinctOn with all projected fields.
// API compatible with cloud.google.com/go/datastore.
func (q *Query) Distinct() *Query {
	// Distinct without fields means distinct on projection
	// This will be handled in buildQueryMap
	if len(q.projection) > 0 {
		q.distinctOn = q.projection
	}
	return q
}

// DistinctOn returns a query that removes duplicates based on the given field names.
// API compatible with cloud.google.com/go/datastore.
func (q *Query) DistinctOn(fieldNames ...string) *Query {
	q.distinctOn = fieldNames
	return q
}

// Namespace sets the namespace for the query.
// API compatible with cloud.google.com/go/datastore.
func (q *Query) Namespace(ns string) *Query {
	q.namespace = ns
	return q
}

// Start sets the starting cursor for the query results.
// API compatible with cloud.google.com/go/datastore.
func (q *Query) Start(c Cursor) *Query {
	q.startCursor = c
	return q
}

// End sets the ending cursor for the query results.
// API compatible with cloud.google.com/go/datastore.
func (q *Query) End(c Cursor) *Query {
	q.endCursor = c
	return q
}

// buildQueryMap creates a Datastore API query map from a Query object.
func buildQueryMap(query *Query) map[string]any {
	queryMap := map[string]any{
		"kind": []map[string]any{{"name": query.kind}},
	}

	// Add namespace via partition ID if specified
	if query.namespace != "" {
		queryMap["partitionId"] = map[string]any{
			"namespaceId": query.namespace,
		}
	}

	// Add filters
	if len(query.filters) > 0 {
		var compositeFilters []map[string]any
		for _, f := range query.filters {
			encodedVal, err := encodeValue(f.value)
			if err != nil {
				// Skip invalid filters
				continue
			}
			compositeFilters = append(compositeFilters, map[string]any{
				"propertyFilter": map[string]any{
					"property": map[string]string{"name": f.property},
					"op":       f.operator,
					"value":    encodedVal,
				},
			})
		}

		if len(compositeFilters) == 1 {
			queryMap["filter"] = compositeFilters[0]
		} else if len(compositeFilters) > 1 {
			queryMap["filter"] = map[string]any{
				"compositeFilter": map[string]any{
					"op":      "AND",
					"filters": compositeFilters,
				},
			}
		}
	}

	// Add ancestor filter
	if query.ancestor != nil {
		ancestorFilter := map[string]any{
			"propertyFilter": map[string]any{
				"property": map[string]string{"name": "__key__"},
				"op":       "HAS_ANCESTOR",
				"value":    map[string]any{"keyValue": keyToJSON(query.ancestor)},
			},
		}

		// Combine with existing filters if present
		if existingFilter, ok := queryMap["filter"]; ok {
			existingMap, ok := existingFilter.(map[string]any)
			if !ok {
				// Skip if filter is invalid
				queryMap["filter"] = ancestorFilter
			} else {
				queryMap["filter"] = map[string]any{
					"compositeFilter": map[string]any{
						"op":      "AND",
						"filters": []map[string]any{existingMap, ancestorFilter},
					},
				}
			}
		} else {
			queryMap["filter"] = ancestorFilter
		}
	}

	// Add ordering
	if len(query.orders) > 0 {
		var orders []map[string]any
		for _, o := range query.orders {
			orders = append(orders, map[string]any{
				"property":  map[string]string{"name": o.property},
				"direction": o.direction,
			})
		}
		queryMap["order"] = orders
	}

	// Add projection
	if len(query.projection) > 0 {
		var projections []map[string]any
		for _, field := range query.projection {
			projections = append(projections, map[string]any{
				"property": map[string]string{"name": field},
			})
		}
		queryMap["projection"] = projections
	} else if query.keysOnly {
		// Keys-only projection
		queryMap["projection"] = []map[string]any{{"property": map[string]string{"name": "__key__"}}}
	}

	// Add distinct on
	if len(query.distinctOn) > 0 {
		var distinctFields []map[string]any
		for _, field := range query.distinctOn {
			distinctFields = append(distinctFields, map[string]any{
				"property": map[string]string{"name": field},
			})
		}
		queryMap["distinctOn"] = distinctFields
	}

	// Add limit
	if query.limit > 0 {
		queryMap["limit"] = query.limit
	}

	// Add offset
	if query.offset > 0 {
		queryMap["offset"] = query.offset
	}

	// Add cursors
	if query.startCursor != "" {
		queryMap["startCursor"] = string(query.startCursor)
	}
	if query.endCursor != "" {
		queryMap["endCursor"] = string(query.endCursor)
	}

	return queryMap
}

// AllKeys returns all keys matching the query.
// This is a convenience method for KeysOnly queries.
func (c *Client) AllKeys(ctx context.Context, q *Query) ([]*Key, error) {
	if !q.keysOnly {
		c.logger.WarnContext(ctx, "AllKeys called on non-KeysOnly query")
		return nil, errors.New("AllKeys requires KeysOnly query")
	}

	c.logger.DebugContext(ctx, "querying for keys", "kind", q.kind, "limit", q.limit)

	token, err := auth.AccessToken(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to get access token", "error", err)
		return nil, fmt.Errorf("failed to get access token: %w", err)
	}

	query := buildQueryMap(q)

	reqBody := map[string]any{"query": query}
	if c.databaseID != "" {
		reqBody["databaseId"] = c.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to marshal request", "error", err)
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:runQuery", c.baseURL, neturl.PathEscape(c.projectID))
	body, err := doRequest(ctx, c.logger, reqURL, jsonData, token, c.projectID, c.databaseID)
	if err != nil {
		c.logger.ErrorContext(ctx, "query request failed", "error", err, "kind", q.kind)
		return nil, err
	}

	var result struct {
		Batch struct {
			EntityResults []struct {
				Entity map[string]any `json:"entity"`
			} `json:"entityResults"`
		} `json:"batch"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		c.logger.ErrorContext(ctx, "failed to parse response", "error", err)
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	keys := make([]*Key, 0, len(result.Batch.EntityResults))
	for _, er := range result.Batch.EntityResults {
		key, err := keyFromJSON(er.Entity["key"])
		if err != nil {
			c.logger.ErrorContext(ctx, "failed to parse key from response", "error", err)
			return nil, err
		}
		keys = append(keys, key)
	}

	c.logger.DebugContext(ctx, "query completed successfully", "kind", q.kind, "keys_found", len(keys))
	return keys, nil
}

// GetAll retrieves all entities matching the query and stores them in dst.
// dst must be a pointer to a slice of structs.
// Returns the keys of the retrieved entities and any error.
// This matches the API of cloud.google.com/go/datastore.
func (c *Client) GetAll(ctx context.Context, query *Query, dst any) ([]*Key, error) {
	c.logger.DebugContext(ctx, "querying for entities", "kind", query.kind, "limit", query.limit)

	token, err := auth.AccessToken(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to get access token", "error", err)
		return nil, fmt.Errorf("failed to get access token: %w", err)
	}

	queryObj := buildQueryMap(query)

	reqBody := map[string]any{"query": queryObj}
	if c.databaseID != "" {
		reqBody["databaseId"] = c.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to marshal request", "error", err)
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:runQuery", c.baseURL, neturl.PathEscape(c.projectID))
	body, err := doRequest(ctx, c.logger, reqURL, jsonData, token, c.projectID, c.databaseID)
	if err != nil {
		c.logger.ErrorContext(ctx, "query request failed", "error", err, "kind", query.kind)
		return nil, err
	}

	var result struct {
		Batch struct {
			EntityResults []struct {
				Entity map[string]any `json:"entity"`
			} `json:"entityResults"`
		} `json:"batch"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		c.logger.ErrorContext(ctx, "failed to parse response", "error", err)
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Verify dst is a pointer to slice
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Slice {
		return nil, errors.New("dst must be a pointer to slice")
	}

	sliceType := v.Elem().Type()
	elemType := sliceType.Elem()

	// Create new slice of correct size
	slice := reflect.MakeSlice(sliceType, 0, len(result.Batch.EntityResults))
	keys := make([]*Key, 0, len(result.Batch.EntityResults))

	for _, er := range result.Batch.EntityResults {
		// Extract key
		key, err := keyFromJSON(er.Entity["key"])
		if err != nil {
			c.logger.ErrorContext(ctx, "failed to parse key from response", "error", err)
			return nil, err
		}
		keys = append(keys, key)

		// Decode entity
		elem := reflect.New(elemType).Elem()
		if err := decodeEntity(er.Entity, elem.Addr().Interface()); err != nil {
			c.logger.ErrorContext(ctx, "failed to decode entity", "error", err)
			return nil, err
		}
		slice = reflect.Append(slice, elem)
	}

	v.Elem().Set(slice)
	c.logger.DebugContext(ctx, "query completed successfully", "kind", query.kind, "entities_found", len(keys))
	return keys, nil
}

// Count returns the number of entities matching the query.
// Deprecated: Use aggregation queries with RunAggregationQuery instead.
// API compatible with cloud.google.com/go/datastore.
func (c *Client) Count(ctx context.Context, q *Query) (int, error) {
	c.logger.DebugContext(ctx, "counting entities", "kind", q.kind)

	token, err := auth.AccessToken(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to get access token", "error", err)
		return 0, fmt.Errorf("failed to get access token: %w", err)
	}

	// Build aggregation query with COUNT
	queryObj := buildQueryMap(q)
	aggregationQuery := map[string]any{
		"aggregations": []map[string]any{
			{
				"alias": "total",
				"count": map[string]any{},
			},
		},
		"nestedQuery": queryObj,
	}

	reqBody := map[string]any{
		"aggregationQuery": aggregationQuery,
	}
	if c.databaseID != "" {
		reqBody["databaseId"] = c.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to marshal request", "error", err)
		return 0, fmt.Errorf("failed to marshal request: %w", err)
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:runAggregationQuery", c.baseURL, neturl.PathEscape(c.projectID))
	body, err := doRequest(ctx, c.logger, reqURL, jsonData, token, c.projectID, c.databaseID)
	if err != nil {
		c.logger.ErrorContext(ctx, "count query failed", "error", err, "kind", q.kind)
		return 0, err
	}

	var result struct {
		Batch struct {
			AggregationResults []struct {
				AggregateProperties map[string]struct {
					IntegerValue string `json:"integerValue"`
				} `json:"aggregateProperties"`
			} `json:"aggregationResults"`
		} `json:"batch"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		c.logger.ErrorContext(ctx, "failed to parse response", "error", err)
		return 0, fmt.Errorf("failed to parse count response: %w", err)
	}

	if len(result.Batch.AggregationResults) == 0 {
		c.logger.DebugContext(ctx, "no results returned", "kind", q.kind)
		return 0, nil
	}

	// Extract count from total aggregation
	countVal, ok := result.Batch.AggregationResults[0].AggregateProperties["total"]
	if !ok {
		c.logger.ErrorContext(ctx, "count not found in response")
		return 0, errors.New("count not found in aggregation response")
	}

	count, err := strconv.Atoi(countVal.IntegerValue)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to parse count", "error", err, "value", countVal.IntegerValue)
		return 0, fmt.Errorf("failed to parse count: %w", err)
	}

	c.logger.DebugContext(ctx, "count completed successfully", "kind", q.kind, "count", count)
	return count, nil
}

// Run executes the query and returns an iterator for the results.
// API compatible with cloud.google.com/go/datastore.
func (c *Client) Run(ctx context.Context, q *Query) *Iterator {
	return &Iterator{
		ctx:       ctx,
		client:    c,
		query:     q,
		fetchNext: true,
	}
}

// MutationOp represents the type of mutation operation.
type MutationOp string

const (
	// MutationInsert represents an insert operation.
	MutationInsert MutationOp = "insert"
	// MutationUpdate represents an update operation.
	MutationUpdate MutationOp = "update"
	// MutationUpsert represents an upsert operation.
	MutationUpsert MutationOp = "upsert"
	// MutationDelete represents a delete operation.
	MutationDelete MutationOp = "delete"
)

// Mutation represents a pending datastore mutation.
type Mutation struct {
	op     MutationOp
	key    *Key
	entity any
}

// NewInsert creates an insert mutation.
// API compatible with cloud.google.com/go/datastore.
func NewInsert(k *Key, src any) *Mutation {
	return &Mutation{
		op:     MutationInsert,
		key:    k,
		entity: src,
	}
}

// NewUpdate creates an update mutation.
// API compatible with cloud.google.com/go/datastore.
func NewUpdate(k *Key, src any) *Mutation {
	return &Mutation{
		op:     MutationUpdate,
		key:    k,
		entity: src,
	}
}

// NewUpsert creates an upsert mutation.
// API compatible with cloud.google.com/go/datastore.
func NewUpsert(k *Key, src any) *Mutation {
	return &Mutation{
		op:     MutationUpsert,
		key:    k,
		entity: src,
	}
}

// NewDelete creates a delete mutation.
// API compatible with cloud.google.com/go/datastore.
func NewDelete(k *Key) *Mutation {
	return &Mutation{
		op:  MutationDelete,
		key: k,
	}
}

// Mutate applies one or more mutations atomically.
// API compatible with cloud.google.com/go/datastore.
func (c *Client) Mutate(ctx context.Context, muts ...*Mutation) ([]*Key, error) {
	if len(muts) == 0 {
		return nil, nil
	}

	c.logger.DebugContext(ctx, "applying mutations", "count", len(muts))

	token, err := auth.AccessToken(ctx)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to get access token", "error", err)
		return nil, fmt.Errorf("failed to get access token: %w", err)
	}

	// Build mutations array
	mutations := make([]map[string]any, 0, len(muts))
	for i, mut := range muts {
		if mut == nil {
			c.logger.ErrorContext(ctx, "nil mutation", "index", i)
			return nil, fmt.Errorf("mutation at index %d is nil", i)
		}
		if mut.key == nil {
			c.logger.ErrorContext(ctx, "nil key in mutation", "index", i)
			return nil, fmt.Errorf("mutation at index %d has nil key", i)
		}

		mutMap := make(map[string]any)

		switch mut.op {
		case MutationInsert:
			if mut.entity == nil {
				c.logger.ErrorContext(ctx, "nil entity for insert", "index", i)
				return nil, fmt.Errorf("insert mutation at index %d has nil entity", i)
			}
			entity, err := encodeEntity(mut.key, mut.entity)
			if err != nil {
				c.logger.ErrorContext(ctx, "failed to encode entity", "index", i, "error", err)
				return nil, fmt.Errorf("failed to encode entity at index %d: %w", i, err)
			}
			mutMap["insert"] = entity

		case MutationUpdate:
			if mut.entity == nil {
				c.logger.ErrorContext(ctx, "nil entity for update", "index", i)
				return nil, fmt.Errorf("update mutation at index %d has nil entity", i)
			}
			entity, err := encodeEntity(mut.key, mut.entity)
			if err != nil {
				c.logger.ErrorContext(ctx, "failed to encode entity", "index", i, "error", err)
				return nil, fmt.Errorf("failed to encode entity at index %d: %w", i, err)
			}
			mutMap["update"] = entity

		case MutationUpsert:
			if mut.entity == nil {
				c.logger.ErrorContext(ctx, "nil entity for upsert", "index", i)
				return nil, fmt.Errorf("upsert mutation at index %d has nil entity", i)
			}
			entity, err := encodeEntity(mut.key, mut.entity)
			if err != nil {
				c.logger.ErrorContext(ctx, "failed to encode entity", "index", i, "error", err)
				return nil, fmt.Errorf("failed to encode entity at index %d: %w", i, err)
			}
			mutMap["upsert"] = entity

		case MutationDelete:
			mutMap["delete"] = keyToJSON(mut.key)

		default:
			c.logger.ErrorContext(ctx, "unknown mutation operation", "index", i, "op", mut.op)
			return nil, fmt.Errorf("unknown mutation operation at index %d: %s", i, mut.op)
		}

		mutations = append(mutations, mutMap)
	}

	reqBody := map[string]any{
		"mutations": mutations,
	}
	if c.databaseID != "" {
		reqBody["databaseId"] = c.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		c.logger.ErrorContext(ctx, "failed to marshal request", "error", err)
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:commit", c.baseURL, neturl.PathEscape(c.projectID))
	body, err := doRequest(ctx, c.logger, reqURL, jsonData, token, c.projectID, c.databaseID)
	if err != nil {
		c.logger.ErrorContext(ctx, "mutate request failed", "error", err)
		return nil, err
	}

	var resp struct {
		MutationResults []struct {
			Key map[string]any `json:"key"`
		} `json:"mutationResults"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		c.logger.ErrorContext(ctx, "failed to parse response", "error", err)
		return nil, fmt.Errorf("failed to parse mutate response: %w", err)
	}

	// Extract resulting keys
	keys := make([]*Key, len(resp.MutationResults))
	for i, result := range resp.MutationResults {
		if result.Key != nil {
			key, err := keyFromJSON(result.Key)
			if err != nil {
				c.logger.ErrorContext(ctx, "failed to parse key", "index", i, "error", err)
				return nil, fmt.Errorf("failed to parse key at index %d: %w", i, err)
			}
			keys[i] = key
		} else {
			// For deletes, use the original key
			keys[i] = muts[i].key
		}
	}

	c.logger.DebugContext(ctx, "mutations applied successfully", "count", len(keys))
	return keys, nil
}

// keyFromJSON converts a JSON key representation to a Key.
func keyFromJSON(keyData any) (*Key, error) {
	keyMap, ok := keyData.(map[string]any)
	if !ok {
		return nil, errors.New("invalid key format")
	}

	path, ok := keyMap["path"].([]any)
	if !ok || len(path) == 0 {
		return nil, errors.New("invalid key path")
	}

	// Build key hierarchy from path elements
	var key *Key
	for _, elem := range path {
		elemMap, ok := elem.(map[string]any)
		if !ok {
			return nil, errors.New("invalid path element")
		}

		newKey := &Key{
			Parent: key,
		}

		if kind, ok := elemMap["kind"].(string); ok {
			newKey.Kind = kind
		}

		if name, ok := elemMap["name"].(string); ok {
			newKey.Name = name
		} else if idVal, exists := elemMap["id"]; exists {
			switch id := idVal.(type) {
			case string:
				if _, err := fmt.Sscanf(id, "%d", &newKey.ID); err != nil {
					return nil, fmt.Errorf("invalid ID format: %w", err)
				}
			case float64:
				newKey.ID = int64(id)
			}
		}

		key = newKey
	}

	return key, nil
}

// Commit represents the result of a committed transaction.
// This is provided for API compatibility with cloud.google.com/go/datastore.
type Commit struct{}

// Transaction represents a Datastore transaction.
// Note: This struct stores context for API compatibility with Google's official
// cloud.google.com/go/datastore library, which uses the same pattern.
type Transaction struct {
	ctx       context.Context //nolint:containedctx // Required for API compatibility with cloud.google.com/go/datastore
	client    *Client
	id        string
	mutations []map[string]any
}

// TransactionOption configures transaction behavior.
type TransactionOption interface {
	apply(*transactionSettings)
}

type transactionSettings struct {
	readTime    time.Time
	maxAttempts int
}

type maxAttemptsOption int

func (o maxAttemptsOption) apply(s *transactionSettings) {
	s.maxAttempts = int(o)
}

// MaxAttempts returns a TransactionOption that specifies the maximum number
// of times a transaction should be attempted before giving up.
func MaxAttempts(n int) TransactionOption {
	return maxAttemptsOption(n)
}

type readTimeOption struct {
	t time.Time
}

func (o readTimeOption) apply(s *transactionSettings) {
	s.readTime = o.t
}

// WithReadTime returns a TransactionOption that sets a specific timestamp
// at which to read data, enabling reading from a particular snapshot in time.
func WithReadTime(t time.Time) TransactionOption {
	return readTimeOption{t: t}
}

// NewTransaction creates a new transaction.
// The caller must call Commit or Rollback when done.
// API compatible with cloud.google.com/go/datastore.
func (c *Client) NewTransaction(ctx context.Context, opts ...TransactionOption) (*Transaction, error) {
	settings := transactionSettings{
		maxAttempts: 3, // default (not used for NewTransaction, but kept for consistency)
	}
	for _, opt := range opts {
		opt.apply(&settings)
	}

	token, err := auth.AccessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get access token: %w", err)
	}

	// Begin transaction
	reqBody := map[string]any{}
	if c.databaseID != "" {
		reqBody["databaseId"] = c.databaseID
	}

	// Add transaction options if needed
	if !settings.readTime.IsZero() {
		reqBody["transactionOptions"] = map[string]any{
			"readOnly": map[string]any{
				"readTime": settings.readTime.Format(time.RFC3339Nano),
			},
		}
	} else {
		reqBody["transactionOptions"] = map[string]any{
			"readWrite": map[string]any{},
		}
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:beginTransaction", c.baseURL, neturl.PathEscape(c.projectID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(jsonData))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	// Add routing header for named databases
	if c.databaseID != "" {
		// URL-encode values to prevent header injection attacks
		routingHeader := fmt.Sprintf("project_id=%s&database_id=%s", neturl.QueryEscape(c.projectID), neturl.QueryEscape(c.databaseID))
		req.Header.Set("X-Goog-Request-Params", routingHeader)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
	closeErr := resp.Body.Close()
	if closeErr != nil {
		c.logger.Warn("failed to close response body", "error", closeErr)
	}
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("begin transaction failed with status %d: %s", resp.StatusCode, string(body))
	}

	var txResp struct {
		Transaction string `json:"transaction"`
	}

	if err := json.Unmarshal(body, &txResp); err != nil {
		return nil, fmt.Errorf("failed to parse transaction response: %w", err)
	}

	tx := &Transaction{
		ctx:    ctx,
		client: c,
		id:     txResp.Transaction,
	}

	return tx, nil
}

// RunInTransaction runs a function in a transaction.
// The function should use the transaction's Get and Put methods.
// API compatible with cloud.google.com/go/datastore.
func (c *Client) RunInTransaction(ctx context.Context, f func(*Transaction) error, opts ...TransactionOption) (*Commit, error) {
	settings := transactionSettings{
		maxAttempts: 3, // default
	}
	for _, opt := range opts {
		opt.apply(&settings)
	}

	var lastErr error

	for attempt := range settings.maxAttempts {
		token, err := auth.AccessToken(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get access token: %w", err)
		}

		// Begin transaction
		reqBody := map[string]any{}
		if c.databaseID != "" {
			reqBody["databaseId"] = c.databaseID
		}

		// Add transaction options if needed
		if !settings.readTime.IsZero() {
			reqBody["transactionOptions"] = map[string]any{
				"readOnly": map[string]any{
					"readTime": settings.readTime.Format(time.RFC3339Nano),
				},
			}
		} else {
			reqBody["transactionOptions"] = map[string]any{
				"readWrite": map[string]any{},
			}
		}

		jsonData, err := json.Marshal(reqBody)
		if err != nil {
			return nil, err
		}

		// URL-encode project ID to prevent injection attacks
		reqURL := fmt.Sprintf("%s/projects/%s:beginTransaction", c.baseURL, neturl.PathEscape(c.projectID))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(jsonData))
		if err != nil {
			return nil, err
		}

		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")

		// Add routing header for named databases
		if c.databaseID != "" {
			// URL-encode values to prevent header injection attacks
			routingHeader := fmt.Sprintf("project_id=%s&database_id=%s", neturl.QueryEscape(c.projectID), neturl.QueryEscape(c.databaseID))
			req.Header.Set("X-Goog-Request-Params", routingHeader)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
		closeErr := resp.Body.Close()
		if closeErr != nil {
			c.logger.Warn("failed to close response body", "error", closeErr)
		}
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("begin transaction failed with status %d: %s", resp.StatusCode, string(body))
		}

		var txResp struct {
			Transaction string `json:"transaction"`
		}

		if err := json.Unmarshal(body, &txResp); err != nil {
			return nil, fmt.Errorf("failed to parse transaction response: %w", err)
		}

		tx := &Transaction{
			ctx:    ctx,
			client: c,
			id:     txResp.Transaction,
		}

		// Run the function
		if err := f(tx); err != nil {
			// Rollback is implicit if commit is not called
			return nil, err
		}

		// Commit the transaction
		err = tx.doCommit(ctx, token)
		if err == nil {
			c.logger.Debug("transaction committed successfully", "attempt", attempt+1)
			return &Commit{}, nil // Success
		}

		c.logger.Warn("transaction commit failed", "attempt", attempt+1, "error", err)

		// Check if error contains 409 ABORTED - if so, retry
		errStr := err.Error()
		is409 := strings.Contains(errStr, "status 409")
		isAborted := strings.Contains(errStr, "ABORTED")

		if is409 || isAborted {
			lastErr = err
			c.logger.Warn("transaction aborted, will retry",
				"attempt", attempt+1,
				"max_attempts", settings.maxAttempts,
				"has_409", is409,
				"has_ABORTED", isAborted,
				"error", err)

			// Exponential backoff: 100ms, 200ms, 400ms
			if attempt < settings.maxAttempts-1 {
				backoffMS := 100 * (1 << attempt)
				c.logger.Debug("sleeping before retry", "backoff_ms", backoffMS)
				time.Sleep(time.Duration(backoffMS) * time.Millisecond)
			}
			continue
		}

		// Non-retriable error
		c.logger.Warn("non-retriable transaction error", "error", err)
		return nil, err
	}

	return nil, fmt.Errorf("transaction failed after %d attempts: %w", settings.maxAttempts, lastErr)
}

// Get retrieves an entity within the transaction.
// API compatible with cloud.google.com/go/datastore.
func (tx *Transaction) Get(key *Key, dst any) error {
	if key == nil {
		return errors.New("key cannot be nil")
	}

	token, err := auth.AccessToken(tx.ctx)
	if err != nil {
		return fmt.Errorf("failed to get access token: %w", err)
	}

	reqBody := map[string]any{
		"keys": []map[string]any{
			keyToJSON(key),
		},
		"readOptions": map[string]any{
			"transaction": tx.id,
		},
	}

	if tx.client.databaseID != "" {
		reqBody["databaseId"] = tx.client.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:lookup", tx.client.baseURL, neturl.PathEscape(tx.client.projectID))
	req, err := http.NewRequestWithContext(tx.ctx, http.MethodPost, reqURL, bytes.NewReader(jsonData))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	// Add routing header for named databases
	if tx.client.databaseID != "" {
		// URL-encode values to prevent header injection attacks
		routingHeader := fmt.Sprintf("project_id=%s&database_id=%s",
			neturl.QueryEscape(tx.client.projectID),
			neturl.QueryEscape(tx.client.databaseID))
		req.Header.Set("X-Goog-Request-Params", routingHeader)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			tx.client.logger.Warn("failed to close response body", "error", closeErr)
		}
	}()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("transaction get failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Found []struct {
			Entity map[string]any `json:"entity"`
		} `json:"found"`
		Missing []struct{} `json:"missing"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if len(result.Found) == 0 {
		return ErrNoSuchEntity
	}

	return decodeEntity(result.Found[0].Entity, dst)
}

// Put stores an entity within the transaction.
func (tx *Transaction) Put(key *Key, src any) (*Key, error) {
	if key == nil {
		return nil, errors.New("key cannot be nil")
	}

	// Encode the entity
	entity, err := encodeEntity(key, src)
	if err != nil {
		return nil, err
	}

	// Create mutation
	mutation := map[string]any{
		"upsert": entity,
	}

	// Accumulate mutation for commit
	tx.mutations = append(tx.mutations, mutation)

	return key, nil
}

// Delete deletes an entity within the transaction.
// API compatible with cloud.google.com/go/datastore.
func (tx *Transaction) Delete(key *Key) error {
	if key == nil {
		return errors.New("key cannot be nil")
	}

	// Create delete mutation
	mutation := map[string]any{
		"delete": keyToJSON(key),
	}

	// Accumulate mutation for commit
	tx.mutations = append(tx.mutations, mutation)

	return nil
}

// DeleteMulti deletes multiple entities within the transaction.
// API compatible with cloud.google.com/go/datastore.
func (tx *Transaction) DeleteMulti(keys []*Key) error {
	for _, key := range keys {
		if err := tx.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// GetMulti retrieves multiple entities within the transaction.
// API compatible with cloud.google.com/go/datastore.
func (tx *Transaction) GetMulti(keys []*Key, dst any) error {
	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr || dstVal.Elem().Kind() != reflect.Slice {
		return errors.New("dst must be a pointer to a slice")
	}

	slice := dstVal.Elem()
	if len(keys) != slice.Len() {
		return fmt.Errorf("keys and dst slices must have same length: %d vs %d", len(keys), slice.Len())
	}

	// Get each entity individually within the transaction
	for i, key := range keys {
		elem := slice.Index(i)
		if elem.Kind() == reflect.Ptr {
			// dst is []*Entity
			if elem.IsNil() {
				elem.Set(reflect.New(elem.Type().Elem()))
			}
			if err := tx.Get(key, elem.Interface()); err != nil {
				return err
			}
		} else {
			// dst is []Entity
			if err := tx.Get(key, elem.Addr().Interface()); err != nil {
				return err
			}
		}
	}

	return nil
}

// PutMulti stores multiple entities within the transaction.
// API compatible with cloud.google.com/go/datastore.
func (tx *Transaction) PutMulti(keys []*Key, src any) ([]*Key, error) {
	srcVal := reflect.ValueOf(src)
	if srcVal.Kind() != reflect.Slice {
		return nil, errors.New("src must be a slice")
	}

	if len(keys) != srcVal.Len() {
		return nil, fmt.Errorf("keys and src slices must have same length: %d vs %d", len(keys), srcVal.Len())
	}

	// Put each entity individually within the transaction
	for i, key := range keys {
		elem := srcVal.Index(i)
		var src any
		if elem.Kind() == reflect.Ptr {
			src = elem.Interface()
		} else {
			src = elem.Addr().Interface()
		}

		if _, err := tx.Put(key, src); err != nil {
			return nil, err
		}
	}

	return keys, nil
}

// Commit applies the transaction's mutations.
// API compatible with cloud.google.com/go/datastore.
func (tx *Transaction) Commit() (*Commit, error) {
	token, err := auth.AccessToken(tx.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get access token: %w", err)
	}

	if err := tx.doCommit(tx.ctx, token); err != nil {
		return nil, err
	}

	return &Commit{}, nil
}

// Rollback abandons the transaction.
// API compatible with cloud.google.com/go/datastore.
func (tx *Transaction) Rollback() error {
	// Datastore transactions are automatically rolled back if not committed
	// So we just need to clear the mutations to prevent accidental commit
	tx.mutations = nil
	return nil
}

// Mutate adds one or more mutations to the transaction.
// API compatible with cloud.google.com/go/datastore.
func (tx *Transaction) Mutate(muts ...*Mutation) ([]*PendingKey, error) {
	if len(muts) == 0 {
		return nil, nil
	}

	// Build mutations array
	pendingKeys := make([]*PendingKey, 0, len(muts))
	for i, mut := range muts {
		if mut == nil {
			return nil, fmt.Errorf("mutation at index %d is nil", i)
		}
		if mut.key == nil {
			return nil, fmt.Errorf("mutation at index %d has nil key", i)
		}

		mutMap := make(map[string]any)

		switch mut.op {
		case MutationInsert:
			if mut.entity == nil {
				return nil, fmt.Errorf("insert mutation at index %d has nil entity", i)
			}
			entity, err := encodeEntity(mut.key, mut.entity)
			if err != nil {
				return nil, fmt.Errorf("failed to encode entity at index %d: %w", i, err)
			}
			mutMap["insert"] = entity

		case MutationUpdate:
			if mut.entity == nil {
				return nil, fmt.Errorf("update mutation at index %d has nil entity", i)
			}
			entity, err := encodeEntity(mut.key, mut.entity)
			if err != nil {
				return nil, fmt.Errorf("failed to encode entity at index %d: %w", i, err)
			}
			mutMap["update"] = entity

		case MutationUpsert:
			if mut.entity == nil {
				return nil, fmt.Errorf("upsert mutation at index %d has nil entity", i)
			}
			entity, err := encodeEntity(mut.key, mut.entity)
			if err != nil {
				return nil, fmt.Errorf("failed to encode entity at index %d: %w", i, err)
			}
			mutMap["upsert"] = entity

		case MutationDelete:
			mutMap["delete"] = keyToJSON(mut.key)

		default:
			return nil, fmt.Errorf("unknown mutation operation at index %d: %s", i, mut.op)
		}

		tx.mutations = append(tx.mutations, mutMap)

		// Create a pending key for the result
		pk := &PendingKey{key: mut.key}
		pendingKeys = append(pendingKeys, pk)
	}

	return pendingKeys, nil
}

// PendingKey represents a key that will be resolved after a transaction commit.
// API compatible with cloud.google.com/go/datastore.
type PendingKey struct {
	key *Key
}

// commit commits the transaction.
func (tx *Transaction) doCommit(ctx context.Context, token string) error {
	reqBody := map[string]any{
		"mode":        "TRANSACTIONAL",
		"transaction": tx.id,
		"mutations":   tx.mutations,
	}

	if tx.client.databaseID != "" {
		reqBody["databaseId"] = tx.client.databaseID
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	// URL-encode project ID to prevent injection attacks
	reqURL := fmt.Sprintf("%s/projects/%s:commit", tx.client.baseURL, neturl.PathEscape(tx.client.projectID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(jsonData))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	// Add routing header for named databases
	if tx.client.databaseID != "" {
		// URL-encode values to prevent header injection attacks
		routingHeader := fmt.Sprintf("project_id=%s&database_id=%s",
			neturl.QueryEscape(tx.client.projectID),
			neturl.QueryEscape(tx.client.databaseID))
		req.Header.Set("X-Goog-Request-Params", routingHeader)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			tx.client.logger.Warn("failed to close response body", "error", closeErr)
		}
	}()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("commit failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
