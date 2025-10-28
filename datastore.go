// Package ds9 provides a zero-dependency Google Cloud Datastore client.
//
// It uses only the Go standard library and makes direct REST API calls
// to the Datastore API. Authentication is handled via the GCP metadata
// server when running on GCP, or via Application Default Credentials.
package ds9

import (
	"bytes"
	"context"
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

	// Package-level variable for easier testing.
	apiURL = "https://datastore.googleapis.com/v1"

	httpClient = &http.Client{
		Timeout: defaultTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			IdleConnTimeout:     30 * time.Second,
			MaxIdleConnsPerHost: 2,
		},
	}
)

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
	oldAPI := apiURL
	apiURL = api
	restoreAuth := auth.SetMetadataURL(metadata)
	return func() {
		apiURL = oldAPI
		restoreAuth()
	}
}

// Client is a Google Cloud Datastore client.
type Client struct {
	logger     *slog.Logger
	projectID  string
	databaseID string
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
	reqURL := fmt.Sprintf("%s/projects/%s:lookup", apiURL, neturl.PathEscape(c.projectID))
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
	reqURL := fmt.Sprintf("%s/projects/%s:commit", apiURL, neturl.PathEscape(c.projectID))
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
	reqURL := fmt.Sprintf("%s/projects/%s:commit", apiURL, neturl.PathEscape(c.projectID))
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
	reqURL := fmt.Sprintf("%s/projects/%s:lookup", apiURL, neturl.PathEscape(c.projectID))
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
	reqURL := fmt.Sprintf("%s/projects/%s:commit", apiURL, neturl.PathEscape(c.projectID))
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
	reqURL := fmt.Sprintf("%s/projects/%s:commit", apiURL, neturl.PathEscape(c.projectID))
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
	default:
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

	if _, ok := prop["nullValue"]; ok {
		// Set to zero value
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}

	return fmt.Errorf("unsupported property type for %s", dst.Type())
}

// Query represents a Datastore query.
type Query struct {
	kind     string
	keysOnly bool
	limit    int
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

	query := map[string]any{
		"kind":       []map[string]any{{"name": q.kind}},
		"projection": []map[string]any{{"property": map[string]string{"name": "__key__"}}},
	}
	if q.limit > 0 {
		query["limit"] = q.limit
	}

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
	reqURL := fmt.Sprintf("%s/projects/%s:runQuery", apiURL, neturl.PathEscape(c.projectID))
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

	queryObj := map[string]any{
		"kind": []map[string]any{{"name": query.kind}},
	}
	if query.limit > 0 {
		queryObj["limit"] = query.limit
	}

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
	reqURL := fmt.Sprintf("%s/projects/%s:runQuery", apiURL, neturl.PathEscape(c.projectID))
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

	// Get the last path element (we only support simple keys)
	lastElem, ok := path[len(path)-1].(map[string]any)
	if !ok {
		return nil, errors.New("invalid path element")
	}

	key := &Key{}

	if kind, ok := lastElem["kind"].(string); ok {
		key.Kind = kind
	}

	if name, ok := lastElem["name"].(string); ok {
		key.Name = name
	} else if idVal, exists := lastElem["id"]; exists {
		switch id := idVal.(type) {
		case string:
			if _, err := fmt.Sscanf(id, "%d", &key.ID); err != nil {
				return nil, fmt.Errorf("invalid ID format: %w", err)
			}
		case float64:
			key.ID = int64(id)
		}
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

// RunInTransaction runs a function in a transaction.
// The function should use the transaction's Get and Put methods.
// API compatible with cloud.google.com/go/datastore.
func (c *Client) RunInTransaction(ctx context.Context, f func(*Transaction) error) (*Commit, error) {
	const maxTxRetries = 3
	var lastErr error

	for attempt := range maxTxRetries {
		token, err := auth.AccessToken(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get access token: %w", err)
		}

		// Begin transaction
		reqBody := map[string]any{}
		if c.databaseID != "" {
			reqBody["databaseId"] = c.databaseID
		}

		jsonData, err := json.Marshal(reqBody)
		if err != nil {
			return nil, err
		}

		// URL-encode project ID to prevent injection attacks
		reqURL := fmt.Sprintf("%s/projects/%s:beginTransaction", apiURL, neturl.PathEscape(c.projectID))
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
		err = tx.commit(ctx, token)
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
				"max_attempts", maxTxRetries,
				"has_409", is409,
				"has_ABORTED", isAborted,
				"error", err)

			// Exponential backoff: 100ms, 200ms, 400ms
			if attempt < maxTxRetries-1 {
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

	return nil, fmt.Errorf("transaction failed after %d attempts: %w", maxTxRetries, lastErr)
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
	reqURL := fmt.Sprintf("%s/projects/%s:lookup", apiURL, neturl.PathEscape(tx.client.projectID))
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

// commit commits the transaction.
func (tx *Transaction) commit(ctx context.Context, token string) error {
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
	reqURL := fmt.Sprintf("%s/projects/%s:commit", apiURL, neturl.PathEscape(tx.client.projectID))
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
