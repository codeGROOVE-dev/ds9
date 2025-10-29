package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	neturl "net/url"
	"reflect"

	"github.com/codeGROOVE-dev/ds9/auth"
)

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
