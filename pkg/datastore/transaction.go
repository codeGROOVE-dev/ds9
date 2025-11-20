package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"reflect"
	"strings"
	"time"

	"github.com/codeGROOVE-dev/ds9/auth"
)

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
	ctx = c.withClientConfig(ctx)
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
	ctx = c.withClientConfig(ctx)
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
		return ErrInvalidKey
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
		return nil, ErrInvalidKey
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
		return ErrInvalidKey
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
		return fmt.Errorf("%w: dst must be a pointer to a slice", ErrInvalidEntityType)
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
		return nil, fmt.Errorf("%w: src must be a slice", ErrInvalidEntityType)
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
