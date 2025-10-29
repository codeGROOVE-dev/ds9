package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	neturl "net/url"

	"github.com/codeGROOVE-dev/ds9/auth"
)

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
	key    *Key
	entity any
	op     MutationOp
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
	ctx = c.withClientConfig(ctx)
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
