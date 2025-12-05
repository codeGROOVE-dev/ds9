package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	neturl "net/url"

	"github.com/codeGROOVE-dev/ds9/auth"
)

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
			return nil, Done
		}

		// Fetch next batch
		if err := it.fetch(); err != nil {
			it.err = err
			return nil, err
		}

		if len(it.results) == 0 {
			return nil, Done
		}
	}

	result := it.results[it.index]
	it.index++

	// Only update cursor if the result has one
	if result.cursor != "" {
		it.cursor = result.cursor
	}

	// For KeysOnly queries, skip entity decoding - just return the key
	// The Datastore API returns entities without properties for keys-only queries
	if !it.query.keysOnly {
		if err := decodeEntity(result.entity, dst); err != nil {
			return nil, err
		}
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
		Batch struct { //nolint:govet // Local anonymous struct for JSON unmarshaling
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
	// MORE_RESULTS_AFTER_LIMIT means we hit the query limit - don't auto-fetch more
	// NOT_FINISHED and MORE_RESULTS_AFTER_CURSOR mean we should continue fetching
	moreResults := result.Batch.MoreResults
	it.fetchNext = moreResults == "NOT_FINISHED" || moreResults == "MORE_RESULTS_AFTER_CURSOR"

	if result.Batch.EndCursor != "" {
		it.cursor = Cursor(result.Batch.EndCursor)
	}

	return nil
}
