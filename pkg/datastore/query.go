package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	neturl "net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/codeGROOVE-dev/ds9/auth"
)

// Query represents a Datastore query.
//
//nolint:govet // Field order prioritizes logical grouping over memory optimization
type Query struct {
	filters     []queryFilter
	orders      []queryOrder
	projection  []string
	distinctOn  []string
	startCursor Cursor
	endCursor   Cursor
	kind        string
	namespace   string
	ancestor    *Key
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
	ctx = c.withClientConfig(ctx)
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
	ctx = c.withClientConfig(ctx)
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
		return nil, fmt.Errorf("%w: dst must be a pointer to slice", ErrInvalidEntityType)
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
	ctx = c.withClientConfig(ctx)
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
	ctx = c.withClientConfig(ctx)
	return &Iterator{
		ctx:       ctx,
		client:    c,
		query:     q,
		fetchNext: true,
	}
}
