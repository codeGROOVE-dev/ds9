// Package datastore provides a zero-dependency Google Cloud Datastore client.
//
// It uses only the Go standard library and makes direct REST API calls
// to the Datastore API. Authentication is handled via the GCP metadata
// server when running on GCP, or via Application Default Credentials.
//
//nolint:revive // Public structs required for API compatibility with cloud.google.com/go/datastore
package datastore

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
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
