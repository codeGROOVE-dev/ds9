// Package datastore provides a zero-dependency Google Cloud Datastore client.
//
// It uses only the Go standard library and makes direct REST API calls
// to the Datastore API. Authentication is handled via the GCP metadata
// server when running on GCP, or via Application Default Credentials.
package datastore

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
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

const (
	defaultAPIURL = "https://datastore.googleapis.com/v1"
)

var (
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

// Config holds datastore client configuration.
type Config struct {
	// AuthConfig is passed to the auth package for authentication.
	// Can be nil to use defaults.
	AuthConfig *auth.Config

	// APIURL is the base URL for the Datastore API.
	// Defaults to production if empty.
	APIURL string
}

// configKey is the key for storing Config in context.
type configKey struct{}

// WithConfig returns a new context with the given datastore config.
// This also sets the auth config if provided.
func WithConfig(ctx context.Context, cfg *Config) context.Context {
	if cfg.AuthConfig != nil {
		ctx = auth.WithConfig(ctx, cfg.AuthConfig)
	}
	return context.WithValue(ctx, configKey{}, cfg)
}

// getConfig retrieves the datastore config from context, or returns defaults.
func getConfig(ctx context.Context) *Config {
	if cfg, ok := ctx.Value(configKey{}).(*Config); ok && cfg != nil {
		return cfg
	}
	return &Config{
		APIURL: defaultAPIURL,
	}
}

// Client is a Google Cloud Datastore client.
type Client struct {
	logger     *slog.Logger
	authConfig *auth.Config // Auth configuration for this client
	projectID  string
	databaseID string
	baseURL    string // API base URL, defaults to production
}

// NewClient creates a new Datastore client.
// If projectID is empty, it will be fetched from the GCP metadata server.
// Configuration can be provided via WithConfig in the context.
func NewClient(ctx context.Context, projectID string) (*Client, error) {
	return NewClientWithDatabase(ctx, projectID, "")
}

// NewClientWithDatabase creates a new Datastore client with a specific database.
// Configuration can be provided via WithConfig in the context.
func NewClientWithDatabase(ctx context.Context, projID, dbID string) (*Client, error) {
	logger := slog.Default()
	cfg := getConfig(ctx)

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

	baseURL := cfg.APIURL
	if baseURL == "" {
		baseURL = defaultAPIURL
	}

	return &Client{
		projectID:  projID,
		databaseID: dbID,
		baseURL:    baseURL,
		authConfig: cfg.AuthConfig,
		logger:     logger,
	}, nil
}

// Close closes the client connection.
// This is a no-op for ds9 since it uses a shared HTTP client with connection pooling,
// but is provided for API compatibility with cloud.google.com/go/datastore.
func (*Client) Close() error {
	return nil
}

// withClientConfig returns a context with the client's auth configuration injected.
// This ensures that operations use the client's auth settings even if the caller
// passes a bare context.Background().
func (c *Client) withClientConfig(ctx context.Context) context.Context {
	if c.authConfig != nil {
		ctx = auth.WithConfig(ctx, c.authConfig)
	}
	return ctx
}
