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

// ClientOption is an option for a Datastore client.
type ClientOption func(*clientOptionsInternal)

// clientOptionsInternal holds internal client configuration that can be modified by ClientOption.
type clientOptionsInternal struct {
	authConfig *auth.Config
	logger     *slog.Logger
	baseURL    string
}

// WithEndpoint returns a ClientOption that sets the API base URL.
func WithEndpoint(url string) ClientOption {
	return func(o *clientOptionsInternal) {
		o.baseURL = url
	}
}

// WithLogger returns a ClientOption that sets the logger.
func WithLogger(logger *slog.Logger) ClientOption {
	return func(o *clientOptionsInternal) {
		o.logger = logger
	}
}

// WithAuth returns a ClientOption that sets the authentication configuration.
func WithAuth(cfg *auth.Config) ClientOption {
	return func(o *clientOptionsInternal) {
		o.authConfig = cfg
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
// Options can be provided to configure the client.
func NewClient(ctx context.Context, projectID string, opts ...ClientOption) (*Client, error) {
	return NewClientWithDatabase(ctx, projectID, "", opts...)
}

// NewClientWithDatabase creates a new Datastore client with a specific database.
// Options can be provided to configure the client.
func NewClientWithDatabase(ctx context.Context, projID, dbID string, opts ...ClientOption) (*Client, error) {
	// Apply default internal options
	options := &clientOptionsInternal{
		baseURL: defaultAPIURL,
		logger:  slog.Default(),
	}

	// Apply provided options
	for _, opt := range opts {
		opt(options)
	}

	// --- Existing NewClientWithDatabase logic starts here ---
	if projID == "" {
		// Inject auth config into context before fetching project ID
		fetchCtx := ctx
		if options.authConfig != nil {
			fetchCtx = auth.WithConfig(ctx, options.authConfig)
		}

		if !testing.Testing() {
			options.logger.InfoContext(ctx, "project ID not provided, fetching from metadata server")
		}
		pid, err := auth.ProjectID(fetchCtx)
		if err != nil {
			options.logger.ErrorContext(ctx, "failed to get project ID from metadata server", "error", err)
			return nil, fmt.Errorf("project ID required: %w", err)
		}
		projID = pid
		if !testing.Testing() {
			options.logger.InfoContext(ctx, "fetched project ID from metadata server", "project_id", projID)
		}
	}

	if !testing.Testing() {
		options.logger.InfoContext(ctx, "creating datastore client", "project_id", projID, "database_id", dbID)
	}

	baseURL := options.baseURL
	if baseURL == "" {
		baseURL = defaultAPIURL
	}

	return &Client{
		projectID:  projID,
		databaseID: dbID,
		baseURL:    baseURL,
		authConfig: options.authConfig, // Use authConfig from options
		logger:     options.logger,     // Use logger from options
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
