package datastore

import (
	"context"

	"github.com/codeGROOVE-dev/ds9/auth"
)

// TestOptions creates client options for test configuration with the given URLs.
// This is a helper for tests to easily configure mock servers.
//
// Example:
//
//	opts := datastore.TestOptions(metadataURL, apiURL)
//	client, err := datastore.NewClient(ctx, "test-project", opts...)
func TestOptions(metadataURL, apiURL string) []ClientOption {
	return []ClientOption{
		WithEndpoint(apiURL),
		WithAuth(&auth.Config{
			MetadataURL: metadataURL,
			SkipADC:     true,
		}),
	}
}

// Deprecated: TestConfig is deprecated. Use TestOptions instead.
func TestConfig(_ context.Context, metadataURL, apiURL string) []ClientOption {
	return TestOptions(metadataURL, apiURL)
}
