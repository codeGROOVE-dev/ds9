package datastore

import (
	"context"

	"github.com/codeGROOVE-dev/ds9/auth"
)

// TestConfig creates a context with test configuration for the given URLs.
// This is a helper for tests to easily configure mock servers.
// Use this with context.Background() or any existing context.
//
// Example:
//
//	ctx := datastore.TestConfig(context.Background(), metadataURL, apiURL)
//	client, err := datastore.NewClient(ctx, "test-project")
func TestConfig(ctx context.Context, metadataURL, apiURL string) context.Context {
	return WithConfig(ctx, &Config{
		APIURL: apiURL,
		AuthConfig: &auth.Config{
			MetadataURL: metadataURL,
			SkipADC:     true,
		},
	})
}
