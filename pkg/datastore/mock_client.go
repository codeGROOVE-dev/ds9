package datastore

import (
	"context"
	"testing"

	"github.com/codeGROOVE-dev/ds9/auth"
	"github.com/codeGROOVE-dev/ds9/pkg/mock"
)

// NewMockClient creates a datastore client connected to mock servers with in-memory storage.
// This is a convenience wrapper for testing.
// Returns the client and a cleanup function that should be deferred.
func NewMockClient(t *testing.T) (client *Client, cleanup func()) {
	t.Helper()

	// Create mock servers
	metadataURL, apiURL, cleanup := mock.NewMockServers(t)

	// Create context with test configuration
	ctx := WithConfig(context.Background(), &Config{
		APIURL: apiURL,
		AuthConfig: &auth.Config{
			MetadataURL: metadataURL,
			SkipADC:     true,
		},
	})

	// Create client
	var err error
	client, err = NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("failed to create mock client: %v", err)
	}

	return client, cleanup
}
