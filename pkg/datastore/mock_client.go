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

	// Create client with mock endpoints
	var err error
	client, err = NewClient(
		context.Background(),
		"test-project",
		WithEndpoint(apiURL),
		WithAuth(&auth.Config{
			MetadataURL: metadataURL,
			SkipADC:     true,
		}),
	)
	if err != nil {
		t.Fatalf("failed to create mock client: %v", err)
	}

	return client, cleanup
}
