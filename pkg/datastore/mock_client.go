package datastore

import (
	"context"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/mock"
)

// NewMockClient creates a datastore client connected to mock servers with in-memory storage.
// This is a convenience wrapper that avoids import cycles when writing tests in package datastore.
// Returns the client and a cleanup function that should be deferred.
func NewMockClient(t *testing.T) (client *Client, cleanup func()) {
	t.Helper()

	// Create mock servers
	metadataURL, apiURL, cleanup := mock.NewMockServers(t)

	// Set test URLs
	restore := SetTestURLs(metadataURL, apiURL)

	// Create client
	ctx := context.Background()
	var err error
	client, err = NewClient(ctx, "test-project")
	if err != nil {
		t.Fatalf("failed to create mock client: %v", err)
	}

	// Wrap cleanup to restore URLs
	originalCleanup := cleanup
	cleanup = func() {
		restore()
		originalCleanup()
	}

	return client, cleanup
}
