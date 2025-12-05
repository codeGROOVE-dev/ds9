package datastore_test

import (
	"context"
	"testing"

	"github.com/codeGROOVE-dev/ds9/pkg/datastore"
)

func TestNamespaceIsolation(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type Data struct {
		Value string
	}

	// Create keys with different namespaces
	keyDefault := datastore.NameKey("Data", "item1", nil)

	keyNS1 := datastore.NameKey("Data", "item1", nil)
	keyNS1.Namespace = "ns1"

	keyNS2 := datastore.NameKey("Data", "item1", nil)
	keyNS2.Namespace = "ns2"

	// Verify keys are different
	if keyDefault.String() == keyNS1.String() {
		t.Fatal("Keys should be different due to namespace")
	}

	// Put entities
	if _, err := client.Put(ctx, keyDefault, &Data{Value: "default"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Put(ctx, keyNS1, &Data{Value: "namespace1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Put(ctx, keyNS2, &Data{Value: "namespace2"}); err != nil {
		t.Fatal(err)
	}

	// Get entities and verify isolation
	var dest Data

	// Check Default
	if err := client.Get(ctx, keyDefault, &dest); err != nil {
		t.Errorf("Get default failed: %v", err)
	}
	if dest.Value != "default" {
		t.Errorf("Expected 'default', got '%s'", dest.Value)
	}

	// Check NS1
	if err := client.Get(ctx, keyNS1, &dest); err != nil {
		t.Errorf("Get NS1 failed: %v", err)
	}
	if dest.Value != "namespace1" {
		t.Errorf("Expected 'namespace1', got '%s'", dest.Value)
	}

	// Check NS2
	if err := client.Get(ctx, keyNS2, &dest); err != nil {
		t.Errorf("Get NS2 failed: %v", err)
	}
	if dest.Value != "namespace2" {
		t.Errorf("Expected 'namespace2', got '%s'", dest.Value)
	}
}

func TestNamespaceQuery(t *testing.T) {
	client, cleanup := datastore.NewMockClient(t)
	defer cleanup()

	ctx := context.Background()

	type Item struct {
		N int
	}

	// Create items in ns1
	for i := range 5 {
		k := datastore.IncompleteKey("Item", nil)
		k.Namespace = "ns1"
		if _, err := client.Put(ctx, k, &Item{N: i}); err != nil {
			t.Fatal(err)
		}
	}

	// Create items in default namespace
	for i := range 3 {
		k := datastore.IncompleteKey("Item", nil)
		if _, err := client.Put(ctx, k, &Item{N: i}); err != nil {
			t.Fatal(err)
		}
	}

	// Query ns1
	q1 := datastore.NewQuery("Item").Namespace("ns1")
	var items1 []Item
	if _, err := client.GetAll(ctx, q1, &items1); err != nil {
		t.Fatalf("Query ns1 failed: %v", err)
	}
	if len(items1) != 5 {
		t.Errorf("Expected 5 items in ns1, got %d", len(items1))
	}

	// Query default
	qDef := datastore.NewQuery("Item")
	var itemsDef []Item
	if _, err := client.GetAll(ctx, qDef, &itemsDef); err != nil {
		t.Fatalf("Query default failed: %v", err)
	}
	if len(itemsDef) != 3 {
		t.Errorf("Expected 3 items in default, got %d", len(itemsDef))
	}
}
