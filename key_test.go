package ds9

import (
	"testing"
)

func TestKeyEqual(t *testing.T) {
	tests := []struct {
		key1     *Key
		key2     *Key
		name     string
		expected bool
	}{
		{
			name:     "both nil",
			key1:     nil,
			key2:     nil,
			expected: true,
		},
		{
			name:     "one nil",
			key1:     NameKey("Kind", "name", nil),
			key2:     nil,
			expected: false,
		},
		{
			name:     "same name keys",
			key1:     NameKey("Kind", "name", nil),
			key2:     NameKey("Kind", "name", nil),
			expected: true,
		},
		{
			name:     "same ID keys",
			key1:     IDKey("Kind", 123, nil),
			key2:     IDKey("Kind", 123, nil),
			expected: true,
		},
		{
			name:     "different kinds",
			key1:     NameKey("Kind1", "name", nil),
			key2:     NameKey("Kind2", "name", nil),
			expected: false,
		},
		{
			name:     "different names",
			key1:     NameKey("Kind", "name1", nil),
			key2:     NameKey("Kind", "name2", nil),
			expected: false,
		},
		{
			name:     "different IDs",
			key1:     IDKey("Kind", 123, nil),
			key2:     IDKey("Kind", 456, nil),
			expected: false,
		},
		{
			name:     "with same parent",
			key1:     NameKey("Child", "c1", NameKey("Parent", "p1", nil)),
			key2:     NameKey("Child", "c1", NameKey("Parent", "p1", nil)),
			expected: true,
		},
		{
			name:     "with different parent",
			key1:     NameKey("Child", "c1", NameKey("Parent", "p1", nil)),
			key2:     NameKey("Child", "c1", NameKey("Parent", "p2", nil)),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.key1.Equal(tt.key2)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestKeyIncomplete(t *testing.T) {
	tests := []struct {
		key      *Key
		name     string
		expected bool
	}{
		{
			name:     "incomplete key",
			key:      IncompleteKey("Kind", nil),
			expected: true,
		},
		{
			name:     "name key",
			key:      NameKey("Kind", "name", nil),
			expected: false,
		},
		{
			name:     "ID key",
			key:      IDKey("Kind", 123, nil),
			expected: false,
		},
		{
			name:     "zero ID is incomplete",
			key:      &Key{Kind: "Kind", ID: 0, Name: ""},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.key.Incomplete()
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIncompleteKey(t *testing.T) {
	key := IncompleteKey("TestKind", nil)

	if key.Kind != "TestKind" {
		t.Errorf("Expected kind 'TestKind', got '%s'", key.Kind)
	}

	if !key.Incomplete() {
		t.Error("Expected key to be incomplete")
	}

	if key.Parent != nil {
		t.Error("Expected nil parent")
	}
}

func TestIncompleteKeyWithParent(t *testing.T) {
	parent := NameKey("Parent", "p1", nil)
	key := IncompleteKey("Child", parent)

	if key.Kind != "Child" {
		t.Errorf("Expected kind 'Child', got '%s'", key.Kind)
	}

	if !key.Incomplete() {
		t.Error("Expected key to be incomplete")
	}

	if !key.Parent.Equal(parent) {
		t.Error("Expected parent to match")
	}
}

func TestKeyString(t *testing.T) {
	tests := []struct {
		name     string
		key      *Key
		expected string
	}{
		{
			name:     "nil key",
			key:      nil,
			expected: "",
		},
		{
			name:     "simple name key",
			key:      NameKey("Kind", "name", nil),
			expected: `/Kind,"name"`,
		},
		{
			name:     "simple ID key",
			key:      IDKey("Kind", 123, nil),
			expected: "/Kind,123",
		},
		{
			name:     "incomplete key",
			key:      IncompleteKey("Kind", nil),
			expected: "/Kind,incomplete",
		},
		{
			name:     "hierarchical key",
			key:      NameKey("Child", "c1", NameKey("Parent", "p1", nil)),
			expected: `/Parent,"p1"/Child,"c1"`,
		},
		{
			name:     "deep hierarchy",
			key:      IDKey("GrandChild", 3, NameKey("Child", "c1", NameKey("Parent", "p1", nil))),
			expected: `/Parent,"p1"/Child,"c1"/GrandChild,3`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.key.String()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestKeyEncodeDecode(t *testing.T) {
	tests := []struct {
		key  *Key
		name string
	}{
		{
			name: "name key",
			key:  NameKey("Kind", "name", nil),
		},
		{
			name: "ID key",
			key:  IDKey("Kind", 123, nil),
		},
		{
			name: "hierarchical key",
			key:  NameKey("Child", "c1", NameKey("Parent", "p1", nil)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.key.Encode()
			if encoded == "" {
				t.Fatal("Encode returned empty string")
			}

			decoded, err := DecodeKey(encoded)
			if err != nil {
				t.Fatalf("DecodeKey failed: %v", err)
			}

			if !decoded.Equal(tt.key) {
				t.Errorf("Decoded key doesn't match original.\nOriginal: %s\nDecoded: %s", tt.key.String(), decoded.String())
			}
		})
	}
}

func TestDecodeKeyErrors(t *testing.T) {
	tests := []struct {
		name    string
		encoded string
	}{
		{
			name:    "empty string",
			encoded: "",
		},
		{
			name:    "invalid base64",
			encoded: "!!!invalid!!!",
		},
		{
			name:    "invalid JSON",
			encoded: "aW52YWxpZCBqc29u", // "invalid json" in base64
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeKey(tt.encoded)
			if err == nil {
				t.Error("Expected error, got nil")
			}
		})
	}
}

func TestKeyEncodeNil(t *testing.T) {
	var key *Key
	encoded := key.Encode()
	if encoded != "" {
		t.Errorf("Expected empty string for nil key, got %q", encoded)
	}
}
