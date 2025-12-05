package datastore

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Key represents a Datastore key.
type Key struct {
	Namespace string
	Parent    *Key // Parent key for hierarchical keys
	Kind      string
	Name      string // For string keys
	ID        int64  // For numeric keys
}

// NameKey creates a new key with a string name.
// The parent parameter can be nil for top-level keys.
// This matches the API of cloud.google.com/go/datastore.
func NameKey(kind, name string, parent *Key) *Key {
	k := &Key{
		Kind:   kind,
		Name:   name,
		Parent: parent,
	}
	if parent != nil {
		k.Namespace = parent.Namespace
	}
	return k
}

// IDKey creates a new key with a numeric ID.
// The parent parameter can be nil for top-level keys.
// This matches the API of cloud.google.com/go/datastore.
func IDKey(kind string, id int64, parent *Key) *Key {
	k := &Key{
		Kind:   kind,
		ID:     id,
		Parent: parent,
	}
	if parent != nil {
		k.Namespace = parent.Namespace
	}
	return k
}

// IncompleteKey creates a new incomplete key.
// The key will be completed (assigned an ID) when the entity is saved.
// API compatible with cloud.google.com/go/datastore.
func IncompleteKey(kind string, parent *Key) *Key {
	k := &Key{
		Kind:   kind,
		Parent: parent,
	}
	if parent != nil {
		k.Namespace = parent.Namespace
	}
	return k
}

// Incomplete returns true if the key does not have an ID or Name.
// API compatible with cloud.google.com/go/datastore.
func (k *Key) Incomplete() bool {
	return k.ID == 0 && k.Name == ""
}

// Equal returns true if this key is equal to the other key.
// API compatible with cloud.google.com/go/datastore.
func (k *Key) Equal(other *Key) bool {
	if k == nil && other == nil {
		return true
	}
	if k == nil || other == nil {
		return false
	}
	if k.Namespace != other.Namespace || k.Kind != other.Kind || k.Name != other.Name || k.ID != other.ID {
		return false
	}
	// Recursively check parent keys
	return k.Parent.Equal(other.Parent)
}

// String returns a human-readable string representation of the key.
// API compatible with cloud.google.com/go/datastore.
func (k *Key) String() string {
	if k == nil {
		return ""
	}

	var parts []string
	for curr := k; curr != nil; curr = curr.Parent {
		var part string
		switch {
		case curr.Name != "":
			part = fmt.Sprintf("%s,%q", curr.Kind, curr.Name)
		case curr.ID != 0:
			part = fmt.Sprintf("%s,%d", curr.Kind, curr.ID)
		default:
			part = fmt.Sprintf("%s,incomplete", curr.Kind)
		}
		// Prepend to maintain correct order (root to leaf)
		parts = append([]string{part}, parts...)
	}

	keyStr := "/" + strings.Join(parts, "/")
	if k.Namespace != "" {
		keyStr = fmt.Sprintf("[%s]%s", k.Namespace, keyStr)
	}
	return keyStr
}

// Encode returns an opaque representation of the key.
// API compatible with cloud.google.com/go/datastore.
func (k *Key) Encode() string {
	if k == nil {
		return ""
	}

	// Convert key to JSON representation
	keyJSON := keyToJSON(k)

	// Marshal to JSON bytes
	jsonBytes, err := json.Marshal(keyJSON)
	if err != nil {
		return ""
	}

	// Base64 encode
	return base64.URLEncoding.EncodeToString(jsonBytes)
}

// DecodeKey decodes a key from its opaque representation.
// API compatible with cloud.google.com/go/datastore.
func DecodeKey(encoded string) (*Key, error) {
	if encoded == "" {
		return nil, errors.New("empty encoded key")
	}

	// Base64 decode
	jsonBytes, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	// Unmarshal JSON
	var keyData any
	if err := json.Unmarshal(jsonBytes, &keyData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	// Convert from JSON representation
	key, err := keyFromJSON(keyData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key: %w", err)
	}

	return key, nil
}

// keyToJSON converts a Key to its JSON representation.
// Supports hierarchical keys with parent relationships.
func keyToJSON(key *Key) map[string]any {
	// Build path from root to leaf (parent -> child)
	var path []map[string]any

	// Collect all keys from root to leaf
	keys := make([]*Key, 0)
	for k := key; k != nil; k = k.Parent {
		keys = append(keys, k)
	}

	// Reverse to go from root to leaf
	for i := len(keys) - 1; i >= 0; i-- {
		k := keys[i]
		elem := map[string]any{
			"kind": k.Kind,
		}

		if k.Name != "" {
			elem["name"] = k.Name
		} else if k.ID != 0 {
			elem["id"] = strconv.FormatInt(k.ID, 10)
		}

		path = append(path, elem)
	}

	m := map[string]any{
		"path": path,
	}

	// Add partitionId if namespace is present
	if key.Namespace != "" {
		m["partitionId"] = map[string]any{
			"namespaceId": key.Namespace,
		}
	}

	return m
}

// keyFromJSON converts a JSON key representation to a Key.
func keyFromJSON(keyData any) (*Key, error) {
	keyMap, ok := keyData.(map[string]any)
	if !ok {
		return nil, errors.New("invalid key format")
	}

	path, ok := keyMap["path"].([]any)
	if !ok || len(path) == 0 {
		return nil, errors.New("invalid key path")
	}

	var namespace string
	if pid, ok := keyMap["partitionId"].(map[string]any); ok {
		if ns, ok := pid["namespaceId"].(string); ok {
			namespace = ns
		}
	}

	// Build key hierarchy from path elements
	var key *Key
	for _, elem := range path {
		elemMap, ok := elem.(map[string]any)
		if !ok {
			return nil, errors.New("invalid path element")
		}

		newKey := &Key{
			Parent:    key,
			Namespace: namespace,
		}

		if kind, ok := elemMap["kind"].(string); ok {
			newKey.Kind = kind
		}

		if name, ok := elemMap["name"].(string); ok {
			newKey.Name = name
		} else if idVal, exists := elemMap["id"]; exists {
			switch id := idVal.(type) {
			case string:
				if _, err := fmt.Sscanf(id, "%d", &newKey.ID); err != nil {
					return nil, fmt.Errorf("invalid ID format: %w", err)
				}
			case float64:
				newKey.ID = int64(id)
			}
		}

		key = newKey
	}

	return key, nil
}
