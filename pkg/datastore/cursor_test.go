package datastore

import (
	"testing"
)

func TestCursorString(t *testing.T) {
	tests := []struct {
		name     string
		cursor   Cursor
		expected string
	}{
		{
			name:     "non-empty cursor",
			cursor:   Cursor("test-cursor-123"),
			expected: "test-cursor-123",
		},
		{
			name:     "empty cursor",
			cursor:   Cursor(""),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.cursor.String()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestDecodeCursor(t *testing.T) {
	tests := []struct {
		name        string
		cursorStr   string
		expectError bool
		expected    Cursor
	}{
		{
			name:        "valid cursor",
			cursorStr:   "valid-cursor-string",
			expectError: false,
			expected:    Cursor("valid-cursor-string"),
		},
		{
			name:        "empty cursor string",
			cursorStr:   "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor, err := DecodeCursor(tt.cursorStr)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if cursor != tt.expected {
					t.Errorf("Expected cursor %q, got %q", tt.expected, cursor)
				}
			}
		})
	}
}
