package datastore

import (
	"testing"
	"time"
)

// Test encodeValue with reflection-based slice handling
func TestEncodeValue_ReflectionSlices(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{
			"array of strings",
			[3]string{"a", "b", "c"},
		},
		{
			"array of ints",
			[2]int{1, 2},
		},
		{
			"array of int64",
			[2]int64{100, 200},
		},
		{
			"nested time slice",
			[]time.Time{
				time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodeValue(tt.value)
			if err != nil {
				t.Errorf("encodeValue(%v) failed: %v", tt.value, err)
			}
			if result == nil {
				t.Error("Expected non-nil result")
			}
		})
	}
}

// Test encodeValue error paths
func TestEncodeValue_Errors(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{
			"map type",
			map[string]int{"key": 1},
		},
		{
			"function type",
			func() {},
		},
		{
			"channel type",
			make(chan int),
		},
		{
			"struct type",
			struct{ Name string }{Name: "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := encodeValue(tt.value)
			if err == nil {
				t.Errorf("encodeValue(%T) should have returned an error", tt.value)
			}
		})
	}
}

// Test encodeValue with slice of time.Time (uses reflection path)
func TestEncodeValue_TimeSlice(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	later := now.Add(time.Hour)

	timeSlice := []time.Time{now, later}

	result, err := encodeValue(timeSlice)
	if err != nil {
		t.Fatalf("encodeValue failed for time slice: %v", err)
	}

	// Verify it's wrapped in arrayValue
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("Expected map[string]any, got %T", result)
	}

	arrayValue, ok := resultMap["arrayValue"]
	if !ok {
		t.Error("Expected arrayValue key in result")
	}

	if arrayValue == nil {
		t.Error("arrayValue should not be nil")
	}
}

// Test encodeValue with empty slices
func TestEncodeValue_EmptySlices(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"empty string slice", []string{}},
		{"empty int slice", []int{}},
		{"empty int64 slice", []int64{}},
		{"empty float64 slice", []float64{}},
		{"empty bool slice", []bool{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodeValue(tt.value)
			if err != nil {
				t.Errorf("encodeValue failed: %v", err)
			}
			if result == nil {
				t.Error("Expected non-nil result for empty slice")
			}
		})
	}
}

// Test encodeValue with single element slices
func TestEncodeValue_SingleElementSlices(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{"single string", []string{"only"}},
		{"single int", []int{42}},
		{"single int64", []int64{42}},
		{"single float64", []float64{3.14}},
		{"single bool", []bool{true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodeValue(tt.value)
			if err != nil {
				t.Errorf("encodeValue failed: %v", err)
			}
			if result == nil {
				t.Error("Expected non-nil result")
			}
		})
	}
}
