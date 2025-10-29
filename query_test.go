package ds9

import (
	"testing"
)

func TestQueryFilter(t *testing.T) {
	q := NewQuery("TestKind").Filter("Count >", 10)

	if len(q.filters) != 1 {
		t.Fatalf("Expected 1 filter, got %d", len(q.filters))
	}

	filter := q.filters[0]
	if filter.property != "Count" {
		t.Errorf("Expected property 'Count', got '%s'", filter.property)
	}
	if filter.operator != "GREATER_THAN" {
		t.Errorf("Expected operator 'GREATER_THAN', got '%s'", filter.operator)
	}
	if filter.value != 10 {
		t.Errorf("Expected value 10, got %v", filter.value)
	}
}

func TestQueryFilterField(t *testing.T) {
	tests := []struct {
		name             string
		field            string
		operator         string
		value            any
		expectedOperator string
	}{
		{
			name:             "equal",
			field:            "Name",
			operator:         "=",
			value:            "test",
			expectedOperator: "EQUAL",
		},
		{
			name:             "less than",
			field:            "Count",
			operator:         "<",
			value:            100,
			expectedOperator: "LESS_THAN",
		},
		{
			name:             "less than or equal",
			field:            "Count",
			operator:         "<=",
			value:            100,
			expectedOperator: "LESS_THAN_OR_EQUAL",
		},
		{
			name:             "greater than",
			field:            "Count",
			operator:         ">",
			value:            10,
			expectedOperator: "GREATER_THAN",
		},
		{
			name:             "greater than or equal",
			field:            "Count",
			operator:         ">=",
			value:            10,
			expectedOperator: "GREATER_THAN_OR_EQUAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery("TestKind").FilterField(tt.field, tt.operator, tt.value)

			if len(q.filters) != 1 {
				t.Fatalf("Expected 1 filter, got %d", len(q.filters))
			}

			filter := q.filters[0]
			if filter.property != tt.field {
				t.Errorf("Expected property '%s', got '%s'", tt.field, filter.property)
			}
			if filter.operator != tt.expectedOperator {
				t.Errorf("Expected operator '%s', got '%s'", tt.expectedOperator, filter.operator)
			}
			if filter.value != tt.value {
				t.Errorf("Expected value %v, got %v", tt.value, filter.value)
			}
		})
	}
}

func TestQueryMultipleFilters(t *testing.T) {
	q := NewQuery("TestKind").
		FilterField("Count", ">", 10).
		FilterField("Name", "=", "test")

	if len(q.filters) != 2 {
		t.Fatalf("Expected 2 filters, got %d", len(q.filters))
	}
}

func TestQueryOrder(t *testing.T) {
	tests := []struct {
		name              string
		fieldName         string
		expectedProperty  string
		expectedDirection string
	}{
		{
			name:              "ascending",
			fieldName:         "Count",
			expectedProperty:  "Count",
			expectedDirection: "ASCENDING",
		},
		{
			name:              "descending",
			fieldName:         "-Count",
			expectedProperty:  "Count",
			expectedDirection: "DESCENDING",
		},
		{
			name:              "ascending with hyphen in name",
			fieldName:         "created-at",
			expectedProperty:  "created-at",
			expectedDirection: "ASCENDING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewQuery("TestKind").Order(tt.fieldName)

			if len(q.orders) != 1 {
				t.Fatalf("Expected 1 order, got %d", len(q.orders))
			}

			order := q.orders[0]
			if order.property != tt.expectedProperty {
				t.Errorf("Expected property '%s', got '%s'", tt.expectedProperty, order.property)
			}
			if order.direction != tt.expectedDirection {
				t.Errorf("Expected direction '%s', got '%s'", tt.expectedDirection, order.direction)
			}
		})
	}
}

func TestQueryMultipleOrders(t *testing.T) {
	q := NewQuery("TestKind").
		Order("Name").
		Order("-Count")

	if len(q.orders) != 2 {
		t.Fatalf("Expected 2 orders, got %d", len(q.orders))
	}
}

func TestQueryOffset(t *testing.T) {
	q := NewQuery("TestKind").Offset(10)

	if q.offset != 10 {
		t.Errorf("Expected offset 10, got %d", q.offset)
	}
}

func TestQueryAncestor(t *testing.T) {
	ancestor := NameKey("Parent", "p1", nil)
	q := NewQuery("TestKind").Ancestor(ancestor)

	if !q.ancestor.Equal(ancestor) {
		t.Error("Expected ancestor to match")
	}
}

func TestQueryProject(t *testing.T) {
	q := NewQuery("TestKind").Project("Name", "Count")

	if len(q.projection) != 2 {
		t.Fatalf("Expected 2 projected fields, got %d", len(q.projection))
	}

	if q.projection[0] != "Name" {
		t.Errorf("Expected first projection 'Name', got '%s'", q.projection[0])
	}
	if q.projection[1] != "Count" {
		t.Errorf("Expected second projection 'Count', got '%s'", q.projection[1])
	}
}

func TestQueryChaining(t *testing.T) {
	q := NewQuery("TestKind").
		FilterField("Count", ">", 10).
		Order("-Count").
		Limit(100).
		Offset(20).
		KeysOnly()

	if len(q.filters) != 1 {
		t.Errorf("Expected 1 filter, got %d", len(q.filters))
	}
	if len(q.orders) != 1 {
		t.Errorf("Expected 1 order, got %d", len(q.orders))
	}
	if q.limit != 100 {
		t.Errorf("Expected limit 100, got %d", q.limit)
	}
	if q.offset != 20 {
		t.Errorf("Expected offset 20, got %d", q.offset)
	}
	if !q.keysOnly {
		t.Error("Expected keysOnly to be true")
	}
}

func TestBuildQueryMapBasic(t *testing.T) {
	q := NewQuery("TestKind")
	queryMap := buildQueryMap(q)

	kind, ok := queryMap["kind"].([]map[string]any)
	if !ok || len(kind) == 0 {
		t.Fatal("Expected kind in query map")
	}

	if kind[0]["name"] != "TestKind" {
		t.Errorf("Expected kind 'TestKind', got '%v'", kind[0]["name"])
	}
}

func TestBuildQueryMapWithLimit(t *testing.T) {
	q := NewQuery("TestKind").Limit(10)
	queryMap := buildQueryMap(q)

	limit, ok := queryMap["limit"]
	if !ok {
		t.Fatal("Expected limit in query map")
	}

	if limit != 10 {
		t.Errorf("Expected limit 10, got %v", limit)
	}
}

func TestBuildQueryMapWithOffset(t *testing.T) {
	q := NewQuery("TestKind").Offset(5)
	queryMap := buildQueryMap(q)

	offset, ok := queryMap["offset"]
	if !ok {
		t.Fatal("Expected offset in query map")
	}

	if offset != 5 {
		t.Errorf("Expected offset 5, got %v", offset)
	}
}

func TestBuildQueryMapWithFilter(t *testing.T) {
	q := NewQuery("TestKind").FilterField("Count", ">", 10)
	queryMap := buildQueryMap(q)

	_, ok := queryMap["filter"]
	if !ok {
		t.Fatal("Expected filter in query map")
	}
}

func TestBuildQueryMapWithOrder(t *testing.T) {
	q := NewQuery("TestKind").Order("-Count")
	queryMap := buildQueryMap(q)

	orders, ok := queryMap["order"].([]map[string]any)
	if !ok || len(orders) == 0 {
		t.Fatal("Expected order in query map")
	}

	if orders[0]["direction"] != "DESCENDING" {
		t.Errorf("Expected DESCENDING, got %v", orders[0]["direction"])
	}
}

func TestBuildQueryMapKeysOnly(t *testing.T) {
	q := NewQuery("TestKind").KeysOnly()
	queryMap := buildQueryMap(q)

	projection, ok := queryMap["projection"].([]map[string]any)
	if !ok || len(projection) == 0 {
		t.Fatal("Expected projection in query map for keys-only")
	}
}
