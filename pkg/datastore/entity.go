package datastore

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// encodeEntity converts a Go struct to a Datastore entity.
func encodeEntity(key *Key, src any) (map[string]any, error) {
	v := reflect.ValueOf(src)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, errors.New("src must be a struct or pointer to struct")
	}

	t := v.Type()
	properties := make(map[string]any)

	for i := range v.NumField() {
		field := t.Field(i)
		value := v.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get field name from datastore tag or use field name
		name := field.Name
		noIndex := false

		if tag := field.Tag.Get("datastore"); tag != "" {
			parts := strings.Split(tag, ",")
			if parts[0] != "" && parts[0] != "-" {
				name = parts[0]
			}
			if len(parts) > 1 && parts[1] == "noindex" {
				noIndex = true
			}
			if parts[0] == "-" {
				continue
			}
		}

		prop, err := encodeValue(value.Interface())
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field.Name, err)
		}

		if noIndex {
			if m, ok := prop.(map[string]any); ok {
				m["excludeFromIndexes"] = true
			}
		}

		properties[name] = prop
	}

	return map[string]any{
		"key":        keyToJSON(key),
		"properties": properties,
	}, nil
}

// encodeValue converts a Go value to a Datastore property value.
func encodeValue(v any) (any, error) {
	if v == nil {
		return map[string]any{"nullValue": nil}, nil
	}

	switch val := v.(type) {
	case string:
		return map[string]any{"stringValue": val}, nil
	case int:
		return map[string]any{"integerValue": strconv.Itoa(val)}, nil
	case int64:
		return map[string]any{"integerValue": strconv.FormatInt(val, 10)}, nil
	case int32:
		return map[string]any{"integerValue": strconv.Itoa(int(val))}, nil
	case bool:
		return map[string]any{"booleanValue": val}, nil
	case float64:
		return map[string]any{"doubleValue": val}, nil
	case time.Time:
		return map[string]any{"timestampValue": val.Format(time.RFC3339Nano)}, nil
	case []string:
		values := make([]map[string]any, len(val))
		for i, s := range val {
			values[i] = map[string]any{"stringValue": s}
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
	case []int64:
		values := make([]map[string]any, len(val))
		for i, n := range val {
			values[i] = map[string]any{"integerValue": strconv.FormatInt(n, 10)}
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
	case []int:
		values := make([]map[string]any, len(val))
		for i, n := range val {
			values[i] = map[string]any{"integerValue": strconv.Itoa(n)}
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
	case []float64:
		values := make([]map[string]any, len(val))
		for i, f := range val {
			values[i] = map[string]any{"doubleValue": f}
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
	case []bool:
		values := make([]map[string]any, len(val))
		for i, b := range val {
			values[i] = map[string]any{"booleanValue": b}
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
	default:
		// Try to handle slices/arrays via reflection
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			length := rv.Len()
			values := make([]map[string]any, length)
			for i := range length {
				elem := rv.Index(i).Interface()
				encodedElem, err := encodeValue(elem)
				if err != nil {
					return nil, fmt.Errorf("failed to encode array element %d: %w", i, err)
				}
				// encodedElem is already a map[string]any with the type wrapper
				m, ok := encodedElem.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("unexpected encoded value type for element %d", i)
				}
				values[i] = m
			}
			return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
		}
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
}

// decodeEntity converts a Datastore entity to a Go struct.
func decodeEntity(entity map[string]any, dst any) error {
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return errors.New("dst must be a pointer to struct")
	}

	v = v.Elem()
	t := v.Type()

	properties, ok := entity["properties"].(map[string]any)
	if !ok {
		return errors.New("invalid entity format")
	}

	for i := range v.NumField() {
		field := t.Field(i)
		value := v.Field(i)

		if !field.IsExported() {
			continue
		}

		// Get field name from datastore tag
		name := field.Name
		if tag := field.Tag.Get("datastore"); tag != "" {
			parts := strings.Split(tag, ",")
			if parts[0] != "" && parts[0] != "-" {
				name = parts[0]
			}
			if parts[0] == "-" {
				continue
			}
		}

		prop, ok := properties[name]
		if !ok {
			continue // Field not in entity
		}

		propMap, ok := prop.(map[string]any)
		if !ok {
			continue
		}

		if err := decodeValue(propMap, value); err != nil {
			return fmt.Errorf("field %s: %w", field.Name, err)
		}
	}

	return nil
}

// decodeValue decodes a Datastore property value into a Go reflect.Value.
func decodeValue(prop map[string]any, dst reflect.Value) error {
	// Handle each type
	if val, ok := prop["stringValue"]; ok {
		if dst.Kind() == reflect.String {
			if s, ok := val.(string); ok {
				dst.SetString(s)
				return nil
			}
		}
	}

	if val, ok := prop["integerValue"]; ok {
		var intVal int64
		switch v := val.(type) {
		case string:
			if _, err := fmt.Sscanf(v, "%d", &intVal); err != nil {
				return fmt.Errorf("invalid integer format: %w", err)
			}
		case float64:
			intVal = int64(v)
		}

		switch dst.Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32:
			dst.SetInt(intVal)
			return nil
		default:
			return fmt.Errorf("unsupported integer type: %v", dst.Kind())
		}
	}

	if val, ok := prop["booleanValue"]; ok {
		if dst.Kind() == reflect.Bool {
			if b, ok := val.(bool); ok {
				dst.SetBool(b)
				return nil
			}
		}
	}

	if val, ok := prop["doubleValue"]; ok {
		if dst.Kind() == reflect.Float64 {
			if f, ok := val.(float64); ok {
				dst.SetFloat(f)
				return nil
			}
		}
	}

	if val, ok := prop["timestampValue"]; ok {
		if dst.Type() == reflect.TypeOf(time.Time{}) {
			if s, ok := val.(string); ok {
				t, err := time.Parse(time.RFC3339Nano, s)
				if err != nil {
					return err
				}
				dst.Set(reflect.ValueOf(t))
				return nil
			}
		}
	}

	if val, ok := prop["arrayValue"]; ok {
		if dst.Kind() != reflect.Slice {
			return fmt.Errorf("cannot decode array into non-slice type: %s", dst.Type())
		}

		arrayMap, ok := val.(map[string]any)
		if !ok {
			return errors.New("invalid arrayValue format")
		}

		valuesAny, ok := arrayMap["values"]
		if !ok {
			// Empty array
			dst.Set(reflect.MakeSlice(dst.Type(), 0, 0))
			return nil
		}

		values, ok := valuesAny.([]any)
		if !ok {
			return errors.New("invalid arrayValue.values format")
		}

		// Create slice with appropriate capacity
		slice := reflect.MakeSlice(dst.Type(), len(values), len(values))

		// Decode each element
		for i, elemAny := range values {
			elemMap, ok := elemAny.(map[string]any)
			if !ok {
				return fmt.Errorf("invalid array element %d format", i)
			}

			elemValue := slice.Index(i)
			if err := decodeValue(elemMap, elemValue); err != nil {
				return fmt.Errorf("failed to decode array element %d: %w", i, err)
			}
		}

		dst.Set(slice)
		return nil
	}

	if _, ok := prop["nullValue"]; ok {
		// Set to zero value
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}

	return fmt.Errorf("unsupported property type for %s", dst.Type())
}
