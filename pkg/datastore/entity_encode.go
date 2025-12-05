package datastore

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// tagOptions holds parsed struct field tag options.
type tagOptions struct {
	name      string
	noIndex   bool
	omitempty bool
	flatten   bool
	skip      bool
}

// encodeEntity converts a Go struct to a Datastore entity.
func encodeEntity(key *Key, src any) (map[string]any, error) {
	v := reflect.ValueOf(src)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, errNotStruct
	}

	properties, err := encodeStruct(v, "")
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"key":        keyToJSON(key),
		"properties": properties,
	}, nil
}

// encodeStruct encodes a struct value to Datastore properties.
// prefix is used for flattened nested structs (e.g., "Address.").
func encodeStruct(v reflect.Value, prefix string) (map[string]any, error) {
	t := v.Type()
	properties := make(map[string]any)

	for i := range v.NumField() {
		field := t.Field(i)
		fieldVal := v.Field(i)

		if !field.IsExported() {
			continue
		}

		opts := parseTag(field)
		if opts.skip {
			continue
		}

		// Handle embedded (anonymous) structs
		if field.Anonymous && fieldVal.Kind() == reflect.Struct {
			embedded, err := encodeStruct(fieldVal, prefix)
			if err != nil {
				return nil, fmt.Errorf("embedded %s: %w", field.Name, err)
			}
			for k, v := range embedded {
				properties[k] = v
			}
			continue
		}

		// Check omitempty before encoding
		if opts.omitempty && isEmpty(fieldVal) {
			continue
		}

		propName := prefix + opts.name

		// Handle flatten for struct fields
		if opts.flatten && isStructOrStructPtr(fieldVal) {
			sv := fieldVal
			if sv.Kind() == reflect.Ptr {
				if sv.IsNil() {
					continue // Skip nil pointers when flattening
				}
				sv = sv.Elem()
			}
			flattened, err := encodeStruct(sv, propName+".")
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", field.Name, err)
			}
			for k, v := range flattened {
				properties[k] = v
			}
			continue
		}

		prop, err := encodeValue(fieldVal)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field.Name, err)
		}

		if opts.noIndex {
			if m, ok := prop.(map[string]any); ok {
				m["excludeFromIndexes"] = true
			}
		}

		properties[propName] = prop
	}

	return properties, nil
}

// parseTag extracts field name and options from datastore tag.
func parseTag(field reflect.StructField) tagOptions {
	opts := tagOptions{name: field.Name}

	tag := field.Tag.Get("datastore")
	if tag == "" {
		return opts
	}

	parts := strings.Split(tag, ",")
	if parts[0] == "-" {
		opts.skip = true
		return opts
	}
	if parts[0] != "" {
		opts.name = parts[0]
	}

	for _, opt := range parts[1:] {
		switch opt {
		case "noindex":
			opts.noIndex = true
		case "omitempty":
			opts.omitempty = true
		case "flatten":
			opts.flatten = true
		default:
			// Ignore unknown options
		}
	}

	return opts
}

// isEmpty reports whether v is the zero value for its type.
func isEmpty(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface:
		return v.IsNil()
	case reflect.Slice, reflect.Map:
		return v.IsNil() || v.Len() == 0
	case reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Struct:
		// Special case for time.Time
		if t, ok := v.Interface().(time.Time); ok {
			return t.IsZero()
		}
		return false
	default:
		return false
	}
}

// isStructOrStructPtr reports whether v is a struct or pointer to struct.
func isStructOrStructPtr(v reflect.Value) bool {
	if v.Kind() == reflect.Struct {
		return true
	}
	if v.Kind() == reflect.Ptr && v.Type().Elem().Kind() == reflect.Struct {
		return true
	}
	return false
}

// encodeAny converts any Go value to a Datastore property value.
func encodeAny(v any) (any, error) {
	if v == nil {
		return map[string]any{"nullValue": nil}, nil
	}
	return encodeValue(reflect.ValueOf(v))
}

// encodeValue converts a Go reflect.Value to a Datastore property value.
func encodeValue(v reflect.Value) (any, error) {
	// Handle pointers - dereference or return null
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return map[string]any{"nullValue": nil}, nil
		}
		v = v.Elem()
	}

	// Handle interface{} - get underlying value
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return map[string]any{"nullValue": nil}, nil
		}
		v = v.Elem()
	}

	// Check for specific types first (before kind switch)
	switch val := v.Interface().(type) {
	case time.Time:
		return map[string]any{"timestampValue": val.Format(time.RFC3339Nano)}, nil
	case *Key:
		if val == nil {
			return map[string]any{"nullValue": nil}, nil
		}
		return map[string]any{"keyValue": keyToJSON(val)}, nil
	}

	// Handle by kind
	switch v.Kind() {
	case reflect.String:
		return map[string]any{"stringValue": v.String()}, nil

	case reflect.Bool:
		return map[string]any{"booleanValue": v.Bool()}, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return map[string]any{"integerValue": strconv.FormatInt(v.Int(), 10)}, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return map[string]any{"integerValue": strconv.FormatUint(v.Uint(), 10)}, nil

	case reflect.Float32, reflect.Float64:
		return map[string]any{"doubleValue": v.Float()}, nil

	case reflect.Slice, reflect.Array:
		return encodeSlice(v)

	case reflect.Struct:
		return encodeNestedStruct(v)

	default:
		return nil, fmt.Errorf("unsupported type: %s", v.Type())
	}
}

// encodeSlice encodes a slice or array to a Datastore array value.
func encodeSlice(v reflect.Value) (any, error) {
	// Special case: []byte becomes blobValue (only for slices, not arrays)
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		data := v.Bytes()
		return map[string]any{"blobValue": base64.StdEncoding.EncodeToString(data)}, nil
	}

	length := v.Len()
	values := make([]map[string]any, length)

	for i := range length {
		elem := v.Index(i)
		encoded, err := encodeValue(elem)
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		m, ok := encoded.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("unexpected encoded type for element %d", i)
		}
		values[i] = m
	}

	return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
}

// encodeNestedStruct encodes a nested struct as an entity value.
func encodeNestedStruct(v reflect.Value) (any, error) {
	properties, err := encodeStruct(v, "")
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"entityValue": map[string]any{
			"properties": properties,
		},
	}, nil
}
