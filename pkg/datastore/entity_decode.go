package datastore

import (
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// decodeEntity converts a Datastore entity to a Go struct.
// It also populates any field tagged with `datastore:"__key__"` with the entity's key.
func decodeEntity(entity map[string]any, dst any) error {
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return errNotStructPtr
	}

	properties, ok := entity["properties"].(map[string]any)
	if !ok {
		return errInvalidEntity
	}

	// Extract key if present
	var key *Key
	if keyData, ok := entity["key"]; ok {
		var err error
		key, err = keyFromJSON(keyData)
		if err != nil {
			// Non-fatal: continue without key
			key = nil
		}
	}

	return decodeStruct(properties, v.Elem(), key, "")
}

// decodeStruct decodes Datastore properties into a struct.
// key is the entity key (for __key__ field population).
// prefix is used for flattened fields (e.g., "Address.").
func decodeStruct(properties map[string]any, v reflect.Value, key *Key, prefix string) error {
	t := v.Type()

	for i := range v.NumField() {
		field := t.Field(i)
		fieldVal := v.Field(i)

		if !field.IsExported() {
			continue
		}

		opts := parseDecodeTag(field)
		if opts.skip {
			continue
		}

		// Handle __key__ field
		if opts.name == "__key__" && key != nil {
			if fieldVal.Type() == reflect.TypeOf((*Key)(nil)) {
				fieldVal.Set(reflect.ValueOf(key))
			}
			continue
		}

		// Handle embedded (anonymous) structs
		if field.Anonymous && fieldVal.Kind() == reflect.Struct {
			if err := decodeStruct(properties, fieldVal, key, prefix); err != nil {
				return fmt.Errorf("embedded %s: %w", field.Name, err)
			}
			continue
		}

		propName := prefix + opts.name

		// Handle flatten for struct fields
		if opts.flatten && isDecodableStruct(fieldVal) {
			sv := fieldVal
			if sv.Kind() == reflect.Ptr {
				// Allocate if nil
				if sv.IsNil() {
					sv.Set(reflect.New(sv.Type().Elem()))
				}
				sv = sv.Elem()
			}
			if err := decodeStruct(properties, sv, key, propName+"."); err != nil {
				return fmt.Errorf("field %s: %w", field.Name, err)
			}
			continue
		}

		prop, ok := properties[propName]
		if !ok {
			continue
		}

		propMap, ok := prop.(map[string]any)
		if !ok {
			continue
		}

		if err := decodeValue(propMap, fieldVal); err != nil {
			return fmt.Errorf("field %s: %w", field.Name, err)
		}
	}

	return nil
}

// decodeTagOptions holds parsed decode tag options.
type decodeTagOptions struct {
	name    string
	flatten bool
	skip    bool
}

// parseDecodeTag extracts field name and options from datastore tag for decoding.
func parseDecodeTag(field reflect.StructField) decodeTagOptions {
	opts := decodeTagOptions{name: field.Name}

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
		if opt == "flatten" {
			opts.flatten = true
		}
	}

	return opts
}

// isDecodableStruct reports whether v is a struct or pointer to struct (excluding time.Time).
func isDecodableStruct(v reflect.Value) bool {
	if v.Kind() == reflect.Struct {
		return v.Type() != reflect.TypeOf(time.Time{})
	}
	if v.Kind() == reflect.Ptr {
		elem := v.Type().Elem()
		return elem.Kind() == reflect.Struct && elem != reflect.TypeOf(time.Time{})
	}
	return false
}

// decodeValue decodes a Datastore property value into a Go reflect.Value.
func decodeValue(prop map[string]any, dst reflect.Value) error {
	// Handle pointer destinations
	if dst.Kind() == reflect.Ptr {
		// Check for null
		if _, ok := prop["nullValue"]; ok {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		// Allocate if nil
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		return decodeValue(prop, dst.Elem())
	}

	// Handle null for non-pointers
	if _, ok := prop["nullValue"]; ok {
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}

	// String
	if val, ok := prop["stringValue"]; ok {
		return decodeString(val, dst)
	}

	// Integer
	if val, ok := prop["integerValue"]; ok {
		return decodeInteger(val, dst)
	}

	// Boolean
	if val, ok := prop["booleanValue"]; ok {
		return decodeBool(val, dst)
	}

	// Double/Float
	if val, ok := prop["doubleValue"]; ok {
		return decodeDouble(val, dst)
	}

	// Timestamp
	if val, ok := prop["timestampValue"]; ok {
		return decodeTimestamp(val, dst)
	}

	// Blob
	if val, ok := prop["blobValue"]; ok {
		return decodeBlob(val, dst)
	}

	// Array
	if val, ok := prop["arrayValue"]; ok {
		return decodeArray(val, dst)
	}

	// Entity (nested struct)
	if val, ok := prop["entityValue"]; ok {
		return decodeEntityValue(val, dst)
	}

	// Key reference
	if val, ok := prop["keyValue"]; ok {
		return decodeKeyValue(val, dst)
	}

	return fmt.Errorf("unsupported property type for %s", dst.Type())
}

func decodeString(val any, dst reflect.Value) error {
	s, ok := val.(string)
	if !ok {
		return errors.New("invalid string value")
	}
	if dst.Kind() != reflect.String {
		return fmt.Errorf("cannot decode string into %s", dst.Type())
	}
	dst.SetString(s)
	return nil
}

func decodeInteger(val any, dst reflect.Value) error {
	var intVal int64

	switch v := val.(type) {
	case string:
		var err error
		intVal, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid integer: %w", err)
		}
	case float64:
		intVal = int64(v)
	default:
		return fmt.Errorf("unexpected integer format: %T", val)
	}

	switch dst.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst.SetInt(intVal)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if intVal < 0 {
			return fmt.Errorf("cannot decode negative value %d into unsigned type", intVal)
		}
		dst.SetUint(uint64(intVal))
	default:
		return fmt.Errorf("cannot decode integer into %s", dst.Type())
	}
	return nil
}

func decodeBool(val any, dst reflect.Value) error {
	b, ok := val.(bool)
	if !ok {
		return errors.New("invalid boolean value")
	}
	if dst.Kind() != reflect.Bool {
		return fmt.Errorf("cannot decode bool into %s", dst.Type())
	}
	dst.SetBool(b)
	return nil
}

func decodeDouble(val any, dst reflect.Value) error {
	f, ok := val.(float64)
	if !ok {
		return errors.New("invalid double value")
	}
	switch dst.Kind() {
	case reflect.Float32, reflect.Float64:
		dst.SetFloat(f)
	default:
		return fmt.Errorf("cannot decode double into %s", dst.Type())
	}
	return nil
}

func decodeTimestamp(val any, dst reflect.Value) error {
	s, ok := val.(string)
	if !ok {
		return errors.New("invalid timestamp value")
	}
	if dst.Type() != reflect.TypeOf(time.Time{}) {
		return fmt.Errorf("cannot decode timestamp into %s", dst.Type())
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return fmt.Errorf("invalid timestamp format: %w", err)
	}
	dst.Set(reflect.ValueOf(t))
	return nil
}

func decodeBlob(val any, dst reflect.Value) error {
	s, ok := val.(string)
	if !ok {
		return errors.New("invalid blob value")
	}
	if dst.Type() != reflect.TypeOf([]byte(nil)) {
		return fmt.Errorf("cannot decode blob into %s", dst.Type())
	}
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("invalid base64: %w", err)
	}
	dst.SetBytes(data)
	return nil
}

func decodeArray(val any, dst reflect.Value) error {
	if dst.Kind() != reflect.Slice {
		return fmt.Errorf("cannot decode array into %s", dst.Type())
	}

	arrayMap, ok := val.(map[string]any)
	if !ok {
		return errors.New("invalid arrayValue format")
	}

	valuesAny, ok := arrayMap["values"]
	if !ok {
		dst.Set(reflect.MakeSlice(dst.Type(), 0, 0))
		return nil
	}

	values, ok := valuesAny.([]any)
	if !ok {
		return errors.New("invalid arrayValue.values format")
	}

	slice := reflect.MakeSlice(dst.Type(), len(values), len(values))

	for i, elemAny := range values {
		elemMap, ok := elemAny.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid array element %d", i)
		}
		if err := decodeValue(elemMap, slice.Index(i)); err != nil {
			return fmt.Errorf("element %d: %w", i, err)
		}
	}

	dst.Set(slice)
	return nil
}

func decodeEntityValue(val any, dst reflect.Value) error {
	entityMap, ok := val.(map[string]any)
	if !ok {
		return errors.New("invalid entityValue format")
	}

	properties, ok := entityMap["properties"].(map[string]any)
	if !ok {
		return errors.New("invalid entityValue.properties format")
	}

	if dst.Kind() != reflect.Struct {
		return fmt.Errorf("cannot decode entity into %s", dst.Type())
	}

	return decodeStruct(properties, dst, nil, "")
}

func decodeKeyValue(val any, dst reflect.Value) error {
	if dst.Type() != reflect.TypeOf((*Key)(nil)) {
		return fmt.Errorf("cannot decode key into %s", dst.Type())
	}

	key, err := keyFromJSON(val)
	if err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	dst.Set(reflect.ValueOf(key))
	return nil
}
