// Adapted from plugin-sdk internal/strictjson at 5c663e7ce74c40ceceb95133c439396316850858 (MIT).
// Package strictjson checks JSON tokens before decoding can discard duplicate
// keys. Opaque values retain their JSON representation and field casing.
package strictjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"unicode/utf8"
)

// MaxDepth bounds recursive JSON inspection independently of transport framing.
const MaxDepth = 128

// Validate accepts one JSON value, rejecting duplicate keys at every depth.
func Validate(data []byte) error {
	return validate(data, false)
}

// ValidatePortable also checks descriptor scope numbers: finite values, and
// integer-form tokens within the common Go/JS safe range. Larger integers must
// be string-encoded by their descriptor schema.
func ValidatePortable(data []byte) error { return validate(data, true) }
func validate(data []byte, portable bool) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("invalid JSON UTF-8")
	}
	if err := stringsValid(data); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := value(d, 0, portable); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("expected one JSON value")
	}
	return nil
}

func value(d *json.Decoder, depth int, portable bool) error {
	if depth > MaxDepth {
		return fmt.Errorf("JSON nesting exceeds limit")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	if n, ok := t.(json.Number); ok && portable {
		f, err := strconv.ParseFloat(string(n), 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return fmt.Errorf("nonfinite scope number")
		}
		if !bytes.ContainsAny([]byte(n), ".eE") {
			i, err := strconv.ParseInt(string(n), 10, 64)
			if err != nil || i < -9007199254740991 || i > 9007199254740991 {
				return fmt.Errorf("unsafe scope integer")
			}
		}
	}
	switch t {
	case json.Delim('{'):
		seen := make(map[string]bool)
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			k, ok := key.(string)
			if !ok {
				return fmt.Errorf("expected JSON object key")
			}
			if seen[k] {
				return fmt.Errorf("duplicate JSON key")
			}
			seen[k] = true
			if err := value(d, depth+1, portable); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return fmt.Errorf("invalid JSON object")
		}
	case json.Delim('['):
		for d.More() {
			if err := value(d, depth+1, portable); err != nil {
				return err
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return fmt.Errorf("invalid JSON array")
		}
	case json.Delim('}'), json.Delim(']'):
		return fmt.Errorf("unexpected JSON delimiter")
	}
	return nil
}

// Object returns raw fields of a closed object. Every named field is required
// and non-null. Exact key casing is enforced; no permissive struct matching.
func Object(data []byte, fields ...string) (map[string]json.RawMessage, error) {
	return ObjectFields(data, fields, nil)
}

// ObjectFields also permits named optional fields, whose present values cannot
// be null. Unknown and incorrectly cased fields remain invalid.
func ObjectFields(data []byte, fields, optional []string) (map[string]json.RawMessage, error) {
	if err := Validate(data); err != nil {
		return nil, err
	}
	if b := bytes.TrimSpace(data); len(b) == 0 || b[0] != '{' {
		return nil, fmt.Errorf("expected JSON object")
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	allowed := make(map[string]bool, len(fields))
	for _, f := range fields {
		allowed[f] = true
	}
	for _, f := range optional {
		allowed[f] = true
	}
	for k := range out {
		if !allowed[k] {
			return nil, fmt.Errorf("unknown or incorrectly cased JSON field")
		}
		if bytes.Equal(bytes.TrimSpace(out[k]), []byte("null")) {
			return nil, fmt.Errorf("null JSON field")
		}
	}
	for _, f := range fields {
		v, ok := out[f]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return nil, fmt.Errorf("required non-null field %s", f)
		}
	}
	return out, nil
}

// The Go decoder replaces unpaired surrogate escapes. Refuse them before that
// conversion so identifiers and opaque strings have identical Go/JS meaning.
func stringsValid(data []byte) error {
	for i := 0; i < len(data); i++ {
		if data[i] != '"' {
			continue
		}
		for i++; i < len(data) && data[i] != '"'; i++ {
			if data[i] != '\\' {
				continue
			}
			if i+1 >= len(data) {
				return fmt.Errorf("invalid JSON string")
			}
			if data[i+1] != 'u' {
				i++
				continue
			}
			if i+6 > len(data) {
				return fmt.Errorf("invalid Unicode escape")
			}
			n, err := strconv.ParseUint(string(data[i+2:i+6]), 16, 16)
			if err != nil {
				return fmt.Errorf("invalid Unicode escape")
			}
			if n >= 0xdc00 && n <= 0xdfff {
				return fmt.Errorf("unpaired Unicode surrogate")
			}
			if n >= 0xd800 && n <= 0xdbff {
				if i+12 > len(data) || string(data[i+6:i+8]) != "\\u" {
					return fmt.Errorf("unpaired Unicode surrogate")
				}
				low, err := strconv.ParseUint(string(data[i+8:i+12]), 16, 16)
				if err != nil || low < 0xdc00 || low > 0xdfff {
					return fmt.Errorf("unpaired Unicode surrogate")
				}
				i += 6
			}
			i += 5
		}
	}
	return nil
}
