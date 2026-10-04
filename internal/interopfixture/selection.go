// Package interopfixture reads SDK-authored test selection metadata. It supplies
// no plugin runtime or host policy and never reports an execution pass.
package interopfixture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

type selector struct {
	Version int                        `json:"version"`
	Kind    string                     `json:"kind"`
	Source  string                     `json:"source_group"`
	Modes   map[string]json.RawMessage `json:"modes"`
}
type modeSelection struct {
	Exclusions []string `json:"exclude_scenarios"`
}

func closed(raw json.RawMessage, out any) error {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("missing object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return fmt.Errorf("object required")
	}
	expected := map[string]bool{}
	typ := reflect.TypeOf(out).Elem()
	for i := 0; i < typ.NumField(); i++ {
		expected[strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]] = true
	}
	for key := range fields {
		if !expected[key] {
			return fmt.Errorf("unknown exact-case member %q", key)
		}
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}
func text(raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", err
	}
	if strings.TrimSpace(s) == "" {
		return "", fmt.Errorf("missing/nonblank text required")
	}
	return s, nil
}

// Select validates the whole referenced source before filtering, retaining
// original objects in source order. Unknown top-level groups remain for the
// inventory caller; they are never implicitly selected.
func Select(raw []byte, mode string, names []string) ([]json.RawMessage, error) {
	if len(raw) > 8<<20 {
		return nil, fmt.Errorf("manifest byte limit")
	}
	var manifest map[string]json.RawMessage
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, err
	}
	var s selector
	if err := closed(manifest["expanded_selection"], &s); err != nil {
		return nil, fmt.Errorf("selector: %w", err)
	}
	if s.Version != 1 || s.Kind != "source-group" || strings.TrimSpace(s.Source) == "" {
		return nil, fmt.Errorf("unsupported selector version/kind/source")
	}
	sourceRaw, ok := manifest[s.Source]
	if !ok {
		return nil, fmt.Errorf("missing referenced source group")
	}
	var source []json.RawMessage
	if err := json.Unmarshal(sourceRaw, &source); err != nil || source == nil {
		return nil, fmt.Errorf("source group must be an array")
	}
	if len(s.Modes) != 2 {
		return nil, fmt.Errorf("unsupported/missing modes")
	}
	validated := make(map[string][]string)
	for _, name := range []string{"internal-test-only", "normal-serve-negotiated"} {
		var m modeSelection
		if err := closed(s.Modes[name], &m); err != nil || m.Exclusions == nil {
			return nil, fmt.Errorf("invalid mode %s", name)
		}
		seen := map[string]bool{}
		for _, v := range m.Exclusions {
			if strings.TrimSpace(v) == "" || seen[v] {
				return nil, fmt.Errorf("invalid/duplicate exclusion")
			}
			seen[v] = true
		}
		validated[name] = m.Exclusions
	}
	exclusions, ok := validated[mode]
	if !ok {
		return nil, fmt.Errorf("unsupported requested mode")
	}
	var negotiated map[string]json.RawMessage
	if err := json.Unmarshal(manifest["negotiated"], &negotiated); err != nil {
		return nil, err
	}
	link, err := text(negotiated["expanded_selection"])
	if err != nil || link != "normal-serve-negotiated" {
		return nil, fmt.Errorf("invalid negotiated selection linkage")
	}
	identities := map[string]bool{}
	var selected []json.RawMessage
	selectedNames := map[string]bool{}
	for _, rawRecipe := range source {
		var recipe map[string]json.RawMessage
		if err := json.Unmarshal(rawRecipe, &recipe); err != nil || recipe == nil {
			return nil, fmt.Errorf("source recipe must be object")
		}
		name, err := text(recipe["name"])
		if err != nil || identities[name] {
			return nil, fmt.Errorf("invalid/duplicate source name")
		}
		identities[name] = true
		level, err := text(recipe["level"])
		if err != nil || level != "normative" {
			return nil, fmt.Errorf("invalid source level")
		}
		status := "observed"
		if rawStatus, exists := recipe["status"]; exists {
			status, err = text(rawStatus)
			if err != nil {
				return nil, fmt.Errorf("invalid source status")
			}
		}
		switch status {
		case "observed":
			for _, field := range []string{"profile", "scenario"} {
				if _, err := text(recipe[field]); err != nil {
					return nil, fmt.Errorf("invalid observed %s", field)
				}
			}
		case "proposed":
			for _, field := range []string{"owner", "reason"} {
				if _, err := text(recipe[field]); err != nil {
					return nil, fmt.Errorf("invalid proposed %s", field)
				}
			}
			for _, field := range []string{"profile", "scenario"} {
				if raw, exists := recipe[field]; exists {
					if _, err := text(raw); err != nil {
						return nil, fmt.Errorf("invalid proposed %s", field)
					}
				}
			}
		default:
			return nil, fmt.Errorf("unsupported source status")
		}
		var scenario string
		_ = json.Unmarshal(recipe["scenario"], &scenario)
		excluded := false
		for _, v := range exclusions {
			excluded = excluded || v == scenario
		}
		if !excluded {
			selected = append(selected, rawRecipe)
			selectedNames[name] = true
		}
	}
	if names == nil {
		return selected, nil
	}
	requested := map[string]bool{}
	for _, name := range names {
		if strings.TrimSpace(name) == "" || requested[name] || !selectedNames[name] {
			return nil, fmt.Errorf("unknown/excluded/duplicate requested case")
		}
		requested[name] = true
	}
	filtered := make([]json.RawMessage, 0, len(names))
	for _, rawRecipe := range selected {
		var v struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(rawRecipe, &v)
		if requested[v.Name] {
			filtered = append(filtered, rawRecipe)
		}
	}
	return filtered, nil
}
