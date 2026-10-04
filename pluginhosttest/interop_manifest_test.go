package pluginhosttest

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"

	"github.com/hollis-labs/plugin-host/internal/interopfixture"
)

const interopSDKSourceCommit = "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21"
const interopAdapterOwner = "plugin-host interop adapter (CW-20261003-0189 slice 5)"
const interopFixtureLimit = 8 << 20

// Recipes retain their authored steps and expected values. The host runner must
// interpret these records, never copy expectations into a second fixture set.
type interopRecipe struct {
	Name             string            `json:"name"`
	Profile          string            `json:"profile"`
	Scenario         string            `json:"scenario"`
	Level            string            `json:"level"`
	Status           string            `json:"status"`
	Owner            string            `json:"owner"`
	UnavailableOwner string            `json:"unavailable_owner"`
	Finding          string            `json:"finding"`
	Reason           string            `json:"reason"`
	Preferred        string            `json:"preferred"`
	Steps            []json.RawMessage `json:"steps"`
	Raw              json.RawMessage   `json:"-"`
}

type interopManifest struct {
	CorpusVersion int               `json:"corpus_version"`
	Coverage      string            `json:"coverage"`
	BaseProfiles  []string          `json:"base_profiles"`
	DuplexSource  string            `json:"duplex_source"`
	DuplexCases   []string          `json:"duplex_runtime_cases"`
	Lifecycle     []json.RawMessage `json:"lifecycle"`
	Expanded      []json.RawMessage `json:"expanded"`
	Negotiated    *struct {
		Mode              string   `json:"mode"`
		Matrix            string   `json:"matrix"`
		Recipes           []string `json:"recipes"`
		ExpandedScenarios string   `json:"expanded_scenarios"`
		ExpandedSelection string   `json:"expanded_selection"`
		Unavailable       []struct {
			Feature string `json:"feature"`
			Owner   string `json:"owner"`
			Reason  string `json:"reason"`
		} `json:"unavailable"`
	} `json:"negotiated"`
	Pending []struct {
		Feature string `json:"feature"`
		Owner   string `json:"owner"`
	} `json:"pending_coverage"`
}

type interopCase struct {
	Name    string        `json:"case"`
	Runtime string        `json:"runtime,omitempty"`
	Mode    string        `json:"mode,omitempty"`
	Family  string        `json:"family"`
	Profile string        `json:"profile,omitempty"`
	Level   string        `json:"level"`
	Status  string        `json:"status"`
	Owner   string        `json:"pending_owner"`
	Reason  string        `json:"reason"`
	Recipe  interopRecipe `json:"-"`
}

func readInteropJSON(source fs.FS, name string, out any) error {
	if !fs.ValidPath(name) {
		return fmt.Errorf("invalid corpus path %q", name)
	}
	f, err := source.Open(name)
	if err != nil {
		return err
	}
	defer f.Close() // read-only corpus; Close has no outstanding writes.
	raw, err := io.ReadAll(io.LimitReader(f, interopFixtureLimit+1))
	if err != nil {
		return err
	}
	if len(raw) > interopFixtureLimit {
		return fmt.Errorf("corpus file %s exceeds byte limit", name)
	}
	return json.Unmarshal(raw, out)
}

func decodeInteropRecipe(raw json.RawMessage, transcript bool) (interopRecipe, error) {
	var recipe interopRecipe
	if err := json.Unmarshal(raw, &recipe); err != nil {
		return recipe, err
	}
	recipe.Raw = raw
	if recipe.Level != "normative" && recipe.Level != "observed-quirk" && recipe.Level != "internal-core" {
		return recipe, fmt.Errorf("invalid recipe level %q", recipe.Level)
	}
	if recipe.Status != "" && recipe.Status != "observed" && recipe.Status != "proposed" {
		return recipe, fmt.Errorf("invalid recipe availability %q", recipe.Status)
	}
	if recipe.Status == "proposed" {
		owner, reason := recipe.Owner, recipe.Reason
		if transcript {
			owner, reason = recipe.UnavailableOwner, recipe.Finding
		}
		if strings.TrimSpace(owner) == "" || strings.TrimSpace(reason) == "" {
			return recipe, fmt.Errorf("unavailable recipe needs owner and reason")
		}
	}
	if transcript && recipe.Steps == nil {
		return recipe, fmt.Errorf("transcript needs steps")
	}
	for _, rawStep := range recipe.Steps {
		var step struct {
			Level     string `json:"level"`
			Preferred string `json:"preferred"`
		}
		if err := json.Unmarshal(rawStep, &step); err != nil {
			return recipe, err
		}
		level := step.Level
		if level == "" {
			level = recipe.Level
		}
		if level != "normative" && level != "observed-quirk" && level != "internal-core" {
			return recipe, fmt.Errorf("invalid step level %q", level)
		}
		if level == "observed-quirk" && strings.TrimSpace(step.Preferred) == "" && strings.TrimSpace(recipe.Preferred) == "" {
			return recipe, fmt.Errorf("historical quirk needs preferred note")
		}
	}
	return recipe, nil
}

// Inventory is generic over case names, profiles and scenario names. It does
// not claim execution: supported handlers will replace pending records later.
func inventoryInterop(source fs.FS) (interopManifest, []interopCase, error) {
	var manifest interopManifest
	var rawManifest json.RawMessage
	if err := readInteropJSON(source, "protocol/v2/fixtures/duplex-child.json", &rawManifest); err != nil {
		return manifest, nil, err
	}
	if err := json.Unmarshal(rawManifest, &manifest); err != nil {
		return manifest, nil, err
	}
	var selectorFields map[string]json.RawMessage
	if err := json.Unmarshal(rawManifest, &selectorFields); err != nil {
		return manifest, nil, err
	}
	if _, authored := selectorFields["expanded_selection"]; authored && (manifest.Negotiated == nil || manifest.Negotiated.ExpandedSelection == "") {
		return manifest, nil, fmt.Errorf("authored selector missing negotiated linkage")
	}
	if manifest.CorpusVersion != 1 {
		return manifest, nil, fmt.Errorf("unsupported corpus version %d", manifest.CorpusVersion)
	}
	profiles := make(map[string]bool)
	for _, profile := range manifest.BaseProfiles {
		profiles[profile] = true
	}
	var cases []interopCase
	seen := make(map[string]bool)
	add := func(family, name string, recipe interopRecipe, transcript bool) error {
		key := family + "/" + name
		if name == "" || seen[key] {
			return fmt.Errorf("missing or duplicate corpus case %q", key)
		}
		seen[key] = true
		item := interopCase{Name: name, Family: family, Profile: recipe.Profile, Level: recipe.Level, Status: "pending", Owner: interopAdapterOwner, Reason: "Host replay handler not implemented in this inventory; actual execution receipts are separate", Recipe: recipe}
		if family == "duplex" || family == "expanded" {
			item.Mode = "internal-test-only"
		}
		if recipe.Scenario == "deadline" {
			item.Owner = "orch-pp0 host/protocol contract owners"
			item.Reason = "FULL arrival-deadline-and-absent-context source-available, host-unsupported, PENDING/incompatible: finite parent required, local and wire budgets coupled"
		}
		if recipe.Status == "proposed" {
			item.Status, item.Owner, item.Reason = "unavailable", recipe.Owner, recipe.Reason
			if transcript {
				item.Owner, item.Reason = recipe.UnavailableOwner, recipe.Finding
			}
		}
		cases = append(cases, item)
		return nil
	}
	entries, err := fs.ReadDir(source, "docs/protocol/v2/transcripts")
	if err != nil {
		return manifest, nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		var raw json.RawMessage
		if err := readInteropJSON(source, path.Join("docs/protocol/v2/transcripts", entry.Name()), &raw); err != nil {
			return manifest, nil, err
		}
		recipe, err := decodeInteropRecipe(raw, true)
		if err != nil {
			return manifest, nil, fmt.Errorf("%s: %w", entry.Name(), err)
		}
		if !profiles[recipe.Profile] && recipe.Status != "proposed" {
			continue
		}
		if err := add("base", entry.Name(), recipe, true); err != nil {
			return manifest, nil, err
		}
	}
	var duplex struct {
		Runtime []json.RawMessage `json:"runtime"`
	}
	if !fs.ValidPath(manifest.DuplexSource) || path.Base(manifest.DuplexSource) != manifest.DuplexSource {
		return manifest, nil, fmt.Errorf("invalid duplex source")
	}
	if err := readInteropJSON(source, path.Join("protocol/v2/fixtures", manifest.DuplexSource), &duplex); err != nil {
		return manifest, nil, err
	}
	recipes := make(map[string]interopRecipe)
	for _, raw := range duplex.Runtime {
		recipe, err := decodeInteropRecipe(raw, false)
		if err != nil {
			return manifest, nil, err
		}
		if recipe.Name == "" {
			return manifest, nil, fmt.Errorf("duplex recipe has no name")
		}
		if _, exists := recipes[recipe.Name]; exists {
			return manifest, nil, fmt.Errorf("duplicate duplex recipe %s", recipe.Name)
		}
		recipes[recipe.Name] = recipe
	}
	for _, name := range manifest.DuplexCases {
		recipe, ok := recipes[name]
		if !ok {
			return manifest, nil, fmt.Errorf("missing duplex recipe %s", name)
		}
		if err := add("duplex", name, recipe, false); err != nil {
			return manifest, nil, err
		}
	}
	for _, group := range []struct {
		name string
		raw  []json.RawMessage
	}{{"lifecycle", manifest.Lifecycle}, {"expanded", manifest.Expanded}} {
		for _, raw := range group.raw {
			recipe, err := decodeInteropRecipe(raw, false)
			if err != nil {
				return manifest, nil, err
			}
			if err := add(group.name, recipe.Name, recipe, false); err != nil {
				return manifest, nil, err
			}
		}
	}
	if negotiated := manifest.Negotiated; negotiated != nil {
		if negotiated.Mode == "" || !fs.ValidPath(negotiated.Matrix) || path.Base(negotiated.Matrix) != negotiated.Matrix {
			return manifest, nil, fmt.Errorf("invalid negotiated manifest metadata")
		}
		var matrix struct {
			Level string            `json:"level"`
			Cases []json.RawMessage `json:"cases"`
		}
		if err := readInteropJSON(source, path.Join("protocol/v2/fixtures", negotiated.Matrix), &matrix); err != nil {
			return manifest, nil, err
		}
		for _, raw := range matrix.Cases {
			var recipe interopRecipe
			if err := json.Unmarshal(raw, &recipe); err != nil {
				return manifest, nil, err
			}
			recipe.Level, recipe.Raw = strings.ToLower(matrix.Level), raw
			if recipe.Level != "normative" && recipe.Level != "observed-quirk" {
				return manifest, nil, fmt.Errorf("invalid negotiation matrix level")
			}
			if err := add("negotiation-matrix", recipe.Name, recipe, false); err != nil {
				return manifest, nil, err
			}
			cases[len(cases)-1].Mode = negotiated.Mode
		}
		for _, name := range negotiated.Recipes {
			if err := add("negotiated", name, interopRecipe{Level: "normative"}, false); err != nil {
				return manifest, nil, err
			}
			cases[len(cases)-1].Mode = negotiated.Mode
		}
		// This is an authored prose selection rather than a machine-readable list.
		// Preserve it as an unsupported selection instead of inventing case filters.
		if negotiated.ExpandedSelection != "" {
			selected, err := interopfixture.Select(rawManifest, negotiated.ExpandedSelection, nil)
			if err != nil {
				return manifest, nil, err
			}
			for _, raw := range selected {
				recipe, err := decodeInteropRecipe(raw, false)
				if err != nil {
					return manifest, nil, err
				}
				if err := add("negotiated-expanded", recipe.Name, recipe, false); err != nil {
					return manifest, nil, err
				}
				cases[len(cases)-1].Mode = negotiated.ExpandedSelection
			}
		} else if negotiated.ExpandedScenarios != "" {
			if err := add("negotiated-selection", negotiated.ExpandedScenarios, interopRecipe{Level: "normative"}, false); err != nil {
				return manifest, nil, err
			}
			cases[len(cases)-1].Mode = negotiated.Mode
			cases[len(cases)-1].Reason = "Manifest expanded_scenarios is prose; explicit negotiated selection adapter is pending"
		}
		for _, unavailable := range negotiated.Unavailable {
			if strings.TrimSpace(unavailable.Owner) == "" || strings.TrimSpace(unavailable.Reason) == "" {
				return manifest, nil, fmt.Errorf("negotiated unavailability needs owner and reason")
			}
			if err := add("negotiated-unavailable", unavailable.Feature, interopRecipe{Level: "normative", Status: "proposed", Owner: unavailable.Owner, Reason: unavailable.Reason}, false); err != nil {
				return manifest, nil, err
			}
			cases[len(cases)-1].Mode = negotiated.Mode
		}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(rawManifest, &fields); err != nil {
		return manifest, nil, err
	}
	for _, known := range []string{"corpus_version", "coverage", "base_profiles", "duplex_source", "duplex_runtime_cases", "lifecycle", "expanded", "expanded_selection", "pending_coverage", "negotiated"} {
		delete(fields, known)
	}
	for field, raw := range fields {
		if err := add("unsupported-manifest-field", field, interopRecipe{Level: "normative", Raw: raw}, false); err != nil {
			return manifest, nil, err
		}
		cases[len(cases)-1].Reason = "Manifest field has no inventory adapter; preserved for pending owner"
	}
	for _, pending := range manifest.Pending {
		if strings.TrimSpace(pending.Feature) == "" || strings.TrimSpace(pending.Owner) == "" {
			return manifest, nil, fmt.Errorf("pending coverage needs feature and owner")
		}
		if err := add("coverage", pending.Feature, interopRecipe{Level: "normative", Status: "proposed", Owner: pending.Owner, Reason: "Listed in SDK pending_coverage"}, false); err != nil {
			return manifest, nil, err
		}
	}
	sort.Slice(cases, func(i, j int) bool { return cases[i].Family+"/"+cases[i].Name < cases[j].Family+"/"+cases[j].Name })
	return manifest, cases, nil
}
