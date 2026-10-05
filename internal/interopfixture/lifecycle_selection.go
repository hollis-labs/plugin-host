package interopfixture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/hollis-labs/plugin-host/internal/strictjson"
)

const LifecyclePublicMode = "public-conn-isolated-transport"
const LifecycleRemoteMode = "sdk-normal-serve-remote-overflow"

var lifecycleIdentifier = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

// SelectLifecycle validates the complete raw standalone corpus before selecting
// any rows. It preserves source metadata and order, including proposed status;
// selection is neither execution nor a conformance verdict. Nil names selects
// all eligible rows; an explicit empty slice selects none after validation.
func SelectLifecycle(raw []byte, mode string, names []string) ([]json.RawMessage, error) {
	if len(raw) > 8<<20 || bytes.HasPrefix(raw, []byte{0xef, 0xbb, 0xbf}) {
		return nil, fmt.Errorf("lifecycle manifest byte limit or BOM")
	}
	top, err := strictjson.Object(raw, "corpus_version", "contract_version", "selection", "groups")
	if err != nil {
		return nil, err
	}
	if !lifecycleVersion(top["corpus_version"], "1") || !lifecycleVersion(top["contract_version"], "2") {
		return nil, fmt.Errorf("unsupported lifecycle corpus/contract version")
	}
	selection, err := strictjson.Object(top["selection"], "version", "kind", "modes")
	if err != nil {
		return nil, err
	}
	kind, err := lifecycleString(selection["kind"])
	if err != nil || kind != "source-group" || !lifecycleVersion(selection["version"], "1") {
		return nil, fmt.Errorf("unsupported lifecycle selector")
	}
	links := map[string]string{LifecyclePublicMode: "lifecycle_compatible_v2", LifecycleRemoteMode: "sdk_remote_overflow_v2"}
	modes, err := strictjson.Object(selection["modes"], LifecyclePublicMode, LifecycleRemoteMode)
	if err != nil {
		return nil, err
	}
	groups, err := strictjson.Object(top["groups"], "lifecycle_compatible_v2", "sdk_remote_overflow_v2")
	if err != nil {
		return nil, err
	}
	rows := make(map[string][]json.RawMessage)
	scenarios := make(map[string]map[string]bool)
	globalNames := make(map[string]bool)
	for _, group := range []string{"lifecycle_compatible_v2", "sdk_remote_overflow_v2"} {
		var source []json.RawMessage
		if err := json.Unmarshal(groups[group], &source); err != nil || len(source) == 0 {
			return nil, fmt.Errorf("nonempty lifecycle source array required")
		}
		scenarios[group] = make(map[string]bool)
		for _, row := range source {
			fields, err := strictjson.Object(row, "name", "scenario", "profile", "level", "status", "owner", "reason")
			if err != nil {
				return nil, err
			}
			values := make(map[string]string)
			for _, field := range []string{"name", "scenario", "profile", "level", "status", "owner", "reason"} {
				value, err := lifecycleString(fields[field])
				if err != nil {
					return nil, err
				}
				values[field] = value
			}
			for _, field := range []string{"name", "scenario", "profile"} {
				if !lifecycleIdentifier.MatchString(values[field]) {
					return nil, fmt.Errorf("invalid lifecycle identifier")
				}
			}
			if values["level"] != "normative" || (values["status"] != "observed" && values["status"] != "proposed") {
				return nil, fmt.Errorf("invalid lifecycle level/status")
			}
			if strings.Trim(values["owner"], " \t\r\n") == "" || strings.Trim(values["reason"], " \t\r\n") == "" {
				return nil, fmt.Errorf("lifecycle owner/reason required")
			}
			if globalNames[values["name"]] || scenarios[group][values["scenario"]] {
				return nil, fmt.Errorf("duplicate lifecycle name/scenario")
			}
			globalNames[values["name"]] = true
			scenarios[group][values["scenario"]] = true
		}
		rows[group] = source
	}
	excluded := make(map[string]map[string]bool)
	for _, modeName := range []string{LifecyclePublicMode, LifecycleRemoteMode} {
		fields, err := strictjson.Object(modes[modeName], "source_group", "exclude_scenarios")
		if err != nil {
			return nil, err
		}
		group, err := lifecycleString(fields["source_group"])
		if err != nil || group != links[modeName] {
			return nil, fmt.Errorf("invalid lifecycle mode linkage")
		}
		var exclusions []string
		if err := json.Unmarshal(fields["exclude_scenarios"], &exclusions); err != nil || exclusions == nil {
			return nil, fmt.Errorf("lifecycle exclusions array required")
		}
		excluded[modeName] = make(map[string]bool)
		for _, scenario := range exclusions {
			if !lifecycleIdentifier.MatchString(scenario) || excluded[modeName][scenario] || !scenarios[group][scenario] {
				return nil, fmt.Errorf("invalid lifecycle exclusion")
			}
			excluded[modeName][scenario] = true
		}
	}
	group, ok := links[mode]
	if !ok {
		return nil, fmt.Errorf("unknown lifecycle mode")
	}
	eligible := make(map[string]bool)
	var selected []json.RawMessage
	for _, row := range rows[group] {
		var identity struct {
			Name     string
			Scenario string
		}
		_ = json.Unmarshal(row, &identity)
		if !excluded[mode][identity.Scenario] {
			eligible[identity.Name] = true
			selected = append(selected, row)
		}
	}
	if names == nil {
		return selected, nil
	}
	requested := make(map[string]bool)
	for _, name := range names {
		if !lifecycleIdentifier.MatchString(name) || requested[name] || !eligible[name] {
			return nil, fmt.Errorf("unknown/excluded/duplicate lifecycle request")
		}
		requested[name] = true
	}
	out := make([]json.RawMessage, 0, len(names))
	for _, row := range selected {
		var identity struct{ Name string }
		_ = json.Unmarshal(row, &identity)
		if requested[identity.Name] {
			out = append(out, append(json.RawMessage(nil), row...))
		}
	}
	return out, nil
}

func lifecycleVersion(raw json.RawMessage, expected string) bool {
	return string(bytes.TrimSpace(raw)) == expected
}
func lifecycleString(raw json.RawMessage) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	return value, nil
}
