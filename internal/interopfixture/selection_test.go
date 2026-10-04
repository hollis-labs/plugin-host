package interopfixture

import (
	"encoding/json"
	"reflect"
	"testing"
)

func fixture() map[string]any {
	return map[string]any{
		"expanded_selection": map[string]any{"version": 1, "kind": "source-group", "source_group": "future_group", "modes": map[string]any{"internal-test-only": map[string]any{"exclude_scenarios": []string{}}, "normal-serve-negotiated": map[string]any{"exclude_scenarios": []string{"base-cancel"}}}},
		"negotiated":         map[string]any{"expanded_selection": "normal-serve-negotiated"},
		"future_group": []any{
			map[string]any{"name": "first", "level": "normative", "profile": "custom", "scenario": "future"},
			map[string]any{"name": "proposed", "level": "normative", "status": "proposed", "owner": "maintainers", "reason": "needs design"},
			map[string]any{"name": "proposed-base", "level": "normative", "status": "proposed", "scenario": "base-cancel", "owner": "maintainers", "reason": "needs design"},
		},
		"another_future_group": []any{map[string]any{"name": "never-inferred"}},
	}
}
func selectFixture(m map[string]any, mode string, names []string) ([]json.RawMessage, error) {
	raw, _ := json.Marshal(m)
	return Select(raw, mode, names)
}

func TestSourceOrderExclusionAndProposalPreservation(t *testing.T) {
	m := fixture()
	selected, err := selectFixture(m, "normal-serve-negotiated", []string{"proposed", "first"})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, raw := range selected {
		var v map[string]any
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v["name"].(string))
		if v["name"] == "proposed" {
			if v["status"] != "proposed" || v["owner"] != "maintainers" || v["reason"] != "needs design" {
				t.Fatal("proposal disposition lost", v)
			}
		}
	}
	if !reflect.DeepEqual(got, []string{"first", "proposed"}) {
		t.Fatal("source order/exclusions lost", got)
	}
	if _, err := selectFixture(m, "normal-serve-negotiated", []string{"proposed-base"}); err == nil {
		t.Fatal("excluded request accepted")
	}
	if _, err := selectFixture(m, "internal-test-only", []string{"proposed-base"}); err != nil {
		t.Fatal("internal proposal excluded", err)
	}
	m["future_group"].([]any)[2].(map[string]any)["owner"] = " "
	if _, err := selectFixture(m, "normal-serve-negotiated", []string{"first"}); err == nil {
		t.Fatal("excluded proposal metadata escaped validation")
	}
}
func TestSelectionRefusesMalformedAndDoesNotInferUnknownGroups(t *testing.T) {
	for _, mutate := range []func(map[string]any){
		func(m map[string]any) { m["expanded_selection"].(map[string]any)["version"] = 2 },
		func(m map[string]any) {
			selector := m["expanded_selection"].(map[string]any)
			delete(selector, "version")
			selector["Version"] = 1
		},
		func(m map[string]any) {
			m["expanded_selection"].(map[string]any)["modes"].(map[string]any)["internal-test-only"] = map[string]any{"Exclude_scenarios": []string{}}
		},
		func(m map[string]any) { m["expanded_selection"].(map[string]any)["include_cases"] = []string{"first"} },
		func(m map[string]any) { m["expanded_selection"].(map[string]any)["source_group"] = "missing" },
		func(m map[string]any) { m["future_group"] = nil },
		func(m map[string]any) { m["future_group"].([]any)[0].(map[string]any)["status"] = nil },
		func(m map[string]any) { m["negotiated"].(map[string]any)["expanded_selection"] = "future-mode" },
		func(m map[string]any) {
			delete(m["expanded_selection"].(map[string]any)["modes"].(map[string]any), "internal-test-only")
		},
	} {
		m := fixture()
		mutate(m)
		if _, err := selectFixture(m, "normal-serve-negotiated", nil); err == nil {
			t.Fatal("malformed selector accepted", m)
		}
	}
	for _, names := range [][]string{{"never-inferred"}, {"first", "first"}} {
		if _, err := selectFixture(fixture(), "internal-test-only", names); err == nil {
			t.Fatal("invalid names accepted", names)
		}
	}
	if _, err := selectFixture(fixture(), "future-mode", nil); err == nil {
		t.Fatal("unknown mode accepted")
	}
	if selected, err := selectFixture(fixture(), "internal-test-only", []string{}); err != nil || len(selected) != 0 {
		t.Fatal("valid empty subset refused", selected, err)
	}
}
