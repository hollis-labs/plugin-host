package pluginhosttest

import (
	"strings"
	"testing"
	"testing/fstest"
)

func interopInventoryFixture(manifest string) fstest.MapFS {
	return fstest.MapFS{
		"protocol/v2/fixtures/duplex-child.json":       {Data: []byte(manifest)},
		"protocol/v2/fixtures/custom-duplex.json":      {Data: []byte(`{"runtime":[{"name":"future-duplex","level":"internal-core","steps":[{"send":{"method":"future/operation"}}]}]}`)},
		"protocol/v2/fixtures/custom-negotiation.json": {Data: []byte(`{"level":"NORMATIVE","cases":[{"name":"future-ack","init":{"future":true},"ack":false}]}`)},
		"docs/protocol/v2/transcripts/future.json":     {Data: []byte(`{"profile":"future-profile","level":"normative","steps":[{"send":{"method":"future/base"}}]}`)},
		"docs/protocol/v2/transcripts/proposed.json":   {Data: []byte(`{"profile":"unavailable-profile","level":"normative","status":"proposed","finding":"not implemented","unavailable_owner":"fixture-owner","steps":[]}`)},
	}
}

func TestInteropInventoryReadsManifestCaseNamesAndOwners(t *testing.T) {
	fixture := interopInventoryFixture(`{
		"corpus_version":1,"base_profiles":["future-profile"],
		"duplex_source":"custom-duplex.json","duplex_runtime_cases":["future-duplex"],
		"lifecycle":[{"name":"future-lifecycle","level":"normative","steps":[]}],
		"expanded":[{"name":"future-expanded","scenario":"future-scenario","level":"normative"}],
		"pending_coverage":[{"feature":"future-coverage","owner":"coverage-owner"}],
		"future_group":[{"name":"unknown-case"}],
		"negotiated":{"mode":"future-negotiated","matrix":"custom-negotiation.json","recipes":["future-serve"],
		"expanded_scenarios":"authored selection","unavailable":[{"feature":"future-unavailable","owner":"negotiated-owner","reason":"needs design"}]}
	}`)
	_, cases, err := inventoryInterop(fixture)
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]interopCase)
	for _, item := range cases {
		if item.Status != "pending" && item.Status != "unavailable" || item.Owner == "" || item.Reason == "" {
			t.Fatalf("unimplemented recipe misreported: %+v", item)
		}
		byName[item.Family+"/"+item.Name] = item
	}
	for _, name := range []string{"base/future.json", "duplex/future-duplex", "lifecycle/future-lifecycle", "expanded/future-expanded", "negotiation-matrix/future-ack", "negotiated/future-serve", "unsupported-manifest-field/future_group", "negotiated-selection/authored selection"} {
		if _, ok := byName[name]; !ok {
			t.Errorf("manifest-driven obligation missing: %s", name)
		}
	}
	for name, owner := range map[string]string{"base/proposed.json": "fixture-owner", "coverage/future-coverage": "coverage-owner", "negotiated-unavailable/future-unavailable": "negotiated-owner"} {
		if item := byName[name]; item.Status != "unavailable" || item.Owner != owner {
			t.Errorf("unavailability owner lost: %+v", item)
		}
	}
	if raw := string(byName["duplex/future-duplex"].Recipe.Raw); !strings.Contains(raw, "future/operation") {
		t.Fatal("authored duplex payload was discarded")
	}
	if item := byName["duplex/future-duplex"]; item.Level != "internal-core" || item.Mode != "internal-test-only" {
		t.Fatal("engine evidence was relabelled as negotiated transport")
	}
	if item := byName["negotiation-matrix/future-ack"]; item.Mode != "future-negotiated" || !strings.Contains(string(item.Recipe.Raw), `"ack":false`) {
		t.Fatal("authored negotiation expectation or mode was discarded")
	}
}

func TestInteropInventoryRejectsUnownedProposalAndMissingRecipe(t *testing.T) {
	for _, manifest := range []string{
		`{"corpus_version":1,"base_profiles":["future-profile"],"duplex_source":"custom-duplex.json","expanded":[{"name":"unowned","level":"normative","status":"proposed","reason":"pending"}]}`,
		`{"corpus_version":1,"base_profiles":["future-profile"],"duplex_source":"custom-duplex.json","duplex_runtime_cases":["missing-recipe"]}`,
		`{"corpus_version":1,"base_profiles":["future-profile"],"duplex_source":"../outside.json"}`,
	} {
		if _, _, err := inventoryInterop(interopInventoryFixture(manifest)); err == nil {
			t.Fatal("invalid corpus silently became a passing inventory")
		}
	}
}
