package interopfixture

import (
	"encoding/json"
	"strings"
	"testing"
)

// Invented identifiers exercise the generic schema rather than recipe dispatch.
const lifecycleExample = `{"corpus_version":1,"contract_version":2,"selection":{"version":1,"kind":"source-group","modes":{"public-conn-isolated-transport":{"source_group":"lifecycle_compatible_v2","exclude_scenarios":[]},"sdk-normal-serve-remote-overflow":{"source_group":"sdk_remote_overflow_v2","exclude_scenarios":[]}}},"groups":{"lifecycle_compatible_v2":[{"name":"invented-first","scenario":"alpha","profile":"different","level":"normative","status":"proposed","owner":" owner π ","reason":" reason "},{"name":"invented-second","scenario":"beta","profile":"different","level":"normative","status":"observed","owner":"owner","reason":"reason"}],"sdk_remote_overflow_v2":[{"name":"remote-invented","scenario":"gamma","profile":"different","level":"normative","status":"proposed","owner":"owner","reason":"reason"}]}}`

func TestLifecycleSelectionPreservesSourceAndSubset(t *testing.T) {
	rows, err := SelectLifecycle([]byte(lifecycleExample), LifecyclePublicMode, []string{"invented-second", "invented-first"})
	if err != nil || len(rows) != 2 {
		t.Fatalf("selection: %v %s", err, rows)
	}
	var first map[string]string
	if err := json.Unmarshal(rows[0], &first); err != nil {
		t.Fatal(err)
	}
	if first["name"] != "invented-first" || first["status"] != "proposed" || first["owner"] != " owner π " || first["reason"] != " reason " {
		t.Fatalf("metadata changed: %#v", first)
	}
	all, err := SelectLifecycle([]byte(lifecycleExample), LifecyclePublicMode, nil)
	if err != nil || len(all) != 2 {
		t.Fatalf("nil selection: %v", err)
	}
	empty, err := SelectLifecycle([]byte(lifecycleExample), LifecyclePublicMode, []string{})
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("explicit empty selection: %v %#v", err, empty)
	}
	for _, names := range [][]string{{"remote-invented"}, {"unknown"}, {"invented-first", "invented-first"}, {"BAD"}} {
		if _, err := SelectLifecycle([]byte(lifecycleExample), LifecyclePublicMode, names); err == nil {
			t.Fatalf("accepted subset %q", names)
		}
	}
}

func TestLifecycleWholeRawManifestRefusal(t *testing.T) {
	cases := map[string]string{
		"version exponent":         strings.Replace(lifecycleExample, `"corpus_version":1`, `"corpus_version":1e0`, 1),
		"version decimal":          strings.Replace(lifecycleExample, `"contract_version":2`, `"contract_version":2.0`, 1),
		"duplicate decoded":        strings.Replace(lifecycleExample, `"corpus_version":1`, `"corpus_version":1,"corpus_\u0076ersion":1`, 1),
		"bad remote status":        strings.Replace(lifecycleExample, `"scenario":"gamma","profile":"different","level":"normative","status":"proposed"`, `"scenario":"gamma","profile":"different","level":"normative","status":"future"`, 1),
		"remote extra member":      strings.Replace(lifecycleExample, `"name":"remote-invented"`, `"extra":true,"name":"remote-invented"`, 1),
		"missing status":           strings.Replace(lifecycleExample, `"status":"proposed",`, ``, 1),
		"blank owner":              strings.Replace(lifecycleExample, `"owner":" owner π "`, `"owner":" \t\r\n"`, 1),
		"surrogate":                strings.Replace(lifecycleExample, `"reason":" reason "`, `"reason":"\ud800"`, 1),
		"duplicate global name":    strings.Replace(lifecycleExample, `"name":"remote-invented"`, `"name":"invented-first"`, 1),
		"duplicate group scenario": strings.Replace(lifecycleExample, `"scenario":"beta"`, `"scenario":"alpha"`, 1),
		"wrong remote linkage":     strings.Replace(lifecycleExample, `"source_group":"sdk_remote_overflow_v2"`, `"source_group":"lifecycle_compatible_v2"`, 1),
		"unknown exclusion":        strings.Replace(lifecycleExample, `"exclude_scenarios":[]`, `"exclude_scenarios":["unknown"]`, 1),
		"null exclusions":          strings.Replace(lifecycleExample, `"exclude_scenarios":[]`, `"exclude_scenarios":null`, 1),
		"BOM":                      "\xef\xbb\xbf" + lifecycleExample,
		"invalid UTF8":             strings.Replace(lifecycleExample, " reason ", "\xff", 1),
		"trailing":                 lifecycleExample + ` {}`,
		"depth":                    strings.Replace(lifecycleExample, `"reason":" reason "`, `"reason":`+strings.Repeat("[", 129)+`0`+strings.Repeat("]", 129), 1),
		"size":                     lifecycleExample + strings.Repeat(" ", 8<<20),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			// Even an empty requested subset cannot bypass whole-corpus validation.
			if _, err := SelectLifecycle([]byte(raw), LifecyclePublicMode, []string{}); err == nil {
				t.Fatal("accepted invalid raw corpus")
			}
		})
	}
}

func TestLifecycleExclusionValidatesExcludedMetadata(t *testing.T) {
	raw := strings.Replace(lifecycleExample, `"exclude_scenarios":[]`, `"exclude_scenarios":["alpha"]`, 1)
	rows, err := SelectLifecycle([]byte(raw), LifecyclePublicMode, nil)
	if err != nil || len(rows) != 1 || !strings.Contains(string(rows[0]), "invented-second") {
		t.Fatalf("exclusion: %v %s", err, rows)
	}
	if _, err := SelectLifecycle([]byte(raw), LifecyclePublicMode, []string{"invented-first"}); err == nil {
		t.Fatal("accepted excluded request")
	}
	raw = strings.Replace(raw, `"status":"proposed"`, `"status":"invalid"`, 1)
	if _, err := SelectLifecycle([]byte(raw), LifecyclePublicMode, nil); err == nil {
		t.Fatal("skipped excluded metadata validation")
	}
}
