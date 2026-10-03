package pluginhost

import (
	"fmt"
	"strings"
)

// VersionBounds are normalized inclusive semantic-version limits. An absent
// side is unbounded; both absent is invalid. No manifest dialect is implied.
type VersionBounds struct {
	Min string `json:"min,omitempty"`
	Max string `json:"max,omitempty"`
}

// VersionRequirement checks a host contract or resolved runtime, including
// development and 0.x builds. Prerelease acceptance is explicit host policy.
type VersionRequirement struct {
	Name, Version   string
	Bounds          VersionBounds
	AllowPrerelease bool
}

type semanticVersion struct {
	core [3]string
	pre  []string
}

func numeric(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
func identifiers(s string, prerelease bool) bool {
	for _, part := range strings.Split(s, ".") {
		if part == "" {
			return false
		}
		for _, c := range part {
			if (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && c != '-' {
				return false
			}
		}
		if prerelease && numeric(part) && len(part) > 1 && part[0] == '0' {
			return false
		}
	}
	return true
}
func parseVersion(s string) (semanticVersion, error) {
	var v semanticVersion
	bad := func() (semanticVersion, error) { return v, fmt.Errorf("pluginhost: invalid semantic version") }
	main, build, hasBuild := strings.Cut(s, "+")
	if hasBuild && !identifiers(build, false) {
		return bad()
	}
	core, pre, hasPre := strings.Cut(main, "-")
	if hasPre {
		if !identifiers(pre, true) {
			return bad()
		}
		v.pre = strings.Split(pre, ".")
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return bad()
	}
	for i, p := range parts {
		if !numeric(p) || len(p) > 1 && p[0] == '0' {
			return bad()
		}
		v.core[i] = p
	}
	return v, nil
}
func compareNumeric(a, b string) int {
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return strings.Compare(a, b)
}
func compareVersions(a, b semanticVersion) int {
	for i := range a.core {
		if n := compareNumeric(a.core[i], b.core[i]); n != 0 {
			return n
		}
	}
	if len(a.pre) == 0 && len(b.pre) > 0 {
		return 1
	}
	if len(b.pre) == 0 && len(a.pre) > 0 {
		return -1
	}
	for i := 0; i < min(len(a.pre), len(b.pre)); i++ {
		x, y := a.pre[i], b.pre[i]
		n := 0
		switch {
		case numeric(x) && numeric(y):
			n = compareNumeric(x, y)
		case numeric(x):
			n = -1
		case numeric(y):
			n = 1
		default:
			n = strings.Compare(x, y)
		}
		if n != 0 {
			return n
		}
	}
	if len(a.pre) < len(b.pre) {
		return -1
	}
	if len(a.pre) > len(b.pre) {
		return 1
	}
	return 0
}

// CompareVersions uses numeric semantic ordering; build metadata is ignored.
func CompareVersions(a, b string) (int, error) {
	x, err := parseVersion(a)
	if err != nil {
		return 0, err
	}
	y, err := parseVersion(b)
	if err != nil {
		return 0, err
	}
	return compareVersions(x, y), nil
}

// CheckVersion rejects malformed declarations, unresolved versions and ranges.
func CheckVersion(r VersionRequirement) error {
	v, err := parseVersion(r.Version)
	if err != nil {
		return err
	}
	if r.Name == "" {
		return fmt.Errorf("pluginhost: required contract name is empty")
	}
	if len(v.pre) > 0 && !r.AllowPrerelease {
		return fmt.Errorf("pluginhost: prerelease requires host approval")
	}
	b := r.Bounds
	if b.Min == "" && b.Max == "" {
		return fmt.Errorf("pluginhost: version bounds are empty")
	}
	if b.Min != "" && b.Max != "" {
		n, e := CompareVersions(b.Min, b.Max)
		if e != nil {
			return e
		}
		if n > 0 {
			return fmt.Errorf("pluginhost: version minimum exceeds maximum")
		}
	}
	if b.Min != "" {
		n, e := CompareVersions(r.Version, b.Min)
		if e != nil {
			return e
		}
		if n < 0 {
			return fmt.Errorf("pluginhost: version below minimum")
		}
	}
	if b.Max != "" {
		n, e := CompareVersions(r.Version, b.Max)
		if e != nil {
			return e
		}
		if n > 0 {
			return fmt.Errorf("pluginhost: version above maximum")
		}
	}
	return nil
}
