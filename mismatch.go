package pluginhost

import (
	"fmt"
	"strings"
	"unicode"
)

// MismatchError retains only bounded printable identity/version metadata.
// Field is "id" or "version"; Expected and Actual never contain configuration,
// arguments, environment or other rejected payload fields. Error also sanitizes
// directly constructed values. errors.Is retains the matching mismatch sentinel.
type MismatchError struct {
	Field    string
	Expected string
	Actual   string
}

func (e *MismatchError) Error() string {
	return fmt.Sprintf("pluginhost: %s mismatch: expected %q, actual %q", printableValue(e.Field), printableValue(e.Expected), printableValue(e.Actual))
}
func (e *MismatchError) Unwrap() error {
	switch e.Field {
	case "id":
		return ErrIdentityMismatch
	case "version":
		return ErrVersionMismatch
	}
	return nil
}
func mismatch(field, expected, actual string) *MismatchError {
	return &MismatchError{Field: field, Expected: printableValue(expected), Actual: printableValue(actual)}
}

func printableValue(value string) string {
	var out strings.Builder
	count := 0
	for _, r := range value {
		if count == 128 {
			break
		}
		if !unicode.IsPrint(r) || unicode.Is(unicode.Cf, r) {
			r = '?'
		}
		out.WriteRune(r)
		count++
	}
	return out.String()
}
