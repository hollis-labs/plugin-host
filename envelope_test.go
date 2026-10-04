package pluginhost

import (
	"strings"
	"testing"
)

func TestStrictWireReplyEnvelopes(t *testing.T) {
	for _, raw := range []string{
		`{"jsonrpc":"2.0","id":1,"result":null}`,
		`{"jsonrpc":"2.0","id":1,"result":{"opaque":true}}`,
		`{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"failure","data":null}}`,
		`{"jsonrpc":"2.0","id":"job","result":0}`,
	} {
		frame, err := decodeWireEnvelope([]byte(raw))
		if err != nil || frame.request {
			t.Errorf("valid response refused: %s: %v", raw, err)
		}
	}
	for _, raw := range []string{
		`{"id":1,"result":true}`,
		`{"jsonrpc":"1.0","id":1,"result":true}`,
		`{"jsonrpc":"2.0","ID":1,"result":true}`,
		`{"jsonrpc":"2.0","id":1}`,
		`{"jsonrpc":"2.0","id":1,"result":true,"error":{"code":-32603,"message":"bad"}}`,
		`{"jsonrpc":"2.0","id":1,"Result":true}`,
		`{"jsonrpc":"2.0","id":1,"result":true,"extra":true}`,
		`{"jsonrpc":"2.0","id":1,"result":true,"params":{}}`,
		`{"jsonrpc":"2.0","id":1,"method":"host/log","result":true}`,
		`{"jsonrpc":"2.0","id":1,"id":1,"result":true}`,
		`{"jsonrpc":"2.0","id":1,"\u0069d":2,"result":true}`,
		`{"jsonrpc":"2.0","id":1,"result":{"nested":{"x":1,"x":2}}}`,
		`{"jsonrpc":"2.0","id":1.0,"result":true}`,
		`{"jsonrpc":"2.0","id":1e0,"result":true}`,
		`{"jsonrpc":"2.0","id":9007199254740992,"result":true}`,
		`{"jsonrpc":"2.0","id":null,"result":true}`,
		`{"jsonrpc":"2.0","id":1,"error":null}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":null,"message":"bad"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"Code":-32603,"message":"bad"}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":null}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"bad","extra":true}}`,
		`{"jsonrpc":"2.0","id":1,"result":"\ud800"}`,
		`{"jsonrpc":"2.0","id":1,"result":"` + "\xff" + `"}`,
		`{"jsonrpc":"2.0","id":1,"result":` + strings.Repeat("[", 129) + "0" + strings.Repeat("]", 129) + `}`,
	} {
		if _, err := decodeWireEnvelope([]byte(raw)); err == nil {
			t.Errorf("invalid response accepted: %s", raw)
		}
	}
}

func TestStrictWireMethodFramesKeepTheirDirection(t *testing.T) {
	for _, raw := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"host/log","params":{}}`,
		`{"jsonrpc":"2.0","method":"rpc/cancel","params":{}}`,
		`{"jsonrpc":"2.0","method":"host/storage/put","params":{}}`,
	} {
		frame, err := decodeWireEnvelope([]byte(raw))
		if err != nil || !frame.request || frame.method == "" {
			t.Errorf("request lost its direction: %s: %v", raw, err)
		}
	}
}

func TestPendingMethodSelectsItsResultContract(t *testing.T) {
	for _, tc := range []struct{ method, valid, invalid string }{
		{"plugin/health", `{"ok":true}`, `{"accepted":true}`},
		{"plugin/load", `{}`, `{"ok":true}`},
		{"plugin/unload", `{"ok":true}`, `{"ok":null}`},
		{"command/execute", `{"action":"noop"}`, `{"status":200}`},
		{"event/handle", `{"cancel":false}`, `{"action":"noop"}`},
		{"mcp/call_tool", `{"content":null}`, `{"action":"noop"}`},
		{"http/handle", `{"status":200}`, `{"ok":true}`},
		{"crud/read", `{"data":null}`, `{"items":[]}`},
		{"crud/list", `{"items":[]}`, `{"data":null}`},
		{"host/storage/get", `{"found":false}`, `{"accepted":true}`},
		{"host/log", `{"accepted":true}`, `{"found":false}`},
	} {
		if err := validatePendingResult(tc.method, []byte(tc.valid)); err != nil {
			t.Errorf("valid %s reply: %v", tc.method, err)
		}
		if err := validatePendingResult(tc.method, []byte(tc.invalid)); err == nil {
			t.Errorf("wrong %s result contract accepted: %s", tc.method, tc.invalid)
		}
	}
}
