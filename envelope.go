package pluginhost

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/hollis-labs/plugin-host/internal/strictjson"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

type wireEnvelope struct {
	id            subprocess.RPCID
	params        json.RawMessage
	method        string
	request       bool
	response      subprocess.RPCResponse
	invalidResult bool
}

// Classification uses exact raw keys, before any permissive struct decoding.
// A method-bearing frame can never consult outgoing reply correlation.
func decodeWireEnvelope(raw []byte) (wireEnvelope, error) {
	var frame wireEnvelope
	fields, outer, err := wireFields(raw)
	if err != nil {
		return frame, err
	}
	if err := strictjson.Validate(outer); err != nil {
		return frame, err
	}
	var version string
	if err := json.Unmarshal(fields["jsonrpc"], &version); err != nil || version != "2.0" {
		return frame, errors.New("invalid protocol version")
	}
	if id, present := fields["id"]; present {
		if err := json.Unmarshal(id, &frame.id); err != nil {
			return frame, err
		}
	}
	_, hasMethod := fields["method"]
	_, hasResult := fields["result"]
	_, hasError := fields["error"]
	if hasMethod {
		if hasResult || hasError {
			return frame, errors.New("mixed request and response")
		}
		for key := range fields {
			if key != "jsonrpc" && key != "id" && key != "method" && key != "params" {
				return frame, errors.New("unknown request field")
			}
		}
		if err := json.Unmarshal(fields["method"], &frame.method); err != nil || frame.method == "" {
			return frame, errors.New("invalid method")
		}
		if id, present := fields["id"]; present && bytes.Equal(bytes.TrimSpace(id), []byte("null")) {
			return frame, errors.New("null request id")
		}
		frame.params = fields["params"]
		frame.request = true
		return frame, nil
	}
	if _, present := fields["id"]; !present || hasResult == hasError {
		return frame, errors.New("response needs id and exactly one result or error")
	}
	for key := range fields {
		if key != "jsonrpc" && key != "id" && key != "result" && key != "error" {
			return frame, errors.New("unknown response field")
		}
	}
	frame.response.JSONRPC, frame.response.ID = "2.0", frame.id
	if hasResult {
		if frame.id == (subprocess.RPCID{}) {
			return frame, errors.New("null success id")
		}
		frame.response.Result = fields["result"]
		if err := strictjson.Validate(raw); err != nil {
			frame.invalidResult = true
			return frame, err
		}
		return frame, nil
	}
	if err := validateWireError(fields["error"]); err != nil {
		return frame, err
	}
	if err := json.Unmarshal(fields["error"], &frame.response.Error); err != nil {
		return frame, err
	}
	if frame.response.Error.Code == -32010 {
		if err := subprocess.ValidateHostRPCDTO("ApplicationErrorResponse", raw); err != nil {
			return frame, err
		}
	}
	return frame, nil
}

// wireFields preserves the original envelope bytes while replacing only the
// result value with null for separate validation. An invalid SDK-owned result
// can then fail its unambiguously correlated call without accepting ambiguous
// envelope keys, identifiers, direction or versions.
func wireFields(raw []byte) (map[string]json.RawMessage, []byte, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, nil, errors.New("expected protocol object")
	}
	fields := map[string]json.RawMessage{}
	outer := raw
	for d.More() {
		token, err = d.Token()
		if err != nil {
			return nil, nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, nil, errors.New("invalid envelope key")
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, nil, errors.New("duplicate envelope key")
		}
		var value json.RawMessage
		if err = d.Decode(&value); err != nil {
			return nil, nil, err
		}
		fields[key] = value
		if key == "result" {
			end := int(d.InputOffset())
			start := end - len(value)
			outer = make([]byte, 0, len(raw)-len(value)+4)
			outer = append(outer, raw[:start]...)
			outer = append(outer, "null"...)
			outer = append(outer, raw[end:]...)
		}
	}
	if _, err = d.Token(); err != nil {
		return nil, nil, err
	}
	return fields, outer, nil
}

func validateWireError(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return errors.New("expected error object")
	}
	for key := range fields {
		if key != "code" && key != "message" && key != "data" {
			return errors.New("unknown error field")
		}
	}
	code, err := strconv.ParseInt(string(bytes.TrimSpace(fields["code"])), 10, 64)
	if err != nil || code < -maxRequestID || code > maxRequestID {
		return errors.New("invalid error code")
	}
	var message string
	if err := json.Unmarshal(fields["message"], &message); err != nil || bytes.Equal(bytes.TrimSpace(fields["message"]), []byte("null")) {
		return errors.New("invalid error message")
	}
	if rawData, present := fields["data"]; present {
		var data map[string]json.RawMessage
		var contract string
		if json.Unmarshal(rawData, &data) == nil && json.Unmarshal(data["contract"], &contract) == nil && contract == "plugin-rpc/2" {
			return subprocess.ValidateRPCControlDTO("PluginRPCErrorData", rawData, false)
		}
	}
	return nil
}

// The result validator is selected from the pending method, never by finding
// any convenient union member. Arbitrary raw extension calls keep their own
// result policy; transport still checks their envelope and JSON syntax.
func validatePendingResult(method string, raw json.RawMessage) error {
	if method == subprocess.MethodInit {
		result, err := decodeInitResult(raw)
		if err != nil {
			return err
		}
		if result.ReverseRPCVersion != nil || result.HooksProfileVersion != nil {
			return &subprocess.InitError{Code: subprocess.InitProfileMismatch, Field: "unsupported_profile", Expected: 0, Received: 1}
		}
	}
	if dto, ok := hostResultDTO(method); ok {
		return subprocess.ValidateHostRPCDTO(dto, raw)
	}
	var out any
	var required, optional []string
	switch method {
	case subprocess.MethodInit:
		out = &subprocess.InitResult{}
		required = []string{"id", "name", "version", "description", "protocol", "capability_contract"}
		optional = []string{"reverse_rpc_version", "hooks_profile_version"}
	case subprocess.MethodLoad:
		out = &subprocess.LoadResult{}
		optional = []string{"skipped_registrations"}
	case subprocess.MethodUnload:
		out = &struct {
			OK bool `json:"ok"`
		}{}
		required = []string{"ok"}
	case subprocess.MethodHealth:
		out = &subprocess.HealthResult{}
		required, optional = []string{"ok"}, []string{"message"}
	case subprocess.MethodCommandExecute:
		out = &subprocess.CommandExecResult{}
		required, optional = []string{"action"}, []string{"content", "envelopes"}
	case subprocess.MethodEventHandle:
		out = &subprocess.EventHandleResult{}
		optional = []string{"cancel", "reason", "envelopes"}
	case subprocess.MethodMCPCallTool:
		out = &subprocess.MCPCallResult{}
		required, optional = []string{"content"}, []string{"is_error", "envelopes"}
	case subprocess.MethodHTTPHandle:
		out = &subprocess.HTTPResponse{}
		required, optional = []string{"status"}, []string{"headers", "body"}
	case subprocess.MethodMigrate:
		out = &subprocess.MigrateResult{}
		optional = []string{"notes"}
	case subprocess.MethodCRUDCreate, subprocess.MethodCRUDRead, subprocess.MethodCRUDUpdate:
		out = &subprocess.CRUDResult{}
		required = []string{"data"}
	case subprocess.MethodCRUDList:
		out = &subprocess.CRUDListResult{}
		required = []string{"items"}
	case subprocess.MethodCRUDDelete:
		out = &struct {
			Deleted bool `json:"deleted"`
		}{}
		required = []string{"deleted"}
	default:
		return nil
	}
	// Opaque forward payloads may be explicit null (e.g. MCP content). Require
	// the SDK-owned key/type, but do not invent a non-null owner schema.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return errors.New("expected method result object")
	}
	allowed := make(map[string]bool)
	for _, name := range required {
		allowed[name] = true
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("missing result field %s", name)
		}
	}
	for _, name := range optional {
		allowed[name] = true
	}
	for name, value := range fields {
		if !allowed[name] {
			return fmt.Errorf("unknown result field %s", name)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) && name != "content" && name != "data" && name != "items" {
			return fmt.Errorf("null result field %s", name)
		}
	}
	return json.Unmarshal(raw, out)
}

func hostResultDTO(method string) (string, bool) {
	switch method {
	case "host/storage/get":
		return "StorageGetResult", true
	case "host/storage/put":
		return "StoragePutResult", true
	case "host/storage/delete":
		return "StorageDeleteResult", true
	case "host/secrets/get":
		return "SecretsGetResult", true
	case "host/egress/request":
		return "EgressRequestResult", true
	case "host/events/publish":
		return "EventsPublishResult", true
	case "host/log":
		return "LogResult", true
	case "host/readonly/query":
		return "ReadonlyQueryResult", true
	case "host/mcp/list_tools":
		return "MCPListToolsResult", true
	case "host/mcp/call_tool":
		return "MCPCallToolResult", true
	case "host/mcp/cancel_call":
		return "MCPCancelCallResult", true
	case "host/bindings/renew":
		return "BindingsRenewResult", true
	}
	return "", false
}
