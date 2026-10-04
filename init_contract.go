package pluginhost

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// initFailure keeps SDK contract errors typed and maps protocol mismatch to
// the host sentinel, including rejected legacy acknowledgements.
func initFailure(s Spec, err error) *Failure {
	err = initRPCError(err)
	step := "init"
	var typed *subprocess.InitError
	if errors.As(err, &typed) {
		switch typed.Code {
		case subprocess.InitProtocolMismatch:
			step = "protocol"
			message := "plugin speaks %d, host speaks %d"
			if typed.Field == "host_info.protocol" {
				message = "Init requests protocol %d, host requires %d"
			}
			err = errors.Join(err, fmt.Errorf("%w: "+message, ErrProtocolMismatch, typed.Received, typed.Expected))
		case subprocess.InitCapabilityContractMismatch:
			step = "capability_contract"
		case subprocess.InitProfileMismatch:
			step = "profile"
		case subprocess.InitInvalid:
			if typed.Field == "id" {
				step = "identity"
				err = errors.Join(ErrNoPluginID, err)
			}
		}
	}
	return processFailure(s, step, err)
}

func validateInitParams(p subprocess.InitParams) error {
	if p.HostServices != nil || p.HooksProfile != nil {
		return &subprocess.InitError{Code: subprocess.InitProfileMismatch, Field: "unsupported_profile", Expected: 0, Received: 1}
	}
	return p.Validate()
}

func validateInit(s Spec) error {
	if strings.TrimSpace(s.ExpectedID) == "" {
		return initFailure(s, &subprocess.InitError{Code: subprocess.InitInvalid, Field: "id"})
	}
	if err := validateInitParams(s.Init); err != nil {
		return initFailure(s, err)
	}
	// Marshal validates opaque JSON and nested SDK values before any child
	// runs. The SDK owns the only successful request/result representation.
	if _, err := json.Marshal(s.Init); err != nil {
		return initFailure(s, err)
	}
	return nil
}

func decodeInitResult(raw json.RawMessage) (subprocess.InitResult, error) {
	var result subprocess.InitResult
	if err := json.Unmarshal(raw, &result); err != nil {
		// A legacy result may omit newly required fields. Inspect only its
		// declared protocol to classify rejection; never accept a legacy DTO.
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) == nil {
			var protocol int
			if p, ok := fields["protocol"]; ok && json.Unmarshal(p, &protocol) == nil && protocol != subprocess.ProtocolVersion {
				return result, errors.Join(&subprocess.InitError{Code: subprocess.InitProtocolMismatch, Field: "protocol", Expected: subprocess.ProtocolVersion, Received: protocol}, err)
			}
		}
		return result, err
	}
	return result, nil
}

// initRPCError preserves the transport error while exposing the SDK contract cause.
func initRPCError(err error) error {
	var typed *subprocess.InitError
	if errors.As(err, &typed) {
		return err
	}
	var rpc *subprocess.RPCError
	if !errors.As(err, &rpc) || rpc.Code != -32602 {
		return err
	}
	raw, marshalErr := json.Marshal(rpc.Data)
	if marshalErr != nil {
		return err
	}
	var data struct {
		Contract string                     `json:"contract"`
		Code     subprocess.InitFailureCode `json:"code"`
		Field    string                     `json:"field"`
		Expected int                        `json:"expected"`
		Received int                        `json:"received"`
	}
	if json.Unmarshal(raw, &data) != nil || data.Contract != "plugin-init/2" || data.Field == "" {
		return err
	}
	switch data.Code {
	case subprocess.InitInvalid, subprocess.InitProtocolMismatch, subprocess.InitCapabilityContractMismatch, subprocess.InitProfileMismatch:
		return errors.Join(err, &subprocess.InitError{Code: data.Code, Field: data.Field, Expected: data.Expected, Received: data.Received})
	default:
		return err
	}
}

func verifyInitResult(params subprocess.InitParams, result subprocess.InitResult) error {
	if err := subprocess.ValidateInitResult(params, result); err != nil {
		return err
	}
	if result.ReverseRPCVersion != nil || result.HooksProfileVersion != nil {
		return &subprocess.InitError{Code: subprocess.InitProfileMismatch, Field: "unsupported_profile", Expected: 0, Received: 1}
	}
	return nil
}
