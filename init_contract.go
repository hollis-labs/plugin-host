package pluginhost

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// initFailure keeps SDK contract errors typed and maps protocol mismatch to
// the host sentinel, including rejected legacy acknowledgements.
func initFailure(s Spec, err error) *Failure {
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
	if err := p.Validate(); err != nil {
		return err
	}
	if p.HostServices != nil && p.HostServices.Limits.MaxFrameBytes > defaultMaxFrame {
		return &subprocess.InitError{Code: subprocess.InitInvalid, Field: "host_services.limits.max_frame_bytes"}
	}
	return nil
}

func validateInit(s Spec) error {
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
