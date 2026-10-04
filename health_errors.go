package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// ErrHealthInconclusive means a probe produced no health verdict. Local
// admission refusal, rate-limit and deadline replies do not count toward the
// supervisor's unhealthy kill threshold.
var ErrHealthInconclusive = errors.New("pluginhost: health probe inconclusive")

// HealthInconclusiveError retains the local refusal or typed RPC reply and any
// joined context cause.
type HealthInconclusiveError struct{ Cause error }

func (e *HealthInconclusiveError) Error() string {
	return fmt.Sprintf("%s: %v", ErrHealthInconclusive, e.Cause)
}
func (e *HealthInconclusiveError) Unwrap() error        { return e.Cause }
func (e *HealthInconclusiveError) Is(target error) bool { return target == ErrHealthInconclusive }

func applicationCode(rpc *subprocess.RPCError) (capability.Code, bool) {
	if rpc.Code != capability.HostRPCErrorCode {
		return "", false
	}
	raw, err := json.Marshal(rpc.Data)
	if err != nil {
		return "", false
	}
	var data subprocess.HostRPCErrorData
	if json.Unmarshal(raw, &data) != nil {
		return "", false
	}
	return data.Code, true
}

func healthError(err error) error {
	if errors.Is(err, ErrAdmissionFull) {
		return &HealthInconclusiveError{Cause: err}
	}
	var rpc *subprocess.RPCError
	if errors.As(err, &rpc) {
		if rpc.Code == subprocess.ErrCodeInternal {
			return errors.Join(ErrUnhealthy, err)
		}
		if code, ok := applicationCode(rpc); ok && (code == capability.RateLimited || code == capability.DeadlineExceeded) {
			return &HealthInconclusiveError{Cause: err}
		}
		return errors.Join(ErrProtocolMismatch, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(ErrUnhealthy, err)
	}
	return err
}
