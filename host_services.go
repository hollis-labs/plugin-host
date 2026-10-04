package pluginhost

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// HostMethod is a member of the closed reverse host-service table.
// It is policy metadata, not a public dispatch entry point.
type HostMethod string

const (
	HostStorageGet    HostMethod = "host/storage/get"
	HostStoragePut    HostMethod = "host/storage/put"
	HostStorageDelete HostMethod = "host/storage/delete"
	HostSecretsGet    HostMethod = "host/secrets/get"
	HostEgressRequest HostMethod = "host/egress/request"
	HostEventsPublish HostMethod = "host/events/publish"
	HostLog           HostMethod = "host/log"
	HostReadonlyQuery HostMethod = "host/readonly/query"
	HostMCPListTools  HostMethod = "host/mcp/list_tools"
	HostMCPCallTool   HostMethod = "host/mcp/call_tool"
	HostMCPCancelCall HostMethod = "host/mcp/cancel_call"
	HostBindingsRenew HostMethod = "host/bindings/renew"
)

// HostServices supplies fixed typed host callbacks. Nil denies that operation.
// Callbacks must validate method-specific targets, scopes, payload schemas and
// receipts. They run without transport/ledger locks. Before an effect commits,
// use HostCall.CheckCommit and serialize that check with the service's actual
// commit/revocation transaction. The library cannot transact a host's backend.
// Business receipts are keyed by owner ID, method, effective target and operation
// key; generation is provenance, not a reason to re-execute a recorded effect.
// No mutation is automatically retried.
//
// Renew and cancel callbacks are optional policy vetoes: the session owns their
// results and performs its own narrow renewal and directional cancellation.
// Supplying services does not enable a Conn profile or send an Init offer.
type HostServices struct {
	StorageGet    func(context.Context, *HostCall, subprocess.StorageGetParams) (subprocess.StorageGetResult, error)
	StoragePut    func(context.Context, *HostCall, subprocess.StoragePutParams) (subprocess.StoragePutResult, error)
	StorageDelete func(context.Context, *HostCall, subprocess.StorageDeleteParams) (subprocess.StorageDeleteResult, error)
	SecretsGet    func(context.Context, *HostCall, subprocess.SecretsGetParams) (subprocess.SecretsGetResult, error)
	EgressRequest func(context.Context, *HostCall, subprocess.EgressRequestParams) (subprocess.EgressRequestResult, error)
	EventsPublish func(context.Context, *HostCall, subprocess.EventsPublishParams) (subprocess.EventsPublishResult, error)
	Log           func(context.Context, *HostCall, subprocess.LogParams) (subprocess.LogResult, error)
	ReadonlyQuery func(context.Context, *HostCall, subprocess.ReadonlyQueryParams) (subprocess.ReadonlyQueryResult, error)
	MCPListTools  func(context.Context, *HostCall, subprocess.MCPListToolsParams) (subprocess.MCPListToolsResult, error)
	MCPCallTool   func(context.Context, *HostCall, subprocess.MCPCallToolParams) (subprocess.MCPCallToolResult, error)
	MCPCancelCall func(context.Context, *HostCall, subprocess.MCPCancelCallParams) error
	BindingsRenew func(context.Context, *HostCall, subprocess.BindingsRenewParams) error
}

// HostAuthority is a private-copy snapshot of host-verified authority. Scope is
// a host-owned narrowing, never a scope submitted by the plugin. Authorize must
// recheck current caller/target policy and interpret this scope under the grant's
// descriptor. No ambient authority follows from a nonempty binding ID.
type HostAuthority struct {
	Owner              Owner
	ConnectionInstance string
	BindingID          subprocess.BindingID
	Grant              capability.Grant
	Scope              json.RawMessage
	Caller             string
	Root               string
	Parent             subprocess.ParentCall
	Depth              uint32
	Origin             []Owner
	Method             HostMethod
	Deadline           time.Time
}

// HostPolicy revalidates current host/caller/target authority at admission and
// before commit. It must honor its finite context. Nil denies all service use.
// Method-specific backend callbacks still own parameter and result policy.
type HostPolicy func(context.Context, HostAuthority) error

// HostBudgets are optional aggregate counters shared by concurrent descendants.
// A nil dimension is untracked, not a declaration of unlimited authority.
type HostBudgets struct{ Bytes, Effects, Tokens *uint64 }

// HostBinding is supplied only by host code after narrowing reviewed authority.
// Parent must be registered from a real selected host invocation; this API does
// not publish it or make any plugin-provided parent trustworthy. Detached and
// workflow delivery need separate contracts and are not implemented here.
type HostBinding struct {
	GrantID      string
	Scope        json.RawMessage
	Caller, Root string
	Parent       subprocess.ParentCall
	Deadline     time.Time
	Depth        uint32
	Origin       []Owner
	Budgets      HostBudgets
}

// HostCall owns one admitted request. Its authority accessor returns independent
// copies. CheckCommit revalidates current grant/binding/parent and host policy;
// the backend must couple it to the actual effect transaction. Charge reserves
// additional shared budget atomically and does not refund consumed work.
type HostCall struct {
	session   *HostSession
	authority HostAuthority
	ctx       context.Context
	started   atomic.Bool
}

func (c *HostCall) Authority() HostAuthority { return copyAuthority(c.authority) }
func (c *HostCall) CheckCommit() error {
	if c == nil || c.session == nil || c.ctx == nil || c.session.policy == nil {
		return hostRefusal(capability.Unauthenticated, "")
	}
	if err := c.ctx.Err(); err != nil {
		return hostContextError(err)
	}
	if err := c.session.checkAuthority(c.authority); err != nil {
		return err
	}
	if err := c.session.policy(c.ctx, c.Authority()); err != nil {
		return err
	}
	if err := c.ctx.Err(); err != nil {
		return hostContextError(err)
	}
	return c.session.checkAuthority(c.authority)
}
func (c *HostCall) Charge(bytes, effects, tokens uint64) error {
	if err := c.CheckCommit(); err != nil {
		return err
	}
	return c.session.charge(c.authority, bytes, effects, tokens)
}
func copyAuthority(a HostAuthority) HostAuthority {
	a.Grant.Scope = append(json.RawMessage(nil), a.Grant.Scope...)
	a.Scope = append(json.RawMessage(nil), a.Scope...)
	a.Origin = append([]Owner(nil), a.Origin...)
	return a
}
