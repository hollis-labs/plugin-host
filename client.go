package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// Client is a thin typed layer over a [Conn]: one method per plugin-sdk
// protocol method, passing the SDK's own request and result structs through
// untouched so fields the SDK adds later flow without a change here.
//
// There is deliberately no typed ListTools. subprocess.MethodListTools is
// declared by the SDK but its Serve has no dispatch case for it; a host that
// wants it sends it through [Conn.Call] or [Call] and chooses its own
// semantics.
type Client struct{ conn *Conn }

// NewClient wraps conn.
func NewClient(conn *Conn) *Client { return &Client{conn: conn} }

// Conn returns the underlying connection, for raw calls.
func (c *Client) Conn() *Conn { return c.conn }

// Call is the decode helper behind the typed methods: it calls method and
// decodes the result into T. An empty result decodes to the zero T.
func Call[T any](ctx context.Context, c *Conn, method string, params any) (T, error) {
	var out T
	raw, err := c.Call(ctx, method, params)
	if err != nil {
		return out, err
	}
	if len(raw) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("pluginhost: %s: decode result: %w", method, err)
	}
	return out, nil
}

// Init sends plugin/init.
func (c *Client) Init(ctx context.Context, params subprocess.InitParams) (subprocess.InitResult, error) {
	if err := validateInitParams(params); err != nil {
		return subprocess.InitResult{}, err
	}
	raw, err := c.conn.Call(ctx, subprocess.MethodInit, params)
	if err != nil {
		return subprocess.InitResult{}, initRPCError(err)
	}
	result, err := decodeInitResult(raw)
	if err == nil {
		err = verifyInitResult(params, result)
	}
	return result, err
}

// Load sends plugin/load.
func (c *Client) Load(ctx context.Context) (subprocess.LoadResult, error) {
	return Call[subprocess.LoadResult](ctx, c.conn, subprocess.MethodLoad, subprocess.LoadParams{})
}

// Unload sends terminal plugin/unload. The SDK cancels and drains admitted
// callbacks, invokes cleanup once, flushes its terminal reply and exits.
func (c *Client) Unload(ctx context.Context) error {
	_, err := c.conn.Call(ctx, subprocess.MethodUnload, nil)
	return err
}

// Health sends plugin/health. A plugin that does not implement the SDK's
// HealthChecker answers {ok:true}, so "healthy" and "never implemented" are
// indistinguishable here.
func (c *Client) Health(ctx context.Context) (subprocess.HealthResult, error) {
	result, err := Call[subprocess.HealthResult](ctx, c.conn, subprocess.MethodHealth, nil)
	var rpcErr *subprocess.RPCError
	if errors.As(err, &rpcErr) {
		return result, errors.Join(ErrUnhealthy, err)
	}
	return result, err
}

// CommandExecute sends command/execute.
func (c *Client) CommandExecute(ctx context.Context, p subprocess.CommandExecParams) (subprocess.CommandExecResult, error) {
	return Call[subprocess.CommandExecResult](ctx, c.conn, subprocess.MethodCommandExecute, p)
}

// EventHandle sends event/handle.
func (c *Client) EventHandle(ctx context.Context, p subprocess.EventHandleParams) (subprocess.EventHandleResult, error) {
	return Call[subprocess.EventHandleResult](ctx, c.conn, subprocess.MethodEventHandle, p)
}

// MCPCallTool sends mcp/call_tool.
func (c *Client) MCPCallTool(ctx context.Context, p subprocess.MCPCallRequest) (subprocess.MCPCallResult, error) {
	return Call[subprocess.MCPCallResult](ctx, c.conn, subprocess.MethodMCPCallTool, p)
}

// HTTPHandle sends http/handle.
func (c *Client) HTTPHandle(ctx context.Context, p subprocess.HTTPRequest) (subprocess.HTTPResponse, error) {
	return Call[subprocess.HTTPResponse](ctx, c.conn, subprocess.MethodHTTPHandle, p)
}

// Migrate sends plugin/migrate.
func (c *Client) Migrate(ctx context.Context, p subprocess.MigrateParams) (subprocess.MigrateResult, error) {
	return Call[subprocess.MigrateResult](ctx, c.conn, subprocess.MethodMigrate, p)
}

// CRUD sends one of the crud/* methods (method is a subprocess.MethodCRUD*
// constant) and returns the raw result, whose shape depends on the method.
func (c *Client) CRUD(ctx context.Context, method string, p subprocess.CRUDParams) (json.RawMessage, error) {
	return c.conn.Call(ctx, method, p)
}
