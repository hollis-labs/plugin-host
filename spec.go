package pluginhost

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

const (
	defaultHandshakeTimeout = 10 * time.Second
	defaultUnloadTimeout    = 2 * time.Second
	defaultReapTimeout      = 2 * time.Second
	defaultStderrBytes      = 4096
)

// Spec is what the host needs to spawn one plugin. The library passes Env,
// Init.Config and Init.Grants through untouched: what a plugin may see,
// hold or be granted is the host's decision, not this library's.
type Spec struct {
	// ID names the plugin in errors before the child has introduced itself.
	ID string
	// ExpectedID defaults to Init.Incarnation.OwnerID and must equal that nonblank ID.
	// It and ExpectedVersion, when set, are checked after init and before load.
	// ID itself remains the pre-handshake diagnostic label.
	ExpectedID      string
	ExpectedVersion string
	// Command is the executable; Args its arguments; Dir its working
	// directory (empty means this process's).
	Command string
	Args    []string
	Dir     string

	// BeforeSpawn runs the host's validation immediately before every spawn,
	// including supervised restarts. A refusal leaves no child. It must honor
	// ctx so cancellation and supervisor shutdown can interrupt validation.
	// The callback owns policy; the library neither interprets it nor grants
	// permissions. Errors are returned to the host with wrapping intact.
	BeforeSpawn func(context.Context) error

	// Env is the child's EXACT environment. Nil means empty: the library
	// never inherits the host's environment implicitly, because an implicit
	// inherit hands every plugin whatever secrets the host happens to hold.
	// Use [InheritEnv] to opt in. (On unix, Go adds PWD when Dir is set.)
	Env []string

	// Init is the plugin/init payload. Lifecycle overwrites Incarnation with its issued tuple.
	// Standalone callers supply Incarnation. The host owns Config,
	// Grants, HostInfo.Version and the directories. The library fills only
	// what is zero: HostInfo.Protocol = 2, CapabilityContract = 1, Config = {} (never null),
	// LogLevel = "info", PluginDir = Dir.
	Init subprocess.InitParams

	// HandshakeTimeout bounds [Process.Handshake] even when the caller's
	// context has no deadline (default 10s). UnloadTimeout is the graceful
	// budget of [Process.Stop]: the plugin/unload call and the wait for exit
	// after stdin closes share it (default 2s). ReapTimeout bounds the wait
	// after SIGKILL, and is the exec.Cmd WaitDelay (default 2s).
	HandshakeTimeout time.Duration
	UnloadTimeout    time.Duration
	ReapTimeout      time.Duration

	// StderrBytes is the size of the retained stderr tail (default 4096).
	StderrBytes int
	// Secrets are exact values scrubbed from the stderr tail; Redact, when
	// set, is then applied to [Process.Diagnostics] and to every error text
	// built from the tail.
	Secrets []string
	Redact  func(string) string

	// ConnOptions configure the [Conn].
	ConnOptions []ConnOption
}

// InheritEnv returns this process's environment followed by extra entries
// (a later entry for the same key wins). It is the explicit way to get the
// inherit-plus-extras behavior a host may want for [Spec.Env].
func InheritEnv(extra ...string) []string {
	return append(os.Environ(), extra...)
}

// normalized returns a copy of s with every default applied.
func (s Spec) normalized() Spec {
	if s.Env == nil {
		s.Env = []string{}
	}
	if s.HandshakeTimeout <= 0 {
		s.HandshakeTimeout = defaultHandshakeTimeout
	}
	if s.UnloadTimeout <= 0 {
		s.UnloadTimeout = defaultUnloadTimeout
	}
	if s.ReapTimeout <= 0 {
		s.ReapTimeout = defaultReapTimeout
	}
	if s.StderrBytes <= 0 {
		s.StderrBytes = defaultStderrBytes
	}
	if s.Init.HostInfo.Protocol == 0 {
		s.Init.HostInfo.Protocol = subprocess.ProtocolVersion
	}
	if s.Init.CapabilityContract == 0 {
		s.Init.CapabilityContract = capability.ContractVersion
	}
	if s.Init.Config == nil {
		s.Init.Config = map[string]string{}
	}
	if s.Init.LogLevel == "" {
		s.Init.LogLevel = "info"
	}
	if s.Init.PluginDir == "" {
		s.Init.PluginDir = s.Dir
	}
	if s.ExpectedID == "" {
		s.ExpectedID = s.Init.Incarnation.OwnerID
	}
	return s
}

func (s Spec) label() string {
	if strings.TrimSpace(s.ID) != "" {
		return s.ID
	}
	return s.Command
}
