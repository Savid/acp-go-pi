package piacp

import (
	"context"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	jsonFieldCwd     = "cwd"
	jsonFieldError   = "error"
	jsonFieldField   = "field"
	jsonFieldIndex   = "index"
	jsonFieldMessage = "message"
	jsonFieldMethod  = "method"
	jsonFieldMode    = "mode"
	jsonFieldServer  = "server"
	jsonFieldURL     = "url"

	acpFieldConfig    = "config"
	acpFieldSessionID = "sessionId"
	acpFieldValue     = "value"

	optionFieldProviderAuthDirectHome = "providerAuthDirectHome"

	validationRequired    = "required"
	validationUnsupported = "unsupported"
	validationDuplicate   = "duplicate"

	listSessionsPageSize = 50

	// configThoughtLevel is the session config option id carrying pi's
	// reasoning-level selector.
	configThoughtLevel acp.SessionConfigId = "thought_level"

	configTypeSelect = "select"

	defaultSessionCloseTurnWait = 5 * time.Second
)

// agentSession is the internal handle for one pi RPC-mode process owned by an
// ACP session.
type agentSession struct {
	agent                 *Agent
	id                    acp.SessionId
	cwd                   string
	additionalDirectories []string
	fingerprint           string

	// launch is the spec used to start the pi process; a crashed process is
	// relaunched lazily on the next turn by re-selecting the same native
	// session file.
	launch      pi.LaunchSpec
	sessionRoot string

	// browserShim shadows the browser launchers the pi process could exec. It
	// belongs to the process, not to a runtime generation, so it survives every
	// relaunch and is deleted only when the session's scratch is. A nil shim
	// means no browser launch can be neutralised, and the provider-auth mint
	// refuses rather than letting pi open the operator's desktop.
	browserShim *pi.BrowserShim

	// authClosed marks this session's provider-auth admission as closed. It is
	// guarded by providerAuth.mu rather than by this session's own mutex,
	// because the broker sets it in the same critical section in which it takes
	// the flows it is about to cancel. It lives on the instance and not in a
	// table keyed by session id, so a session reloaded under an id that was
	// closed before is admitted afresh.
	authClosed bool

	sessionFilePath string
	permissionMode  string

	// autoRetry is the session's native auto-retry election, re-applied on
	// every lazy relaunch so the retry posture survives process death.
	autoRetry bool

	// mcpRefreshPending forces the first user turn to rebuild pi's fixed
	// extension-tool registry while that turn's MCP authority is active.
	// Session establishment may intentionally expose only a side-effect-free
	// readiness surface; retaining that provisional snapshot would hide the
	// operation's real tools for the lifetime of the native process.
	mcpRefreshPending bool

	proc   piProcess
	client piClient

	// pumpCtx/pumpDone bound the event pump goroutine that drains the client
	// event and UI request streams for the life of the process.
	pumpCancel context.CancelFunc
	pumpDone   chan struct{}
	dialogWG   sync.WaitGroup

	turn       chan struct{}
	cancelMu   sync.Mutex
	toolMu     sync.Mutex
	rawEventMu sync.Mutex

	mu                   sync.Mutex
	title                string
	updatedAt            string
	model                string
	availableModels      []pi.Model
	thinkingLevel        string
	contextWindowSize    int64
	availableCommands    []pi.SlashCommand
	advertisedCommands   []acp.AvailableCommand
	poisonCause          string
	cancel               context.CancelFunc
	turnCancelled        bool
	turnNonce            string
	turnSink             *turnSink
	turnNativeSettled    bool
	turnFenceStarted     bool
	turnFenceDone        chan struct{}
	turnFenceErr         error
	turnSettling         bool
	turnCommitOnCancel   bool
	pendingDialogs       map[string]*dialogCancel
	turnTools            map[string]*turnToolCall
	rawMessages          rawMessageConfig
	rawEventSequence     int64
	mirroredRows         int
	turnImagesEmitted    bool
	closeTurnWait        time.Duration
	nativeRootRelease    func()
	scratchRootRelease   func()
	nativeContainmentErr error
	providerProcessRoot  *providerProcessRoot
}

// turnToolCall is the exact-ID lifecycle published for one native tool call.
// Its lock serializes ACP publication with permission admission so concurrent
// bridge-dialog and native-event delivery cannot create two starts or
// authorize an already-terminal call. agentSession.toolMu protects only the
// turnTools index, allowing unrelated native tool ids to progress independently.
type turnToolCall struct {
	mu                   sync.Mutex
	published            bool
	nativeStartPublished bool
	permissionRequested  bool
	terminalPublished    bool
	status               acp.ToolCallStatus
	// content is the last emitted complete content snapshot; each later
	// content-bearing update merges onto it so no delivered item disappears
	// under ACP's whole-array replacement.
	content []toolContentItem
}

// dialogCancel tracks one pending extension UI dialog so session/cancel and
// teardown can resolve it as cancelled.
type dialogCancel struct {
	cancel context.CancelFunc
}

// promptTurnState accumulates per-turn results while streaming events.
type promptTurnState struct {
	usage           *acp.Usage
	cost            *pi.UsageCost
	stopReason      string
	errorMessage    string
	model           string
	provider        string
	nativeMessageID string
	// agentImages de-duplicates assistant image artifacts across the turn's
	// agent chunks, keyed by native message identity plus fingerprint.
	agentImages map[string]struct{}
}
