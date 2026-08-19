package piacp

import (
	"context"
	"sync"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-pi/internal/pi"
)

const (
	jsonFieldCursor  = "cursor"
	jsonFieldCwd     = "cwd"
	jsonFieldError   = "error"
	jsonFieldField   = "field"
	jsonFieldIndex   = "index"
	jsonFieldMessage = "message"
	jsonFieldMethod  = "method"
	jsonFieldMode    = "mode"
	jsonFieldParams  = "params"
	jsonFieldServer  = "server"
	jsonFieldType    = "type"
	jsonFieldURL     = "url"

	acpFieldConfigID  = "configId"
	acpFieldSessionID = "sessionId"
	acpFieldValue     = "value"

	optionFieldConcurrencyLimits = "concurrencyLimits"
	optionFieldContainment       = "containment"
	optionFieldDefaultModel      = "defaultModel"
	optionFieldEnv               = "env"
	optionFieldHome              = "home"
	optionFieldImageLimits       = "imageLimits"
	optionFieldInputHandoffRoot  = "inputHandoffRoot"
	optionFieldProviderAuthRoot  = "providerAuthRoot"

	validationRequired    = "required"
	validationUnsupported = "unsupported"
	validationDuplicate   = "duplicate"

	listSessionsPageSize = 50

	// configThoughtLevel is the session config option id carrying pi's
	// reasoning-level selector.
	configThoughtLevel acp.SessionConfigId = "thought_level"

	configTypeSelect = "select"
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

	// pumpCancel/pumpDone bound the event pump goroutine that drains the
	// client event and UI request streams for the life of the process.
	pumpCancel context.CancelFunc
	pumpDone   chan struct{}
	dialogWG   sync.WaitGroup

	turn       chan struct{}
	cancelMu   sync.Mutex
	toolMu     sync.Mutex
	rawEventMu sync.Mutex

	// lcMu serializes the ordered lifecycle stream through delivery, so a
	// sequence claimed before a send is also delivered in that order.
	lcMu sync.Mutex
	lc   lifecycleState

	// commitMu linearizes every durable write against the fence a delete
	// installs, so no write can recreate a row the delete removed.
	commitMu      sync.Mutex
	persistFenced bool

	mu                 sync.Mutex
	title              string
	updatedAt          string
	model              string
	availableModels    []pi.Model
	thinkingLevel      string
	contextWindowSize  int64
	availableCommands  []pi.SlashCommand
	advertisedCommands []acp.AvailableCommand
	poisonCause        string
	// opened records that the session published its establishing snapshot: the
	// explicit command catalog and the opening lifecycle stream, exactly once.
	opened        bool
	cancel        context.CancelFunc
	turnCancelled bool
	turnNonce     string
	// pumpGeneration counts native process generations. One generation owns
	// one outbox, one lifecycle incarnation, and every event either produced.
	pumpGeneration       uint64
	outbox               *sessionOutbox
	turnEvents           *turnDelivery
	turnNativeSettled    bool
	settlement           *turnSettlement
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
	nativeRootRelease    func()
	scratchRootRelease   func()
	nativeContainmentErr error
	providerProcessRoot  *providerProcessRoot
	browserShim          *pi.BrowserShim
	residence            *pi.SessionResidence
	authClosed           bool

	// closing is the session's terminal state: once the first Close claims it
	// the session admits no prompt, relaunch, or MCP-tool refresh ever again,
	// and every later Close waits on closeDone and reports closeErr rather than
	// tearing the same resources down a second time.
	closing   bool
	closeDone chan struct{}
	closeErr  error
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
