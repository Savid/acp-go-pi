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
	jsonFieldServer  = "server"

	acpFieldConfig    = "config"
	acpFieldSessionID = "sessionId"
	acpFieldValue     = "value"

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
	launch          pi.LaunchSpec
	sessionRoot     string
	sessionFilePath string
	permissionMode  string

	// autoRetry is the session's native auto-retry election, re-applied on
	// every lazy relaunch so the retry posture survives process death.
	autoRetry bool

	proc   piProcess
	client piClient

	// pumpCtx/pumpDone bound the event pump goroutine that drains the client
	// event and UI request streams for the life of the process.
	pumpCancel context.CancelFunc
	pumpDone   chan struct{}
	dialogWG   sync.WaitGroup

	turn chan struct{}

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
	cancel             context.CancelFunc
	turnCancelled      bool
	turnSink           *turnSink
	pendingDialogs     map[string]*dialogCancel
	rawMessages        rawMessageConfig
	rawEventSequence   int64
	mirroredRows       int
	closeTurnWait      time.Duration
}

// dialogCancel tracks one pending extension UI dialog so session/cancel and
// teardown can resolve it as cancelled.
type dialogCancel struct {
	cancel context.CancelFunc
}

// promptTurnState accumulates per-turn results while streaming events.
type promptTurnState struct {
	usage        *acp.Usage
	cost         *pi.UsageCost
	stopReason   string
	errorMessage string
	model        string
	provider     string
}
