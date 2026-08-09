//go:build linux

package pi

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sharedSupervisorIsolation() *ProcessIsolation {
	return &ProcessIsolation{UID: 1000, GID: 1000, BaseEnvironment: map[string]string{}}
}

// TestSupervisorIdentityRuleAcceptsTheIdentityItAlreadyRuns proves the identity
// rule only stops demanding the trusted root when the native identity is the
// one the supervisor already holds. Every other shape keeps its refusal, and a
// non-root supervisor asked for a different identity is still refused word for
// word.
func TestSupervisorIdentityRuleAcceptsTheIdentityItAlreadyRuns(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)

	processIsolationGeteuid = func() int { return 1000 }
	turnSupervisorEffectiveUID = func() int { return 1000 }

	if err := validateTurnSupervisorIdentity(sharedSupervisorIsolation()); err != nil {
		t.Fatalf("shared identity = %v", err)
	}

	differentGroup := sharedSupervisorIsolation()
	differentGroup.GID = 1001

	if err := validateTurnSupervisorIdentity(differentGroup); err != nil {
		t.Fatalf("shared identity in another group = %v", err)
	}

	err := validateTurnSupervisorIdentity(&ProcessIsolation{UID: 64251, GID: 64252})
	if err == nil || err.Error() != "trusted root identity is required, effective uid is 1000" {
		t.Fatalf("distinct identity under a non-root supervisor = %v", err)
	}

	processIsolationGeteuid = func() int { return 0 }
	turnSupervisorEffectiveUID = func() int { return 0 }

	err = validateTurnSupervisorIdentity(&ProcessIsolation{})
	if err == nil || err.Error() != "native target identity must differ from the trusted supervisor" {
		t.Fatalf("root identity under a root supervisor = %v", err)
	}

	if err = validateTurnSupervisorIdentity(&ProcessIsolation{UID: 64251, GID: 64252}); err != nil {
		t.Fatalf("distinct identity under a root supervisor = %v", err)
	}

	if err = validateTurnSupervisorIdentity(nil); err == nil {
		t.Fatal("missing isolation was accepted")
	}
}

// TestPrepareTurnSupervisorStampsTheAuthorityItsIdentityAllows proves the
// parent records the one decision it made in the sealed config, so the guardian
// and the liveness child inherit it instead of each inventing their own.
func TestPrepareTurnSupervisorStampsTheAuthorityItsIdentityAllows(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)

	var stamped turnSupervisorConfig

	turnSupervisorWriteConfig = func(file io.WriteSeeker, config turnSupervisorConfig) error {
		stamped = config

		return writeTurnSupervisorConfig(file, config)
	}

	processIsolationGeteuid = func() int { return 1000 }
	turnSupervisorEffectiveUID = func() int { return 1000 }

	launch, err := prepareProcessTreeCommand(
		exec.Command("/bin/true"), ContainmentSpec{Isolation: sharedSupervisorIsolation()},
	)
	if err != nil {
		t.Fatalf("shared preparation = %v", err)
	}

	launch.close()

	if stamped.AuthorityOrigin != turnSupervisorOriginShared || stamped.IdentityLock || stamped.AuthorityDomain {
		t.Fatalf(
			"shared stamp = %q, identity lock %t, authority domain %t",
			stamped.AuthorityOrigin, stamped.IdentityLock, stamped.AuthorityDomain,
		)
	}

	processIsolationGeteuid = func() int { return 0 }
	turnSupervisorEffectiveUID = func() int { return 0 }
	stamped = turnSupervisorConfig{}

	launch, err = prepareProcessTreeCommand(
		exec.Command("/bin/true"), ContainmentSpec{Isolation: supervisorTestIsolation()},
	)
	if err != nil {
		t.Fatalf("isolated preparation = %v", err)
	}

	launch.close()

	if stamped.AuthorityOrigin != "" {
		t.Fatalf("isolated stamp = %q", stamped.AuthorityOrigin)
	}
}

// TestSupervisorConfigRefusesAnAuthorityOriginItsIdentityContradicts proves each
// child re-derives the decision from its own identity and refuses a sealed
// config that disagrees, in either direction, rather than following a stamp
// that cannot describe the process running it.
func TestSupervisorConfigRefusesAnAuthorityOriginItsIdentityContradicts(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)

	processIsolationGeteuid = func() int { return 1000 }
	turnSupervisorEffectiveUID = func() int { return 1000 }

	base := turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"true"},
		AuthorityOrigin: turnSupervisorOriginShared,
		Isolation:       *sharedSupervisorIsolation(),
	}
	if err := validateTurnSupervisorConfig(base); err != nil {
		t.Fatalf("shared config = %v", err)
	}

	mismatch := base
	mismatch.AuthorityOrigin = ""

	err := validateTurnSupervisorConfig(mismatch)
	if err == nil || err.Error() != "pi native supervisor authority origin does not match the identity it runs as" {
		t.Fatalf("unstamped shared identity = %v", err)
	}

	processIsolationGeteuid = func() int { return 0 }
	turnSupervisorEffectiveUID = func() int { return 0 }

	err = validateTurnSupervisorConfig(base)
	if err == nil || err.Error() != "pi native supervisor authority origin does not match the identity it runs as" {
		t.Fatalf("shared stamp in a trusted supervisor = %v", err)
	}

	processIsolationGeteuid = func() int { return 1000 }
	turnSupervisorEffectiveUID = func() int { return 1000 }

	for name, mutate := range map[string]func(*turnSupervisorConfig){
		"identity_lock": func(config *turnSupervisorConfig) {
			config.IdentityLock, config.AuthorityDomain = true, true
		},
		"standalone_owner": func(config *turnSupervisorConfig) {
			config.StandaloneOwner = &agentStandaloneOwner{Version: 1}
		},
		"owner_id": func(config *turnSupervisorConfig) {
			config.Isolation.StandaloneOwnerID = "shared-config-test"
		},
		"state_root": func(config *turnSupervisorConfig) {
			config.Isolation.StandaloneStateRoot = "/var/tmp/acp-go-pi-shared"
		},
	} {
		t.Run(name, func(t *testing.T) {
			config := base
			mutate(&config)

			configErr := validateTurnSupervisorConfig(config)
			if configErr == nil ||
				configErr.Error() != "pi native supervisor shared authority origin is inconsistent" {
				t.Fatalf("shared config carrying %s = %v", name, configErr)
			}
		})
	}
}

// TestSharedIdentityGuardianTakesNoAgentAuthority proves the guardian claims
// nothing durable for an identity it already occupies: no standalone claim is
// attempted, the authority it carries is empty, and both the disposition check
// and the release accept it.
func TestSharedIdentityGuardianTakesNoAgentAuthority(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)

	processIsolationGeteuid = func() int { return 1000 }
	turnSupervisorEffectiveUID = func() int { return 1000 }
	turnSupervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		t.Error("a shared identity claimed a standalone agent authority")

		return nil, errors.New("unreachable")
	}

	config := turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"true"},
		AuthorityOrigin: turnSupervisorOriginShared,
		Isolation:       *sharedSupervisorIsolation(),
	}

	authority, err := acquireTurnSupervisorAuthority(config, 7, 8, nil, nil)
	if err != nil {
		t.Fatalf("shared authority = %v", err)
	}

	if authority.identity != nil || authority.domain != nil || authority.standalone != nil {
		t.Fatalf("shared guardian took an authority: %#v", authority)
	}

	if err = validateTurnSupervisorAuthorityDisposition(config, authority); err != nil {
		t.Fatalf("shared disposition = %v", err)
	}

	if err = authority.Close(); err != nil {
		t.Fatalf("shared authority release = %v", err)
	}
}

// TestSharedIdentityLivenessConfigKeepsItsOrigin proves the guardian hands the
// liveness child the same decision it runs under, so the child's own derivation
// agrees with the stamp it decodes.
func TestSharedIdentityLivenessConfigKeepsItsOrigin(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)

	processIsolationGeteuid = func() int { return 1000 }
	turnSupervisorEffectiveUID = func() int { return 1000 }

	var stamped turnSupervisorConfig

	turnSupervisorWriteConfig = func(file io.WriteSeeker, config turnSupervisorConfig) error {
		stamped = config

		return writeTurnSupervisorConfig(file, config)
	}
	turnSupervisorCommand = func(string, ...string) *exec.Cmd { return exec.Command("/bin/true") }

	control, controlWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	defer controlWrite.Close()

	completion, completionWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer completion.Close()
	defer completionWrite.Close()

	config := turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"true"},
		AuthorityOrigin: turnSupervisorOriginShared,
		Isolation:       *sharedSupervisorIsolation(),
	}

	liveness, data, peer, err := startTurnSupervisorLiveness(config, control, completionWrite, &turnSupervisorAuthority{})
	if err != nil {
		t.Fatalf("shared liveness start = %v", err)
	}

	_ = liveness.Wait()
	_ = data.Close()
	_ = peer.Close()

	if stamped.AuthorityOrigin != turnSupervisorOriginShared || stamped.IdentityLock || stamped.StandaloneOwner != nil {
		t.Fatalf("shared liveness stamp = %+v", stamped)
	}

	if err = validateTurnSupervisorConfig(stamped); err != nil {
		t.Fatalf("shared liveness config = %v", err)
	}
}

// TestRunTurnSupervisorNativeLaunchesUnderASharedIdentity drives the liveness
// core over a config that names the identity it already runs as. No credential
// change is requested, no agent authority is claimed, and the readiness and
// completion frames are published exactly as the isolated arm publishes them.
func TestRunTurnSupervisorNativeLaunchesUnderASharedIdentity(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)

	processIsolationGeteuid = func() int { return 1000 }
	processIsolationGetegid = func() int { return 1000 }
	turnSupervisorEffectiveUID = func() int { return 1000 }
	turnSupervisorEnable = func() error { return nil }
	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}
	turnSupervisorAcquireStandalone = func(
		uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal,
	) (*agentStandaloneIdentity, error) {
		t.Error("a shared identity claimed a standalone agent authority")

		return nil, errors.New("unreachable")
	}

	var native *exec.Cmd

	turnSupervisorCommand = func(name string, args ...string) *exec.Cmd {
		native = exec.Command(name, args...)

		return native
	}

	marker := filepath.Join(t.TempDir(), "native-launched")
	config := encodeSupervisorConfig(t, turnSupervisorConfig{
		Path: "/bin/sh", Args: []string{"sh", "-c", `touch "$1"`, "probe", marker},
		Env:             []string{"PATH=/usr/bin:/bin"},
		AuthorityOrigin: turnSupervisorOriginShared,
		Isolation:       *sharedSupervisorIsolation(),
	})

	control, controlWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = controlWrite.Close(); _ = control.Close() })

	var ready, completion bytes.Buffer

	err = runTurnSupervisorNative(
		config, []io.Reader{control}, nil, &ready, &completion, 6, 7, true, true,
	)
	if err != nil {
		t.Fatalf("shared native launch = %v", err)
	}

	if native == nil || native.SysProcAttr == nil || native.SysProcAttr.Credential != nil {
		t.Fatalf("shared launch requested a credential change: %#v", native)
	}

	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("shared native command did not run: %v", statErr)
	}

	if !strings.HasPrefix(ready.String(), "ready:") || !strings.HasSuffix(ready.String(), turnSupervisorDoneLine) {
		t.Fatalf("shared readiness = %q", ready.String())
	}

	if completion.String() != turnSupervisorCompleteLine {
		t.Fatalf("shared completion = %q", completion.String())
	}
}

// TestSharedIdentityGroupRefusalNamesItselfBeforeReadiness proves the one
// refusal the shared arm can still raise inside the liveness core reaches the
// caller: it happens before anything is contained, so the completion frame and
// the terminal done frame are both withheld and the reason travels to the
// bootstrap's readiness frame instead of being overwritten by it.
func TestSharedIdentityGroupRefusalNamesItselfBeforeReadiness(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	restoreSharedIdentitySeams(t)

	processIsolationGeteuid = func() int { return 1000 }
	processIsolationGetegid = func() int { return 1000 }
	turnSupervisorEffectiveUID = func() int { return 1000 }
	turnSupervisorEnable = func() error { return nil }
	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}

	marker := filepath.Join(t.TempDir(), "native-launched")
	isolation := sharedSupervisorIsolation()
	isolation.GID = 1001
	config := encodeSupervisorConfig(t, turnSupervisorConfig{
		Path: "/bin/sh", Args: []string{"sh", "-c", `touch "$1"`, "probe", marker},
		Env:             []string{"PATH=/usr/bin:/bin"},
		AuthorityOrigin: turnSupervisorOriginShared,
		Isolation:       *isolation,
	})

	var ready, completion bytes.Buffer

	err := runTurnSupervisorNative(
		config, []io.Reader{strings.NewReader("")}, nil, &ready, &completion, 6, 7, true, true,
	)
	if err == nil || !strings.Contains(err.Error(), "native group 1001 cannot be entered from group 1000") {
		t.Fatalf("shared group refusal = %v", err)
	}

	if !strings.Contains(err.Error(), sharedIdentitySupervisorRemedy) {
		t.Fatalf("shared group refusal dropped its remedy: %v", err)
	}

	if ready.Len() != 0 || completion.Len() != 0 {
		t.Fatalf("refused launch published ready %q and completion %q", ready.String(), completion.String())
	}

	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("native command ran in a group the supervisor cannot enter: %v", statErr)
	}
}

// TestSharedIdentitySupervisorContainsARealNativeLaunch runs the whole tree with
// no seams at all under the process's own identity: the parent stamps, the
// guardian and the liveness child self-exec, the native command runs without a
// credential change and the tree is contained. It is the proof that an
// unprivileged deployment can perform the only launch available to it.
func TestSharedIdentitySupervisorContainsARealNativeLaunch(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires a supervisor whose own identity is the native identity")
	}

	searchPath := os.Getenv("PATH")
	if searchPath == "" {
		searchPath = "/usr/bin:/bin"
	}

	parent := t.TempDir()

	root, err := os.MkdirTemp(parent, "acp-go-pi-shared-*")
	if err != nil {
		t.Fatal(err)
	}

	marker := filepath.Join(t.TempDir(), "native-launched")
	script := writeScript(t, `touch "$MARKER"; cat >/dev/null`)
	process, err := StartProcess(t.Context(), LaunchSpec{
		ExecutablePath: script,
		AgentDir:       t.TempDir(),
		Env:            map[string]string{"MARKER": marker},
		Containment: ContainmentSpec{
			ScratchParent:  parent,
			GenerationRoot: filepath.Clean(root),
			RuntimeID:      "shared-identity-launch",
			LifecycleKind:  "discovery",
			Isolation: &ProcessIsolation{
				UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()),
				BaseEnvironment: map[string]string{"PATH": searchPath, "HOME": os.Getenv("HOME")},
			},
		},
	})
	if err != nil {
		t.Fatalf("shared identity launch = %v", err)
	}

	t.Cleanup(func() { _ = process.Close() })

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, statErr := os.Stat(marker); statErr == nil {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("shared identity native command never ran")
		}

		time.Sleep(20 * time.Millisecond)
	}

	if err = process.Close(); err != nil {
		t.Fatalf("shared identity containment = %v", err)
	}

	select {
	case <-process.Exited():
	case <-time.After(10 * time.Second):
		t.Fatal("shared identity tree did not exit")
	}
}
