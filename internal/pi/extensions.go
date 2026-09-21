package pi

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

//go:embed ext/acp-bridge.ts
var bridgeExtensionSource []byte

//go:embed ext/acp-path.ts
var pathExtensionSource []byte

//go:embed ext/acp-usage.ts
var usageExtensionSource []byte

const (
	// BridgeExtensionFileName is the wrapper-owned permission bridge extension.
	BridgeExtensionFileName = "acp-bridge.ts"
	// PathExtensionFileName is the wrapper-owned native-shell PATH extension.
	PathExtensionFileName  = "acp-path.ts"
	UsageExtensionFileName = "acp-usage.ts"

	// InternalEnvPrefix names the process markers the adapter sets for its own
	// extensions. They are dropped from every inherited layer and set only for
	// the child that needs them.
	InternalEnvPrefix = "ACP_GO_PI_INTERNAL_"
	// EnvPermissionMode selects the bridge permission mode for one pi child.
	EnvPermissionMode = InternalEnvPrefix + "PERMISSION"
	// EnvExtraPathDirs carries the ordered native-shell PATH prefixes for one
	// pi child, encoded with the platform path-list separator.
	EnvExtraPathDirs = InternalEnvPrefix + "EXTRA_PATH_DIRS"

	// PermissionModeAsk raises a permission dialog for every tool call.
	PermissionModeAsk = "ask"
	// PermissionModeAllow auto-allows every tool call without a dialog.
	PermissionModeAllow = "allow"

	// PermissionTitleMarker prefixes the select-dialog title the bridge
	// extension raises for a tool-call permission request. The remainder of
	// the title is the JSON-encoded PermissionPrompt.
	PermissionTitleMarker = "acp-go-pi:permission:"

	// PermissionOptionAllow is the select option that allows the tool call.
	PermissionOptionAllow = "allow"
	// PermissionOptionDeny is the select option that blocks the tool call.
	PermissionOptionDeny = "deny"
)

// PermissionPrompt is the payload the bridge extension encodes into a
// permission select-dialog title.
type PermissionPrompt struct {
	ToolCallID string          `json:"toolCallId"`
	ToolName   string          `json:"toolName"`
	Input      json.RawMessage `json:"input,omitempty"`
}

// ParsePermissionTitle recognizes a bridge permission dialog title and
// decodes its payload. It reports false for ordinary extension dialogs.
func ParsePermissionTitle(title string) (PermissionPrompt, bool) {
	payload, found := strings.CutPrefix(title, PermissionTitleMarker)
	if !found {
		return PermissionPrompt{}, false
	}

	var prompt PermissionPrompt
	if err := json.Unmarshal([]byte(payload), &prompt); err != nil {
		return PermissionPrompt{}, false
	}

	if strings.TrimSpace(prompt.ToolCallID) == "" {
		return PermissionPrompt{}, false
	}

	return prompt, true
}

// ExtensionPaths locates the published wrapper extensions.
type ExtensionPaths struct {
	Bridge string
	Path   string
	Usage  string
}

// Paths lists the extensions in pi load order.
func (p ExtensionPaths) Paths() []string {
	return []string{p.Bridge, p.Path, p.Usage}
}

// IsWrapperExtension reports whether a native extension path names one of the
// wrapper's own extensions.
func (p ExtensionPaths) IsWrapperExtension(path string) bool {
	return path != "" && (path == p.Bridge || path == p.Path || path == p.Usage)
}

// ExtensionDigest names the directory holding this build's extension sources.
// pi caches compiled extensions by source path, so a stable content-addressed
// path compiles them once per adapter build.
func ExtensionDigest() string {
	digest := sha256.New()

	for _, source := range [][]byte{bridgeExtensionSource, pathExtensionSource, usageExtensionSource} {
		digest.Write(source)
		digest.Write([]byte{0})
	}

	return hex.EncodeToString(digest.Sum(nil))[:16]
}

// PublishExtensions writes the wrapper extension sources into dir, which the
// caller derives from ExtensionDigest, and reports where they landed. A file
// already holding the right bytes is left alone.
func PublishExtensions(dir string) (ExtensionPaths, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return ExtensionPaths{}, fmt.Errorf("create extension directory: %w", err)
	}

	paths := ExtensionPaths{
		Bridge: filepath.Join(dir, BridgeExtensionFileName),
		Path:   filepath.Join(dir, PathExtensionFileName),
		Usage:  filepath.Join(dir, UsageExtensionFileName),
	}

	for path, contents := range map[string][]byte{paths.Bridge: bridgeExtensionSource, paths.Path: pathExtensionSource, paths.Usage: usageExtensionSource} {
		if err := publishFile(path, contents); err != nil {
			return ExtensionPaths{}, err
		}
	}

	return paths, nil
}

func publishFile(path string, contents []byte) error {
	current, err := os.ReadFile(path)
	if err == nil && bytes.Equal(current, contents) {
		return nil
	}

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read extension %s: %w", path, err)
	}

	staging, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("stage extension %s: %w", path, err)
	}

	_, writeErr := staging.Write(contents)
	if closeErr := staging.Close(); writeErr == nil {
		writeErr = closeErr
	}

	if writeErr != nil {
		_ = os.Remove(staging.Name())

		return fmt.Errorf("write extension %s: %w", path, writeErr)
	}

	if err := os.Rename(staging.Name(), path); err != nil {
		_ = os.Remove(staging.Name())

		return fmt.Errorf("publish extension %s: %w", path, err)
	}

	return nil
}
