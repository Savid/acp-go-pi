package pi

import (
	"fmt"
	"strings"
)

// ModelRef addresses one pi model as "provider/id". Model ids may themselves
// contain slashes; the provider is everything before the first slash.
type ModelRef struct {
	Provider string
	ID       string
}

// String returns the "provider/id" form.
func (r ModelRef) String() string {
	return r.Provider + "/" + r.ID
}

// ParseModelRef parses "provider/id"; both parts are required.
func ParseModelRef(model string) (ModelRef, error) {
	provider, id, found := strings.Cut(model, "/")
	if !found || provider == "" || id == "" {
		return ModelRef{}, fmt.Errorf("model %q must be \"provider/id\"", model)
	}

	return ModelRef{Provider: provider, ID: id}, nil
}

// pi's reasoning levels in ascending order.
const (
	ThinkingLevelOff     = "off"
	ThinkingLevelMinimal = "minimal"
	ThinkingLevelLow     = "low"
	ThinkingLevelMedium  = "medium"
	ThinkingLevelHigh    = "high"
	ThinkingLevelXHigh   = "xhigh"
	ThinkingLevelMax     = "max"
)

// thinkingLevels are pi's reasoning levels in ascending order.
var thinkingLevels = []string{
	ThinkingLevelOff,
	ThinkingLevelMinimal,
	ThinkingLevelLow,
	ThinkingLevelMedium,
	ThinkingLevelHigh,
	ThinkingLevelXHigh,
	ThinkingLevelMax,
}

// ThinkingLevels returns pi's reasoning levels in ascending order.
func ThinkingLevels() []string {
	return append([]string(nil), thinkingLevels...)
}

// IsValidThinkingLevel reports whether level is one of pi's reasoning levels.
// The wrapper validates levels itself because pi accepts invalid levels with
// success and silently coerces them.
func IsValidThinkingLevel(level string) bool {
	for _, candidate := range thinkingLevels {
		if level == candidate {
			return true
		}
	}

	return false
}
