package piacp

import (
	"fmt"
	"strings"

	"github.com/savid/acp-go-pi/internal/pi"
)

// validateConfiguredModels refuses an id that could not name a pi model:
// empty, carrying surrounding space, not a "provider/id" reference, or listed
// twice.
func validateConfiguredModels(ids []string) error {
	seen := make(map[string]struct{}, len(ids))
	for index, id := range ids {
		if id == "" || strings.TrimSpace(id) != id {
			return fmt.Errorf("configured model %d %q is not a model id", index, id)
		}

		if _, err := pi.ParseModelRef(id); err != nil {
			return fmt.Errorf("configured model %d: %w", index, err)
		}

		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("configured model %q is listed twice", id)
		}

		seen[id] = struct{}{}
	}

	return nil
}
