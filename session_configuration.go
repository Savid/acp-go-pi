package piacp

import (
	"slices"

	"github.com/coder/acp-go-sdk"
)

type sessionConfigurationRecord struct {
	Env           map[string]string `json:"env"`
	ExtraPathDirs []string          `json:"extraPathDirs"`
}

func sessionConfiguration(options PiOptions) sessionConfigurationRecord {
	return sessionConfigurationRecord{
		Env:           cloneStringMap(options.Env),
		ExtraPathDirs: slices.Clone(options.ExtraPathDirs),
	}
}

func resolveSessionConfiguration(
	options PiOptions,
	presence sessionConfigurationPresence,
	stored sessionConfigurationRecord,
) (PiOptions, error) {
	if err := validateEnvironment(stored.Env, metaOptionPath(metaEnvKey), blockedSessionEnvKey); err != nil {
		return PiOptions{}, sessionResumeIncompatibleError(metaOptionPath(metaEnvKey))
	}

	if err := validateExtraPathDirs(stored.ExtraPathDirs, metaOptionPath(metaExtraPathDirsKey)); err != nil {
		return PiOptions{}, sessionResumeIncompatibleError(metaOptionPath(metaExtraPathDirsKey))
	}

	if !presence.Env {
		options.Env = cloneStringMap(stored.Env)
	}

	if !presence.ExtraPathDirs {
		options.ExtraPathDirs = slices.Clone(stored.ExtraPathDirs)
	}

	return options, nil
}

func sessionResumeIncompatibleError(field string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: "session resume incompatible",
		jsonFieldField: field,
	})
}
