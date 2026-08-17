//go:build !linux

package pi

func validateStandaloneIdentityDispositionPlatform(*ProcessIsolation) error { return nil }
