//go:build !darwin

package pi

import "errors"

type ContainmentCandidate struct{}
type ContainmentDiagnostic struct{}
type ContainmentCleanupResult struct{}

func DiagnoseContainment(string) ([]ContainmentDiagnostic, error) {
	return nil, errors.New("containment diagnostics are available only on darwin")
}

func CleanupContainment(string, string, bool) (ContainmentCleanupResult, error) {
	return ContainmentCleanupResult{}, errors.New("containment cleanup is available only on darwin")
}
