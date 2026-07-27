//go:build !darwin

package pi

// containmentRecord carries no state where the native boundary is
// authoritative and needs no registry entry.
type containmentRecord struct{}

func prepareContainmentRecord(ContainmentSpec) (containmentRecord, error) {
	return containmentRecord{}, nil
}

func activateContainmentRecord(containmentRecord, int, int) error { return nil }

func completeContainmentRecord(containmentRecord, string) error { return nil }
