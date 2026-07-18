//go:build !darwin

package pi

func prepareContainmentRecord(ContainmentSpec) (containmentRecord, error) {
	return containmentRecord{}, nil
}

func activateContainmentRecord(containmentRecord, int, int) error { return nil }

func completeContainmentRecord(containmentRecord, string) error { return nil }
