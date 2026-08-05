package piacp

func handoffGeneratedNativeTree(root string, isolation *ProcessIsolation) error {
	if isolation == nil {
		return nil
	}

	return handoffGeneratedNativeTreePlatform(root, isolation.UID, isolation.GID)
}
