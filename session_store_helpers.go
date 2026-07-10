package piacp

func (a *Agent) sessionStore() SessionStore {
	if a.options.SessionStore != nil {
		return a.options.SessionStore
	}

	return a.store
}

func validUUIDShape(value string) bool {
	if len(value) != 36 {
		return false
	}

	for i, char := range value {
		switch i {
		case 8, 13, 18, 23:
			if char != '-' {
				return false
			}
		default:
			if !isUUIDHex(char) {
				return false
			}
		}
	}

	return true
}

func isUUIDHex(char rune) bool {
	return (char >= '0' && char <= '9') ||
		(char >= 'a' && char <= 'f') ||
		(char >= 'A' && char <= 'F')
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}
