//go:build linux

package pi

// descendantCount reports the absolute membership of the native containment
// boundary. Only the dedicated subreaper is authoritative: every descendant
// that escapes its parent reparents onto the supervisor rather than onto init,
// so a walk rooted at the supervisor is the whole tree and not a floor. Every
// other boundary reports no observation at all, because a count that could miss
// an escaped descendant would be read as vacancy it never proved.
func (t *processTree) descendantCount() (int, bool) {
	if t == nil || !t.supervised || t.process == nil {
		return 0, false
	}

	descendants, err := turnSupervisorDescendants(t.process.Pid)
	if err != nil {
		return 0, false
	}

	return len(descendants), true
}
