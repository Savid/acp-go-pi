//go:build darwin || freebsd || openbsd

package pi

// descendantCount reports no observation. These boundaries signal a process
// group and reap the direct child; neither enumerates the membership a vacancy
// claim would need, and a partial count read as a whole one is the exact
// mistake the unavailable answer prevents.
func (*processTree) descendantCount() (int, bool) {
	return 0, false
}
