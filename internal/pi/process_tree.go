package pi

import "errors"

// ErrProcessTreeNotQuiescent means the adapter could not prove that every
// descendant in a native command's containment boundary exited. Managed-root
// callers must retain their permit while this error remains unresolved.
var ErrProcessTreeNotQuiescent = errors.New("pi process tree quiescence not proven")

// ProcessTreeQuiescent reports whether err proves that no native descendant
// remains. Native command failures do not retain a permit unless they include
// the containment sentinel.
func ProcessTreeQuiescent(err error) bool {
	return !errors.Is(err, ErrProcessTreeNotQuiescent)
}
