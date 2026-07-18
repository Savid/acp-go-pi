//go:build !darwin

package pi

import "testing"

func testContainmentSpec(*testing.T) ContainmentSpec { return ContainmentSpec{} }
