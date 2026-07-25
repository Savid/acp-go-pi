//go:build unix

package piacp

import "syscall"

// handoffOpenFlags carries the one flag a root-relative handoff open still
// needs. Containment is the read root's and needs no help from an open flag: the
// kernel refuses a name that leaves the root as part of the open itself, so
// there is no moment at which a swapped component could be followed out.
// O_NONBLOCK means a FIFO or a device fails the descriptor's regular-file check
// instead of blocking the turn inside open(2), and it changes nothing about how
// a regular file reads.
const handoffOpenFlags = syscall.O_NONBLOCK
