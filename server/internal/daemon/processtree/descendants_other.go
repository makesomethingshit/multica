//go:build !windows && !linux

package processtree

import "syscall"

// Procfs descendant snapshots are a Linux containment layer. Other Unix
// platforms retain the existing process-group behavior until they gain a
// native process-table implementation.
func discoverOwnedDescendants(_ int) ([]ownedDescendant, error) { return nil, nil }

func signalOwnedDescendant(_ ownedDescendant, _ syscall.Signal) error { return nil }

func ownedDescendantAlive(_ ownedDescendant) bool { return false }
