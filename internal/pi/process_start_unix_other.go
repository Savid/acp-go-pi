//go:build linux || freebsd || openbsd

package pi

func handleVanishedProcessGroupLeader(*processTreeCommand, *directChildWait) (*processTree, bool, error) {
	return nil, false, nil
}
