//go:build linux || freebsd || openbsd

package pi

func handleVanishedProcessGroupLeader(launch *processTreeCommand, direct *directChildWait) (*processTree, bool, error) {
	if launch != nil && launch.ordinary && launch.cmd != nil && launch.cmd.Process != nil {
		tree := &processTree{
			containment: launch.containment, pgid: launch.cmd.Process.Pid,
			process: launch.cmd.Process, direct: direct, ordinary: true,
		}
		direct.begin()
		launch.close()

		return tree, true, nil
	}

	return nil, false, nil
}
